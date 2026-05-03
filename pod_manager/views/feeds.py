"""
RSS feed generation, episode chapter exposure, and audio playback redirector.
The feed shell + per-episode fragment cache lives here too — it's called from
Celery tasks (lazy-imported) when fragments need to be rebuilt.
"""
import asyncio
import hashlib
import hmac
import html
import logging
import re
import warnings
from datetime import timedelta
from email.utils import format_datetime

from django.core.cache import cache
from django.http import (
    HttpResponse, HttpResponseNotModified, HttpResponseRedirect,
    JsonResponse, Http404, StreamingHttpResponse,
)
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.utils import timezone

from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media, Person
from lxml import etree

from ..models import (
    PatronProfile, NetworkMembership, Podcast, Episode, NetworkMix, UserMix,
)
from ..services.access import _evaluate_access, _build_episode_description
from ..services.analytics import _record_active_user

warnings.filterwarnings("ignore", message=".*Image URL must end with.*")
warnings.filterwarnings("ignore", message=".*Size is set to 0.*")

logger = logging.getLogger(__name__)


def etag_xml_response(request, xml_bytes: bytes) -> HttpResponse:
    etag = f'"{hashlib.md5(xml_bytes).hexdigest()}"'
    if request.META.get('HTTP_IF_NONE_MATCH') == etag:
        logger.info(f"[ETag MATCH] {request.path} | Served: 0 bytes")
        resp = HttpResponseNotModified()
    else:
        resp = HttpResponse(xml_bytes, content_type='application/xml')
        resp['ETag'] = etag
        resp['Cache-Control'] = 'public, max-age=0, must-revalidate'
        size_mb = len(xml_bytes) / (1024 * 1024)
        logger.info(f"[ETag MISS] {request.path} | New Hash: {etag} | Served: {size_mb:.2f} MB")
    resp['Access-Control-Allow-Origin'] = '*'
    return resp


def pin_last_build_date(header: str, episodes) -> str:
    if not episodes:
        return header
    latest_date_str = format_datetime(episodes[0].pub_date)
    return re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)


def parse_duration(duration_str: str) -> timedelta | None:
    if not duration_str: return None
    try:
        parts = duration_str.split(':')
        sec = int(float(parts[-1]))
        if len(parts) == 3: return timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=sec)
        elif len(parts) == 2: return timedelta(minutes=int(parts[0]), seconds=sec)
        return timedelta(seconds=int(float(duration_str)))
    except ValueError:
        return None


class RSSFeedBuilder:
    def __init__(self, base_url, title, description, image_url, network, feed_type='private'):
        # Strip trailing slashes to prevent double-slashing when concatenating URLs
        self.base_url = base_url.rstrip('/')
        self.feed_type = feed_type
        self.network = network
        self.episodes_data = []

        safe_description = description or network.summary or f"{title} on {network.name}."

        self.feed = PodgenPodcast(
            name=title,
            description=safe_description,
            website=network.website_url or self.base_url,
            explicit=True,
            image=image_url or network.default_image_url or "https://example.com/logo.png",
            authors=[Person(name=network.name, email=network.contact_email or "hosts@example.com")],
            owner=Person(name=network.name, email=network.contact_email or "hosts@example.com"),
            withhold_from_itunes=True,
        )

    def add_episode(self, episode, has_access, display_title=None):
        desc = _build_episode_description(episode, has_access)
        self.episodes_data.append(episode)

        # Build the URL internally with the universal placeholder
        target_audio_url = f"{self.base_url}{reverse('play_episode', args=[episode.id])}?auth=__VECTO_AUTH_TOKEN__"

        self.feed.episodes.append(PodgenEpisode(
            id=episode.guid_public or episode.guid_private or str(episode.id),
            title=display_title or episode.title,
            summary=desc,
            publication_date=episode.pub_date,
            media=Media(
                url=target_audio_url, size=0, type="audio/mpeg",
                duration=parse_duration(episode.duration)
            )
        ))

    def render(self, access_map=None):
        raw_xml = self.feed.rss_str()

        if 'xmlns:podcast=' not in raw_xml:
            raw_xml = raw_xml.replace('<rss ', '<rss xmlns:podcast="https://podcastindex.org/namespace/1.0" ', 1)

        tag_map = {str(ep.guid_public or ep.guid_private or ep.id): ep for ep in self.episodes_data}
        if not tag_map: return raw_xml

        root = etree.fromstring(raw_xml.encode('utf-8'))
        podcast_ns = "https://podcastindex.org/namespace/1.0"
        etree.register_namespace('podcast', podcast_ns)

        for item in root.findall('.//item'):
            guid_elem = item.find('guid')
            if guid_elem is not None and guid_elem.text in tag_map:
                ep = tag_map[guid_elem.text]

                for tag in ep.tags:
                    cat_elem = etree.SubElement(item, 'category')
                    cat_elem.text = etree.CDATA(str(tag))

                ep_access = access_map.get(ep.podcast_id, False) if access_map else (self.feed_type == 'private')
                ftype = 'private' if ep_access else 'public'

                if ep.chapters_private or ep.chapters_public:
                    # Swap request object for base_url
                    chapter_url = f"{self.base_url}{reverse('episode_chapters', args=[ep.id, ftype])}"
                    chap_elem = etree.SubElement(item, f'{{{podcast_ns}}}chapters')
                    chap_elem.set('url', chapter_url)

                    # Updated to the official Podcasting 2.0 MIME type requirement
                    chap_elem.set('type', 'application/json+chapters')

        # 1. Convert the lxml tree back into a raw string
        final_xml = etree.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')

        # 2. Strip the inline namespace lxml tried to force onto the child tags
        final_xml = final_xml.replace(f' xmlns:podcast="{podcast_ns}"', '')

        # 3. Force it into the root <rss> tag exactly where PocketCasts expects it
        if 'xmlns:podcast' not in final_xml:
            final_xml = final_xml.replace('<rss ', f'<rss xmlns:podcast="{podcast_ns}" ', 1)

        return final_xml


def get_or_build_feed_shell(podcast, base_url, has_access):
    """Caches the top-level RSS metadata (Header and Footer) without any episodes."""
    feed_type = 'private' if has_access else 'public'
    cache_key = f"feed_shell_{feed_type}_{podcast.id}"
    shell = cache.get(cache_key)
    if shell: return shell

    title = f"{podcast.title} (Private)" if has_access else podcast.title
    builder = RSSFeedBuilder(base_url, title, podcast.description or "", podcast.image_url, podcast.network, feed_type)
    raw_xml = builder.render()

    # Split the XML to grab everything before the closing </channel> tag
    header = raw_xml.split('</channel>')[0]
    footer = "</channel></rss>"

    shell = (header, footer)
    cache.set(cache_key, shell, timeout=604800) # 7 Days
    return shell


def get_or_build_episode_fragment(episode, base_url, has_access):
    """Caches a single <item>...</item> block."""
    feed_type = 'private' if has_access else 'public'
    cache_key = f"ep_frag_{feed_type}_{episode.id}"
    fragment = cache.get(cache_key)

    # Check for None explicitly so we don't infinitely rebuild empty strings
    if fragment is not None: return fragment

    # Build a temporary shell to render just this one episode
    builder = RSSFeedBuilder(base_url, "Temp", "Temp", "", episode.podcast.network, feed_type)
    builder.add_episode(episode, has_access)
    raw_xml = builder.render(access_map={episode.podcast_id: has_access})

    # Use robust regex to extract the item block, ignoring whitespace/attributes
    match = re.search(r'(<item.*?>.*?</item>)', raw_xml, re.DOTALL | re.IGNORECASE)
    fragment = match.group(1) if match else ""

    cache.set(cache_key, fragment, timeout=604800) # 7 Days
    return fragment


def episode_chapters(request, episode_id, feed_type):
    ep = get_object_or_404(Episode, id=episode_id)
    data = ep.chapters_public or ep.chapters_private if feed_type == 'public' else ep.chapters_private or ep.chapters_public
    if not data: raise Http404("Chapters not found.")

    # Check if the DB holds a raw legacy list or the new dict format
    if isinstance(data, list):
        payload = {
            "version": "1.2.0",
            "chapters": data
        }
    elif isinstance(data, dict):
        payload = data
        # Inject the mandatory version string if the database object lacks it
        if "version" not in payload:
            payload["version"] = "1.2.0"
        if "chapters" not in payload:
            payload["chapters"] = []
    else:
        raise Http404("Invalid chapter format in database.")

    response = JsonResponse(payload, safe=False)
    response["Access-Control-Allow-Origin"] = "*"
    return response


def generate_custom_feed(request):
    feed_token = request.GET.get('auth')
    podcast = get_object_or_404(Podcast, slug=request.GET.get('show'), network=request.network)
    profile = get_object_or_404(PatronProfile, feed_token=feed_token)
    has_access, _ = _evaluate_access(profile.user, podcast, podcast.network)

    base_url = request.build_absolute_uri('/')[:-1]
    header, footer = get_or_build_feed_shell(podcast, base_url, has_access)

    # Get valid episodes
    episodes = [ep for ep in podcast.episodes.all().order_by('-pub_date')[:1000] if ep.has_public_audio or ep.is_premium]
    if not has_access: episodes = [ep for ep in episodes if ep.has_public_audio]

    header = pin_last_build_date(header, episodes)

    feed_type = 'private' if has_access else 'public'
    cache_keys = [f"ep_frag_{feed_type}_{ep.id}" for ep in episodes]

    # Bulk fetch fragments from Redis
    fragments_dict = cache.get_many(cache_keys)

    # Assemble missing fragments inline if cache missed
    items_xml = ""
    for i, ep in enumerate(episodes):
        frag = fragments_dict.get(cache_keys[i])
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, has_access)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(profile.feed_token))

    _record_active_user({podcast.network_id}, profile.user_id)
    return etag_xml_response(request, final_xml.encode('utf-8'))


def generate_public_feed(request, podcast_slug):
    podcast = get_object_or_404(Podcast, slug=podcast_slug, network=request.network)
    base_url = request.build_absolute_uri('/')[:-1]

    header, footer = get_or_build_feed_shell(podcast, base_url, False)
    episodes = [ep for ep in podcast.episodes.all().order_by('-pub_date')[:500] if ep.has_public_audio]

    header = pin_last_build_date(header, episodes)

    cache_keys = [f"ep_frag_public_{ep.id}" for ep in episodes]
    fragments_dict = cache.get_many(cache_keys)

    items_xml = ""
    for i, ep in enumerate(episodes):
        frag = fragments_dict.get(cache_keys[i])
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, False)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('?auth=__VECTO_AUTH_TOKEN__', '')
    return etag_xml_response(request, final_xml.encode('utf-8'))


def generate_mix_feed(request, unique_id):
    user_mix = get_object_or_404(UserMix.objects.select_related('user__patron_profile'), unique_id=unique_id, is_active=True)

    # Require the mix owner's feed_token. Without this check, anyone holding
    # the (UUID) mix URL gets the resulting RSS — and that RSS embeds the
    # user's primary feed_token in every audio URL, which can then be replayed
    # against other private feeds. With the check, the URL is no more
    # privileged than the feed_token itself.
    feed_token = request.GET.get('auth')
    owner_token = getattr(getattr(user_mix.user, 'patron_profile', None), 'feed_token', None)
    if not feed_token or not owner_token or not hmac.compare_digest(str(feed_token), str(owner_token)):
        raise Http404("Mix feed not found.")

    base_url = request.build_absolute_uri('/')[:-1]
    cache_key = f"shell_user_mix_{user_mix.id}"
    shell = cache.get(cache_key)

    if not shell:
        # Generate mix shell on the fly (lightweight)
        builder = RSSFeedBuilder(base_url, user_mix.name, f"Custom blended feed for {user_mix.user.first_name}.", user_mix.display_image or user_mix.network.default_image_url, user_mix.network)
        raw_xml = builder.render()
        shell = (raw_xml.split('</channel>')[0], "</channel></rss>")
        cache.set(cache_key, shell, timeout=None)

    header, footer = shell

    episodes = Episode.objects.filter(podcast__in=user_mix.selected_podcasts.all()).select_related('podcast', 'podcast__network').order_by('-pub_date')[:500]
    if episodes:
        latest_date_str = format_datetime(episodes[0].pub_date)
        header = re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)

    keys_and_eps = []
    for ep in episodes:
        if not ep.has_public_audio and not ep.is_premium: continue
        ep_has_access, _ = _evaluate_access(user_mix.user, ep.podcast, ep.podcast.network)
        feed_type = 'private' if ep_has_access else 'public'
        keys_and_eps.append((f"ep_frag_{feed_type}_{ep.id}", ep, ep_has_access))

    fragments_dict = cache.get_many([k[0] for k in keys_and_eps])

    items_xml = ""
    for key, ep, ep_has_access in keys_and_eps:
        frag = fragments_dict.get(key)
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, ep_has_access)

        # Inject Podcast Title into episode title for mix context
        safe_title = html.escape(ep.podcast.title)
        frag = frag.replace('<title>', f'<title>[{safe_title}] ', 1)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(user_mix.user.patron_profile.feed_token))

    network_ids = set(user_mix.selected_podcasts.values_list('network_id', flat=True))
    _record_active_user(network_ids, user_mix.user_id)
    return etag_xml_response(request, final_xml.encode('utf-8'))


def generate_network_mix_feed(request, network_slug, mix_slug):
    network_mix = get_object_or_404(NetworkMix, slug=mix_slug, network__slug=network_slug)
    feed_token = request.GET.get('auth')
    profile = PatronProfile.objects.filter(feed_token=feed_token).first() if feed_token else None
    user = profile.user if profile else request.user

    mix_req_cents = network_mix.required_tier.minimum_cents if network_mix.required_tier else 0
    mix_membership = NetworkMembership.objects.filter(user=user, network=network_mix.network).first() if user.is_authenticated else None
    user_cents = mix_membership.patreon_pledge_cents if mix_membership else 0
    is_owner = network_mix.network.owners.filter(id=user.id).exists() if user.is_authenticated else False
    user_meets_mix_tier = is_owner or (mix_req_cents == 0) or (user_cents >= mix_req_cents)

    base_url = request.build_absolute_uri('/')[:-1]
    cache_key = f"shell_net_mix_{network_mix.id}"
    shell = cache.get(cache_key)

    if not shell:
        builder = RSSFeedBuilder(base_url, network_mix.name, f"A curated network mix by {network_mix.network.name}.", network_mix.display_image or network_mix.network.default_image_url, network_mix.network)
        raw_xml = builder.render()
        shell = (raw_xml.split('</channel>')[0], "</channel></rss>")
        cache.set(cache_key, shell, timeout=None)

    header, footer = shell

    episodes = Episode.objects.filter(podcast__in=network_mix.selected_podcasts.all()).select_related('podcast', 'podcast__network').order_by('-pub_date')[:5000]
    if episodes:
        latest_date_str = format_datetime(episodes[0].pub_date)
        header = re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)

    keys_and_eps = []
    for ep in episodes:
        ep_has_access, _ = _evaluate_access(user, ep.podcast, ep.podcast.network)
        total_access = user_meets_mix_tier and ep_has_access
        if not total_access and not ep.audio_url_public: continue

        feed_type = 'private' if total_access else 'public'
        keys_and_eps.append((f"ep_frag_{feed_type}_{ep.id}", ep, total_access))

    fragments_dict = cache.get_many([k[0] for k in keys_and_eps])

    items_xml = ""
    for key, ep, total_access in keys_and_eps:
        frag = fragments_dict.get(key)
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, total_access)
        safe_title = html.escape(ep.podcast.title)
        frag = frag.replace('<title>', f'<title>[{safe_title}] ', 1)
        items_xml += frag

    final_xml = header + items_xml + footer
    if feed_token: final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(feed_token))
    else: final_xml = final_xml.replace('?auth=__VECTO_AUTH_TOKEN__', '')
    if user and user.is_authenticated:
        network_ids = set(network_mix.selected_podcasts.values_list('network_id', flat=True))
        network_ids.add(network_mix.network_id)
        _record_active_user(network_ids, user.id)
    return etag_xml_response(request, final_xml.encode('utf-8'))


def play_episode(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network'), id=episode_id)
    feed_token = request.GET.get('auth')

    has_access = False
    if feed_token:
        profile = PatronProfile.objects.filter(feed_token=feed_token).first()
        if profile:
            has_access, _ = _evaluate_access(profile.user, ep.podcast, ep.podcast.network)
            if has_access:
                ck = f"analytics:play:{profile.id}:{ep.id}:{ep.podcast_id}"
                cache.incr(ck) if cache.get(ck) else cache.set(ck, 1, 172800)
                billing_key = f"billing:active:{ep.podcast.network_id}:{profile.user_id}:{timezone.now().strftime('%Y-%m-%d')}"
                cache.set(billing_key, 1, timeout=172800)

    target_url = ep.audio_url_subscriber if (has_access and ep.audio_url_subscriber) else ep.audio_url_public
    if not target_url: raise Http404("Audio file not found.")

    # Defense-in-depth: Django's HttpResponseRedirect already rejects exotic
    # schemes, but we'd rather 404 cleanly. Audio URLs are populated by the
    # ingest pipeline from external feeds — a malicious feed publisher could
    # write a non-http value into the row, so explicitly require http(s).
    if not target_url.lower().startswith(('http://', 'https://')):
        logger.warning(f"[play_episode] Refusing redirect to non-http(s) URL for ep {ep.id}: {target_url[:64]}")
        raise Http404("Audio file not found.")

    response = HttpResponseRedirect(target_url)
    response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response
