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

import icalendar
from podgen import Podcast as PodgenPodcast, Episode as PodgenEpisode, Media, Person
from lxml import etree

from django.db.models import Q

from ..models import (
    CalendarEntry, Network, Podcast, Episode, EpisodeCrossPublication, NetworkMix, UserMix,
)
from ..services.access import (_evaluate_access, _build_episode_description,
                               _evaluate_mix_access, can_view_transcript,
                               patron_profile_for_token)
from ..services.analytics import _record_active_user

warnings.filterwarnings("ignore", message=".*Image URL must end with.*")
warnings.filterwarnings("ignore", message=".*Size is set to 0.*")

logger = logging.getLogger(__name__)


def _strip_auth_placeholder(xml: str) -> str:
    """Remove the auth placeholder from a tokenless render (public feed or a
    session-authenticated request that carries no feed_token). The audio URL
    appends '?auth=__VECTO_AUTH_TOKEN__' (no ampersand), while transcript URLs
    carry '?v=N' first, so their auth arrives as '&auth=...' — and lxml
    serialises the '&' as '&amp;'. All three forms must be scrubbed or the raw
    placeholder leaks into the XML (see planned_features.txt Section C2)."""
    return (
        xml.replace('?auth=__VECTO_AUTH_TOKEN__', '')
           .replace('&amp;auth=__VECTO_AUTH_TOKEN__', '')
           .replace('&auth=__VECTO_AUTH_TOKEN__', '')
    )


def etag_xml_response(request, xml_bytes: bytes) -> HttpResponse:
    etag = f'"{hashlib.md5(xml_bytes).hexdigest()}"'
    if request.META.get('HTTP_IF_NONE_MATCH') == etag:
        logger.debug(f"[ETag MATCH] {request.path} | Served: 0 bytes")
        resp = HttpResponseNotModified()
    else:
        resp = HttpResponse(xml_bytes, content_type='application/xml')
        resp['ETag'] = etag
        resp['Cache-Control'] = 'public, max-age=0, must-revalidate'
        size_mb = len(xml_bytes) / (1024 * 1024)
        logger.debug(f"[ETag MISS] {request.path} | New Hash: {etag} | Served: {size_mb:.2f} MB")
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
            image=image_url or network.display_default_image or "https://example.com/logo.png",
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
        tag_map = {str(ep.guid_public or ep.guid_private or ep.id): ep for ep in self.episodes_data}
        return self._finalize_xml(raw_xml, tag_map, access_map)

    def _finalize_xml(self, raw_xml: str, tag_map: dict, access_map) -> str:
        """Applies the podcast namespace and attaches per-episode metadata
        (category tags, chapter URLs, iTunes season/episode/type) that podgen
        cannot emit on its own.

        lxml will re-inline namespaces on child elements during serialisation;
        we strip those and re-anchor declarations at the root."""
        podcast_ns = "https://podcastindex.org/namespace/1.0"
        itunes_ns  = "http://www.itunes.com/dtds/podcast-1.0.dtd"

        if 'xmlns:podcast=' not in raw_xml:
            raw_xml = raw_xml.replace('<rss ', f'<rss xmlns:podcast="{podcast_ns}" ', 1)

        if not tag_map:
            return raw_xml

        etree.register_namespace('podcast', podcast_ns)
        etree.register_namespace('itunes',  itunes_ns)
        root = etree.fromstring(raw_xml.encode('utf-8'))

        from pod_manager.models import Transcript
        episode_ids = [ep.id for ep in tag_map.values()]
        transcript_map = {
            t.episode_id: t
            for t in Transcript.objects.filter(
                episode_id__in=episode_ids,
                status=Transcript.Status.COMPLETED,
            )
        }

        for item in root.findall('.//item'):
            guid_elem = item.find('guid')
            if guid_elem is None or guid_elem.text not in tag_map:
                continue
            ep = tag_map[guid_elem.text]

            for tag in ep.tags:
                cat_elem = etree.SubElement(item, 'category')
                cat_elem.text = etree.CDATA(str(tag))

            ep_access = access_map.get(ep.podcast_id, False) if access_map else (self.feed_type == 'private')
            ftype = 'private' if ep_access else 'public'

            if ep.chapters_private or ep.chapters_public:
                chapter_url = f"{self.base_url}{reverse('episode_chapters', args=[ep.id, ftype])}"
                chap_elem = etree.SubElement(item, f'{{{podcast_ns}}}chapters')
                chap_elem.set('url', chapter_url)
                chap_elem.set('type', 'application/json+chapters')

            if ep.season_number:
                etree.SubElement(item, f'{{{itunes_ns}}}season').text = str(ep.season_number)
            if ep.episode_number:
                etree.SubElement(item, f'{{{itunes_ns}}}episode').text = str(ep.episode_number)
            if ep.episode_type and ep.episode_type != 'full':
                etree.SubElement(item, f'{{{itunes_ns}}}episodeType').text = ep.episode_type
            # None inherits the channel-level rating (emit nothing); True/False
            # override it per-episode.
            if ep.explicit is not None:
                etree.SubElement(item, f'{{{itunes_ns}}}explicit').text = 'true' if ep.explicit else 'false'

            if ep.id in transcript_map and can_view_transcript(ep, ep_access):
                # ?v=N so the on-platform URL (which 302s to the immutable cdn
                # object) busts when a re-transcribe bumps the version.
                t_version = transcript_map[ep.id].version or 0
                for ext, mime in (
                    ('vtt',  'text/vtt'),
                    ('json', 'application/json'),
                    ('srt',  'application/x-subrip'),
                    ('html', 'text/html'),
                ):
                    t_elem = etree.SubElement(item, f'{{{podcast_ns}}}transcript')
                    t_url = reverse('serve_transcript', kwargs={'episode_id': ep.id, 'ext': ext})
                    # Private-variant fragments carry the auth placeholder so an
                    # entitled app follows our endpoint's 302 to the keyed CDN
                    # object; ?v is first, so auth arrives as '&auth=...' and the
                    # tokenless strips must remove that variant too.
                    full_url = f"{self.base_url}{t_url}?v={t_version}"
                    if ep_access:
                        full_url += "&auth=__VECTO_AUTH_TOKEN__"
                    t_elem.set('url', full_url)
                    t_elem.set('type', mime)

        final_xml = etree.tostring(root, encoding='utf-8', xml_declaration=True).decode('utf-8')
        # Strip inline namespace re-declarations lxml adds to child elements.
        final_xml = final_xml.replace(f' xmlns:podcast="{podcast_ns}"', '')
        final_xml = final_xml.replace(f' xmlns:itunes="{itunes_ns}"', '')
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

    # Guarantee the podcast namespace is on the root element regardless of
    # whether _finalize_xml ran with a non-empty tag_map.
    podcast_ns = "https://podcastindex.org/namespace/1.0"
    if f'xmlns:podcast="{podcast_ns}"' not in raw_xml:
        raw_xml = raw_xml.replace('<rss ', f'<rss xmlns:podcast="{podcast_ns}" ', 1)

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
    if not match:
        logger.warning(f"Fragment extraction failed for episode {episode.id} ({feed_type}); caching empty string.")
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


def get_podcast_for_request(request, slug: str):
    return get_object_or_404(Podcast, slug=slug, network=request.network)


def _podcast_feed_episode_qs(podcast):
    """Episodes carried by this podcast's feeds: its own plus any
    cross-published into it. The OR-join duplicates rows when an episode is
    reachable both ways, so .distinct() is mandatory."""
    return (
        Episode.objects
        .filter(Q(podcast=podcast) | Q(cross_publications__podcast=podcast), is_published=True)
        .select_related('podcast', 'podcast__network', 'podcast__required_tier')
        .order_by('-pub_date')
        .distinct()
    )


def _cross_override_targets(selected_podcasts):
    """{episode_id: [target podcast, ...]} for cross-publications whose
    access_mode lets the target podcast's tier gate the episode."""
    override_map = {}
    rows = EpisodeCrossPublication.objects.filter(
        podcast__in=selected_podcasts,
        access_mode=EpisodeCrossPublication.AccessMode.TARGET,
    ).select_related('podcast', 'podcast__network')
    for cp in rows:
        override_map.setdefault(cp.episode_id, []).append(cp.podcast)
    return override_map


def _parse_feed_date(value: str):
    """Parse YYYYMMDD or YYYY-MM-DD into an aware datetime, or return None."""
    from datetime import datetime as _dt
    for fmt in ('%Y%m%d', '%Y-%m-%d'):
        try:
            return timezone.make_aware(_dt.strptime(value.strip(), fmt))
        except (ValueError, AttributeError):
            continue
    return None


def _parse_feed_limit(value: str, default: int = 500):
    """Return a slice limit integer, None (no cap), or the default on invalid input."""
    if not value:
        return default
    if value.strip().lower() == 'none':
        return None
    try:
        n = int(value.strip())
        return n if n > 0 else default
    except ValueError:
        return default


def generate_custom_feed(request):
    feed_token = request.GET.get('auth')
    podcast = get_podcast_for_request(request, request.GET.get('show'))
    profile = patron_profile_for_token(feed_token)
    if profile is None:
        raise Http404("Feed not found.")
    has_access, _ = _evaluate_access(profile.user, podcast, podcast.network)

    base_url = request.build_absolute_uri('/')[:-1]
    header, footer = get_or_build_feed_shell(podcast, base_url, has_access)

    episode_qs = _podcast_feed_episode_qs(podcast)
    before_date = _parse_feed_date(request.GET.get('before', ''))
    after_date = _parse_feed_date(request.GET.get('after', ''))
    limit = _parse_feed_limit(request.GET.get('limit', ''), default=500)
    if before_date:
        episode_qs = episode_qs.filter(pub_date__lt=before_date)
    if after_date:
        episode_qs = episode_qs.filter(pub_date__gt=after_date)

    # Per-episode access: native episodes use this podcast's gate; cross-published
    # episodes inherit their parent's gate unless the link overrides to 'target'.
    cross_modes = dict(
        EpisodeCrossPublication.objects.filter(podcast=podcast).values_list('episode_id', 'access_mode')
    )
    parent_access = {podcast.id: has_access}

    def _ep_access(ep):
        if ep.podcast_id == podcast.id or cross_modes.get(ep.id) == EpisodeCrossPublication.AccessMode.TARGET:
            return has_access
        if ep.podcast_id not in parent_access:
            parent_access[ep.podcast_id] = _evaluate_access(profile.user, ep.podcast, ep.podcast.network)[0]
        return parent_access[ep.podcast_id]

    keys_and_eps = []
    for ep in (episode_qs if limit is None else episode_qs[:limit]):
        ep_access = _ep_access(ep)
        if not ep.has_public_audio and not (ep.is_premium and ep_access):
            continue
        # Withhold episodes still pointing at the dead S3 bucket (mid-recovery).
        if ep.serves_s3_audio(ep_access):
            continue
        feed_type = 'private' if ep_access else 'public'
        keys_and_eps.append((f"ep_frag_{feed_type}_{ep.id}", ep, ep_access))

    episodes = [ep for _, ep, _ in keys_and_eps]
    header = pin_last_build_date(header, episodes)

    # Bulk fetch fragments from Redis
    fragments_dict = cache.get_many([key for key, _, _ in keys_and_eps])

    # Assemble missing fragments inline if cache missed
    items_xml = ""
    for key, ep, ep_access in keys_and_eps:
        frag = fragments_dict.get(key)
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, ep_access)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(profile.feed_token))

    _record_active_user({podcast.network_id}, profile.user_id)
    return etag_xml_response(request, final_xml.encode('utf-8'))


def generate_public_feed(request, podcast_slug):
    podcast = get_podcast_for_request(request, podcast_slug)
    base_url = request.build_absolute_uri('/')[:-1]

    header, footer = get_or_build_feed_shell(podcast, base_url, False)

    episode_qs = _podcast_feed_episode_qs(podcast)
    before_date = _parse_feed_date(request.GET.get('before', ''))
    after_date = _parse_feed_date(request.GET.get('after', ''))
    limit = _parse_feed_limit(request.GET.get('limit', ''), default=500)
    if before_date:
        episode_qs = episode_qs.filter(pub_date__lt=before_date)
    if after_date:
        episode_qs = episode_qs.filter(pub_date__gt=after_date)

    # has_access is always False here, so the served URL is the public one;
    # skip any still pointing at the dead S3 bucket (mid-recovery).
    episodes = [
        ep for ep in (episode_qs if limit is None else episode_qs[:limit])
        if ep.has_public_audio and not ep.serves_s3_audio(False)
    ]

    header = pin_last_build_date(header, episodes)

    cache_keys = [f"ep_frag_public_{ep.id}" for ep in episodes]
    fragments_dict = cache.get_many(cache_keys)

    items_xml = ""
    for i, ep in enumerate(episodes):
        frag = fragments_dict.get(cache_keys[i])
        if frag is None: frag = get_or_build_episode_fragment(ep, base_url, False)
        items_xml += frag

    final_xml = header + items_xml + footer
    final_xml = _strip_auth_placeholder(final_xml)
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
        builder = RSSFeedBuilder(base_url, user_mix.name, f"Custom blended feed for {user_mix.user.first_name}.", user_mix.display_image or user_mix.network.display_default_image, user_mix.network)
        raw_xml = builder.render()
        shell = (raw_xml.split('</channel>')[0], "</channel></rss>")
        cache.set(cache_key, shell, timeout=None)

    header, footer = shell

    before_date = _parse_feed_date(request.GET.get('before', ''))
    after_date = _parse_feed_date(request.GET.get('after', ''))
    limit = _parse_feed_limit(request.GET.get('limit', ''), default=500)
    selected_podcasts = user_mix.selected_podcasts.all()
    # Include episodes cross-published into selected shows; .distinct() dedupes
    # an episode whose parent AND target are both selected (parent row wins —
    # ep.podcast is always the parent).
    episode_qs = Episode.objects.filter(
        Q(podcast__in=selected_podcasts) | Q(cross_publications__podcast__in=selected_podcasts),
        is_published=True,
    ).select_related('podcast', 'podcast__network').order_by('-pub_date').distinct()
    if before_date:
        episode_qs = episode_qs.filter(pub_date__lt=before_date)
    if after_date:
        episode_qs = episode_qs.filter(pub_date__gt=after_date)
    episodes = episode_qs if limit is None else episode_qs[:limit]
    if episodes:
        latest_date_str = format_datetime(episodes[0].pub_date)
        header = re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)

    override_map = _cross_override_targets(selected_podcasts)

    keys_and_eps = []
    for ep in episodes:
        if not ep.has_public_audio and not ep.is_premium: continue
        ep_has_access, _ = _evaluate_access(user_mix.user, ep.podcast, ep.podcast.network)
        if not ep_has_access:
            for target in override_map.get(ep.id, []):
                if _evaluate_access(user_mix.user, target, target.network)[0]:
                    ep_has_access = True
                    break
        # Withhold episodes still pointing at the dead S3 bucket (mid-recovery).
        if ep.serves_s3_audio(ep_has_access):
            continue
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
    profile = patron_profile_for_token(feed_token)
    user = profile.user if profile else request.user

    user_meets_mix_tier = _evaluate_mix_access(user, network_mix)

    base_url = request.build_absolute_uri('/')[:-1]
    cache_key = f"shell_net_mix_{network_mix.id}"
    shell = cache.get(cache_key)

    if not shell:
        builder = RSSFeedBuilder(base_url, network_mix.name, f"A curated network mix by {network_mix.network.name}.", network_mix.display_image or network_mix.network.display_default_image, network_mix.network)
        raw_xml = builder.render()
        shell = (raw_xml.split('</channel>')[0], "</channel></rss>")
        cache.set(cache_key, shell, timeout=None)

    header, footer = shell

    before_date = _parse_feed_date(request.GET.get('before', ''))
    after_date = _parse_feed_date(request.GET.get('after', ''))
    limit = _parse_feed_limit(request.GET.get('limit', ''), default=1000)
    selected_podcasts = network_mix.selected_podcasts.all()
    episode_qs = Episode.objects.filter(
        Q(podcast__in=selected_podcasts) | Q(cross_publications__podcast__in=selected_podcasts),
        is_published=True,
    ).select_related('podcast', 'podcast__network').order_by('-pub_date').distinct()
    if before_date:
        episode_qs = episode_qs.filter(pub_date__lt=before_date)
    if after_date:
        episode_qs = episode_qs.filter(pub_date__gt=after_date)
    episodes = episode_qs if limit is None else episode_qs[:limit]
    if episodes:
        latest_date_str = format_datetime(episodes[0].pub_date)
        header = re.sub(r'<lastBuildDate>.*?</lastBuildDate>', f'<lastBuildDate>{latest_date_str}</lastBuildDate>', header)

    override_map = _cross_override_targets(selected_podcasts)

    keys_and_eps = []
    for ep in episodes:
        ep_has_access, _ = _evaluate_access(user, ep.podcast, ep.podcast.network)
        if not ep_has_access:
            for target in override_map.get(ep.id, []):
                if _evaluate_access(user, target, target.network)[0]:
                    ep_has_access = True
                    break
        total_access = user_meets_mix_tier and ep_has_access
        if not total_access and not ep.audio_url_public: continue
        # Withhold episodes still pointing at the dead S3 bucket (mid-recovery).
        if ep.serves_s3_audio(total_access):
            continue

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
    if feed_token:
        final_xml = final_xml.replace('__VECTO_AUTH_TOKEN__', str(feed_token))
    else:
        final_xml = _strip_auth_placeholder(final_xml)
    if user and user.is_authenticated:
        network_ids = set(network_mix.selected_podcasts.values_list('network_id', flat=True))
        network_ids.add(network_mix.network_id)
        _record_active_user(network_ids, user.id)
    return etag_xml_response(request, final_xml.encode('utf-8'))


def generate_calendar_feed(request, network_slug):
    """Public per-network release-calendar ICS feed — subscribable from
    Apple/Google/Outlook. Fully public by design (decision #3), like the
    podcast RSS feeds themselves.

    Events are timed and zero-duration (no DTEND) — calendar apps render an
    instant at the release time. The episode URL is computed live from
    is_published (A13): a scheduled-unpublished entry appears with no URL,
    then carries the episode link once it publishes — never stored."""
    network = get_object_or_404(Network, slug=network_slug)
    cal = icalendar.Calendar()
    cal.add('prodid', f'-//Vecto//{network.name} Release Calendar//EN')
    cal.add('version', '2.0')
    cal.add('x-wr-calname', f'{network.name} Releases')
    now = timezone.now()
    entries = network.calendar_entries.select_related('episode', 'podcast').order_by('scheduled_at')
    for entry in entries:
        # Pre-publish visibility (A16), public perspective: a 'hidden' entry is
        # omitted until it publishes; a 'teaser' entry shows placeholder text
        # with SxE suppressed and the type line dropped. A published entry always
        # shows the actual info — public_* short-circuit on is_revealed.
        if entry.public_hidden():
            continue
        teased = entry.prepublish_visibility == CalendarEntry.PrepublishVisibility.TEASER and not entry.is_revealed
        event = icalendar.Event()
        event.add('uid', f'calendar-entry-{entry.id}@vecto')
        # RFC 5545 requires DTSTAMP on every VEVENT; the icalendar lib does
        # NOT add it automatically.
        event.add('dtstamp', now)
        event.add('last-modified', entry.updated_at)
        summary = (
            f'S{entry.season_number}E{entry.episode_number} · {entry.public_title()}'
            if entry.public_show_sxe() else entry.public_title()
        )
        event.add('summary', summary)
        event.add('dtstart', entry.scheduled_at)
        if entry.episode_id and entry.episode.is_published:
            event.add('url', request.build_absolute_uri(
                reverse('episode_detail', args=[entry.episode_id])))
        elif entry.external_link:
            event.add('url', entry.external_link)
        # DESCRIPTION: a podcast/type header line, then the public notes. The
        # type is dropped while teased (it can spoil), matching the hidden SxE.
        header_bits = [b for b in (
            entry.podcast.title if entry.podcast else '',
            '' if teased else entry.episode_type) if b]
        desc_parts = [b for b in (' · '.join(header_bits), entry.public_notes()) if b]
        if desc_parts:
            event.add('description', '\n\n'.join(desc_parts))
        cal.add_component(event)
    resp = HttpResponse(cal.to_ical(), content_type='text/calendar')
    # Calendar apps poll this URL on their own schedule; make sure nothing
    # between us and them serves a stale copy.
    resp['Cache-Control'] = 'no-cache'
    return resp


def play_episode(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network'), id=episode_id)
    feed_token = request.GET.get('auth')

    has_access = False
    if feed_token:
        profile = patron_profile_for_token(feed_token)
        if profile:
            has_access, _ = _evaluate_access(profile.user, ep.podcast, ep.podcast.network)
            if not has_access:
                # A cross-publication in 'target' mode lets the target podcast's
                # tier gate this episode. The play URL is feed-agnostic, so an
                # override on any link applies everywhere the episode appears.
                for cp in ep.cross_publications.filter(
                    access_mode=EpisodeCrossPublication.AccessMode.TARGET
                ).select_related('podcast', 'podcast__network'):
                    if _evaluate_access(profile.user, cp.podcast, cp.podcast.network)[0]:
                        has_access = True
                        break
            if has_access:
                ck = f"analytics:play:{profile.id}:{ep.id}:{ep.podcast_id}"
                cache.incr(ck) if cache.get(ck) else cache.set(ck, 1, 172800)
                billing_key = f"billing:active:{ep.podcast.network_id}:{profile.user_id}:{timezone.now().strftime('%Y-%m-%d')}"
                cache.set(billing_key, 1, timeout=172800)

    target_url = ep.playback_url(has_access)
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
