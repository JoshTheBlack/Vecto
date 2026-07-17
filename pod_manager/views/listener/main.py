"""
Listener-facing views: home/episode browser, feed listing, episode detail,
and the per-tenant user profile dashboard.
"""
import json as _json
import logging
import os

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.db.models import Q, Exists, OuterRef
from django.http import Http404
from django.shortcuts import redirect, get_object_or_404, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_date

from ...models import (
    PatronProfile, NetworkMembership, Podcast, Episode, NetworkMix, UserMix,
    EpisodeEditSuggestion, Transcript, EpisodeCrossPublication,
)
from ...services.access import (_evaluate_access, _build_episode_description,
                                _evaluate_mix_access, can_view_transcript)
from ...services.analytics import get_live_user_stats
from ...utils import get_membership
from .actions import MIX_ACTION_HANDLERS

logger = logging.getLogger(__name__)


def _get_user_networks(user):
    if not user.is_authenticated:
        return []
    # Only include networks with active paid membership; a free NetworkMembership
    # row grants no extra access over an anonymous user, so it shouldn't qualify
    # the user for cross-network browsing.
    premium_networks = [
        m.network for m in user.network_memberships.filter(is_active_patron=True).select_related('network')
    ]
    seen = {n.id for n in premium_networks}
    owned = [n for n in user.owned_networks.all() if n.id not in seen]
    return premium_networks + owned


def _build_feed_base_url(podcast, request):
    """Returns the base URL for the podcast's feed endpoint, or None if unresolvable."""
    if podcast.network_id == request.network.id:
        return request.build_absolute_uri('/')[:-1]
    if podcast.network.custom_domain:
        return f"{request.scheme}://{podcast.network.custom_domain}"
    logger.warning(f"Cannot build feed URL for podcast '{podcast.title}' (id={podcast.id}): cross-network with no custom domain")
    return None


_SHOW_HIDDEN_SESSION_KEY = 'show_hidden_feeds'


def _resolve_show_hidden(request):
    """Owner 'reveal hidden feeds' toggle (D5), persisted in the session.

    ?show_hidden=1 (or =0) sets it; when the param is absent the stored value is
    reused. Per-network scoping (reveal only feeds of networks the user owns) is
    enforced at the queryset, so this returning True for a user who owns none of
    the viewed networks reveals nothing — the param is effectively ignored.
    """
    param = request.GET.get('show_hidden')
    if param is not None:
        value = param == '1'
        request.session[_SHOW_HIDDEN_SESSION_KEY] = value
        return value
    return bool(request.session.get(_SHOW_HIDDEN_SESSION_KEY, False))


def _feed_cross_published_exists():
    """Two Exists() subqueries keyed on the OUTER podcast id: whether that feed
    has >=1 auto_crosspublish_targets, and whether any EpisodeCrossPublication
    exists on its episodes. Used to keep a hidden-but-cross-published feed (and
    its episodes) on the Dashboard per D6."""
    has_auto = Exists(Podcast.objects.filter(
        pk=OuterRef('pk'), auto_crosspublish_targets__isnull=False))
    has_links = Exists(EpisodeCrossPublication.objects.filter(
        episode__podcast=OuterRef('pk')))
    return has_auto, has_links


def home(request):
    show_slugs = request.GET.getlist('show')
    search_query = request.GET.get('q', '').strip()
    older_than = request.GET.get('older_than', '').strip()
    newer_than = request.GET.get('newer_than', '').strip()
    include_transcripts = request.GET.get('transcripts') == '1'
    # Defaults ON: a show chip surfaces episodes cross-published INTO that feed,
    # not just those whose parent IS that feed. ?crosspub=0 restricts to parent.
    include_cross_published = request.GET.get('crosspub', '1') != '0'

    tenant_profile = getattr(request, 'tenant_profile', None)

    user_networks = _get_user_networks(request.user)
    selected_networks = request.GET.getlist('network')

    if 'all' in selected_networks:
        target_network_slugs = [n.slug for n in user_networks] if user_networks else [request.network.slug]
    elif selected_networks:
        valid_slugs = {n.slug for n in user_networks}
        target_network_slugs = [slug for slug in selected_networks if slug in valid_slugs] or [request.network.slug]
    else:
        target_network_slugs = [request.network.slug]
        selected_networks = [request.network.slug]

    if request.user.is_authenticated:
        owned_network_ids = set(request.user.owned_networks.values_list('id', flat=True))
    else:
        owned_network_ids = set()
    show_hidden = _resolve_show_hidden(request)

    # D6 dashboard visibility: a feed is visible when it is not hidden, OR it is
    # cross-published (>=1 auto target OR any link on its episodes), OR the owner
    # revealed it — the last arm scoped to networks the requester owns. Applied
    # identically to the episode stream and the chip queryset so they stay in
    # lockstep.
    ep_has_auto = Exists(Podcast.objects.filter(
        pk=OuterRef('podcast_id'), auto_crosspublish_targets__isnull=False))
    ep_has_links = Exists(EpisodeCrossPublication.objects.filter(
        episode__podcast=OuterRef('podcast_id')))
    ep_visible = Q(podcast__is_hidden=False) | Q(_feed_has_auto=True) | Q(_feed_has_links=True)
    if show_hidden and owned_network_ids:
        ep_visible |= Q(podcast__network_id__in=owned_network_ids)

    feed_has_auto, feed_has_links = _feed_cross_published_exists()
    chip_visible = Q(is_hidden=False) | Q(_feed_has_auto=True) | Q(_feed_has_links=True)
    if show_hidden and owned_network_ids:
        chip_visible |= Q(network_id__in=owned_network_ids)

    query = (Episode.objects
             .select_related('podcast', 'podcast__network', 'podcast__required_tier', 'transcript')
             .prefetch_related('cross_publications__podcast')
             .filter(podcast__network__slug__in=target_network_slugs, is_published=True)
             .annotate(_feed_has_auto=ep_has_auto, _feed_has_links=ep_has_links)
             .filter(ep_visible))
    podcasts = (Podcast.objects
                .filter(network__slug__in=target_network_slugs)
                .annotate(_feed_has_auto=feed_has_auto, _feed_has_links=feed_has_links)
                .filter(chip_visible)
                .order_by('title'))

    if show_slugs:
        if include_cross_published:
            query = query.filter(
                Q(podcast__slug__in=show_slugs)
                | Q(cross_publications__podcast__slug__in=show_slugs)
            ).distinct()
        else:
            query = query.filter(podcast__slug__in=show_slugs)
    if search_query:
        base_q = Q(title__icontains=search_query) | Q(clean_description__icontains=search_query)
        if include_transcripts:
            base_q |= Q(transcript__transcript_text__icontains=search_query, transcript__status='completed')
        query = query.filter(base_q)

    if newer_than:
        parsed_newer = parse_date(newer_than)
        if parsed_newer:
            query = query.filter(pub_date__gt=parsed_newer)
    if older_than:
        parsed_older = parse_date(older_than)
        if parsed_older:
            query = query.filter(pub_date__lt=parsed_older)

    page_obj = Paginator(query.order_by('-pub_date'), 20).get_page(request.GET.get('page', 1))

    page_number = page_obj.number
    total_pages = page_obj.paginator.num_pages
    custom_page_range = range(max(1, page_number - 3), min(total_pages, page_number + 3) + 1)

    if request.user.is_authenticated:
        page_network_ids = {ep.podcast.network_id for ep in page_obj}
        home_memberships = {
            m.network_id: m
            for m in request.user.network_memberships.filter(network_id__in=page_network_ids)
        }
        home_owned_ids = owned_network_ids
    else:
        home_memberships = {}
        home_owned_ids = set()

    for ep in page_obj:
        ep.user_has_access, _ = _evaluate_access(
            request.user, ep.podcast, ep.podcast.network,
            membership=home_memberships.get(ep.podcast.network_id),
            is_owner=ep.podcast.network_id in home_owned_ids,
        )
        # Effective playback URL (R2-aware) for the dashboard now-playing player.
        ep.raw_audio_url = ep.playback_url(ep.user_has_access)

    context = {
        'episodes': page_obj, 'page_obj': page_obj, 'podcasts': podcasts,
        'selected_shows': show_slugs, 'current_network': request.network,
        'search_query': search_query, 'tenant_profile': tenant_profile,
        'older_than': older_than, 'newer_than': newer_than,
        'include_transcripts': include_transcripts,
        'include_cross_published': include_cross_published,
        'custom_page_range': custom_page_range,
        'user_networks': user_networks,
        'selected_networks': selected_networks,
    }
    return render(request, 'pod_manager/home.html', context)


def user_feeds(request):
    tenant_profile = getattr(request, 'tenant_profile', None)
    profile = getattr(request.user, 'patron_profile', None) if request.user.is_authenticated else None

    if request.method == 'POST':
        if not request.user.is_authenticated:
            return redirect('patreon_login')
        for action_key, handler in MIX_ACTION_HANDLERS.items():
            if request.POST.get(action_key):
                handler(request)
                break
        network_qs = '&'.join(f'network={s}' for s in request.GET.getlist('network'))
        redirect_url = reverse('user_feeds')
        if network_qs:
            redirect_url += f'?{network_qs}'
        return redirect(redirect_url)

    user_networks = _get_user_networks(request.user)
    selected_networks = request.GET.getlist('network')

    if 'all' in selected_networks:
        target_network_slugs = [n.slug for n in user_networks] if user_networks else [request.network.slug]
    elif selected_networks:
        valid_slugs = {n.slug for n in user_networks}
        target_network_slugs = [slug for slug in selected_networks if slug in valid_slugs] or [request.network.slug]
    else:
        target_network_slugs = [request.network.slug]
        selected_networks = [request.network.slug]

    if request.user.is_authenticated:
        memberships_by_network = {
            m.network_id: m
            for m in request.user.network_memberships.filter(
                network__slug__in=target_network_slugs
            ).select_related('network')
        }
        owned_network_ids = set(request.user.owned_networks.values_list('id', flat=True))
    else:
        memberships_by_network = {}
        owned_network_ids = set()

    show_hidden = _resolve_show_hidden(request)
    show_hidden_available = bool(owned_network_ids) and request.user.owned_networks.filter(
        slug__in=target_network_slugs).exists()

    # Directory visibility (section 1): hidden feeds drop out for everyone, but
    # an owner with the toggle on sees them back — scoped to the networks they
    # own, so hidden feeds of a co-viewed non-owned network stay filtered.
    visible_feed = Q(is_hidden=False)
    if show_hidden and owned_network_ids:
        visible_feed |= Q(network_id__in=owned_network_ids)

    feed_data = []
    available_podcasts = []

    network_mixes = NetworkMix.objects.filter(network__slug__in=target_network_slugs).select_related('network', 'required_tier')
    for mix in network_mixes:
        mix.has_access = _evaluate_mix_access(
            request.user, mix,
            membership=memberships_by_network.get(mix.network_id),
            is_owner=mix.network_id in owned_network_ids,
        )
        mix.feed_url = (
            request.build_absolute_uri(reverse('network_mix_feed', args=[mix.network.slug, mix.slug]))
            + (f"?auth={profile.feed_token}" if profile else "")
        )
        feed_data.append({
            'is_network_mix': True, 'mix': mix,
            'has_access': mix.has_access, 'feed_url': mix.feed_url,
        })

    for podcast in Podcast.objects.filter(network__slug__in=target_network_slugs).filter(visible_feed).select_related('network', 'required_tier'):
        has_access, _ = _evaluate_access(
            request.user, podcast, podcast.network,
            membership=memberships_by_network.get(podcast.network_id),
            is_owner=podcast.network_id in owned_network_ids,
        )
        available_podcasts.append({'podcast': podcast, 'has_access': has_access})

        feed_base = _build_feed_base_url(podcast, request)
        if feed_base is None:
            continue

        if profile is not None:
            raw_url = reverse('custom_feed') + f"?auth={profile.feed_token}&show={podcast.slug}"
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': has_access, 'feed_url': feed_base + raw_url})
        elif not podcast.required_tier or podcast.public_feed_url:
            raw_url = reverse('public_feed', args=[podcast.slug])
            feed_data.append({'is_network_mix': False, 'podcast': podcast, 'has_access': False, 'feed_url': feed_base + raw_url})

    user_mixes = UserMix.objects.filter(user=request.user, network__slug__in=target_network_slugs, is_active=True).prefetch_related('selected_podcasts') if request.user.is_authenticated else []

    context = {
        'profile': profile, 'tenant_profile': tenant_profile,
        'feed_data': feed_data, 'user_mixes': user_mixes,
        'current_network': request.network,
        'available_podcasts': available_podcasts,
        'user_networks': user_networks,
        'selected_networks': selected_networks,
        'show_hidden': show_hidden,
        'show_hidden_available': show_hidden_available,
    }
    return render(request, 'pod_manager/user_feeds.html', context)


def _fmt_timecode(seconds: float) -> str:
    """Seconds -> M:SS, or H:MM:SS past an hour (chapter link display)."""
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"


def _episode_chapter_list(ep, has_access):
    """Normalize an episode's saved chapters into a sorted list of
    {start, time, title} for the clickable episode-detail links.

    Mirrors the feed's access choice (private chapters with access, else public)
    and handles both storage shapes — a legacy bare list or the Podcast Index
    {"chapters": [...]} dict.
    """
    raw = (ep.chapters_private or ep.chapters_public) if has_access else (ep.chapters_public or ep.chapters_private)
    if isinstance(raw, dict):
        items = raw.get('chapters', [])
    elif isinstance(raw, list):
        items = raw
    else:
        items = []

    chapters = []
    for c in items:
        if not isinstance(c, dict) or c.get('startTime') is None:
            continue
        try:
            start = float(c['startTime'])
        except (TypeError, ValueError):
            continue
        chapters.append({
            'start': start,
            'time': _fmt_timecode(start),
            'title': (c.get('title') or '').strip() or 'Untitled chapter',
        })
    chapters.sort(key=lambda c: c['start'])
    return chapters


def episode_detail(request, episode_id):
    ep = get_object_or_404(Episode.objects.select_related('podcast', 'podcast__network'), id=episode_id)
    ep.user_has_access, _ = _evaluate_access(request.user, ep.podcast, ep.podcast.network)

    if ep.podcast.network != request.network and not ep.user_has_access:
        raise Http404("No Episode matches the given query.")

    is_owner = (
        request.user.is_authenticated and (
            request.user.is_superuser or
            ep.podcast.network.owners.filter(pk=request.user.pk).exists()
        )
    )

    if not ep.is_published and not is_owner:
        raise Http404("No Episode matches the given query.")

    ep.display_description = _build_episode_description(ep, ep.user_has_access)
    # The URL the on-site player actually loads — routed through the same R2
    # serving precedence as feeds (/play), so a mirrored GDrive episode streams
    # inline from R2 instead of the un-loadable Drive link.
    ep.raw_audio_url = ep.playback_url(ep.user_has_access)
    # The reachability probe exists for flaky origins (GDrive/dead-S3). When we're
    # serving from R2 it's pointless — and a HEAD per page view is a needless R2
    # Class B op — so skip it.
    ep.is_r2_served = bool(ep.r2_url) and ep.raw_audio_url == ep.r2_url

    trust_score = None
    if request.user.is_authenticated:
        membership = request.user.network_memberships.filter(network=ep.podcast.network).first()
        trust_score = membership.trust_score if membership else 0

    transcript = getattr(ep, 'transcript', None)
    # ENFORCEMENT (not cosmetic): the inline HTML + words JSON are delivered
    # server-side here, a content path that never touches serve_transcript. Gate
    # the reads themselves on the shared predicate so a non-viewer's page carries
    # no transcript bytes — and skip two R2 Class B reads per non-viewer pageview.
    transcript_viewable = is_owner or can_view_transcript(ep, ep.user_has_access)
    transcript_html = None
    # transcript_speakers: ordered distinct speaker_id set (timeline order) — used
    # only as the "any speakers?" guard now that the form boxes are JS-rendered.
    transcript_speakers = []
    # transcript_speaker_names: speaker_id -> current resolved name (the fold), the
    # authoritative mapping buildEnhancedTranscript overrides doc.speaker_mappings with.
    transcript_speaker_names = {}
    # transcript_speaker_data: per speaker_id {id, name} in timeline order — drives
    # the combined/split form boxes (combined groups these by shared name).
    transcript_speaker_data = []
    # Inline render reads the html + words FROM R2 (or local when not R2-backed)
    # via the transcript store, so the page no longer depends on local disk.
    from pod_manager.services.transcription import read_transcript_bytes, fold_speaker_mappings
    if transcript_viewable and transcript and transcript.status == Transcript.Status.COMPLETED and transcript.html_file:
        try:
            transcript_html = read_transcript_bytes(ep.id, 'html', transcript.version, transcript.r2_key_token).decode('utf-8')
        except Exception:
            pass

    if transcript_viewable and transcript and transcript.status == Transcript.Status.COMPLETED and transcript.words_json_file:
        try:
            words_doc = _json.loads(read_transcript_bytes(ep.id, 'words', transcript.version, transcript.r2_key_token).decode('utf-8'))
            # Resolved names come from the approved-edit fold over the immutable
            # speaker_id base (not from distinct seg.speaker), so the form reflects
            # the same source of truth replay writes. Pre-backfill .words without a
            # speaker_id fall back to seg.speaker (split degrades to combined there).
            mapping = fold_speaker_mappings(ep.id)
            seen = set()
            for seg in words_doc.get('segments', []):
                sid = seg.get('speaker_id') or seg.get('speaker') or ''
                if sid and sid not in seen:
                    seen.add(sid)
                    transcript_speakers.append(sid)
                    transcript_speaker_data.append({'id': sid, 'name': mapping.get(sid, sid)})
            transcript_speaker_names = mapping or words_doc.get('speaker_mappings', {})
        except Exception:
            pass

    chapters = _episode_chapter_list(ep, ep.user_has_access)

    # A private-only episode (no public fallback) has no raw_audio_url for a
    # viewer without access — nothing to play, so the transcript tab (which
    # is keyed to that audio) shouldn't appear for them either. Owners always
    # see it regardless.
    show_transcript_tab = is_owner or bool(transcript and ep.raw_audio_url)

    cross_targets = ep.cross_publications.select_related('podcast').order_by('podcast__title')
    network_podcasts = []
    if request.user.is_authenticated:
        network_podcasts = ep.podcast.network.podcasts.exclude(id=ep.podcast_id).order_by('title')

    # Release-calendar linking (owner controls only): the entry this episode is
    # already tied to, and the pool of still-unlinked entries it could adopt
    # (this podcast's planned entries plus any freeform ones).
    from django.db.models import Q as _Q
    calendar_entry = getattr(ep, 'calendar_entry', None)
    unlinked_calendar_entries = []
    if is_owner and calendar_entry is None:
        unlinked_calendar_entries = (
            ep.podcast.network.calendar_entries
            .filter(episode__isnull=True)
            .filter(_Q(podcast_id=ep.podcast_id) | _Q(podcast__isnull=True))
            .order_by('scheduled_at')
        )

    return render(request, 'pod_manager/episode_detail.html', {
        'ep': ep,
        'is_owner': is_owner,
        'calendar_entry': calendar_entry,
        'unlinked_calendar_entries': unlinked_calendar_entries,
        'show_transcript_tab': show_transcript_tab,
        'chapters': chapters,
        'chapters_json': _json.dumps(chapters),
        'cross_targets': cross_targets,
        'cross_target_ids': [cp.podcast_id for cp in cross_targets],
        'network_podcasts': network_podcasts,
        'trust_score': trust_score,
        'transcript': transcript,
        'transcript_viewable': transcript_viewable,
        'transcript_html': transcript_html,
        'transcript_speakers': transcript_speakers,
        'transcript_speaker_names': transcript_speaker_names,
        'transcript_speaker_names_json': _json.dumps(transcript_speaker_names),
        'transcript_speaker_data_json': _json.dumps(transcript_speaker_data),
    })


@login_required(login_url='/login/')
def user_profile(request):
    tenant_profile = getattr(request, 'tenant_profile', None)
    current_membership = get_membership(request)
    has_active_totp = request.user.totpdevice_set.filter(confirmed=True).exists()
    patron_profile = getattr(request.user, 'patron_profile', None)
    totp_mode = patron_profile.totp_mode if patron_profile else 'replace'

    if not tenant_profile:
        return render(request, 'pod_manager/user_profile.html', {
            'level': 0, 'title': "Commoner", 'progress_percent': 0,
            'total_approved': 0, 'account_vintage': None,
            'has_active_totp': has_active_totp,
            'totp_mode': totp_mode,
            'membership': current_membership,
            'live_stats': {
                'playback_hits': 0, 'hours_accessed': 0.0, 'streak_days': 0, 'streak_weeks': 0,
                'obsession_title': "Wandering Adventurer",
                'notfound_seen': 0, 'notfound_total': 0, 'notfound_reveal_total': True,
            },
        })

    account_vintage = tenant_profile.patreon_join_date
    joined_after_launch_days = None
    account_age_years = None

    if account_vintage:
        account_age_years = (timezone.now() - account_vintage).days / 365.25
        if request.network.patreon_campaign_created_at:
            delta = account_vintage - request.network.patreon_campaign_created_at
            joined_after_launch_days = max(0, delta.days)

    total_approved = EpisodeEditSuggestion.objects.filter(
        user=request.user, episode__podcast__network=request.network,
        status=EpisodeEditSuggestion.Status.APPROVED,
    ).count()

    level, title, next_level_goal, progress_percent = 0, "Commoner", 1, 0
    if total_approved >= 1000:
        level, title, next_level_goal, progress_percent = 5, "Keeper of the Tome", 1000, 100
    elif total_approved >= 500:
        level, title, next_level_goal, progress_percent = 4, "Grand Archivist", 1000, (total_approved / 1000) * 100
    elif total_approved >= 100:
        level, title, next_level_goal, progress_percent = 3, "Archivist", 500, (total_approved / 500) * 100
    elif total_approved >= 25:
        level, title, next_level_goal, progress_percent = 2, "Scout", 100, (total_approved / 100) * 100
    elif total_approved >= 1:
        level, title, next_level_goal, progress_percent = 1, "Initiate", 25, (total_approved / 25) * 100

    context = {
        'profile': tenant_profile, 'total_approved': total_approved,
        'level': level, 'title': title, 'next_level_goal': next_level_goal,
        'progress_percent': min(progress_percent, 100),
        'live_stats': get_live_user_stats(tenant_profile),
        'account_vintage': account_vintage,
        'joined_after_launch_days': joined_after_launch_days,
        'account_age_years': account_age_years,
        'has_active_totp': has_active_totp,
        'totp_mode': totp_mode,
        'membership': current_membership,
    }
    return render(request, 'pod_manager/user_profile.html', context)
