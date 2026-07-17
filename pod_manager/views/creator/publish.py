"""
Network-owner episode publishing: create, schedule, draft, and manage
unpublished/scheduled episodes.
"""
import datetime
import json
import logging
import uuid

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden, JsonResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.views.decorators.http import require_POST

from ...models import Episode, EpisodeCrossPublication, Network, Podcast
from ...services.cross_publish import (
    apply_auto_cross_publish, current_target_ids, sync_cross_publications,
    validate_cross_targets,
)
from ...services.release_calendar import (
    create_calendar_entry_from_episode, ensure_calendar_entry_for_episode,
    link_episode_to_entry, set_prepublish_visibility,
)

_CAL_VIS_FIELDS = ['prepublish_visibility', 'placeholder_title', 'placeholder_notes', 'updated_at']


def _apply_calendar_visibility(entry, request):
    """Persist pre-publish visibility + teaser text from a form that carries the
    controls (schedule/publish form). No-op when the entry is None or the form
    didn't include the control, so callers without it don't clobber the setting."""
    if entry is None or 'prepublish_visibility' not in request.POST:
        return
    set_prepublish_visibility(
        entry, request.POST.get('prepublish_visibility', ''),
        request.POST.get('placeholder_title', ''), request.POST.get('placeholder_notes', ''))
    entry.save(update_fields=_CAL_VIS_FIELDS)
from ...tasks import task_rebuild_episode_fragments, task_refresh_live_schedules
from ...utils import sanitize_user_html

logger = logging.getLogger(__name__)


def _get_owner_networks(user):
    if user.is_superuser:
        return Network.objects.all()
    return user.owned_networks.all()


def _parse_explicit(raw):
    """Map the Content Rating form value to the tri-state explicit field:
    '' / None -> None (inherit show), 'true' -> True, 'false' -> False."""
    if raw is None or raw == '':
        return None
    return str(raw).strip().lower() == 'true'


def _require_owner(user, network):
    # Admins (staff / superuser) manage cross-publish alongside owners
    # (user_edit_rollback.md §8a — cross-publish is owner/admin only).
    if user.is_superuser or user.is_staff:
        return True
    return network.owners.filter(pk=user.pk).exists()


AUDIO_EXTS = ('.mp3', '.m4a', '.wav', '.aac', '.ogg')
MAX_AUDIO_BYTES = 500 * 1024 * 1024


def _audio_upload_error(audio_file):
    """Cheap validation that needs no episode id — extension and size. Returns an
    error string or None. Kept separate from _ingest_episode_audio so the publish
    flow can reject an obviously-bad file BEFORE it creates the episode, rather
    than half-publishing and then failing on the mirror."""
    if not audio_file.name.lower().endswith(AUDIO_EXTS):
        return "Unsupported file type — please upload an audio file."
    if audio_file.size > MAX_AUDIO_BYTES:
        return "File too large (max 500MB)."
    return None


def _ingest_episode_audio(request, ep, audio_file):
    """Mirror one uploaded audio file to R2, attach it to the episode's Premium /
    Ad-Free slot, and reset + re-queue transcription. The single audio-ingest
    path, shared by manage_episode's upload_audio action (episode edit page) and
    the publish submit (where the file rides Publish / Save Draft / Schedule).

    Assumes _audio_upload_error already passed. Messages the user and returns True
    on success, False if the mirror rejected or failed. The episode must already
    be saved — the mirror keys on ep.id.
    """
    import tempfile
    from pathlib import Path
    from django.core.cache import cache
    from ...models import Transcript
    from ...services.r2_mirror import mirror_episode_audio, MirrorSkipped
    from ...services.transcription import dispatch_transcription

    suffix = Path(audio_file.name).suffix or '.mp3'
    tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=False)
    try:
        for chunk in audio_file.chunks():
            tmp.write(chunk)
        tmp.close()
        try:
            result = mirror_episode_audio(ep.id, local_path=tmp.name, force=True, manual=True)
        except MirrorSkipped as exc:
            messages.error(request, f"Audio upload rejected: {exc}")
            return False

        if ep.audio_url_subscriber != result['r2_url']:
            ep.audio_url_subscriber = result['r2_url']
            ep.save(update_fields=['audio_url_subscriber'])
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        base_url = request.build_absolute_uri('/')
        task_rebuild_episode_fragments.delay(ep.id, base_url)

        # Reset (or create) the transcript row so a prior failed / awaiting-
        # recovery record doesn't linger orphaned now that real audio exists —
        # same reset the manual re-transcribe button uses.
        transcript, _ = Transcript.objects.get_or_create(episode=ep)
        transcript.status = Transcript.Status.PENDING
        transcript.error_message = None
        transcript.save(update_fields=['status', 'error_message'])
        dispatch_transcription(ep.id)

        logger.info(
            f"[audio] Episode {ep.id} '{ep.title}' audio uploaded by "
            f"{request.user.username} -> {result['r2_url']} (status={result['status']})"
        )
        messages.success(request, "Audio uploaded and mirrored to R2. Transcription queued.")
        return True
    finally:
        Path(tmp.name).unlink(missing_ok=True)


@login_required(login_url='/login/')
def publish_episode(request):
    networks = list(_get_owner_networks(request.user))
    if not networks:
        return HttpResponseForbidden("No creator access.")

    # Resolve active network (query param or first owned)
    slug = request.GET.get('network') or request.POST.get('network_slug')
    current_network = next((n for n in networks if n.slug == slug), networks[0])

    podcasts = list(Podcast.objects.filter(network=current_network).order_by('title'))

    if request.method == 'POST':
        return _handle_publish_post(request, current_network, podcasts, networks)

    # Check if editing an existing scheduled/draft episode
    edit_ep = None
    edit_id = request.GET.get('edit')
    if edit_id:
        edit_ep = Episode.objects.filter(
            pk=edit_id, podcast__network=current_network, is_published=False
        ).first()

    scheduled = Episode.objects.filter(
        podcast__network=current_network,
        is_published=False,
    ).select_related('podcast').order_by('scheduled_at', '-pub_date')

    # Unlinked upcoming calendar entries for the "Link to Calendar Entry"
    # selector (A9) — filtered client-side against the chosen podcast.
    from ...models import CalendarEntry
    calendar_entries = CalendarEntry.objects.filter(
        network=current_network, episode__isnull=True,
        scheduled_at__gte=timezone.now(),
    ).order_by('scheduled_at')

    return render(request, 'pod_manager/publish_episode.html', {
        'networks': networks,
        'current_network': current_network,
        'podcasts': podcasts,
        'scheduled': scheduled,
        'edit_ep': edit_ep,
        'edit_ep_cross_ids': current_target_ids(edit_ep) if edit_ep else [],
        'calendar_entries': calendar_entries,
        'now': timezone.now(),
    })


def _handle_publish_post(request, current_network, podcasts, networks):
    action = request.POST.get('action', 'publish')  # publish | draft | schedule | delete | update

    # --- Delete ---
    if action == 'delete':
        ep_id = request.POST.get('episode_id')
        ep = get_object_or_404(Episode, pk=ep_id, podcast__network=current_network, is_published=False)
        ep.delete()
        messages.success(request, "Episode deleted.")
        return redirect(f"{reverse('publish_episode')}?network={current_network.slug}&tab=scheduled")

    # --- Publish Now (existing scheduled/draft) ---
    if action == 'publish_now':
        ep_id = request.POST.get('episode_id')
        ep = get_object_or_404(Episode, pk=ep_id, podcast__network=current_network, is_published=False)
        ep.is_published = True
        ep.scheduled_at = None
        ep.save(update_fields=['is_published', 'scheduled_at'])
        base_url = request.build_absolute_uri('/')
        task_rebuild_episode_fragments.delay(ep.id, base_url)
        messages.success(request, f'“{ep.title}” published.')
        return redirect(f"{reverse('publish_episode')}?network={current_network.slug}&tab=scheduled")

    # --- Create / Update episode ---
    podcast_id = request.POST.get('podcast_id')
    podcast = get_object_or_404(Podcast, pk=podcast_id, network=current_network)

    title = request.POST.get('title', '').strip()
    episode_id = request.POST.get('episode_id')

    def _back_to_form():
        """Redirect that keeps the user on the same compose/edit form on an error
        BEFORE anything is saved, rather than dropping them on a blank Compose."""
        url = f"{reverse('publish_episode')}?network={current_network.slug}"
        if episode_id:
            url += f"&edit={episode_id}"
        return redirect(url)

    if not title:
        messages.error(request, "Title is required.")
        return _back_to_form()

    # The audio file (Upload tab) rides this submit. Validate it up front —
    # extension and size need no episode id — so a bad file is rejected BEFORE we
    # create/publish the episode, rather than half-publishing then failing on the
    # mirror. The actual mirror runs after the save, once ep.id exists.
    audio_file = request.FILES.get('audio_file') or None
    if audio_file:
        audio_err = _audio_upload_error(audio_file)
        if audio_err:
            messages.error(request, audio_err)
            return _back_to_form()

    raw_desc   = request.POST.get('description', '')
    clean_desc = sanitize_user_html(raw_desc)

    try:
        tags = json.loads(request.POST.get('tags_json', '[]'))
        if not isinstance(tags, list):
            tags = []
    except (json.JSONDecodeError, ValueError):
        tags = []

    try:
        chapters_raw = json.loads(request.POST.get('chapters_json', 'null'))
    except (json.JSONDecodeError, ValueError):
        chapters_raw = None

    audio_public     = request.POST.get('audio_url_public', '').strip() or None
    audio_subscriber = request.POST.get('audio_url_subscriber', '').strip() or None
    duration         = request.POST.get('duration', '').strip()

    try:
        season_number = int(request.POST.get('season_number') or 0) or None
    except ValueError:
        season_number = None
    try:
        episode_number = int(request.POST.get('episode_number') or 0) or None
    except ValueError:
        episode_number = None

    episode_type = request.POST.get('episode_type', '').strip()[:50]
    explicit = _parse_explicit(request.POST.get('explicit'))

    # Resolve existing episode (update) or create new
    if episode_id:
        ep = get_object_or_404(Episode, pk=episode_id, podcast__network=current_network, is_published=False)
    else:
        ep = Episode(
            podcast=podcast,
            guid_public=str(uuid.uuid4()),
        )

    ep.title              = title
    ep.raw_description    = raw_desc
    ep.clean_description  = clean_desc
    ep.tags               = tags
    ep.chapters_public    = chapters_raw
    ep.audio_url_public   = audio_public
    ep.audio_url_subscriber = audio_subscriber if audio_subscriber and audio_subscriber != audio_public else None
    ep.duration           = duration
    ep.season_number      = season_number
    ep.episode_number     = episode_number
    ep.episode_type       = episode_type
    ep.explicit           = explicit

    def _sync_cross(saved_ep):
        targets = validate_cross_targets(saved_ep, request.POST.getlist('cross_publish_ids'), current_network)
        added, removed = sync_cross_publications(saved_ep, targets, added_by=request.user)
        if added or removed:
            logger.info(
                f"[publish] Episode {saved_ep.id} '{saved_ep.title}' cross-publish targets "
                f"synced by {request.user.username}: +{added} -{removed}"
            )
        # Published episodes never pass through commit_episode, so the
        # feed-level auto cross-publish hook fires here instead.
        apply_auto_cross_publish(saved_ep)

    def _attach_audio():
        """Mirror the uploaded file, if any, AFTER the episode is saved (the
        mirror keys on ep.id). Deliberately non-fatal: the episode is already
        saved in whatever state the action chose, so a mirror failure leaves it
        as-is and messages the error rather than unwinding a publish. The user
        can retry the upload from the edit page. Was pre-validated above."""
        if audio_file:
            _ingest_episode_audio(request, ep, audio_file)

    if action == 'schedule':
        scheduled_str = request.POST.get('scheduled_at', '').strip()
        scheduled_dt  = parse_datetime(scheduled_str)
        if not scheduled_dt:
            messages.error(request, "Invalid schedule date/time.")
            return redirect(f"{reverse('publish_episode')}?network={current_network.slug}")
        # Make timezone-aware if naive
        if timezone.is_naive(scheduled_dt):
            scheduled_dt = scheduled_dt.replace(tzinfo=datetime.timezone.utc)
        ep.is_published = False
        ep.scheduled_at = scheduled_dt
        ep.pub_date     = scheduled_dt
        ep.save()
        _sync_cross(ep)
        _attach_audio()
        entry = ensure_calendar_entry_for_episode(ep, calendar_entry_id=request.POST.get('calendar_entry_id'))
        _apply_calendar_visibility(entry, request)
        logger.info(f"[publish] Episode {ep.id} '{ep.title}' scheduled for {scheduled_dt.isoformat()} by {request.user.username}")
        messages.success(request, f'"{ep.title}" scheduled for {scheduled_dt.strftime("%b %d, %Y %H:%M")}.')
        return redirect(f"{reverse('publish_episode')}?network={current_network.slug}&tab=scheduled")

    elif action == 'draft':
        ep.is_published = False
        ep.scheduled_at = None
        if not ep.pub_date:
            ep.pub_date = timezone.now()
        ep.save()
        _sync_cross(ep)
        _attach_audio()
        logger.info(f"[publish] Episode {ep.id} '{ep.title}' saved as draft by {request.user.username}")
        messages.success(request, f'"{ep.title}" saved as draft.')
        return redirect(f"{reverse('publish_episode')}?network={current_network.slug}&tab=scheduled")

    else:  # publish immediately
        ep.is_published = True
        ep.scheduled_at = None
        ep.pub_date     = timezone.now()
        ep.save()
        _sync_cross(ep)
        _attach_audio()
        ensure_calendar_entry_for_episode(ep, calendar_entry_id=request.POST.get('calendar_entry_id'))
        logger.info(f"[publish] Episode {ep.id} '{ep.title}' published to '{ep.podcast.title}' by {request.user.username}")
        base_url = request.build_absolute_uri('/')
        task_rebuild_episode_fragments.delay(ep.id, base_url)
        task_refresh_live_schedules.delay(ep.podcast.network_id)
        messages.success(request, f'"{ep.title}" published.')
        return redirect(f"{reverse('publish_episode')}?network={current_network.slug}")


@login_required(login_url='/login/')
@require_POST
def manage_episode(request, episode_id):
    """Owner direct-edit for an existing (any) episode: season/ep metadata,
    unpublish, or move to scheduled."""
    ep = get_object_or_404(Episode.objects.select_related('podcast__network'), pk=episode_id)
    if not _require_owner(request.user, ep.podcast.network):
        return HttpResponseForbidden()

    action = request.POST.get('action')

    if action == 'update_audio':
        from django.core.cache import cache
        audio_public = request.POST.get('audio_url_public', '').strip() or None
        audio_subscriber = request.POST.get('audio_url_subscriber', '').strip() or None
        ep.audio_url_public = audio_public
        ep.audio_url_subscriber = (
            audio_subscriber if audio_subscriber and audio_subscriber != audio_public else None
        )
        ep.save(update_fields=['audio_url_public', 'audio_url_subscriber'])
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        base_url = request.build_absolute_uri('/')
        task_rebuild_episode_fragments.delay(ep.id, base_url)
        messages.success(request, "Audio URLs updated.")

    elif action == 'update_meta':
        try:
            ep.season_number  = int(request.POST.get('season_number') or 0) or None
        except ValueError:
            ep.season_number  = None
        try:
            ep.episode_number = int(request.POST.get('episode_number') or 0) or None
        except ValueError:
            ep.episode_number = None
        ep.episode_type = request.POST.get('episode_type', '').strip()[:50]
        ep.save(update_fields=['season_number', 'episode_number', 'episode_type'])
        # Invalidate cached fragments so the feed picks up new iTunes tags
        from django.core.cache import cache
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        messages.success(request, "Episode metadata updated.")

    elif action == 'update_explicit':
        ep.explicit = _parse_explicit(request.POST.get('explicit'))
        ep.save(update_fields=['explicit'])
        from django.core.cache import cache
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        messages.success(request, "Content rating updated.")

    elif action == 'update_cross_publish':
        targets = validate_cross_targets(
            ep, request.POST.getlist('cross_publish_ids'), ep.podcast.network
        )
        modes = {}
        for pod in targets:
            mode = request.POST.get(f'access_mode_{pod.id}', '')
            if mode in EpisodeCrossPublication.AccessMode:
                modes[pod.id] = mode
        added, removed = sync_cross_publications(
            ep, targets, added_by=request.user, modes=modes
        )
        logger.info(
            f"[manage] Episode {ep.id} '{ep.title}' cross-publish updated by {request.user.username}: "
            f"+{added} -{removed} modes={modes}"
        )
        # Feed membership is resolved at request time from the DB, so no
        # fragment invalidation is needed here.
        messages.success(
            request,
            f"Cross-publish targets updated ({len(added)} added, {len(removed)} removed)."
        )

    elif action == 'publish_now':
        ep.is_published = True
        ep.scheduled_at = None
        ep.save(update_fields=['is_published', 'scheduled_at'])
        ensure_calendar_entry_for_episode(ep, calendar_entry_id=request.POST.get('calendar_entry_id'))
        from django.core.cache import cache
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        base_url = request.build_absolute_uri('/')
        task_rebuild_episode_fragments.delay(ep.id, base_url)
        task_refresh_live_schedules.delay(ep.podcast.network_id)
        messages.success(request, f'"{ep.title}" published.')

    elif action == 'unpublish':
        ep.is_published = False
        ep.scheduled_at = None
        ep.save(update_fields=['is_published', 'scheduled_at'])
        from django.core.cache import cache
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        messages.success(request, f'"{ep.title}" unpublished.')

    elif action == 'upload_audio':
        # The episode-edit page's standalone upload. The publish page folds the
        # same ingest into its main submit; both go through _ingest_episode_audio.
        audio_file = request.FILES.get('audio_file')
        if not audio_file:
            messages.error(request, "No audio file selected.")
        else:
            err = _audio_upload_error(audio_file)
            if err:
                messages.error(request, err)
            else:
                _ingest_episode_audio(request, ep, audio_file)

    elif action == 'schedule':
        scheduled_str = request.POST.get('scheduled_at', '').strip()
        scheduled_dt  = parse_datetime(scheduled_str)
        if not scheduled_dt:
            messages.error(request, "Invalid schedule date/time.")
        else:
            if timezone.is_naive(scheduled_dt):
                scheduled_dt = timezone.make_aware(scheduled_dt)
            ep.is_published = False
            ep.scheduled_at = scheduled_dt
            ep.pub_date     = scheduled_dt
            ep.save(update_fields=['is_published', 'scheduled_at', 'pub_date'])
            entry = ensure_calendar_entry_for_episode(ep, calendar_entry_id=request.POST.get('calendar_entry_id'))
            _apply_calendar_visibility(entry, request)
            from django.core.cache import cache
            cache.delete(f"ep_frag_public_{ep.id}")
            cache.delete(f"ep_frag_private_{ep.id}")
            messages.success(request, f'"{ep.title}" moved to scheduled.')

    elif action == 'move_episode':
        target_id = request.POST.get('target_podcast_id')
        target_pod = get_object_or_404(Podcast, pk=target_id, network=ep.podcast.network)
        if target_pod.id == ep.podcast_id:
            messages.info(request, "Episode is already in that feed.")
        else:
            from ...services.episode_move import move_episodes
            move_episodes([ep.id], target_pod, base_url=request.build_absolute_uri('/'), moved_by=request.user)
            messages.success(request, f'"{ep.title}" moved to "{target_pod.title}".')

    elif action == 'link_calendar':
        # Adopt a planned, still-unlinked entry from this network into this
        # episode (the manual fallback for a missed auto-match). Network-scoped:
        # the id arrives from request.POST.
        from ...models import CalendarEntry
        entry = get_object_or_404(
            CalendarEntry, pk=request.POST.get('calendar_entry_id'),
            network=ep.podcast.network, episode__isnull=True)
        if getattr(ep, 'calendar_entry', None) is not None:
            messages.error(request, "This episode is already on the calendar.")
        else:
            link_episode_to_entry(ep, entry)
            messages.success(request, f'Linked to calendar entry "{entry.title}".')

    elif action == 'unlink_calendar':
        entry = getattr(ep, 'calendar_entry', None)
        if entry is not None:
            entry.episode = None
            entry.save(update_fields=['episode', 'updated_at'])
            messages.success(request, "Removed from the calendar entry (the entry was kept).")
        else:
            messages.info(request, "This episode isn't linked to a calendar entry.")

    elif action == 'create_calendar_entry':
        entry = create_calendar_entry_from_episode(ep)
        messages.success(request, f'Added "{entry.title}" to the release calendar.')

    elif action == 'set_calendar_visibility':
        entry = getattr(ep, 'calendar_entry', None)
        if entry is None:
            messages.error(request, "This episode isn't on the calendar yet.")
        else:
            set_prepublish_visibility(
                entry, request.POST.get('prepublish_visibility', ''),
                request.POST.get('placeholder_title', ''), request.POST.get('placeholder_notes', ''))
            entry.save(update_fields=[
                'prepublish_visibility', 'placeholder_title', 'placeholder_notes', 'updated_at'])
            messages.success(request, "Calendar visibility updated.")

    return redirect(request.META.get('HTTP_REFERER', reverse('episode_detail', args=[ep.id])))
