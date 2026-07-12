import hashlib
import logging
from pathlib import Path

from django.conf import settings
from django.http import (Http404, HttpResponse, HttpResponseNotModified,
                         HttpResponseRedirect)

from pod_manager.services.transcription import (ALLOWED_EXTENSIONS,
                                                CONTENT_TYPES,
                                                source_audio_filename,
                                                transcript_path,
                                                transcript_r2_key)

logger = logging.getLogger(__name__)


def _resolve_transcript_access(request, episode) -> bool:
    """Resolve premium access to `episode`'s transcript for the two audiences that
    reach this endpoint — mirroring play_episode / episode_detail so the gate can't
    diverge:

      a) Site visitors (session): owner/superuser, or _evaluate_access against the
         episode's PARENT podcast (network owners covered there; superusers are not,
         so they're added explicitly the way episode_detail's is_owner does).
      b) Podcast apps (?auth=<feed_token>): PatronProfile lookup, _evaluate_access
         against the PARENT podcast, then the cross-publication TARGET override loop
         verbatim from play_episode — a subscriber to a TARGET-mode target gets in.

    This only resolves PREMIUM access; the public-flag decision is layered on top by
    can_view_transcript at the call site.
    """
    from pod_manager.services.access import (_evaluate_access,
                                              patron_profile_for_token)

    podcast = episode.podcast

    user = request.user
    if user.is_authenticated:
        if user.is_superuser or podcast.network.owners.filter(pk=user.pk).exists():
            return True
        if _evaluate_access(user, podcast, podcast.network)[0]:
            return True

    feed_token = request.GET.get('auth')
    if feed_token:
        from pod_manager.models import EpisodeCrossPublication
        profile = patron_profile_for_token(feed_token)
        if profile:
            if _evaluate_access(profile.user, podcast, podcast.network)[0]:
                return True
            for cp in episode.cross_publications.filter(
                access_mode=EpisodeCrossPublication.AccessMode.TARGET
            ).select_related('podcast', 'podcast__network'):
                if _evaluate_access(profile.user, cp.podcast, cp.podcast.network)[0]:
                    return True

    return False


def _download_filename(episode_id, ext):
    """``<audio-stem>.<ext>`` so a transcript download matches the MP3's name."""
    try:
        from pod_manager.models import Episode
        episode = (
            Episode.objects
            .filter(pk=episode_id)
            .only('pk', 'audio_url_subscriber', 'audio_url_public')
            .first()
        )
        if episode:
            stem = Path(source_audio_filename(episode)).stem
            if stem:
                return f"{stem}.{ext}"
    except Exception:
        pass
    return None


def serve_transcript(request, episode_id, ext: str):
    """Serve a transcript format — the single chokepoint for feed/inline/download.

    Routes:
      ``/transcripts/<id>.<ext>``            -> 302 to the cdn object (``?v=N``);
                                                the bytes come from the edge, only
                                                the redirect touches Django.
      ``/transcripts/<id>.<ext>?download=1`` -> stream the bytes with a
                                                ``Content-Disposition: attachment``
                                                filename. Stays same-origin so the
                                                ``<a download>`` rename works (a
                                                cross-origin 302 would drop it).

    R2-backed transcripts (``version >= 1`` and R2 enabled) 302/stream from the
    cdn. Legacy local transcripts (version 0 / R2 disabled) are served from disk
    with the existing ETag-revalidate behavior.
    """
    from pod_manager.models import Episode
    from pod_manager.services.access import can_view_transcript

    if ext not in ALLOWED_EXTENSIONS:
        raise Http404

    # One round trip for episode + podcast + network + transcript (OneToOne).
    # The transcript's fat columns are deferred: transcript_text is the whole
    # episode as plain text (full-text-search fodder, easily 100KB+) and
    # error_message can hold tracebacks — far too heavy to drag through every
    # 302 on an endpoint podcast apps hammer. Episode/podcast/network stay
    # fully loaded: the access predicates touch enough of their fields that
    # trimming them risks silent per-request refetch queries.
    episode = (
        Episode.objects
        .select_related('podcast', 'podcast__network', 'transcript')
        .defer('transcript__transcript_text', 'transcript__error_message',
               'transcript__source_audio_url')
        .filter(pk=episode_id)
        .first()
    )
    if episode is None:
        raise Http404

    transcript = getattr(episode, 'transcript', None)
    if transcript is None:
        raise Http404
    field = 'words_json_file' if ext == 'words' else f'{ext}_file'
    if not getattr(transcript, field, None):
        raise Http404

    # Access gate — closes both exposure layers: this Django endpoint AND (via the
    # keyed CDN object) the bytes. 404 (not 403) matches the file's existing pattern
    # and hides existence from probers. Applies uniformly to all five exts (words
    # carries the full text too) and to the ?download=1 branch and legacy local path
    # below, since it runs before either.
    if not can_view_transcript(episode, _resolve_transcript_access(request, episode)):
        logger.info(
            "serve_transcript: denied episode=%s ext=%s (no premium access, "
            "allow_public_transcripts off)", episode_id, ext,
        )
        raise Http404

    version = transcript.version or 0
    download = request.GET.get('download')

    # --- R2-backed: 302 to the cdn (default) or stream with attachment (download) ---
    if settings.R2_MEDIA_ENABLED and version >= 1:
        from botocore.exceptions import ClientError
        from pod_manager.services.r2_storage import (get_media_object,
                                                     media_public_url)
        key = transcript_r2_key(int(episode_id), ext, transcript.r2_key_token)
        if download:
            try:
                data, ctype = get_media_object(key)
            except ClientError:
                raise Http404
            resp = HttpResponse(data, content_type=ctype or CONTENT_TYPES[ext])
            fn = _download_filename(episode_id, ext)
            if fn:
                resp['Content-Disposition'] = f'attachment; filename="{fn}"'
            resp['Access-Control-Allow-Origin'] = '*'
            return resp
        # Default: 302 to the immutable cdn object, version-busted.
        resp = HttpResponseRedirect(f"{media_public_url(key)}?v={version}")
        resp['Access-Control-Allow-Origin'] = '*'
        return resp

    # --- Legacy local: serve from disk, ETag so the browser revalidates ---
    try:
        path = transcript_path(int(episode_id), ext)
    except (ValueError, TypeError):
        raise Http404
    if not path.exists():
        raise Http404

    data = path.read_bytes()
    etag = f'"{hashlib.md5(data).hexdigest()}"'
    if request.META.get('HTTP_IF_NONE_MATCH') == etag:
        return HttpResponseNotModified()

    resp = HttpResponse(data, content_type=CONTENT_TYPES[ext])
    resp['ETag'] = etag
    resp['Cache-Control'] = 'public, no-cache'
    resp['Access-Control-Allow-Origin'] = '*'
    fn = _download_filename(episode_id, ext)
    if fn:
        resp['Content-Disposition'] = f'{"attachment" if download else "inline"}; filename="{fn}"'
    return resp
