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
    from pod_manager.models import Transcript

    if ext not in ALLOWED_EXTENSIONS:
        raise Http404

    transcript = (
        Transcript.objects
        .filter(episode_id=episode_id)
        .only('episode_id', 'version', 'vtt_file', 'json_file', 'srt_file',
              'html_file', 'words_json_file')
        .first()
    )
    if transcript is None:
        raise Http404
    field = 'words_json_file' if ext == 'words' else f'{ext}_file'
    if not getattr(transcript, field, None):
        raise Http404

    version = transcript.version or 0
    download = request.GET.get('download')

    # --- R2-backed: 302 to the cdn (default) or stream with attachment (download) ---
    if settings.R2_MEDIA_ENABLED and version >= 1:
        from botocore.exceptions import ClientError
        from pod_manager.services.r2_storage import (get_media_object,
                                                     media_public_url)
        key = transcript_r2_key(int(episode_id), ext)
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
