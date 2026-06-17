import hashlib
import logging
from pathlib import Path

from django.http import Http404, HttpResponse, HttpResponseNotModified

from pod_manager.services.transcription import CONTENT_TYPES, transcript_path

logger = logging.getLogger(__name__)


def serve_transcript(request, episode_id, ext: str):
    """Serve a transcript file with ETag caching.

    Route: /transcripts/<episode_id>.<ext>
    Transcript files are immutable once written, so we set a 1-year max-age.
    """
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
    resp['Cache-Control'] = 'public, max-age=31536000, immutable'
    resp['Access-Control-Allow-Origin'] = '*'

    try:
        from pod_manager.models import Episode
        from pod_manager.services.transcription import source_audio_filename
        episode = (
            Episode.objects
            .filter(pk=episode_id)
            .only('pk', 'audio_url_subscriber', 'audio_url_public')
            .first()
        )
        if episode:
            stem = Path(source_audio_filename(episode)).stem
            if stem:
                resp['Content-Disposition'] = f'inline; filename="{stem}.{ext}"'
    except Exception:
        pass

    return resp
