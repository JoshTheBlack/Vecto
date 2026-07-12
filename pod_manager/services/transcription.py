import hashlib
import json
import logging
import os
import shutil
import socket
import tempfile
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from django.conf import settings
from django.utils import timezone

from pod_manager.services import gdrive_download
from pod_manager.services.audio_sniff import is_audio_file

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'vtt', 'json', 'srt', 'html', 'words'}

# HTTP statuses that mean the source audio is gone for good, not a blip.
_PERMANENT_SOURCE_STATUSES = {401, 403, 404, 410}

# Non-audio-source guard (Google Drive et al. hand back an HTML interstitial
# instead of the MP3). A candidate is accepted only when its header positively
# matches an audio container (see services/audio_sniff.is_audio_file).
_MAX_AUDIO_DOWNLOAD_ATTEMPTS = 5
# Short pause between redownload attempts — a Drive quota interstitial won't
# clear instantly, and we don't want to hammer the source.
_HTML_RETRY_BACKOFF_SECONDS = 3


class PermanentSourceError(Exception):
    """Raised when source audio is unrecoverably unavailable (dead bucket,
    404/403, host no longer resolves). The transcript parks in
    AWAITING_RECOVERY and is NOT retried — it requeues only when the
    episode's audio URL changes."""


class HtmlSourceError(Exception):
    """Raised when the source keeps returning an HTML page (e.g. a Google Drive
    download interstitial or quota wall) instead of audio, after exhausting all
    download attempts. The transcript fails WITHOUT ever being sent to the ASR
    endpoint, so we never waste a GPU run on a web page."""


def _error_detail(exc: Exception) -> str:
    """Build a human-useful error string for Transcript.error_message.

    The whisperx ASR service raises FastAPI HTTPExceptions, so a failed /asr
    call comes back as an HTTP error whose response body is JSON
    ``{"detail": "<the real engine error>"}`` (OOM, diarization failure, bad
    audio, unsupported format, 413 too-large, …). requests attaches that
    response to the raised HTTPError, so we surface the engine's own message
    instead of the opaque "500 Server Error for url: …".
    """
    resp = getattr(exc, 'response', None)
    if resp is not None:
        detail = None
        try:
            detail = resp.json().get('detail')
        except (ValueError, AttributeError, TypeError):
            detail = (getattr(resp, 'text', '') or '').strip()[:1000]
        status = getattr(resp, 'status_code', '?')
        if detail:
            return f"HTTP {status}: {detail}"
        return f"HTTP {status}: {exc}"
    return str(exc)


def _chmod_777(path: Path) -> None:
    """Make a persisted source MP3 world-rwx (777). These files hold no secrets,
    and loosening them now keeps them accessible once the worker stops running
    as root. Best-effort: a no-op / partial on Windows, never fatal."""
    try:
        os.chmod(path, 0o777)
    except OSError as exc:
        logger.debug("chmod 777 failed for %s — %s", path, exc)


def _is_permanent_source_error(exc: Exception) -> bool:
    resp = getattr(exc, 'response', None)
    if resp is not None and resp.status_code in _PERMANENT_SOURCE_STATUSES:
        return True
    # A deleted bucket's host often stops resolving / refuses connections.
    # Timeouts are a subclass we deliberately exclude — those are transient.
    return (isinstance(exc, requests.exceptions.ConnectionError)
            and not isinstance(exc, requests.exceptions.Timeout))

CONTENT_TYPES = {
    'vtt':   'text/vtt',
    'json':  'application/json',
    'srt':   'application/x-subrip',
    'html':  'text/html; charset=utf-8',
    'words': 'application/json',
}


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def transcript_path(episode_id: int, ext: str) -> Path:
    """Absolute filesystem path for a transcript file.

    Files are stored at:
        MEDIA_ROOT/transcriptions/{episode_id // 1000}/{episode_id}.{ext}

    Raises ValueError for disallowed extensions or path traversal attempts.
    """
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(
            f"Extension '{ext}' not allowed. Must be one of: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )

    root = Path(settings.MEDIA_ROOT) / 'transcriptions'
    bucket = episode_id // 1000
    path = root / str(bucket) / f'{episode_id}.{ext}'

    if not path.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"Resolved path escapes transcriptions root: {path.resolve()}")

    return path


def transcript_r2_key(episode_id: int, ext: str, token: str | None = None) -> str:
    """Cdn key for a transcript format. The single derivation chokepoint — every
    read/write/serve path routes through here so the two key shapes stay in sync:

        token is None -> transcripts/{id // 1000}/{id}.{ext}          (legacy)
        token set     -> transcripts/{id // 1000}/{id}.{token}.{ext}  (keyed)

    The {id} prefix stays for bucket navigability/recovery; secrecy lives entirely
    in the ~128-bit token, which makes the object key non-derivable from the (small,
    enumerable) episode id. The serve view recomputes the target from the key +
    Transcript.version; the token is stored on the row, never in a feed URL.

    The // 1000 bucket folder mirrors the local layout and keeps each prefix to
    ~1000 episodes so the bucket stays navigable for recovery. Overwritten in place
    on re-transcribe (transcripts are tiny single GETs, no in-flight-stream concern).
    """
    if ext not in ALLOWED_EXTENSIONS:
        raise ValueError(
            f"Extension '{ext}' not allowed. Must be one of: {', '.join(sorted(ALLOWED_EXTENSIONS))}"
        )
    stem = f"{episode_id}.{token}" if token else str(episode_id)
    return f"transcripts/{episode_id // 1000}/{stem}.{ext}"


def episode_recovery_metadata(episode) -> dict:
    """Fields embedded in the .words header so a transcript can be re-matched to
    its episode after a DB rebuild — episode IDs change on re-import, but title /
    GUIDs / the original subscriber URL survive. Written at upload time so no
    second PUT is ever needed to add them.
    """
    return {
        'title':        episode.title,
        'guid_public':  episode.guid_public,
        'guid_private': episode.guid_private,
        'audio_url':    episode.audio_url_subscriber,
    }


def write_transcript_file(episode_id: int, ext: str, content: bytes, token: str | None = None) -> str:
    """Write one transcript format and return its Transcript.*_file marker.

    R2 (vecto-cdn) when R2_MEDIA_ENABLED, else the legacy MEDIA_ROOT path (dev /
    pre-cutover). Callers bump Transcript.version after writing all formats.
    ``token`` (the row's r2_key_token) selects the keyed vs legacy object key.
    """
    if settings.R2_MEDIA_ENABLED:
        from pod_manager.services.r2_storage import put_media_object
        key = transcript_r2_key(episode_id, ext, token)
        put_media_object(key, content, CONTENT_TYPES[ext])
        return key
    p = transcript_path(episode_id, ext)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return str(p.relative_to(settings.MEDIA_ROOT))


def _is_plain_md5(etag: str) -> bool:
    """True if an R2 ETag is a bare 32-char MD5 hex (a single, non-multipart
    PUT) rather than a multipart marker like ``<md5>-<part-count>``."""
    return len(etag) == 32 and all(c in '0123456789abcdef' for c in etag.lower())


def _r2_format_matches(key: str, new_bytes: bytes) -> bool:
    """True if the R2 object at `key` already holds exactly `new_bytes`, so the
    PUT can be skipped (§4). Compares md5(new_bytes) to the object's ETag (a
    cheap Class B HEAD). For a single-PUT object the ETag IS the body's md5 hex,
    so the compare is exact; if it isn't a plain md5 (multipart), fall back to a
    GET (edge-cached, Class B) and hash the body. A missing object → no match
    (must PUT)."""
    from pod_manager.services.r2_storage import get_media_object, media_object_etag

    etag = media_object_etag(key)
    if etag is None:
        return False
    if _is_plain_md5(etag):
        return etag.lower() == hashlib.md5(new_bytes).hexdigest()
    # Multipart / non-md5 ETag — compare the actual bytes.
    try:
        existing, _ = get_media_object(key)
    except Exception:
        return False
    return existing == new_bytes


def write_transcript_formats(
    episode_id: int, rendered: list[tuple[str, bytes]], token: str | None = None,
) -> tuple[dict[str, str], list[str]]:
    """Idempotently write rendered transcript formats; return ({ext: marker},
    changed_exts).

    On R2 (R2_MEDIA_ENABLED) each format is hash-checked against the live object
    and only changed formats are PUT (§4) — a HEAD is Class B (cheap), a PUT is
    Class A, so unchanged formats cost only a HEAD. Locally (R2 disabled) every
    format is written to MEDIA_ROOT (disk writes are free) and reported changed.

    The marker (R2 key or MEDIA_ROOT-relative path) is deterministic from id+ext,
    so it is returned for every format whether or not its bytes changed; callers
    persist the markers and bump Transcript.version only when changed_exts is
    non-empty — and only after this call returns (all writes succeeded), so a
    mid-write failure can't leave a version-advanced, partially-updated set.
    """
    markers: dict[str, str] = {}
    changed: list[str] = []
    if settings.R2_MEDIA_ENABLED:
        from pod_manager.services.r2_storage import put_media_object
        for ext, content in rendered:
            key = transcript_r2_key(episode_id, ext, token)
            markers[ext] = key
            if _r2_format_matches(key, content):
                continue
            put_media_object(key, content, CONTENT_TYPES[ext])
            changed.append(ext)
    else:
        for ext, content in rendered:
            markers[ext] = write_transcript_file(episode_id, ext, content, token)
            changed.append(ext)
    return markers, changed


def read_transcript_bytes(episode_id: int, ext: str, version: int, token: str | None = None) -> bytes:
    """Read one transcript format's bytes.

    Reads from R2 when the transcript is R2-backed (version >= 1 and R2 enabled),
    else from the legacy local file (version 0 / R2 disabled). Raises on miss.
    ``token`` (the row's r2_key_token) selects the keyed vs legacy object key.
    """
    if settings.R2_MEDIA_ENABLED and (version or 0) >= 1:
        from pod_manager.services.r2_storage import get_media_object
        data, _ = get_media_object(transcript_r2_key(episode_id, ext, token))
        return data
    return transcript_path(episode_id, ext).read_bytes()


def source_audio_filename(episode) -> str:
    """Human-readable download filename for an episode's audio.

    Derived from the subscriber URL, falling back to the public URL when the
    subscriber URL has no file extension (e.g. Google Drive /uc?export=download
    links). This is the same name a browser keeps when downloading the MP3, so
    transcript downloads can be named to match instead of ending up as "uc".
    """
    def _name_from_url(url):
        name = Path(unquote(urlparse(url).path)).name
        return name if name and '.' in name else None

    return (
        _name_from_url(episode.audio_url_subscriber)
        or _name_from_url(episode.audio_url_public or '')
        or f"episode_{episode.pk}.mp3"
    )


def _hash_file(path: Path) -> str:
    """SHA-256 hexdigest of a file, streamed. Matches r2_mirror's hashing so a
    locally-computed name lands on the same {stem}-{shorthash} as the R2 key."""
    h = hashlib.sha256()
    with path.open('rb') as fp:
        for chunk in iter(lambda: fp.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _source_audio_root() -> Path:
    return Path(settings.MEDIA_ROOT) / 'source_audio'


def _strip_key_prefix(key: str) -> str:
    """Drop the R2 bucket key-prefix (e.g. 'dev/') so the local mirror tree is
    laid out identically under dev and prod."""
    prefix = settings.R2_KEY_PREFIX or ''
    if prefix and key.startswith(prefix):
        return key[len(prefix):]
    return key


def source_audio_path_for_key(key: str) -> Path:
    """Local path that mirrors a (possibly prefixed) R2 object key:
    MEDIA_ROOT/source_audio/{network_id}/{podcast_id}/{stem}-{shorthash}.{ext}.
    The single place that maps an R2 key to its on-disk twin (used by the
    rekey-on-move relocation and the backfill rename command)."""
    return _source_audio_root() / _strip_key_prefix(key)


def source_audio_path(episode, content_hash: str | None = None) -> Path | None:
    """Persistent save path for a subscriber MP3 when WHISPER_KEEP_SOURCE_AUDIO=True.

    The local copy mirrors the R2 object key EXACTLY — same id-based folders and
    same ``{stem}-{shorthash}.{ext}`` filename — so it stays in lockstep with R2
    and a feed move re-keys both together. The content hash is taken from
    ``episode.r2_url`` when the episode is already mirrored (exact parity, nothing
    to recompute); otherwise from a freshly computed ``content_hash``.

    Returns None when neither is available — the canonical name isn't yet knowable
    (e.g. the very first transcription, before the inline mirror has run).
    """
    if episode.r2_url:
        from pod_manager.services.r2_client import key_from_public_url
        return source_audio_path_for_key(key_from_public_url(episode.r2_url))
    if content_hash:
        from pod_manager.services.r2_mirror import object_key_bare
        return _source_audio_root() / object_key_bare(episode, content_hash)
    return None


def ensure_source_audio(episode_id: int) -> bool:
    """Download subscriber audio for an episode if not already on disk.

    No-ops when WHISPER_KEEP_SOURCE_AUDIO is False, the episode has no audio,
    or the file already exists. Returns True if a download was performed.
    """
    from pod_manager.models import Episode

    if not settings.WHISPER_KEEP_SOURCE_AUDIO:
        return False

    try:
        episode = Episode.objects.select_related('podcast__network').get(id=episode_id)
    except Episode.DoesNotExist:
        logger.warning("ensure_source_audio: episode %d not found", episode_id)
        return False

    if not episode.audio_url_subscriber:
        return False

    # Canonical (R2-mirroring) destination. None until we know the content hash —
    # i.e. when the episode hasn't been mirrored yet — in which case we hash the
    # download as it streams and name the file the same way R2 would.
    dest = source_audio_path(episode)
    if dest is not None and dest.exists():
        return False
    need_hash = dest is None

    logger.info("ensure_source_audio: downloading audio for episode %d", episode_id)
    dl = requests.get(episode.audio_url_subscriber, stream=True, timeout=300)
    dl.raise_for_status()

    root = _source_audio_root()
    root.mkdir(parents=True, exist_ok=True)
    h = hashlib.sha256() if need_hash else None
    with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False, dir=root) as tmp:
        tmp_path = Path(tmp.name)
        for chunk in dl.iter_content(chunk_size=65536):
            tmp.write(chunk)
            if h is not None:
                h.update(chunk)

    if need_hash:
        dest = source_audio_path(episode, content_hash=h.hexdigest())
    if dest.exists():
        tmp_path.unlink(missing_ok=True)
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp_path.rename(dest)
    _chmod_777(dest)

    logger.info("ensure_source_audio: saved episode %d at %s", episode_id, dest)
    return True


def _download_audio_to_temp(url: str) -> Path:
    """Stream a URL to a temp .mp3 file and return its path. Caller owns the
    file and must unlink it. Translates dead-source HTTP errors into
    PermanentSourceError so the caller can park the transcript without retrying.
    """
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
            tmp_path = Path(tmp.name)
            session = requests.Session()
            session.headers['User-Agent'] = gdrive_download.USER_AGENT
            dl = session.get(url, stream=True, timeout=300)
            dl.raise_for_status()
            # Click through GDrive's "couldn't scan for viruses" interstitial (a
            # 200 HTML page) so large recovered files download as real audio.
            if gdrive_download.is_gdrive_url(url) and gdrive_download.looks_like_interstitial(dl):
                dl = gdrive_download.follow_confirmation(session, url, dl, 300)
                dl.raise_for_status()
            for chunk in dl.iter_content(chunk_size=65536):
                tmp.write(chunk)
        return tmp_path
    except requests.exceptions.RequestException as exc:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
        if _is_permanent_source_error(exc):
            status = getattr(getattr(exc, 'response', None), 'status_code', 'no response')
            raise PermanentSourceError(
                f"source audio unavailable ({status}) at {url}"
            ) from exc
        raise


# ---------------------------------------------------------------------------
# Format converters
# ---------------------------------------------------------------------------

def _vtt_timestamp(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


def _srt_timestamp(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    ms = int(round((seconds % 1) * 1000))
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def _to_vtt(segments: list) -> bytes:
    lines = ["WEBVTT", ""]
    for seg in segments:
        speaker = seg.get("speaker")
        text = seg["text"].strip()
        if speaker:
            text = f"<v {speaker}>{text}</v>"
        lines += [
            f"{_vtt_timestamp(seg['start'])} --> {_vtt_timestamp(seg['end'])}",
            text,
            "",
        ]
    return "\n".join(lines).encode("utf-8")


def _to_srt(segments: list) -> bytes:
    lines = []
    for i, seg in enumerate(segments, 1):
        speaker = seg.get("speaker")
        text = seg["text"].strip()
        if speaker:
            text = f"[{speaker}]: {text}"
        lines += [
            str(i),
            f"{_srt_timestamp(seg['start'])} --> {_srt_timestamp(seg['end'])}",
            text,
            "",
        ]
    return "\n".join(lines).encode("utf-8")


def _to_html(segments: list) -> bytes:
    parts = ['<article class="transcript">']
    for seg in segments:
        speaker = seg.get("speaker")
        attrs = f'data-start="{seg["start"]}" data-end="{seg["end"]}"'
        if speaker:
            attrs += f' data-speaker="{speaker}"'
        parts.append(f'<p {attrs}>{seg["text"].strip()}</p>')
    parts.append("</article>")
    return "\n".join(parts).encode("utf-8")


def _to_podcast_index_json(segments: list) -> bytes:
    out_segments = []
    for seg in segments:
        entry = {
            "startTime": seg["start"],
            "endTime":   seg["end"],
            "body":      seg["text"].strip(),
        }
        if seg.get("speaker"):
            entry["speaker"] = seg["speaker"]
        out_segments.append(entry)
    return json.dumps({"version": "1.0.0", "segments": out_segments}, ensure_ascii=False, indent=2).encode("utf-8")


def _to_words_json(segments: list, *, metadata: dict | None = None) -> bytes:
    out_segments = []
    for seg in segments:
        entry = {
            "startTime": seg["start"],
            "endTime":   seg["end"],
            "body":      seg["text"].strip(),
        }
        # speaker_id is the immutable diarization anchor (written once at parse
        # time); speaker is the current resolved name. .words carries BOTH — the
        # feed formats (vtt/srt/json/html) keep emitting `speaker` only, so
        # speaker_id never reaches an external client. (schema >= 1.1.0)
        if seg.get("speaker_id"):
            entry["speaker_id"] = seg["speaker_id"]
        if seg.get("speaker"):
            entry["speaker"] = seg["speaker"]
        if seg.get("words"):
            entry["words"] = [
                {k: v for k, v in {
                    "word":       w.get("word"),
                    "startTime":  w.get("start"),
                    "endTime":    w.get("end"),
                    "score":      w.get("score"),
                    "speaker_id": w.get("speaker_id"),
                    "speaker":    w.get("speaker"),
                }.items() if v is not None}
                for w in seg["words"]
                if w.get("start") is not None and w.get("end") is not None
            ]
        out_segments.append(entry)
    doc = {"version": "1.1.0"}
    if metadata:
        doc.update(metadata)
    doc["segments"] = out_segments
    return json.dumps(doc, ensure_ascii=False, indent=2).encode("utf-8")


def _plain_text(segments: list) -> str:
    return " ".join(seg["text"].strip() for seg in segments)


# ---------------------------------------------------------------------------
# Response parser
# ---------------------------------------------------------------------------

def _parse_srt_timestamp(ts: str) -> float:
    """Convert SRT timestamp HH:MM:SS,mmm to seconds."""
    ts = ts.strip()
    hms, ms = ts.split(',')
    h, m, s = hms.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000


def _stamp_speaker_ids(segments: list) -> list:
    """Seed the write-once `speaker_id` from the raw diarization label carried in
    `speaker`, at segment and word level. Called at parse time so every freshly
    parsed transcript is born with its immutable SPEAKER_XX anchor — the base the
    speaker-label edit chain replays over (see user_edit_rollback.md §3).

    Idempotent: only sets `speaker_id` where it is not already present, so a
    re-parse never rewrites the anchor. Segments/words with no diarization label
    (e.g. the SRT fallback) get no speaker_id. Mutates and returns `segments`.
    """
    for seg in segments:
        if seg.get('speaker') is not None and seg.get('speaker_id') is None:
            seg['speaker_id'] = seg['speaker']
        for w in seg.get('words', None) or []:
            if w.get('speaker') is not None and w.get('speaker_id') is None:
                w['speaker_id'] = w['speaker']
    return segments


def _parse_whisper_response(text: str, fallback_language: str) -> tuple[list, str]:
    """Parse whisper ASR response text into (segments, language).

    Primary format: SRT (what this whisper build returns for output=srt).
    Fallback: single JSON object with a top-level "segments" key.

    Each parsed segment/word is stamped with a write-once `speaker_id` (the raw
    diarization label) — the immutable base for speaker-label replay (§3.5).

    SRT block structure:
        <index>
        HH:MM:SS,mmm --> HH:MM:SS,mmm
        line of text
        [optional second line of text]
        <blank line>
    """
    text = text.strip()

    # --- attempt 1: JSON object with top-level "segments" key (primary format) ---
    try:
        data = json.loads(text)
        return _stamp_speaker_ids(data.get('segments', [])), data.get('language', fallback_language)
    except json.JSONDecodeError:
        pass

    # --- attempt 2: SRT fallback (older whisper builds always return SRT) ---
    srt_segments = _parse_srt(text)
    if srt_segments:
        logger.debug("transcribe: parsed response as SRT (%d segments)", len(srt_segments))
        return _stamp_speaker_ids(srt_segments), fallback_language

    # --- attempt 3: strip a non-JSON prefix line and retry ---
    lines = text.splitlines()
    for skip in range(1, min(4, len(lines))):
        candidate = "\n".join(lines[skip:]).strip()
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            logger.debug("transcribe: parsed JSON after skipping %d line(s)", skip)
            return _stamp_speaker_ids(data.get('segments', [])), data.get('language', fallback_language)
        except json.JSONDecodeError:
            continue

    preview = repr(text[:500])
    raise ValueError(f"Unable to parse whisper ASR response. First 500 chars: {preview}")


def _parse_srt(text: str) -> list:
    """Parse an SRT document into a list of segment dicts with start/end/text keys."""
    segments = []
    # Split on blank lines to get blocks; each block is one subtitle entry
    blocks = [b.strip() for b in text.split('\n\n') if b.strip()]
    for block in blocks:
        lines = block.splitlines()
        # Find the timestamp line (contains ' --> ')
        ts_line_idx = next(
            (i for i, l in enumerate(lines) if ' --> ' in l),
            None,
        )
        if ts_line_idx is None:
            continue
        try:
            start_str, end_str = lines[ts_line_idx].split(' --> ', 1)
            start = _parse_srt_timestamp(start_str)
            end = _parse_srt_timestamp(end_str)
        except (ValueError, IndexError):
            continue
        # Text is everything after the timestamp line
        text_lines = lines[ts_line_idx + 1:]
        if not text_lines:
            continue
        segments.append({
            'start': start,
            'end': end,
            'text': ' '.join(text_lines),
        })
    return segments


# ---------------------------------------------------------------------------
# Core service
# ---------------------------------------------------------------------------

# Whisper models heavy enough to need the high-VRAM GPU. Their jobs route to the
# transcription_heavy queue so the box draining it (e.g. the 3060) takes them and
# the normal-only worker (e.g. the 1080) never tries to load them.
HEAVY_MODELS = frozenset({'large', 'large-v2', 'large-v3'})

# Priority within each queue (Redis: lower = served sooner). Heavy jobs sit in
# band 0-2, normal in 7-9, so a worker draining both queues finishes any heavy
# job before any normal one. Must line up with priority_steps in
# settings.CELERY_BROKER_TRANSPORT_OPTIONS. 3-6 are reserved for future bands.
_PRIORITY_BAND = {
    True:  {'high': 0, 'default': 1, 'low': 2},   # heavy queue
    False: {'high': 7, 'default': 8, 'low': 9},   # normal queue
}


def resolve_effective_model(episode, override: str | None = None) -> str:
    """The model used to pick the queue at dispatch time: per-call → podcast →
    network → WHISPER_DEFAULT_MODEL → WHISPER_MODEL. WHISPER_FORCE_MODEL is
    deliberately excluded — it's a per-worker run-time pin, not known here in the
    dispatching process, so it can't influence routing."""
    podcast = episode.podcast
    network = podcast.network
    return (override or podcast.whisper_model or network.whisper_model
            or (getattr(settings, 'WHISPER_DEFAULT_MODEL', '') or None)
            or settings.WHISPER_MODEL)


def route_transcription(episode, *, level: str = 'default', model: str | None = None) -> tuple[str, int]:
    """Return (queue, priority) for an episode's transcription. Large models go
    to the heavy queue/band; everything else to the normal queue/band. `level`
    is one of 'high' | 'default' | 'low'."""
    heavy = resolve_effective_model(episode, model) in HEAVY_MODELS
    band = _PRIORITY_BAND[heavy]
    queue = 'transcription_heavy' if heavy else 'transcription'
    return queue, band.get(level, band['default'])


def dispatch_transcription(episode_id: int, *, level: str = 'default', **task_kwargs) -> None:
    """Queue a transcription WITHOUT ever blocking the caller, routed to the
    correct queue + priority band for its effective model.

    With a real broker this is a normal Celery enqueue. Under eager Celery
    (IDE mode) `.delay()` would run whisper inline — inside whatever called
    Episode.save(), e.g. the publish view, hanging the request for the whole
    ASR run and starving SQLite of its write lock. Instead, run it on a
    daemon thread once the current transaction commits.

    task_kwargs are forwarded to run_transcription / transcribe_episode (model,
    language, initial_prompt, *_speakers); 'model' also steers routing.
    """
    if getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False):
        import threading
        from django.db import transaction
        logger.info(
            f"[transcribe] Eager broker detected — dispatching episode {episode_id} "
            f"on a background thread after commit (request will not block)."
        )
        transaction.on_commit(lambda: threading.Thread(
            target=run_transcription,
            args=(episode_id,),
            kwargs=task_kwargs,
            daemon=True,
            name=f"transcribe-ep-{episode_id}",
        ).start())
    else:
        from pod_manager.models import Episode
        from pod_manager.tasks import transcribe_episode
        episode = Episode.objects.select_related('podcast__network').get(id=episode_id)
        queue, priority = route_transcription(episode, level=level, model=task_kwargs.get('model'))
        logger.info(
            f"[transcribe] Queued episode {episode_id} via Celery "
            f"(queue={queue}, priority={priority})."
        )
        transcribe_episode.apply_async(
            args=[episode_id], kwargs=task_kwargs, queue=queue, priority=priority,
        )


def purge_transcription_queue() -> dict:
    """Empty the transcription pipeline: purge the Celery 'transcription' queue
    on the broker and delete every PENDING Transcript row.

    Returns {'purged': int | None, 'deleted': int}. 'purged' is None under eager
    Celery (IDE mode) where there is no broker to purge. PENDING transcripts have
    no on-disk files yet (those are written only on completion), so deleting the
    rows leaves nothing orphaned.
    """
    from pod_manager.models import Transcript

    purged = None
    if not getattr(settings, 'CELERY_TASK_ALWAYS_EAGER', False):
        from kombu import Queue
        from config.celery import app
        # Queue.purge() walks every priority sublist the Redis transport keeps
        # for this queue, so soft-priority messages are cleared too.
        with app.connection_for_write() as conn:
            purged = Queue('transcription', channel=conn.default_channel).purge()

    deleted, _ = Transcript.objects.filter(status=Transcript.Status.PENDING).delete()
    logger.info(
        "purge_transcription_queue: purged %s queued task(s), deleted %d pending transcript(s)",
        'n/a (eager)' if purged is None else purged, deleted,
    )
    return {'purged': purged, 'deleted': deleted}


def run_transcription(
    episode_id: int,
    *,
    model: str | None = None,
    language: str | None = None,
    initial_prompt: str | None = None,
    min_speakers: int | None = None,
    num_speakers: int | None = None,
    max_speakers: int | None = None,
    audio_source_url: str | None = None,
) -> None:
    """Transcribe one episode. Safe to call directly (no Celery required).

    Downloads the subscriber audio, POSTs to whisper ASR, converts the result to
    VTT / SRT / HTML / Podcast-Index JSON, writes files to disk, and updates
    the Transcript record. Raises on failure so the Celery task can retry.

    Per-call kwargs override podcast → network → settings.WHISPER_* fallback chain.

    audio_source_url : optional explicit source (episode-detail picker). Must be
    one of THIS episode's own audio URLs (r2_url / subscriber / public). Forces a
    fresh download, clears any cached source file, and — per the subscriber-only
    mirror rule — only writes R2 when it points at the subscriber URL.
    """
    from pod_manager.models import Episode, Transcript  # avoid circular import
    from pod_manager.utils import validate_public_url

    # 1. Fetch episode
    try:
        episode = Episode.objects.select_related('podcast__network').get(id=episode_id)
    except Episode.DoesNotExist:
        logger.error("transcribe: episode %d not found", episode_id)
        return

    audio_url = episode.audio_url_subscriber
    if not audio_url:
        logger.info("transcribe: episode %d has no subscriber audio — skipping", episode_id)
        return

    if not settings.WHISPER_ENABLED:
        logger.info("transcribe: WHISPER_ENABLED=False — skipping episode %d", episode_id)
        return

    # Resolve transcription options: per-call override → podcast override → network default → global settings
    podcast = episode.podcast
    network = podcast.network
    # Model resolution (see settings.WHISPER_FORCE_MODEL / WHISPER_DEFAULT_MODEL):
    #   FORCE (worker pin) → per-call → podcast → network → DEFAULT (worker) → global
    eff_model    = (getattr(settings, 'WHISPER_FORCE_MODEL', '') or None) \
                   or model or podcast.whisper_model or network.whisper_model \
                   or (getattr(settings, 'WHISPER_DEFAULT_MODEL', '') or None) \
                   or settings.WHISPER_MODEL
    eff_language = language or podcast.whisper_language or network.whisper_language or settings.WHISPER_LANGUAGE
    eff_prompt   = initial_prompt if initial_prompt is not None else (
        podcast.whisper_initial_prompt if podcast.whisper_initial_prompt is not None
        else network.whisper_initial_prompt
    )
    eff_min_sp   = min_speakers  if min_speakers  is not None else (podcast.whisper_min_speakers  or network.whisper_min_speakers)
    eff_num_sp   = num_speakers  if num_speakers  is not None else (podcast.whisper_num_speakers  or network.whisper_num_speakers)
    eff_max_sp   = max_speakers  if max_speakers  is not None else (podcast.whisper_max_speakers  or network.whisper_max_speakers)

    # 2. Upsert Transcript record and mark processing
    transcript, _ = Transcript.objects.get_or_create(episode=episode)
    if transcript.status == Transcript.Status.COMPLETED:
        logger.info("transcribe: episode %d already completed — skipping duplicate run", episode_id)
        return
    if (
        transcript.status == Transcript.Status.PROCESSING
        and transcript.started_at
        and (timezone.now() - transcript.started_at).total_seconds() < 7200  # task time_limit
    ):
        logger.info(
            "transcribe: episode %d already in-flight (started %s) — skipping duplicate run",
            episode_id, transcript.started_at,
        )
        return
    if not transcript.requested_at:
        transcript.requested_at = timezone.now()
    transcript.status = Transcript.Status.PROCESSING
    transcript.started_at = timezone.now()
    transcript.source_audio_url = audio_url
    # Prefer an explicit WORKER_NAME (set per worker in compose) — Docker
    # otherwise leaves the hostname as a random container id.
    transcript.worker = os.environ.get('WORKER_NAME') or socket.gethostname()
    transcript.save(update_fields=['status', 'requested_at', 'started_at', 'source_audio_url', 'worker'])

    # Resolve & validate an explicit source override (episode-detail "audio
    # source" picker). It must be one of THIS episode's own audio URLs — never
    # an arbitrary client-supplied URL. A chosen source forces a fresh download
    # and bypasses (and clears) the WHISPER_KEEP_SOURCE_AUDIO cache.
    source_override = None
    if audio_source_url:
        allowed = {u for u in (episode.r2_url, episode.audio_url_subscriber,
                               episode.audio_url_public) if u}
        if audio_source_url not in allowed:
            raise ValueError(
                f"audio_source_url {audio_source_url!r} is not one of episode "
                f"{episode_id}'s audio URLs"
            )
        ok, reason = validate_public_url(audio_source_url)
        if not ok:
            raise ValueError(f"audio_source_url failed SSRF validation: {reason}")
        source_override = audio_source_url

    download_url = source_override or audio_url

    # R2 mirror gating (PRIVATE-only decision): the inline mirror reuses these
    # bytes as the subscriber backup, so it may run ONLY when the bytes ARE the
    # subscriber audio — the default flow, or an override pointing at the
    # subscriber URL. R2/public overrides transcribe but never write the mirror.
    # When forced (subscriber override) the mirror is content-addressed: an
    # identical file dedupes (no R2 write), a changed file uploads + GCs the old.
    mirror_subscriber_bytes = (source_override is None
                               or source_override == episode.audio_url_subscriber)

    tmp_path = None
    try:
        # 3. Acquire audio, guarding against HTML interstitials (Google Drive et
        # al. hand back a download/quota web page instead of the MP3). Sniff each
        # candidate; on HTML, discard and redownload up to _MAX_AUDIO_DOWNLOAD_
        # ATTEMPTS times, then fail WITHOUT ever calling ASR.
        cached_path = source_audio_path(episode) if settings.WHISPER_KEEP_SOURCE_AUDIO else None

        if source_override and cached_path and cached_path.exists():
            cached_path.unlink(missing_ok=True)
            logger.info(
                "transcribe: cleared cached audio for episode %d (source override -> %s)",
                episode_id, source_override,
            )

        audio_fp_path = None
        downloads = 0
        # The cache is attempt 0 (not counted against the download budget) and
        # only on the default flow — a chosen source always forces fresh bytes.
        use_cache = bool(cached_path and cached_path.exists() and not source_override)

        while True:
            if use_cache:
                candidate = cached_path
                candidate_is_cache = True
                use_cache = False
                logger.info("transcribe: using cached audio for episode %d at %s", episode_id, candidate)
            else:
                downloads += 1
                logger.info(
                    "transcribe: downloading audio for episode %d from %s (attempt %d/%d)",
                    episode_id, download_url, downloads, _MAX_AUDIO_DOWNLOAD_ATTEMPTS,
                )
                tmp_path = _download_audio_to_temp(download_url)
                candidate = tmp_path
                candidate_is_cache = False

            if is_audio_file(candidate):
                audio_fp_path = candidate
                break

            # A non-audio body where audio should be (HTML interstitial / quota
            # wall) — discard this copy and try again.
            logger.warning(
                "transcribe: episode %d source returned a non-audio body (%s) — discarding",
                episode_id, 'cached file' if candidate_is_cache else f'download attempt {downloads}',
            )
            candidate.unlink(missing_ok=True)
            if not candidate_is_cache:
                tmp_path = None
            if downloads >= _MAX_AUDIO_DOWNLOAD_ATTEMPTS:
                raise HtmlSourceError(
                    f"source returned a non-audio body after {downloads} download "
                    f"attempt(s) at {download_url}"
                )
            time.sleep(_HTML_RETRY_BACKOFF_SECONDS)

        # 3b. Mirror to R2 — reuse THIS download (one download, two consumers).
        # Best-effort: a mirror failure must never fail the transcription. The
        # mirror only READS audio_fp_path; it never moves/deletes it, so the
        # existing retention behavior is unchanged. Subscriber-only / dead-S3
        # guards live in the service (raises MirrorSkipped for non-applicable
        # episodes). Runs before whisper so a backup exists even if ASR fails.
        if getattr(settings, 'R2_MIRROR_ENABLED', True) and mirror_subscriber_bytes:
            try:
                from pod_manager.services.r2_mirror import (
                    MirrorSkipped, mirror_episode_audio,
                )
                try:
                    # force on an explicit subscriber source so the content-hash
                    # comparison runs (the default HEAD gate would mirror-once
                    # and skip); identical bytes still dedupe with no R2 write.
                    res = mirror_episode_audio(
                        episode_id, local_path=audio_fp_path, force=bool(source_override),
                    )
                    # Reflect the freshly-set r2_url in our in-memory episode so
                    # the source-audio cache below names the file from it.
                    episode.r2_url = res.get('r2_url') or episode.r2_url
                    logger.info(
                        "transcribe: r2 mirror for episode %d -> %s (%s)",
                        episode_id, res.get('status'), res.get('key'),
                    )
                except MirrorSkipped as exc:
                    logger.info("transcribe: r2 mirror skipped for episode %d — %s", episode_id, exc)
            except Exception as exc:
                logger.warning(
                    "transcribe: r2 mirror FAILED for episode %d (continuing) — %s",
                    episode_id, exc,
                )
        elif not mirror_subscriber_bytes:
            logger.info(
                "transcribe: r2 mirror skipped for episode %d — transcribing from a "
                "non-subscriber source (%s)", episode_id, source_override,
            )

        # 3c. Persist a freshly downloaded file for reuse (WHISPER_KEEP_SOURCE_AUDIO,
        # dev only). Saved AFTER the mirror so it can adopt the R2 key's exact name
        # (id folders + {stem}-{shorthash}.{ext}), keeping the on-disk copy in
        # lockstep with R2 and relocated alongside it on a feed move. Default flow
        # only — never cache an override's bytes under the subscriber name (a
        # public/R2 file would then masquerade as the subscriber audio).
        if (settings.WHISPER_KEEP_SOURCE_AUDIO and audio_fp_path is tmp_path
                and not source_override):
            dest = source_audio_path(episode)
            if dest is None:
                # Mirror disabled/skipped — no r2_url to name from. Hash the bytes
                # we already have so the name still matches what R2 WOULD assign.
                dest = source_audio_path(episode, content_hash=_hash_file(audio_fp_path))
            if dest is not None and not dest.exists():
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(tmp_path, dest)
                _chmod_777(dest)
                logger.info("transcribe: saved source audio to %s", dest)

        # 4. POST to whisper /asr
        logger.info(
            "transcribe: sending episode %d to whisper ASR (%s, model=%s)",
            episode_id, settings.WHISPER_URL, eff_model,
        )
        with audio_fp_path.open('rb') as audio_fp:
            asr_params = {
                'task':            'transcribe',
                'model':           eff_model,
                'language':        eff_language,
                'output_format':   'json',
                'word_timestamps': True,
                'diarize':         True,
                'min_speakers':    eff_min_sp,
                'max_speakers':    eff_max_sp,
                'num_speakers':    eff_num_sp,
            }
            if eff_prompt:
                asr_params['initial_prompt'] = eff_prompt
            asr = requests.post(
                f"{settings.WHISPER_URL}/asr",
                files={'audio_file': ('audio.mp3', audio_fp, 'audio/mpeg')},
                params=asr_params,
                timeout=settings.WHISPER_TIMEOUT,
            )
            if not asr.ok:
                logger.error(
                    "transcribe: whisper ASR returned %d for episode %d — body: %s",
                    asr.status_code, episode_id, asr.text[:2000],
                )
            asr.raise_for_status()

        # 5. Parse response (handles standard JSON, prefixed, and NDJSON formats)
        logger.debug("transcribe: raw response (%d bytes): %r", len(asr.text), asr.text[:300])

        # In dev, save the raw whisper response alongside the transcript files for analysis
        if settings.WHISPER_KEEP_SOURCE_AUDIO:
            raw_path = transcript_path(episode_id, 'vtt').parent / f'{episode_id}.whisper_raw.txt'
            raw_path.parent.mkdir(parents=True, exist_ok=True)
            raw_path.write_text(asr.text, encoding='utf-8')
            logger.info("transcribe: saved raw whisper response to %s", raw_path)

        segments, detected_language = _parse_whisper_response(asr.text, eff_language)

        if not segments:
            logger.warning("transcribe: episode %d returned 0 segments from whisper ASR", episode_id)

        # 6. Write output files (R2 when enabled, else local)
        words_metadata = {
            'episode_id':     episode_id,
            'language':       detected_language,
            'model':          eff_model,
            'transcribed_at': timezone.now().isoformat(),
            # title / GUIDs / audio_url for post-DB-rebuild recovery matching.
            **episode_recovery_metadata(episode),
        }
        # Idempotent write: hash-check every format and PUT only those that
        # changed (§4). All writes complete here before the version bump below,
        # so a mid-write failure raises and leaves version untouched.
        #
        # Re-read the key token first: download + ASR take minutes, long enough
        # for the rekey churn to token this row mid-task. Writing with the stale
        # null token would recreate content at the fuzzable plain key right
        # after the rekey deleted and purged it, while serving points at the
        # (now stale) keyed copy.
        try:
            transcript.refresh_from_db(fields=['r2_key_token'])
        except Transcript.DoesNotExist:
            pass  # row deleted mid-task — keep prior behavior (save re-inserts)
        written, changed_formats = write_transcript_formats(episode_id, [
            ('vtt',   _to_vtt(segments)),
            ('json',  _to_podcast_index_json(segments)),
            ('srt',   _to_srt(segments)),
            ('html',  _to_html(segments)),
            ('words', _to_words_json(segments, metadata=words_metadata)),
        ], transcript.r2_key_token)

        # 7. Mark completed. Bump the cache-bust version only when ≥1 format
        # actually changed, so a re-transcribe that produces identical bytes
        # doesn't needlessly bust the immutable cdn cache (?v=N).
        transcript.status = Transcript.Status.COMPLETED
        transcript.completed_at = timezone.now()
        transcript.language = detected_language
        transcript.whisper_model_used = eff_model
        if changed_formats:
            transcript.version = (transcript.version or 0) + 1
        transcript.vtt_file        = written['vtt']
        transcript.json_file       = written['json']
        transcript.srt_file        = written['srt']
        transcript.html_file       = written['html']
        transcript.words_json_file = written['words']
        transcript.transcript_text = _plain_text(segments)
        transcript.error_message = None
        transcript.save(update_fields=[
            'status', 'completed_at', 'language', 'whisper_model_used', 'version',
            'vtt_file', 'json_file', 'srt_file', 'html_file', 'words_json_file',
            'transcript_text', 'error_message',
        ])
        logger.info(
            "transcribe: episode %d completed — %d segments, lang=%s",
            episode_id, len(segments), detected_language,
        )

        # Re-transcription renumbers diarization, so any prior speaker-edit chain
        # no longer aligns with this fresh base — supersede it (§7). The names are
        # already reset to raw labels by the formats just written above; this only
        # stops the old mappings from being re-applied by a future replay. No-op
        # for a brand-new transcript (no prior edits). Takes the per-episode
        # Transcript lock so it can't interleave with a live approve/rollback.
        superseded = supersede_speaker_edits(episode_id)
        if superseded:
            logger.info(
                "transcribe: episode %d — superseded %d prior speaker edit(s)",
                episode_id, superseded,
            )

        # Bust the cached RSS fragment so the podcast:transcript tag appears immediately.
        network = episode.podcast.network
        if network.custom_domain:
            base_url = f"https://{network.custom_domain}".rstrip('/')
        else:
            base_url = getattr(settings, 'SITE_URL', 'http://localhost:8000').rstrip('/')
        from pod_manager.tasks import task_rebuild_episode_fragments
        task_rebuild_episode_fragments.delay(episode_id, base_url)

    except PermanentSourceError as exc:
        # Source is gone for good — park without retrying. Returning (not
        # raising) means the Celery task succeeds and won't retry; the episode
        # requeues only when its audio URL changes (see queue signal).
        transcript.status = Transcript.Status.AWAITING_RECOVERY
        transcript.error_message = str(exc)
        transcript.save(update_fields=['status', 'error_message'])
        logger.warning(
            "transcribe: episode %d awaiting recovery — %s", episode_id, exc,
        )
        return

    except Exception as exc:
        transcript.refresh_from_db(fields=['retry_count'])
        transcript.status = Transcript.Status.FAILED
        transcript.retry_count += 1
        # Surface the engine's own error when present — a failed /asr call comes
        # back as an HTTP error whose JSON body is {"detail": "<real error>"}.
        transcript.error_message = _error_detail(exc)
        transcript.save(update_fields=['status', 'retry_count', 'error_message'])
        logger.error(
            "transcribe: episode %d failed (attempt %d): %s",
            episode_id, transcript.retry_count, transcript.error_message,
        )
        raise

    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Speaker label editing
# ---------------------------------------------------------------------------

def fold_speaker_mappings(episode_id: int) -> dict:
    """The cumulative speaker mapping = last-writer-wins fold over the episode's
    APPROVED speaker edits, ordered by resolved_at (user_edit_rollback.md §3.2).

    Each EpisodeEditSuggestion with suggested_data['speaker_mappings'] is one
    delta keyed on speaker_id; later edits overwrite earlier ones per key,
    unmentioned keys keep their prior value. Only APPROVED rows count —
    PENDING / REJECTED / ROLLED_BACK / SUPERSEDED are excluded, so removing an
    edit (rollback) or re-transcribing (supersede) is just a re-fold.
    """
    from pod_manager.models import EpisodeEditSuggestion

    mapping: dict = {}
    edits = (EpisodeEditSuggestion.objects
             .filter(episode_id=episode_id, status=EpisodeEditSuggestion.Status.APPROVED)
             .order_by('resolved_at', 'id'))
    for edit in edits:
        delta = (edit.suggested_data or {}).get('speaker_mappings')
        if isinstance(delta, dict):
            mapping.update(delta)
    return mapping


def speaker_edit_points(edit_mappings: dict, prior_mapping: dict) -> tuple:
    """Per-speaker contribution points for one approved speaker-label edit
    (user_edit_rollback.md §3.4). Returns ``(points, newly_named)``.

    ``prior_mapping`` is the cumulative last-writer-wins fold of the already-APPROVED
    speaker edits *before* this one (e.g. ``fold_speaker_mappings`` excluding the
    edit being scored), so each speaker_id's *current* (prior) name is known.

    **Score = the number of distinct ``(prior name → new name)`` changes.** Group the
    edited speaker_ids by their current name and, within each group, count the
    distinct new names assigned (ignoring any id that maps to its own current name).
    Equivalently: ``len({(prior_value, new_name) : new_name != prior_value})``.

    - A *rename that cascades to several speaker_ids* (all sharing one current name,
      all going to one new name) is **one** change → **+1**.
    - A *split* (one current name sent to several different new names — e.g. two
      diarized speakers wrongly merged into ``Aron``, now ``Aron`` and ``Roy``) scores
      **+1 per distinct target**, because the lossless ``speaker_id`` base lets you
      un-collapse what the old in-place rename could not.
    - First-time identification falls out for free: an unnamed id's prior value is its
      own raw ``SPEAKER_XX`` label (unique per id), so naming N raw speakers is N
      distinct changes → **+N**.

    ``newly_named`` (second return value) is the count of those first-time
    identifications — speaker_ids whose prior value was still the raw label.

    Reused by both the live approve handler and the §6 backfill recompute, so the
    caller owns the fold ordering and passes ``prior_mapping`` in.
    """
    prior_mapping = prior_mapping or {}
    changed_pairs = set()
    newly_named = 0
    for sid, new_name in (edit_mappings or {}).items():
        prior_value = prior_mapping.get(sid, sid)
        if new_name == prior_value:
            continue  # no-op (incl. a rename back to the raw label) → not scored
        changed_pairs.add((prior_value, new_name))
        if prior_value == sid:
            # Unnamed before (resolved to its own raw label) → first identification.
            newly_named += 1
    points = len(changed_pairs)
    return points, newly_named


def apply_speaker_labels(episode_id: int) -> None:
    """Recompute and re-materialise all transcript formats from the immutable
    speaker_id base + the approved speaker-edit chain (user_edit_rollback.md §3.3).

    Replay, not in-place mutation: the current state is a pure function of the
    pristine speaker_id (written once at transcription) folded with the APPROVED,
    non-superseded speaker edits (fold_speaker_mappings). The same function serves
    approve, rollback, and re-fold — callers only set edit statuses, then call this.

    The whole read→render→write→version-bump runs inside a transaction holding
    select_for_update() on the episode's Transcript row (per-episode lock, §3.3),
    so two concurrent approvals — or an approval racing a re-transcription — block
    rather than clobber each other's .words; other episodes still run in parallel.

    ROBUSTNESS: until the §6 backfill stamps speaker_id onto the existing
    catalogue, some .words have only the mutable `speaker`. We fall back to that
    value as the id so live edits keep working (split == combined there until the
    episode is backfilled or re-transcribed).
    """
    from django.db import transaction
    from pod_manager.models import Transcript

    with transaction.atomic():
        try:
            transcript = (Transcript.objects
                          .select_for_update()
                          .get(episode_id=episode_id, status=Transcript.Status.COMPLETED))
        except Transcript.DoesNotExist:
            logger.error("apply_speaker_labels: no completed transcript for episode %d", episode_id)
            return

        try:
            words_bytes = read_transcript_bytes(episode_id, 'words', transcript.version, transcript.r2_key_token)
        except Exception as exc:
            logger.error("apply_speaker_labels: .words unreadable for episode %d — %s", episode_id, exc)
            return

        doc = json.loads(words_bytes.decode('utf-8'))
        segments_raw = doc.get('segments', [])
        mapping = fold_speaker_mappings(episode_id)

        # Rebuild segments from the speaker_id base, resolving each id through the
        # folded mapping. speaker_id is preserved verbatim (never rewritten); only
        # the resolved `speaker` changes.
        segments = []
        for seg in segments_raw:
            seg_id = seg.get('speaker_id') or seg.get('speaker')  # fallback pre-backfill
            words = []
            for w in seg.get('words', []):
                w_id = w.get('speaker_id') or w.get('speaker')
                words.append({
                    'word':       w.get('word', ''),
                    'start':      w.get('startTime'),
                    'end':        w.get('endTime'),
                    'score':      w.get('score'),
                    'speaker_id': w_id,
                    'speaker':    mapping.get(w_id, w_id) if w_id else None,
                })
            segments.append({
                'start':      seg.get('startTime'),
                'end':        seg.get('endTime'),
                'text':       seg.get('body', ''),
                'speaker_id': seg_id,
                'speaker':    mapping.get(seg_id, seg_id) if seg_id else None,
                'words':      words,
            })

        # Preserve the existing header (transcribed_at, model, language, recovery
        # anchors) and refresh the cached resolved-mapping note.
        metadata = {k: v for k, v in doc.items() if k not in ('segments', 'version')}
        metadata['speaker_mappings'] = mapping

        # Idempotent write (§4): hash-check all five formats, PUT only those that
        # changed, bump version only on real change — so a rollback to a prior
        # state or a no-op re-fold costs no Class A writes and no cache bust.
        written, changed = write_transcript_formats(episode_id, [
            ('vtt',   _to_vtt(segments)),
            ('json',  _to_podcast_index_json(segments)),
            ('srt',   _to_srt(segments)),
            ('html',  _to_html(segments)),
            ('words', _to_words_json(segments, metadata=metadata)),
        ], transcript.r2_key_token)

        if changed:
            transcript.version = (transcript.version or 0) + 1
            transcript.vtt_file        = written['vtt']
            transcript.json_file       = written['json']
            transcript.srt_file        = written['srt']
            transcript.html_file       = written['html']
            transcript.words_json_file = written['words']
            transcript.save(update_fields=[
                'version', 'vtt_file', 'json_file', 'srt_file', 'html_file', 'words_json_file',
            ])

    logger.info(
        "apply_speaker_labels: episode %d — replayed %d mapping(s), %d format(s) changed (v%d)",
        episode_id, len(mapping), len(changed), transcript.version,
    )


def supersede_speaker_edits(episode_id: int) -> int:
    """On re-transcription the diarization is renumbered (SPEAKER_00 in v2 ≠ v1),
    so the prior speaker-edit chain no longer aligns with the fresh base. Mark all
    prior APPROVED / PENDING speaker edits SUPERSEDED — retained for audit & trust
    history, excluded from the replay fold (user_edit_rollback.md §7). Returns the
    number of edits superseded (0 for a brand-new transcript with no prior edits).

    Takes the same per-episode Transcript lock as apply_speaker_labels (§3.3) so a
    re-transcription can't interleave with a live approve/rollback replay.
    """
    from django.db import transaction
    from pod_manager.models import EpisodeEditSuggestion, Transcript

    with transaction.atomic():
        # Acquire the per-episode lock (no-op result if the row doesn't exist yet).
        list(Transcript.objects.select_for_update().filter(episode_id=episode_id))
        return (EpisodeEditSuggestion.objects
                .filter(episode_id=episode_id,
                        status__in=[EpisodeEditSuggestion.Status.APPROVED,
                                    EpisodeEditSuggestion.Status.PENDING],
                        suggested_data__has_key='speaker_mappings')
                .update(status=EpisodeEditSuggestion.Status.SUPERSEDED,
                        resolved_at=timezone.now()))
