import json
import logging
import shutil
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from django.conf import settings
from django.utils import timezone

logger = logging.getLogger(__name__)

ALLOWED_EXTENSIONS = {'vtt', 'json', 'srt', 'html', 'words'}

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


def source_audio_path(episode) -> Path:
    """Persistent save path for a subscriber MP3 when WHISPER_KEEP_SOURCE_AUDIO=True.

    Layout: MEDIA_ROOT/source_audio/{network_slug}/{podcast_slug}/{original_filename}
    Filename is derived from the subscriber URL. Falls back to the public URL when
    the subscriber URL has no file extension (e.g. Google Drive /uc?export=download links).
    """
    def _name_from_url(url):
        name = Path(unquote(urlparse(url).path)).name
        return name if name and '.' in name else None

    filename = (
        _name_from_url(episode.audio_url_subscriber)
        or _name_from_url(episode.audio_url_public or '')
        or f"episode_{episode.pk}.mp3"
    )
    return (
        Path(settings.MEDIA_ROOT)
        / 'source_audio'
        / episode.podcast.network.slug
        / episode.podcast.slug
        / filename
    )


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

    dest = source_audio_path(episode)
    if dest.exists():
        return False

    logger.info("ensure_source_audio: downloading audio for episode %d to %s", episode_id, dest)
    dl = requests.get(episode.audio_url_subscriber, stream=True, timeout=300)
    dl.raise_for_status()

    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False, dir=dest.parent) as tmp:
        tmp_path = Path(tmp.name)
        for chunk in dl.iter_content(chunk_size=65536):
            tmp.write(chunk)
    tmp_path.rename(dest)

    logger.info("ensure_source_audio: saved episode %d at %s", episode_id, dest)
    return True


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
        if seg.get("speaker"):
            entry["speaker"] = seg["speaker"]
        if seg.get("words"):
            entry["words"] = [
                {k: v for k, v in {
                    "word":      w.get("word"),
                    "startTime": w.get("start"),
                    "endTime":   w.get("end"),
                    "score":     w.get("score"),
                    "speaker":   w.get("speaker"),
                }.items() if v is not None}
                for w in seg["words"]
                if w.get("start") is not None and w.get("end") is not None
            ]
        out_segments.append(entry)
    doc = {"version": "1.0.0"}
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


def _parse_whisper_response(text: str, fallback_language: str) -> tuple[list, str]:
    """Parse whisper ASR response text into (segments, language).

    Primary format: SRT (what this whisper build returns for output=srt).
    Fallback: single JSON object with a top-level "segments" key.

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
        return data.get('segments', []), data.get('language', fallback_language)
    except json.JSONDecodeError:
        pass

    # --- attempt 2: SRT fallback (older whisper builds always return SRT) ---
    srt_segments = _parse_srt(text)
    if srt_segments:
        logger.debug("transcribe: parsed response as SRT (%d segments)", len(srt_segments))
        return srt_segments, fallback_language

    # --- attempt 3: strip a non-JSON prefix line and retry ---
    lines = text.splitlines()
    for skip in range(1, min(4, len(lines))):
        candidate = "\n".join(lines[skip:]).strip()
        if not candidate:
            continue
        try:
            data = json.loads(candidate)
            logger.debug("transcribe: parsed JSON after skipping %d line(s)", skip)
            return data.get('segments', []), data.get('language', fallback_language)
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

def dispatch_transcription(episode_id: int) -> None:
    """Queue a transcription WITHOUT ever blocking the caller.

    With a real broker this is a normal Celery enqueue. Under eager Celery
    (IDE mode) `.delay()` would run whisper inline — inside whatever called
    Episode.save(), e.g. the publish view, hanging the request for the whole
    ASR run and starving SQLite of its write lock. Instead, run it on a
    daemon thread once the current transaction commits.
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
            daemon=True,
            name=f"transcribe-ep-{episode_id}",
        ).start())
    else:
        from pod_manager.tasks import transcribe_episode
        logger.info(f"[transcribe] Queued episode {episode_id} via Celery.")
        transcribe_episode.delay(episode_id)


def run_transcription(
    episode_id: int,
    *,
    model: str | None = None,
    language: str | None = None,
    initial_prompt: str | None = None,
    min_speakers: int | None = None,
    num_speakers: int | None = None,
    max_speakers: int | None = None,
) -> None:
    """Transcribe one episode. Safe to call directly (no Celery required).

    Downloads the subscriber audio, POSTs to whisper ASR, converts the result to
    VTT / SRT / HTML / Podcast-Index JSON, writes files to disk, and updates
    the Transcript record. Raises on failure so the Celery task can retry.

    Per-call kwargs override podcast → network → settings.WHISPER_* fallback chain.
    """
    from pod_manager.models import Episode, Transcript  # avoid circular import

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
    eff_model    = model    or podcast.whisper_model    or network.whisper_model    or settings.WHISPER_MODEL
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
    transcript.save(update_fields=['status', 'requested_at', 'started_at', 'source_audio_url'])

    tmp_path = None
    try:
        # 3. Get audio — use cached file if available, otherwise download
        if settings.WHISPER_KEEP_SOURCE_AUDIO:
            cached = source_audio_path(episode)
            if cached.exists():
                audio_fp_path = cached
                logger.info("transcribe: using cached audio for episode %d at %s", episode_id, cached)
            else:
                audio_fp_path = None
        else:
            audio_fp_path = None

        if audio_fp_path is None:
            logger.info("transcribe: downloading audio for episode %d", episode_id)
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp_path = Path(tmp.name)
                dl = requests.get(audio_url, stream=True, timeout=300)
                dl.raise_for_status()
                for chunk in dl.iter_content(chunk_size=65536):
                    tmp.write(chunk)
            audio_fp_path = tmp_path

            # Persist the download if configured
            if settings.WHISPER_KEEP_SOURCE_AUDIO:
                dest = source_audio_path(episode)
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(tmp_path, dest)
                logger.info("transcribe: saved source audio to %s", dest)

        # 4. POST to whisper /asr
        logger.info(
            "transcribe: sending episode %d to whisper ASR (%s, model=%s)",
            episode_id, settings.WHISPER_URL, eff_model,
        )
        with audio_fp_path.open('rb') as audio_fp:
            asr_params = {
                'task':            'transcribe',
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

        # 6. Write output files
        transcript_path(episode_id, 'vtt').parent.mkdir(parents=True, exist_ok=True)

        words_metadata = {
            'episode_id':     episode_id,
            'audio_url':      audio_url,
            'language':       detected_language,
            'model':          eff_model,
            'transcribed_at': timezone.now().isoformat(),
        }
        written = {}
        for ext, content in [
            ('vtt',   _to_vtt(segments)),
            ('json',  _to_podcast_index_json(segments)),
            ('srt',   _to_srt(segments)),
            ('html',  _to_html(segments)),
            ('words', _to_words_json(segments, metadata=words_metadata)),
        ]:
            p = transcript_path(episode_id, ext)
            p.write_bytes(content)
            written[ext] = str(p.relative_to(settings.MEDIA_ROOT))

        # 7. Mark completed
        transcript.status = Transcript.Status.COMPLETED
        transcript.completed_at = timezone.now()
        transcript.language = detected_language
        transcript.whisper_model_used = eff_model
        transcript.vtt_file        = written['vtt']
        transcript.json_file       = written['json']
        transcript.srt_file        = written['srt']
        transcript.html_file       = written['html']
        transcript.words_json_file = written['words']
        transcript.transcript_text = _plain_text(segments)
        transcript.error_message = None
        transcript.save(update_fields=[
            'status', 'completed_at', 'language', 'whisper_model_used',
            'vtt_file', 'json_file', 'srt_file', 'html_file', 'words_json_file',
            'transcript_text', 'error_message',
        ])
        logger.info(
            "transcribe: episode %d completed — %d segments, lang=%s",
            episode_id, len(segments), detected_language,
        )

        # Bust the cached RSS fragment so the podcast:transcript tag appears immediately.
        network = episode.podcast.network
        if network.custom_domain:
            base_url = f"https://{network.custom_domain}".rstrip('/')
        else:
            base_url = getattr(settings, 'SITE_URL', 'http://localhost:8000').rstrip('/')
        from pod_manager.tasks import task_rebuild_episode_fragments
        task_rebuild_episode_fragments.delay(episode_id, base_url)

    except Exception as exc:
        transcript.refresh_from_db(fields=['retry_count'])
        transcript.status = Transcript.Status.FAILED
        transcript.retry_count += 1
        transcript.error_message = str(exc)
        transcript.save(update_fields=['status', 'retry_count', 'error_message'])
        logger.error(
            "transcribe: episode %d failed (attempt %d): %s",
            episode_id, transcript.retry_count, exc,
        )
        raise

    finally:
        if tmp_path and tmp_path.exists():
            tmp_path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Speaker label editing
# ---------------------------------------------------------------------------

def apply_speaker_labels(episode_id: int, speaker_mappings: dict) -> None:
    """Apply speaker name mappings to all transcript formats for an episode.

    Reads the stored .words JSON, replaces SPEAKER_XX labels in segment.speaker
    and word.speaker fields (key lookup only — never touches transcript text),
    then regenerates and overwrites VTT, SRT, HTML, JSON, and words files.

    speaker_mappings: e.g. {"SPEAKER_00": "Jim", "SPEAKER_01": "A.Ron"}
    """
    from pod_manager.models import Transcript

    try:
        transcript = Transcript.objects.get(episode_id=episode_id, status=Transcript.Status.COMPLETED)
    except Transcript.DoesNotExist:
        logger.error("apply_speaker_labels: no completed transcript for episode %d", episode_id)
        return

    words_path = transcript_path(episode_id, 'words')
    if not words_path.exists():
        logger.error("apply_speaker_labels: .words file missing for episode %d", episode_id)
        return

    doc = json.loads(words_path.read_text(encoding='utf-8'))
    segments_raw = doc.get('segments', [])

    # Rebuild segments in the internal format run_transcription uses, applying mappings
    segments = []
    for seg in segments_raw:
        raw_speaker = seg.get('speaker', '')
        new_speaker = speaker_mappings.get(raw_speaker, raw_speaker)
        words = []
        for w in seg.get('words', []):
            raw_ws = w.get('speaker', '')
            words.append({
                'word':    w.get('word', ''),
                'start':   w.get('startTime'),
                'end':     w.get('endTime'),
                'score':   w.get('score'),
                'speaker': speaker_mappings.get(raw_ws, raw_ws) if raw_ws else None,
            })
        segments.append({
            'start':   seg.get('startTime'),
            'end':     seg.get('endTime'),
            'text':    seg.get('body', ''),
            'speaker': new_speaker,
            'words':   words,
        })

    # Preserve existing metadata header fields, update with new mappings note
    metadata = {k: v for k, v in doc.items() if k not in ('segments', 'version')}
    metadata['speaker_mappings'] = speaker_mappings

    # Regenerate and overwrite all formats
    for ext, content in [
        ('vtt',   _to_vtt(segments)),
        ('json',  _to_podcast_index_json(segments)),
        ('srt',   _to_srt(segments)),
        ('html',  _to_html(segments)),
        ('words', _to_words_json(segments, metadata=metadata)),
    ]:
        transcript_path(episode_id, ext).write_bytes(content)

    logger.info(
        "apply_speaker_labels: episode %d — updated %d mappings, regenerated all formats",
        episode_id, len(speaker_mappings),
    )
