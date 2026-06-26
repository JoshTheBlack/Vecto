"""Mirror subscriber audio into Cloudflare R2.

Phase 3 of the audio-mirror feature (see planned_features.txt). The single
public entry point is mirror_episode_audio(); everything else is a private
helper. The mirror is a pure ADDITIONAL consumer of the audio bytes — it never
mutates the audio (byte-exact backup) and never moves/deletes the local file it
is handed.

KEY INVARIANTS (section B / I):
  - SUBSCRIBER AUDIO ONLY. Public/Megaphone is never mirrored; dead-S3 has no
    fetchable source and is refused.
  - Key = {prefix}{network_id}/{podcast_id}/{stem}-{shorthash}.mp3, where the
    shorthash is the first 16 hex of the SHA-256 of the audio bytes. A changed
    file -> a new hash -> a new key -> a new public URL (the cache-bust token).
  - Episode.r2_url is the single source of truth. Re-versioning records the old
    key as an orphan (left in place); re-adoption clears an orphan row when a key
    becomes referenced again. Object stays immutable forever — keys never mutate.
"""

import hashlib
import logging
import tempfile
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from botocore.exceptions import ClientError
from django.conf import settings
from django.db import transaction
from django.utils import timezone
from django.utils.text import slugify

from pod_manager.services.r2_client import (
    get_r2_client,
    key_from_public_url,
    prefixed_key,
    public_url,
)
from pod_manager.services.transcription import source_audio_filename
from pod_manager.utils import validate_public_url

logger = logging.getLogger(__name__)

# Audio container extensions we strip when normalizing a filename to a stem.
_AUDIO_EXTS = {
    'mp3', 'm4a', 'm4b', 'aac', 'ogg', 'oga', 'wav', 'flac', 'opus', 'mp4', 'm4v',
}
_CONTENT_TYPE_BY_EXT = {
    'mp3': 'audio/mpeg', 'm4a': 'audio/mp4', 'm4b': 'audio/mp4',
    'aac': 'audio/aac', 'ogg': 'audio/ogg', 'oga': 'audio/ogg',
    'wav': 'audio/wav', 'flac': 'audio/flac', 'opus': 'audio/opus',
    'mp4': 'audio/mp4', 'm4v': 'audio/mp4',
}
_CACHE_CONTROL = "public, max-age=31536000, immutable"
_HASH_PREFIX_LEN = 16  # 64 bits — see section B birthday-bound rationale.


class MirrorSkipped(Exception):
    """Raised when an episode cannot/should not be mirrored (not an error):
    mirror disabled, no subscriber source, public/dead-S3 audio, etc."""


# ---------------------------------------------------------------------------
# Filename / key helpers
# ---------------------------------------------------------------------------

def _split_stem_ext(filename: str) -> tuple[str, str]:
    """Return (stem, ext) by stripping ALL trailing audio extensions.

    Many source filenames end in ".mp3.mp3"; we strip every trailing audio
    extension and keep the OUTERMOST one as the canonical container so the R2
    object gets a correct Content-Type (.m4a/.m4b stay audio/mp4). Defaults to
    "mp3" when no audio extension is present.
    """
    parts = filename.split('.')
    chosen_ext = 'mp3'
    stripped = False
    while len(parts) > 1 and parts[-1].lower() in _AUDIO_EXTS:
        if not stripped:
            chosen_ext = parts[-1].lower()
            stripped = True
        parts = parts[:-1]
    stem = '.'.join(parts) or filename
    return stem, chosen_ext


def object_key_bare(episode, content_hash: str) -> str:
    """The R2 object key WITHOUT the bucket prefix:
    {network_id}/{podcast_id}/{stem}-{shorthash}.{ext}.

    Folders are IMMUTABLE network_id/podcast_id (slug changes are free); meaning
    is carried by the readable filename stem. Reuses transcription's
    source_audio_filename() so GDrive /uc?... links resolve to episode_{pk}.mp3
    instead of a garbage "uc" stem.

    Exposed (not underscore-private) because the local WHISPER_KEEP_SOURCE_AUDIO
    copy reuses this byte-for-byte, so the on-disk mirror and the R2 object share
    one naming scheme (folders + filename) and stay in lockstep.
    """
    raw_name = source_audio_filename(episode)
    # Drop any query string / tracking prefix that slipped through.
    raw_name = unquote(urlparse(raw_name).path).rsplit('/', 1)[-1] or raw_name
    stem, ext = _split_stem_ext(raw_name)
    safe_stem = slugify(stem) or f"episode-{episode.pk}"
    shorthash = content_hash[:_HASH_PREFIX_LEN]
    return f"{episode.podcast.network_id}/{episode.podcast_id}/{safe_stem}-{shorthash}.{ext}"


def _object_key(episode, content_hash: str) -> tuple[str, str]:
    """Build the (prefixed_key, content_type) for an episode's mirror."""
    bare = object_key_bare(episode, content_hash)
    ext = bare.rsplit('.', 1)[-1]
    content_type = _CONTENT_TYPE_BY_EXT.get(ext, 'audio/mpeg')
    return prefixed_key(bare), content_type


# ---------------------------------------------------------------------------
# Byte acquisition + hashing
# ---------------------------------------------------------------------------

def _hash_file(path: Path) -> str:
    """SHA-256 hexdigest of a file, streamed (never buffers the whole file)."""
    h = hashlib.sha256()
    with path.open('rb') as fp:
        for chunk in iter(lambda: fp.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def _download_to_temp(url: str) -> tuple[Path, str]:
    """Stream a URL to a temp file, hashing as we go. Returns (path, sha256).

    SSRF-guarded with the same validator the chapter fetch uses. Caller owns the
    returned temp file and must unlink it.
    """
    ok, reason = validate_public_url(url)
    if not ok:
        raise MirrorSkipped(f"refusing to fetch unsafe URL: {reason}")

    h = hashlib.sha256()
    with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
        tmp_path = Path(tmp.name)
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=65536):
            tmp.write(chunk)
            h.update(chunk)
    return tmp_path, h.hexdigest()


def _head_signature(url: str) -> str:
    """Cheap change-detection fingerprint via HTTP HEAD: "{etag}:{length}".

    Returns '' when HEAD is unsupported or gives us nothing usable (e.g. GDrive),
    in which case the caller falls back to mirror-once.
    """
    ok, _ = validate_public_url(url)
    if not ok:
        return ''
    try:
        resp = requests.head(url, timeout=30, allow_redirects=True)
        if not resp.ok:
            return ''
        etag = (resp.headers.get('ETag') or '').strip('"')
        length = resp.headers.get('Content-Length') or ''
        sig = f"{etag}:{length}"
        return sig if sig != ':' else ''
    except requests.RequestException:
        return ''


# ---------------------------------------------------------------------------
# R2 object helpers
# ---------------------------------------------------------------------------

def _object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get('Error', {}).get('Code', '')
        if code in ('404', 'NoSuchKey', 'NotFound'):
            return False
        raise


def _key_referenced_by_other(key: str, exclude_episode_id: int) -> bool:
    """True if any OTHER episode's r2_url resolves to this key (content-hash
    dedupe means a key can legitimately be shared). Cheap targeted query."""
    from pod_manager.models import Episode
    host = settings.R2_PUBLIC_HOST
    candidate_url = public_url(key)
    qs = Episode.objects.exclude(pk=exclude_episode_id).filter(r2_url__isnull=False)
    # r2_url stores the full public URL; match on exact URL (host + key).
    return qs.filter(r2_url=candidate_url).exists()


def _clear_orphan_row(key: str):
    """Re-adoption: a key is referenced again, so it is no longer an orphan."""
    from pod_manager.models import R2OrphanedObject
    R2OrphanedObject.objects.filter(key=key).delete()


def _record_orphan(key: str, episode, reason: str):
    """Record a superseded key as a deletion candidate (left in place in R2).
    No-op if the key is still referenced by some episode."""
    from pod_manager.models import R2OrphanedObject
    R2OrphanedObject.objects.get_or_create(
        key=key,
        defaults={'episode': episode, 'reason': reason, 'orphaned_at': timezone.now()},
    )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def mirror_episode_audio(episode_id: int, local_path=None, force: bool = False) -> dict:
    """Mirror one episode's subscriber audio to R2.

    local_path : if given (e.g. transcription's temp MP3), upload it directly
                 with NO re-download. Only READ; never moved or deleted here.
    force      : re-mirror even if r2_url is already set / source unchanged.

    Returns a small result dict: {'status': 'mirrored'|'deduped'|'skipped',
    'r2_url': ..., 'key': ..., 'reason': ...}. Raises MirrorSkipped for
    not-applicable episodes so callers can log-and-continue.
    """
    from pod_manager.models import Episode

    if not getattr(settings, 'R2_MIRROR_ENABLED', True):
        raise MirrorSkipped("R2_MIRROR_ENABLED is False")

    episode = Episode.objects.select_related('podcast', 'podcast__network').get(pk=episode_id)

    # SUBSCRIBER ONLY — never mirror public/Megaphone; dead-S3 has no source.
    audio_url = episode.audio_url_subscriber
    if not audio_url:
        raise MirrorSkipped(f"episode {episode_id} has no subscriber audio")
    if not episode.is_premium:
        raise MirrorSkipped(f"episode {episode_id} subscriber audio is the public URL")
    origin = episode.audio_origin()
    if origin == 's3_dead':
        raise MirrorSkipped(f"episode {episode_id} source is the dead S3 bucket (unfetchable)")

    old_key = key_from_public_url(episode.r2_url) if episode.r2_url else None

    # Idempotency gate: only re-mirror when forced or when HEAD proves the source
    # changed. If we can't tell (no usable signature), mirror-once -> skip.
    if episode.r2_url and not force:
        fresh_sig = _head_signature(audio_url)
        if not (fresh_sig and episode.r2_source_signature and fresh_sig != episode.r2_source_signature):
            return {'status': 'skipped', 'r2_url': episode.r2_url, 'key': old_key,
                    'reason': 'r2_url set and source unchanged'}
        signature = fresh_sig
    else:
        signature = _head_signature(audio_url)

    # Acquire bytes + content hash.
    tmp_path = None
    try:
        if local_path is not None:
            src = Path(local_path)
            if not src.exists():
                raise MirrorSkipped(f"local_path does not exist: {src}")
            content_hash = _hash_file(src)
            upload_path = src
        else:
            tmp_path, content_hash = _download_to_temp(audio_url)
            upload_path = tmp_path

        key, content_type = _object_key(episode, content_hash)
        new_url = public_url(key)
        client = get_r2_client()
        bucket = settings.R2_BUCKET

        # DEDUPE + RE-ADOPTION: identical bytes+name already in R2 -> skip upload.
        if _object_exists(client, bucket, key):
            status = 'deduped'
            logger.info("r2_mirror: object already present for episode %d at %s", episode_id, key)
        else:
            client.upload_file(
                str(upload_path), bucket, key,
                ExtraArgs={'ContentType': content_type, 'CacheControl': _CACHE_CONTROL},
            )
            # Verify before committing the DB pointer (section I, strict order).
            if not _object_exists(client, bucket, key):
                raise RuntimeError(f"R2 upload verify failed for {key}")
            status = 'mirrored'
            logger.info("r2_mirror: uploaded episode %d -> %s", episode_id, key)

        # Commit pointer + clear any orphan row for the now-live key, then record
        # the superseded key as an orphan — all atomically.
        with transaction.atomic():
            episode.r2_url = new_url
            episode.r2_uploaded_at = timezone.now()
            episode.r2_source_signature = signature or ''
            episode.save(update_fields=['r2_url', 'r2_uploaded_at', 'r2_source_signature'])
            _clear_orphan_row(key)
            if old_key and old_key != key and not _key_referenced_by_other(old_key, episode_id):
                _record_orphan(old_key, episode, reason='reversion')

        return {'status': status, 'r2_url': new_url, 'key': key, 'reason': ''}
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)
