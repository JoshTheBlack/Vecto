"""R2 orphan lifecycle + maintenance (planned_features.txt sections I & J).

Four operations, all keyed on the rule that Episode.r2_url is the single source
of truth and the prod keyspace is everything NOT under the dev/ prefix:

  reconcile_orphans()  — Layer 2 sweep: list the whole bucket, RECORD objects no
                         Episode.r2_url points at (and old enough to not be
                         mid-pipeline) as R2OrphanedObject rows. Never deletes.
  cleanup_orphans()    — the only hard-delete: for orphan rows past their
                         per-reason retention, RE-VALIDATE against live r2_url and
                         delete the R2 object (+ row) only if still unreferenced;
                         drop rows that got re-adopted.
  rekey_episode_audio()— move an episode's object to its current-parent key
                         (CopyObject, no egress) so the bucket layout matches
                         parentage; records the old key as a 'move_rekey' orphan.
  purge_dev_prefix()   — hard-delete everything under dev/ (disposable test data).

All re-use the per-episode mirror's orphan helpers so recording/clearing logic
lives in exactly one place.
"""

import logging
import shutil
from datetime import timedelta

from botocore.exceptions import ClientError
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from pod_manager.services.r2_client import (
    get_r2_client,
    key_from_public_url,
    prefixed_key,
    public_url,
)
from pod_manager.services.r2_mirror import (
    _clear_orphan_row,
    _key_referenced_by_other,
    _object_exists,
    _record_orphan,
)

logger = logging.getLogger(__name__)

# The dev/test namespace. The prod keyspace is everything NOT under this prefix;
# GC/cleanup never touch it (purge_dev_prefix handles dev separately).
DEV_PREFIX = "dev/"

# Bare transcript keys (transcripts/{id//1000}/{id}[.{token}].{ext}). Orphan rows
# with this prefix belong to the MEDIA bucket, not the audio mirror — cleanup
# routes them via media_object_key() and pairs the delete with a CDN purge.
TRANSCRIPTS_PREFIX = "transcripts/"

# ?v purge range used when an orphaned transcript key has no live Transcript row
# to read the real version from (see _delete_and_purge_transcript_key).
_FALLBACK_PURGE_VERSIONS = 25

# R2 DeleteObjects accepts up to 1000 keys per call.
_DELETE_BATCH = 1000


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _referenced_keys() -> set[str]:
    """Host-stripped key for every non-null Episode.r2_url (the live set)."""
    from pod_manager.models import Episode
    return {
        key_from_public_url(url)
        for url in Episode.objects.filter(r2_url__isnull=False)
        .exclude(r2_url="")
        .values_list("r2_url", flat=True)
    }


def _iter_bucket_objects(client, bucket, prefix=None):
    """Yield (key, last_modified) for every object, paginated (1000/page)."""
    paginator = client.get_paginator("list_objects_v2")
    kwargs = {"Bucket": bucket}
    if prefix:
        kwargs["Prefix"] = prefix
    for page in paginator.paginate(**kwargs):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj.get("LastModified")


def _batch_delete(client, bucket, keys):
    """DeleteObjects in chunks of 1000. Returns the number requested."""
    for i in range(0, len(keys), _DELETE_BATCH):
        chunk = keys[i:i + _DELETE_BATCH]
        client.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
        )
    return len(keys)


# ---------------------------------------------------------------------------
# Layer 2 — reconciliation sweep
# ---------------------------------------------------------------------------

def reconcile_orphans(apply: bool = False, age_days: int = 7) -> dict:
    """List the whole prod keyspace and record unreferenced objects as orphans.

    Records ONLY; never deletes. Default is a dry run (apply=False). Skips the
    dev/ namespace, keys already referenced, keys already in the orphan table,
    and objects newer than age_days (which may still be mid-pipeline)."""
    from pod_manager.models import R2OrphanedObject

    client = get_r2_client()
    bucket = settings.R2_BUCKET
    referenced = _referenced_keys()
    existing = set(R2OrphanedObject.objects.values_list("key", flat=True))
    cutoff = timezone.now() - timedelta(days=age_days)

    scanned = 0
    new_orphans = []
    for key, last_modified in _iter_bucket_objects(client, bucket):
        scanned += 1
        if key.startswith(DEV_PREFIX):
            continue
        if key in referenced or key in existing:
            continue
        if last_modified and last_modified > cutoff:
            continue  # too new — guard against objects mid-mirror
        new_orphans.append(key)

    if apply and new_orphans:
        R2OrphanedObject.objects.bulk_create(
            [R2OrphanedObject(key=k, reason=R2OrphanedObject.Reason.RECONCILIATION)
             for k in new_orphans],
            ignore_conflicts=True,
        )
    logger.info(
        "r2 reconcile: scanned=%d unreferenced=%d applied=%s",
        scanned, len(new_orphans), apply,
    )
    return {"scanned": scanned, "orphans": new_orphans, "applied": apply}


# ---------------------------------------------------------------------------
# Cleanup — the only hard-delete
# ---------------------------------------------------------------------------

def cleanup_orphans(apply: bool = False) -> dict:
    """Delete orphan-table objects past their per-reason retention, re-validating
    at the last moment. Routes by key prefix (section E3):

      transcripts/... -> the MEDIA bucket (via media_object_key), re-validated
        against live Transcript resolution, deleted + CDN-purged as a pair — the
        row is only dropped after BOTH succeed. ZERO retention: a byte-identical
        copy already lives at the keyed location, and for a flag-off feed a
        retention window would BE the leak window.
      everything else -> the audio mirror bucket, re-validated against live
        Episode.r2_url, per-reason retention as before.

    A row whose key is referenced again at delete time was re-adopted — drop the
    row, keep the object. Default dry run."""
    from pod_manager.models import R2OrphanedObject

    now = timezone.now()
    referenced = _referenced_keys()
    rekey_cut = now - timedelta(days=settings.R2_REKEY_GRACE_DAYS)
    other_cut = now - timedelta(days=settings.R2_ORPHAN_RETENTION_DAYS)

    to_delete = []   # confirmed-unreferenced audio rows -> batch-delete object + row
    tx_rows = []     # transcript rows -> media-bucket delete + CDN purge, per key
    readopted = []   # key referenced again -> drop row only
    for row in R2OrphanedObject.objects.all():
        if row.key.startswith(DEV_PREFIX):
            continue  # cleanup never touches dev
        if row.key.startswith(TRANSCRIPTS_PREFIX):
            if _transcript_key_still_live(row.key):
                readopted.append(row.id)
            else:
                tx_rows.append(row)
            continue
        cut = rekey_cut if row.reason == R2OrphanedObject.Reason.MOVE_REKEY else other_cut
        if row.orphaned_at > cut:
            continue  # not expired yet
        if row.key in referenced:
            readopted.append(row.id)
        else:
            to_delete.append(row)

    deleted_keys = [r.key for r in to_delete]
    tx_keys = [r.key for r in tx_rows]
    tx_retained = 0
    if apply:
        if deleted_keys:
            client = get_r2_client()
            _batch_delete(client, settings.R2_BUCKET, deleted_keys)
            R2OrphanedObject.objects.filter(id__in=[r.id for r in to_delete]).delete()
        for row in tx_rows:
            if _delete_and_purge_transcript_key(row.key):
                row.delete()
            else:
                tx_retained += 1  # row kept — the retry ledger
        if readopted:
            R2OrphanedObject.objects.filter(id__in=readopted).delete()
    logger.info(
        "r2 cleanup: deleted=%d transcripts=%d retained=%d readopted=%d applied=%s",
        len(deleted_keys), len(tx_keys), tx_retained, len(readopted), apply,
    )
    return {
        "deleted": deleted_keys,
        "transcripts": tx_keys,
        "transcripts_retained": tx_retained,
        "readopted": len(readopted),
        "applied": apply,
    }


# ---------------------------------------------------------------------------
# Transcript rekey churn + media-bucket orphan handling (transcript plan E2-E4)
# ---------------------------------------------------------------------------

def _transcript_marker_exts(transcript) -> list[str]:
    """The exts whose file markers are populated on the row (the objects that
    actually exist and must move)."""
    from pod_manager.services.transcription import ALLOWED_EXTENSIONS
    return [
        ext for ext in sorted(ALLOWED_EXTENSIONS)
        if getattr(transcript, 'words_json_file' if ext == 'words' else f'{ext}_file', None)
    ]


def _parse_transcript_key(key: str):
    """(episode_id, token, ext) from a bare transcript key, or None if the key
    doesn't match either shape (legacy {id}.{ext} / keyed {id}.{token}.{ext})."""
    from pod_manager.services.transcription import ALLOWED_EXTENSIONS
    parts = key.rsplit('/', 1)[-1].split('.')
    if len(parts) == 2:
        ep_raw, token, ext = parts[0], None, parts[1]
    elif len(parts) == 3:
        ep_raw, token, ext = parts
    else:
        return None
    if ext not in ALLOWED_EXTENSIONS:
        return None
    try:
        return int(ep_raw), token, ext
    except ValueError:
        return None


def _transcript_key_still_live(key: str) -> bool:
    """Re-validation for transcript orphan rows: True if the episode's live
    Transcript still RESOLVES to this key (token null and key is the plain shape,
    or the token matches) — i.e. deleting it would break the serve path."""
    from pod_manager.models import Transcript
    from pod_manager.services.transcription import transcript_r2_key
    parsed = _parse_transcript_key(key)
    if parsed is None:
        return False
    episode_id, _, ext = parsed
    row = Transcript.objects.filter(episode_id=episode_id).only('r2_key_token').first()
    if row is None:
        return False
    return transcript_r2_key(episode_id, ext, row.r2_key_token) == key


def _transcript_purge_urls(key: str, version: int) -> list[str]:
    """Every CDN cache entry a transcript key may live under: the bare URL plus
    ?v=k for k=1..version — Cloudflare's cache key includes the query string and
    the 302 always emitted ?v=N, so versioned entries are what's cached."""
    from pod_manager.services.r2_storage import media_public_url
    base = media_public_url(key)
    return [base] + [f"{base}?v={k}" for k in range(1, (version or 0) + 1)]


def _delete_and_purge_transcript_key(key: str) -> bool:
    """Hard-delete one bare transcript key from the MEDIA bucket, then purge its
    CDN URLs. True only when both succeed — callers keep their orphan row (the
    retry ledger) on any failure."""
    from pod_manager.models import Transcript
    from pod_manager.services.cloudflare import purge_urls
    from pod_manager.services.r2_storage import delete_media_object

    # When the Transcript row is gone (deleted to force re-transcription, or an
    # Episode cascade) its version — and with it the exact set of ?v=N entries
    # the 302 ever pushed into the edge cache — is unknowable, so purge a
    # generous fixed range instead. Versions are tiny ints (a handful of
    # re-transcribes), and the whole range still fits one 30-URL purge call.
    version = _FALLBACK_PURGE_VERSIONS
    parsed = _parse_transcript_key(key)
    if parsed:
        row = Transcript.objects.filter(episode_id=parsed[0]).only('version').first()
        if row is not None:
            version = row.version or 0
    try:
        delete_media_object(key)
    except ClientError as exc:
        logger.warning("transcript orphan delete failed for %s: %s", key, exc)
        return False
    return purge_urls(_transcript_purge_urls(key, version))


def _rekey_one_transcript(client, bucket, transcript) -> str:
    """Move one legacy transcript's objects to a keyed location. Strict order:
    record-orphan -> copy -> set token -> delete -> purge -> clear rows, so a
    crash at any point leaves a durable retry record and a rerun converges.

    Returns 'rekeyed' | 'retry_pending' (token set; delete/purge left to the
    orphan rows) | 'error' (copy failed; token NOT set — rerun retries)."""
    from pod_manager.models import R2OrphanedObject, Transcript, new_transcript_token
    from pod_manager.services.cloudflare import purge_urls
    from pod_manager.services.r2_storage import delete_media_object, media_object_key
    from pod_manager.services.transcription import transcript_r2_key

    episode_id = transcript.episode_id
    old_keys = [
        transcript_r2_key(episode_id, ext, None)
        for ext in _transcript_marker_exts(transcript)
    ]

    # 1. Durable retry records BEFORE the risky steps (mirror's record-then-clear
    #    pattern) — a crash mid-operation leaves rows cleanup_orphans can finish.
    for key in old_keys:
        _record_orphan(key, transcript.episode, reason=R2OrphanedObject.Reason.MOVE_REKEY)

    token = new_transcript_token()

    # 2. Server-side copies old -> keyed (Class A; no egress).
    try:
        for old_key in old_keys:
            new_key = transcript_r2_key(episode_id, old_key.rsplit('.', 1)[-1], token)
            client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': media_object_key(old_key)},
                Key=media_object_key(new_key),
            )
    except ClientError as exc:
        logger.warning(
            "transcript rekey: copy failed for episode %d (orphan rows retained "
            "for retry): %s", episode_id, exc,
        )
        return 'error'

    # 3. Single UPDATE — the 302 target flips atomically here. The isnull guard
    #    means a concurrent rekey can't clobber an already-set token (one-way
    #    ratchet); if we lost that race our copies are unreferenced cruft and the
    #    orphan rows still converge via cleanup re-validation.
    updated = Transcript.objects.filter(
        pk=transcript.pk, r2_key_token__isnull=True,
    ).update(r2_key_token=token)
    if not updated:
        logger.warning("transcript rekey: episode %d was tokened concurrently", episode_id)
        return 'error'
    transcript.r2_key_token = token

    # 4./5. Delete the old objects, purge their CDN URLs, and only then clear the
    #       orphan rows — any failure keeps the rows for cleanup_orphans to retry.
    #       On failure the ledger is re-recorded first: a cleanup run racing the
    #       pre-token window sees the plain key still resolving live, classifies
    #       the rows as re-adopted, and DROPS them — so they can't be assumed to
    #       still exist here (_record_orphan is get_or_create, so this is free
    #       when they do).
    def _reassert_ledger():
        for key in old_keys:
            _record_orphan(key, transcript.episode, reason=R2OrphanedObject.Reason.MOVE_REKEY)

    try:
        for old_key in old_keys:
            delete_media_object(old_key)
    except ClientError as exc:
        logger.warning(
            "transcript rekey: old-object delete failed for episode %d (orphan "
            "rows retained for retry): %s", episode_id, exc,
        )
        _reassert_ledger()
        return 'retry_pending'
    urls = [
        url for old_key in old_keys
        for url in _transcript_purge_urls(old_key, transcript.version)
    ]
    if not purge_urls(urls):
        logger.warning(
            "transcript rekey: CDN purge failed for episode %d (orphan rows "
            "retained for retry)", episode_id,
        )
        _reassert_ledger()
        return 'retry_pending'
    if old_keys:
        R2OrphanedObject.objects.filter(key__in=old_keys).delete()
    return 'rekeyed'


def rekey_transcripts(podcast_slug: str | None = None, podcast_id: int | None = None,
                      limit: int | None = None, apply: bool = False) -> dict:
    """Churn legacy (untokened) transcripts to keyed R2 locations (section E2).

    Scope: COMPLETED, version >= 1 (R2-backed), r2_key_token null — so the run is
    idempotent and resumable (tokened rows never re-enter the queryset). Optional
    podcast scoping (slug or id) and a --limit-style stop after N tokens set.
    Default is a dry run reporting the candidate episode ids; apply=True executes.
    """
    from pod_manager.models import Transcript

    if not settings.R2_MEDIA_ENABLED:
        raise RuntimeError("R2_MEDIA_ENABLED is off — transcripts are not R2-backed here.")

    qs = (
        Transcript.objects
        .filter(status=Transcript.Status.COMPLETED, version__gte=1,
                r2_key_token__isnull=True)
        .select_related('episode')
        .order_by('episode_id')
    )
    if podcast_slug:
        qs = qs.filter(episode__podcast__slug=podcast_slug)
    if podcast_id:
        qs = qs.filter(episode__podcast_id=podcast_id)

    if not apply:
        ids = list(qs.values_list('episode_id', flat=True))
        if limit is not None:
            ids = ids[:limit]
        return {'applied': False, 'candidates': ids,
                'rekeyed': 0, 'retry_pending': 0, 'errors': 0}

    client = get_r2_client()
    bucket = settings.R2_MEDIA_BUCKET
    counts = {'rekeyed': 0, 'retry_pending': 0, 'errors': 0}
    for transcript in qs.iterator():
        tokened = counts['rekeyed'] + counts['retry_pending']
        if limit is not None and tokened >= limit:
            break
        status = _rekey_one_transcript(client, bucket, transcript)
        counts['errors' if status == 'error' else status] += 1
    logger.info(
        "transcript rekey: rekeyed=%d retry_pending=%d errors=%d (podcast=%s limit=%s)",
        counts['rekeyed'], counts['retry_pending'], counts['errors'],
        podcast_slug or podcast_id or 'ALL', limit,
    )
    return {'applied': True, 'candidates': [], **counts}


# ---------------------------------------------------------------------------
# Re-key on move (section J)
# ---------------------------------------------------------------------------

def rekey_episode_audio(episode_id: int) -> dict:
    """Relocate an episode's R2 object to its CURRENT-parent key.

    The filename (stem + content hash) is byte-identical and preserved; only the
    network_id/podcast_id folder prefix changes. Server-side CopyObject (no
    egress), then commit r2_url and record the old key as a 'move_rekey' orphan
    (short grace — a byte-identical copy already exists at the new key)."""
    from pod_manager.models import Episode, R2OrphanedObject

    episode = Episode.objects.select_related("podcast", "podcast__network").get(pk=episode_id)
    if not episode.r2_url:
        return {"status": "skipped", "reason": "no r2_url"}

    old_key = key_from_public_url(episode.r2_url)
    filename = old_key.rsplit("/", 1)[-1]
    new_key = prefixed_key(f"{episode.podcast.network_id}/{episode.podcast_id}/{filename}")
    if new_key == old_key:
        return {"status": "noop", "key": old_key}

    client = get_r2_client()
    bucket = settings.R2_BUCKET
    client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": old_key},
        Key=new_key,
    )
    if not _object_exists(client, bucket, new_key):
        raise RuntimeError(f"rekey copy verify failed for {new_key}")

    new_url = public_url(new_key)
    with transaction.atomic():
        episode.r2_url = new_url
        episode.save(update_fields=["r2_url"])
        _clear_orphan_row(new_key)
        if not _key_referenced_by_other(old_key, episode_id):
            _record_orphan(old_key, episode, reason=R2OrphanedObject.Reason.MOVE_REKEY)
    logger.info("r2 rekey: ep %d %s -> %s", episode_id, old_key, new_key)

    _rekey_local_source_audio(old_key, new_key, episode_id)
    return {"status": "rekeyed", "old_key": old_key, "new_key": new_key}


def _rekey_local_source_audio(old_key: str, new_key: str, episode_id: int) -> None:
    """Relocate the local WHISPER_KEEP_SOURCE_AUDIO copy alongside the R2 move so
    the on-disk mirror keeps parity with R2. Best-effort: the R2 object is the
    source of truth, so a local FS hiccup must never fail the rekey. No-op in prod
    (retention is dev-only) and whenever the file isn't present."""
    try:
        from pod_manager.services.transcription import source_audio_path_for_key
        old_local = source_audio_path_for_key(old_key)
        new_local = source_audio_path_for_key(new_key)
        if old_local == new_local or not old_local.exists():
            return
        new_local.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(old_local), str(new_local))
        logger.info("r2 rekey: moved local source audio %s -> %s", old_local, new_local)
    except Exception as exc:
        logger.warning(
            "r2 rekey: local source-audio move failed for ep %d (continuing) — %s",
            episode_id, exc,
        )


# ---------------------------------------------------------------------------
# Dev purge
# ---------------------------------------------------------------------------

def purge_dev_prefix(dry_run: bool = False) -> dict:
    """Hard-delete every object under the dev/ prefix (disposable test data).

    dry_run lists what would be deleted and removes nothing."""
    client = get_r2_client()
    bucket = settings.R2_BUCKET
    keys = [k for k, _ in _iter_bucket_objects(client, bucket, prefix=DEV_PREFIX)]
    if keys and not dry_run:
        _batch_delete(client, bucket, keys)
    logger.info("r2 purge dev/: %s=%d", "would_delete" if dry_run else "deleted", len(keys))
    return {"deleted": 0 if dry_run else len(keys), "keys": keys, "dry_run": dry_run}


def purge_media_dev_prefix(dry_run: bool = False) -> dict:
    """Hard-delete every object under dev/ in the vecto-cdn (media) bucket —
    disposable test avatars / covers / transcripts.

    Uses the hardcoded DEV_PREFIX (never R2_MEDIA_KEY_PREFIX, which is "" in prod)
    so it can only ever touch the dev namespace, even if run against prod creds.
    dry_run lists what would be deleted and removes nothing.
    """
    client = get_r2_client()
    bucket = settings.R2_MEDIA_BUCKET
    keys = [k for k, _ in _iter_bucket_objects(client, bucket, prefix=DEV_PREFIX)]
    if keys and not dry_run:
        _batch_delete(client, bucket, keys)
    logger.info(
        "r2 purge media dev/: %s=%d (bucket=%s)",
        "would_delete" if dry_run else "deleted", len(keys), bucket,
    )
    return {"deleted": 0 if dry_run else len(keys), "keys": keys, "dry_run": dry_run}
