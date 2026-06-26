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
    against live Episode.r2_url at the last moment.

    move_rekey rows expire after R2_REKEY_GRACE_DAYS (byte-identical copy already
    lives at the new key); all other reasons after R2_ORPHAN_RETENTION_DAYS. A
    row whose key is referenced again at delete time was re-adopted — drop the
    row, keep the object. Default dry run."""
    from pod_manager.models import R2OrphanedObject

    now = timezone.now()
    referenced = _referenced_keys()
    rekey_cut = now - timedelta(days=settings.R2_REKEY_GRACE_DAYS)
    other_cut = now - timedelta(days=settings.R2_ORPHAN_RETENTION_DAYS)

    to_delete = []   # confirmed-unreferenced rows -> delete object + row
    readopted = []   # key referenced again -> drop row only
    for row in R2OrphanedObject.objects.all():
        cut = rekey_cut if row.reason == R2OrphanedObject.Reason.MOVE_REKEY else other_cut
        if row.orphaned_at > cut:
            continue  # not expired yet
        if row.key.startswith(DEV_PREFIX):
            continue  # cleanup never touches dev
        if row.key in referenced:
            readopted.append(row.id)
        else:
            to_delete.append(row)

    deleted_keys = [r.key for r in to_delete]
    if apply:
        if deleted_keys:
            client = get_r2_client()
            _batch_delete(client, settings.R2_BUCKET, deleted_keys)
            R2OrphanedObject.objects.filter(id__in=[r.id for r in to_delete]).delete()
        if readopted:
            R2OrphanedObject.objects.filter(id__in=readopted).delete()
    logger.info(
        "r2 cleanup: deleted=%d readopted=%d applied=%s",
        len(deleted_keys), len(readopted), apply,
    )
    return {"deleted": deleted_keys, "readopted": len(readopted), "applied": apply}


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

def purge_dev_prefix() -> dict:
    """Hard-delete every object under the dev/ prefix (disposable test data)."""
    client = get_r2_client()
    bucket = settings.R2_BUCKET
    keys = [k for k, _ in _iter_bucket_objects(client, bucket, prefix=DEV_PREFIX)]
    if keys:
        _batch_delete(client, bucket, keys)
    logger.info("r2 purge dev/: deleted=%d", len(keys))
    return {"deleted": len(keys)}


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
