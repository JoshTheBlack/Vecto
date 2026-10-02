"""Keep each transcript's .words recovery header in step with its episode.

Every ``.words`` object starts with a header — ``episode_id``, ``title``,
``guid_public``, ``guid_private``, ``audio_url`` — written when the transcript was
made. ``recover_transcripts`` matches orphaned objects to episodes by it, so it
should not drift: titles get edited, GUIDs and audio URLs change on feed polls and
merges. Hooking every one of those events would mean rewriting a ~2MB object (R2 can't
patch in place) from the request path and racing speaker-label replays, so instead:

  * ``Transcript.header_stamp`` records a hash of the five fields as last written;
  * a nightly task compares it with the episode's CURRENT values — in the database,
    zero R2 operations when nothing changed — and refreshes only the transcripts that
    differ, a bounded batch per run. It catches every source of change at once and
    coalesces many edits into one rewrite;
  * a merge also queues an immediate refresh for the surviving episode.

A refresh is NOT a re-transcription: it reads the ``.words`` object, replaces only the
header fields (segments stay last, so a ranged read of the first bytes still reaches
the whole header), and writes it back — one GET and, only if the header actually
differs, one PUT. No version bump: nothing renders from these fields, so the edge's
immutable copies being a header behind is harmless; the Redis byte cache entry is
cleared so a later speaker-label replay never rewrites a stale copy over the refresh.
It runs under the same Transcript row lock as the replay.
"""
import hashlib
import json
import logging

from django.conf import settings
from django.core.cache import cache
from django.db import transaction

logger = logging.getLogger(__name__)

HEADER_KEYS = ("episode_id", "title", "guid_public", "guid_private", "audio_url")


def header_fields(episode) -> dict:
    """The episode values that belong in the .words header."""
    from pod_manager.services.transcription import episode_recovery_metadata
    return {"episode_id": episode.id, **episode_recovery_metadata(episode)}


def _stamp(fields: dict) -> str:
    return hashlib.sha1(json.dumps(fields, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def header_stamp_for(episode) -> str:
    """Stable hash of header_fields(episode)."""
    return _stamp(header_fields(episode))


def _refreshed_document(doc: dict, fields: dict) -> dict:
    """``doc`` with the header fields replaced and "segments" kept LAST (new keys are
    appended to the header, not after the segments)."""
    header = {k: v for k, v in doc.items() if k != "segments"}
    header.update(fields)
    return {**header, "segments": doc.get("segments", [])}


def refresh_transcript_header(transcript_id: int, client=None) -> str:
    """Bring one transcript's header up to date. Returns
    'unchanged' (stamp already current — no R2 use) | 'stamped' (header was already
    right; stamp recorded) | 'refreshed' (header rewritten) | 'skipped' (not an
    R2-resident completed transcript) | 'error' (R2 failure; stamp untouched so the
    next run retries)."""
    from pod_manager.models import Transcript
    from pod_manager.services.r2_client import get_r2_client
    from pod_manager.services.r2_storage import media_object_key, put_media_object
    from pod_manager.services.transcription import CONTENT_TYPES, transcript_bytes_cache_key

    if not settings.R2_MEDIA_ENABLED:
        return "skipped"
    with transaction.atomic():
        # Same per-transcript lock apply_speaker_labels takes, so a refresh can't
        # interleave with a replay rewriting this file.
        try:
            transcript = Transcript.objects.select_for_update().get(pk=transcript_id)
        except Transcript.DoesNotExist:
            return "skipped"
        if (transcript.status != Transcript.Status.COMPLETED or not transcript.words_json_file
                or (transcript.version or 0) < 1):
            return "skipped"
        fields = header_fields(transcript.episode)
        stamp = _stamp(fields)
        if transcript.header_stamp == stamp:
            return "unchanged"

        key = transcript.r2_key("words")
        try:
            client = client or get_r2_client()
            raw = client.get_object(Bucket=settings.R2_MEDIA_BUCKET, Key=media_object_key(key))["Body"].read()
            doc = json.loads(raw)
            if not isinstance(doc, dict):
                raise ValueError("a .words document must be a JSON object")
            if all(doc.get(k) == v for k, v in fields.items()):
                status = "stamped"
            else:
                body = json.dumps(_refreshed_document(doc, fields), ensure_ascii=False, indent=2).encode("utf-8")
                put_media_object(key, body, CONTENT_TYPES["words"], client=client)
                cache.delete(transcript_bytes_cache_key(key, transcript.version))
                status = "refreshed"
        except Exception as exc:
            logger.warning("transcript header refresh failed for transcript %s (%s): %s", transcript_id, key, exc)
            return "error"
        Transcript.objects.filter(pk=transcript.pk).update(header_stamp=stamp)
    return status


def refresh_transcript_headers(*, network_slug=None, podcast_slug=None, limit=None,
                               apply=False, client=None) -> dict:
    """Refresh the header of every R2-resident transcript whose stamp no longer
    matches its episode. Finding the stale ones is a database-only comparison;
    R2 is touched only for those (and only ``limit`` of them per call). Dry run
    unless ``apply``."""
    from pod_manager.models import Transcript

    if not settings.R2_MEDIA_ENABLED:
        raise RuntimeError("R2_MEDIA_ENABLED is off — transcripts are not R2-backed here.")

    qs = (Transcript.objects
          .filter(status=Transcript.Status.COMPLETED, version__gte=1)
          .exclude(words_json_file__isnull=True).exclude(words_json_file="")
          .select_related("episode").order_by("pk"))
    if network_slug:
        qs = qs.filter(episode__podcast__network__slug=network_slug)
    if podcast_slug:
        qs = qs.filter(episode__podcast__slug=podcast_slug)

    checked, stale = 0, []
    for transcript in qs.iterator():
        checked += 1
        if transcript.header_stamp != header_stamp_for(transcript.episode):
            stale.append(transcript.pk)
    todo = stale[:limit] if limit is not None else stale

    report = {"applied": apply, "checked": checked, "stale": len(stale), "batch": len(todo),
              "stale_ids": todo, "refreshed": 0, "stamped": 0, "unchanged": 0,
              "skipped": 0, "errors": 0}
    if not apply:
        return report
    for transcript_id in todo:
        status = refresh_transcript_header(transcript_id, client=client)
        report["errors" if status == "error" else status] += 1
    logger.info("transcript headers: checked=%d stale=%d refreshed=%d stamped=%d errors=%d",
                checked, len(stale), report["refreshed"], report["stamped"], report["errors"])
    return report
