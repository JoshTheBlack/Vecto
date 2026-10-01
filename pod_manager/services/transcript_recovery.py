"""Recover transcripts from what is already in the R2 bucket.

After a database loss/rebuild or a re-import, episode ids change, so Transcript rows
may be missing — or point at objects that are there but under another stem — while
every transcript object is still sitting in the vecto-cdn bucket. Each ``.words``
object carries a recovery header (written at transcription time, see
``episode_recovery_metadata``): ``episode_id``, ``title``, ``guid_public``,
``guid_private``, ``audio_url``, ``language``, ``model``, ``transcribed_at``. This
module re-attaches those objects to their episodes WITHOUT moving or renaming any
object: because ``Transcript.r2_key_stem`` is stored, recovery just records the stem
it finds.

What it does, per transcript stem found in the bucket that no Transcript row claims:

  1. read the ``.words`` header (a small ranged GET — the header precedes the
     segments, so the multi-MB body is never downloaded for matching);
  2. match it to an episode, strongest evidence first
       EXACT   guid_private / guid_public equals the episode's
       HIGH    audio_url equals the episode's subscriber audio, or the header's
               episode_id is a real episode id AND the titles agree
       MEDIUM  the normalized title is unique among the episodes in scope
     More than one candidate at the strongest level that has any is AMBIGUOUS and
     is never auto-linked;
  3. create the row (or fill an empty placeholder, or repoint a row whose objects
     are gone) at the found stem, marking the formats that exist.

Never touched: a transcript whose objects exist and are claimed by a row, an episode
that already has a live transcript at another stem (reported as a conflict), and
stems on the orphan ledger (slated for deletion by cleanup).
"""
import json
import logging
import re
import string
from dataclasses import dataclass

from django.conf import settings
from django.core.cache import cache
from django.db import transaction
from django.utils import timezone
from django.utils.dateparse import parse_datetime

logger = logging.getLogger(__name__)

# The header is the first few hundred bytes to a few KB (it precedes "segments");
# 16KB covers even a large speaker_mappings. A header that doesn't fit falls back to
# one full read.
HEADER_WINDOW = 16 * 1024

CONFIDENCE_RANK = {"medium": 1, "high": 2, "exact": 3}

_TOKEN_RE = r"(?P<token>[A-Za-z0-9_-]{16,32})"
_PLACEHOLDER_RE = {
    "bucket": r"\d+",
    "episode_id": r"\d+",
    "network_slug": r"[A-Za-z0-9_-]+",
    "podcast_slug": r"[A-Za-z0-9_-]+",
}


@dataclass
class Match:
    episode_id: int
    confidence: str          # 'exact' | 'high' | 'medium'
    reason: str              # what matched, for the report


# ---------------------------------------------------------------------------
# Reading the bucket
# ---------------------------------------------------------------------------

def scan_transcript_bucket(client) -> dict:
    """{stem: {'formats': {ext: bare_key}, 'modified': datetime|None}} for every
    transcript object in the media bucket. Keys come back with this environment's
    prefix (``R2_MEDIA_KEY_PREFIX``, ``dev/`` in IDE); they are returned BARE, which
    is the form the stored stem uses."""
    from pod_manager.services.r2_maintenance import (TRANSCRIPTS_PREFIX, _iter_bucket_objects,
                                                     _split_transcript_key)
    from pod_manager.services.r2_storage import media_object_key

    env_prefix = settings.R2_MEDIA_KEY_PREFIX or ""
    found: dict = {}
    for key, modified in _iter_bucket_objects(client, settings.R2_MEDIA_BUCKET,
                                              prefix=media_object_key(TRANSCRIPTS_PREFIX)):
        bare = key[len(env_prefix):] if env_prefix and key.startswith(env_prefix) else key
        split = _split_transcript_key(bare)
        if split is None:
            continue
        stem, ext = split
        entry = found.setdefault(stem, {"formats": {}, "modified": None})
        entry["formats"][ext] = bare
        if ext == "words":
            entry["modified"] = modified
    return found


def parse_words_header(data: bytes):
    """The metadata header of a ``.words`` document from its leading bytes, or None
    if "segments" isn't reached (header larger than the window / not a words file).
    The writer emits ``{"version", <metadata...>, "segments": [...]}``, so everything
    before the "segments" key is the header."""
    text = data.decode("utf-8", errors="ignore")
    idx = text.find('\n  "segments"')
    if idx == -1:
        idx = text.find('"segments"')
    if idx == -1:
        return None
    head = text[:idx].rstrip()
    if head.endswith(","):
        head = head[:-1]
    try:
        header = json.loads(head + "\n}")
    except ValueError:
        return None
    return header if isinstance(header, dict) else None


def read_words_header(client, bare_key: str):
    """The recovery header of one ``.words`` object, or None if unreadable."""
    from pod_manager.services.r2_storage import media_object_key
    bucket, key = settings.R2_MEDIA_BUCKET, media_object_key(bare_key)
    try:
        data = client.get_object(Bucket=bucket, Key=key,
                                 Range=f"bytes=0-{HEADER_WINDOW - 1}")["Body"].read()
        header = parse_words_header(data)
        if header is None and len(data) >= HEADER_WINDOW:
            doc = json.loads(client.get_object(Bucket=bucket, Key=key)["Body"].read())
            doc.pop("segments", None)
            header = doc if isinstance(doc, dict) else None
        return header
    except Exception as exc:                       # missing / not JSON / network
        logger.warning("recover_transcripts: unreadable header %s: %s", bare_key, exc)
        return None


def read_words_text(client, bare_key: str):
    """Plain text of a ``.words`` object (for Transcript.transcript_text search), or
    None when it can't be read — the link still goes ahead without it."""
    from pod_manager.services.r2_storage import media_object_key
    try:
        doc = json.loads(client.get_object(
            Bucket=settings.R2_MEDIA_BUCKET, Key=media_object_key(bare_key))["Body"].read())
        return " ".join((seg.get("body") or "").strip() for seg in doc.get("segments", [])).strip()
    except Exception as exc:
        logger.warning("recover_transcripts: could not read text of %s: %s", bare_key, exc)
        return None


def token_from_stem(stem: str, patterns) -> str | None:
    """The r2_key_token embedded in ``stem`` under the first pattern that fits it, or
    None (an untokened/legacy stem, or one no known pattern describes). A row without
    a token is treated as legacy, so the default `rekey_transcripts` run re-tokens it
    — the safe outcome for a stem we can't take a secret token from."""
    for pattern in patterns:
        regex, seen_token = "", False
        try:
            parts = list(string.Formatter().parse(pattern))
        except ValueError:
            continue
        for literal, field, _, _ in parts:
            regex += re.escape(literal)
            if field == "token":
                regex += "(?P=token)" if seen_token else _TOKEN_RE
                seen_token = True
            elif field in _PLACEHOLDER_RE:
                regex += _PLACEHOLDER_RE[field]
            elif field is not None:
                regex = None
                break
        if regex is None or not seen_token:
            continue
        m = re.fullmatch(regex, stem)
        if m:
            return m.group("token")
    return None


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------

def _norm_title(title) -> str:
    return " ".join((title or "").casefold().split())


class EpisodeIndex:
    """In-memory lookup of the episodes a recovery run may attach transcripts to."""

    def __init__(self, queryset):
        self.rows = {}
        self.by_guid_public, self.by_guid_private = {}, {}
        self.by_audio, self.by_title = {}, {}
        for row in queryset.values("id", "title", "guid_public", "guid_private",
                                   "audio_url_subscriber",
                                   "podcast__network__transcript_key_pattern"):
            self.rows[row["id"]] = row
            for index, value in ((self.by_guid_public, row["guid_public"]),
                                 (self.by_guid_private, row["guid_private"]),
                                 (self.by_audio, row["audio_url_subscriber"]),
                                 (self.by_title, _norm_title(row["title"]))):
                if value:
                    index.setdefault(value, set()).add(row["id"])

    def patterns_for(self, episode_id):
        from pod_manager.services.transcript_keys import DEFAULT_KEY_PATTERN
        custom = self.rows[episode_id]["podcast__network__transcript_key_pattern"]
        return [custom, DEFAULT_KEY_PATTERN] if custom else [DEFAULT_KEY_PATTERN]


def match_header(header: dict, index: EpisodeIndex):
    """('match', Match) | ('ambiguous', [episode ids]) | ('none', None).

    Walks the evidence levels strongest-first and stops at the first level that
    produces any candidate: one candidate is a match, several are ambiguous."""
    guid_hits = set()
    for guid, lookup in ((header.get("guid_private"), index.by_guid_private),
                         (header.get("guid_public"), index.by_guid_public)):
        if guid:
            guid_hits |= lookup.get(guid, set())
    audio = header.get("audio_url")
    audio_hits = set(index.by_audio.get(audio, set())) if audio else set()

    header_id = header.get("episode_id")
    id_hits = set()
    if isinstance(header_id, int) and header_id in index.rows and header.get("title") and \
            _norm_title(index.rows[header_id]["title"]) == _norm_title(header["title"]):
        id_hits = {header_id}

    title_hits = set(index.by_title.get(_norm_title(header.get("title")), set()))

    levels = (("exact", "guid", guid_hits),
              ("high", "audio url", audio_hits),
              ("high", "episode id + title", id_hits),
              ("medium", "title", title_hits))
    for confidence, reason, hits in levels:
        if len(hits) == 1:
            return "match", Match(next(iter(hits)), confidence, reason)
        if len(hits) > 1:
            return "ambiguous", sorted(hits)
    return "none", None


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def _apply_link(client, index, episode_id, stem, entry, header, existing):
    """Create / fill / repoint the Transcript for one confident match. Everything
    else about the row is derived from the header and the objects found; nothing in
    the bucket is touched."""
    from pod_manager.models import Transcript

    marker_field = {"vtt": "vtt_file", "json": "json_file", "srt": "srt_file",
                    "html": "html_file", "words": "words_json_file"}
    completed_at = (parse_datetime(header.get("transcribed_at") or "")
                    or entry.get("modified") or timezone.now())
    if timezone.is_naive(completed_at):
        completed_at = timezone.make_aware(completed_at)
    audio_url = header.get("audio_url") or None
    fields = {
        "status": Transcript.Status.COMPLETED,
        "language": (header.get("language") or "en")[:10],
        "whisper_model_used": (header.get("model") or "")[:50],
        "completed_at": completed_at,
        "error_message": None,
        "source_audio_url": audio_url if audio_url and len(audio_url) <= 2000 else None,
        "r2_key_stem": stem,
        "r2_key_token": token_from_stem(stem, index.patterns_for(episode_id)),
        "transcript_text": read_words_text(client, entry["formats"]["words"]),
        **{field: entry["formats"].get(ext) for ext, field in marker_field.items()},
    }
    with transaction.atomic():
        if existing is None:
            Transcript.objects.create(episode_id=episode_id, version=1,
                                      requested_at=completed_at, **fields)
        else:
            # Past versions' ?v=N URLs may still be cached at the edge; moving on to
            # the next version means the recovered bytes are fetched fresh.
            Transcript.objects.filter(pk=existing.pk).update(
                version=(existing.version or 0) + 1, **fields)
    cache.delete(f"ep_frag_public_{episode_id}")
    cache.delete(f"ep_frag_private_{episode_id}")


def recover_transcripts(*, network_slug=None, podcast_slug=None, min_confidence="high",
                        limit=None, apply=False, purge_cdn=False, client=None) -> dict:
    """Scan the media bucket and re-attach orphaned transcripts to their episodes.

    Dry run unless ``apply``. Scope (network/podcast) limits which EPISODES may be
    matched; bucket objects that match none in scope are reported as unmatched, which
    for a scoped run includes other networks' transcripts. ``limit`` stops after N
    links. ``purge_cdn`` additionally purges the recovered objects' ?v=N URLs from
    the Cloudflare edge (past versions may be cached)."""
    from pod_manager.models import Episode, R2OrphanedObject, Transcript
    from pod_manager.services.cloudflare import purge_urls
    from pod_manager.services.r2_client import get_r2_client
    from pod_manager.services.r2_maintenance import (_FALLBACK_PURGE_VERSIONS, TRANSCRIPTS_PREFIX,
                                                     _split_transcript_key, _transcript_purge_urls)

    if min_confidence not in CONFIDENCE_RANK:
        raise ValueError(f"min_confidence must be one of {sorted(CONFIDENCE_RANK)}")
    if not settings.R2_MEDIA_ENABLED:
        raise RuntimeError("R2_MEDIA_ENABLED is off — there is no media bucket to recover from.")

    client = client or get_r2_client()
    scan = scan_transcript_bucket(client)

    episodes = Episode.objects.all()
    if network_slug:
        episodes = episodes.filter(podcast__network__slug=network_slug)
    if podcast_slug:
        episodes = episodes.filter(podcast__slug=podcast_slug)
    index = EpisodeIndex(episodes)

    claimed = set(Transcript.objects.exclude(r2_key_stem__isnull=True)
                  .values_list("r2_key_stem", flat=True))
    ledger = set()
    for key in R2OrphanedObject.objects.filter(key__startswith=TRANSCRIPTS_PREFIX).values_list("key", flat=True):
        split = _split_transcript_key(key)
        if split:
            ledger.add(split[0])

    report = {"applied": apply, "mode": "recover", "scanned": len(scan),
              "already_linked": sum(1 for stem in scan if stem in claimed),
              "ledger_skipped": sorted(stem for stem in scan if stem in ledger and stem not in claimed),
              "matches": [], "skipped_low_confidence": [], "ambiguous": [], "unmatched": [],
              "unreadable": [], "no_words": [], "conflicts": [],
              "recovered": 0, "errors": 0, "purged": None}

    to_purge, claimed_episodes = [], {}
    for stem in sorted(scan):
        if stem in claimed or stem in ledger:
            continue
        entry = scan[stem]
        if "words" not in entry["formats"]:
            report["no_words"].append(stem)
            continue
        if limit is not None and len(report["matches"]) >= limit:
            break

        header = read_words_header(client, entry["formats"]["words"])
        if header is None:
            report["unreadable"].append(stem)
            continue
        outcome, found = match_header(header, index)
        info = {"stem": stem, "title": header.get("title"),
                "guid_public": header.get("guid_public"), "guid_private": header.get("guid_private")}
        if outcome == "none":
            report["unmatched"].append(info)
            continue
        if outcome == "ambiguous":
            report["ambiguous"].append({**info, "episode_ids": found})
            continue
        if CONFIDENCE_RANK[found.confidence] < CONFIDENCE_RANK[min_confidence]:
            report["skipped_low_confidence"].append(
                {**info, "episode_id": found.episode_id, "confidence": found.confidence, "reason": found.reason})
            continue
        if found.episode_id in claimed_episodes:
            report["ambiguous"].append({**info, "episode_ids": [found.episode_id],
                                        "note": f"also claimed by {claimed_episodes[found.episode_id]}"})
            continue

        existing = Transcript.objects.filter(episode_id=found.episode_id).first()
        if existing is None:
            action = "create"
        elif existing.r2_key_stem and existing.r2_key_stem in scan and \
                (existing.words_json_file or existing.vtt_file):
            report["conflicts"].append({**info, "episode_id": found.episode_id,
                                        "live_stem": existing.r2_key_stem})
            continue
        elif existing.status != Transcript.Status.COMPLETED or not any(
                (existing.vtt_file, existing.json_file, existing.srt_file,
                 existing.html_file, existing.words_json_file)):
            action = "fill"
        else:
            action = "relink"                  # the row's own objects are gone from the bucket

        claimed_episodes[found.episode_id] = stem
        record = {**info, "episode_id": found.episode_id, "confidence": found.confidence,
                  "reason": found.reason, "action": action}
        report["matches"].append(record)
        if not apply:
            continue
        try:
            _apply_link(client, index, found.episode_id, stem, entry, header, existing)
        except Exception:
            logger.exception("recover_transcripts: failed to link %s -> episode %s", stem, found.episode_id)
            record["action"] = "error"
            report["errors"] += 1
            continue
        report["recovered"] += 1
        for bare_key in entry["formats"].values():
            to_purge.extend(_transcript_purge_urls(bare_key, _FALLBACK_PURGE_VERSIONS))

    if apply and purge_cdn and to_purge:
        report["purged"] = purge_urls(to_purge)
    logger.info("recover_transcripts: scanned=%d linked_already=%d matched=%d recovered=%d "
                "ambiguous=%d unmatched=%d conflicts=%d errors=%d applied=%s",
                report["scanned"], report["already_linked"], len(report["matches"]),
                report["recovered"], len(report["ambiguous"]), len(report["unmatched"]),
                len(report["conflicts"]), report["errors"], apply)
    return report
