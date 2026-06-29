"""Backfill the write-once ``speaker_id`` anchor onto existing transcripts.

Phase 6 of transcript_rollback.md (§6). New transcriptions are born with
``speaker_id`` (Phase 1); this is the one-time migration for the existing
catalogue. Lossless where the raw ASR dump survives, degraded-but-working where
it doesn't — no re-transcription either way.

Per completed transcript (preview unless ``--apply``):

  * Skip if the existing ``.words`` schema is already ``1.1.0`` (idempotent
    re-runs).
  * If ``{id}.whisper_raw.txt`` exists → re-parse via ``_parse_whisper_response``
    to recover the pristine ``SPEAKER_XX`` per segment/word as ``speaker_id``
    (write-once), fold the episode's APPROVED speaker edits to set the resolved
    ``speaker``, PRESERVE the existing ``.words`` header (transcribed_at, model,
    language, recovery anchors — only ADD speaker_id), and re-render all 5 formats.
  * Fallback when the raw dump is missing → seed ``speaker_id`` from the current
    resolved ``speaker`` (split == combined; no un-merge until re-transcribed).

Writes back to wherever the transcript currently lives: local ``MEDIA_ROOT``
files in place for the majority (free disk writes, NO R2 I/O — the separate
``backfill_transcripts_to_r2`` pushes them later), and R2 in place with the §4
idempotent hash-check (``write_transcript_formats``) for the few already mirrored
(``version >= 1``).

Because the fold EXCLUDES ``ROLLED_BACK`` edits, names may change on the handful
of test-rollback episodes whose files still bake in a since-reverted edit; every
such episode is logged (before → after) for post-run review.

Also (``--apply``) retroactively recomputes each ``NetworkMembership.edits_speakers``
from the historical approved chain (§6 "Retroactively credit edits_speakers"):
the counter is SET (not incremented) so the pass is idempotent and independent of
the per-episode ``1.1.0`` skip. Trust score is NOT re-credited (the historical
award stands).

    # rehearse — change nothing (preview is the default)
    python manage.py backfill_speaker_ids --all

    # convert + recompute counters
    python manage.py backfill_speaker_ids --all --apply

    # scope + sample
    python manage.py backfill_speaker_ids --network baldmove --limit 10 --apply
"""

import json
import logging
from collections import defaultdict

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import EpisodeEditSuggestion, NetworkMembership, Transcript
from pod_manager.services.transcription import (
    CONTENT_TYPES,
    _parse_whisper_response,
    _to_html,
    _to_podcast_index_json,
    _to_srt,
    _to_vtt,
    _to_words_json,
    fold_speaker_mappings,
    read_transcript_bytes,
    speaker_edit_points,
    transcript_path,
    write_transcript_formats,
)

logger = logging.getLogger(__name__)

_FORMATS = ['vtt', 'json', 'srt', 'html', 'words']
_MARKER = {ext: ('words_json_file' if ext == 'words' else f'{ext}_file') for ext in _FORMATS}


def _schema_at_least_110(doc: dict) -> bool:
    """True if a parsed ``.words`` doc is already at schema ``1.1.0`` or newer
    (i.e. carries ``speaker_id``) — the idempotency marker the backfill skips."""
    try:
        parts = tuple(int(x) for x in str(doc.get('version') or '0').split('.')[:3])
    except ValueError:
        return False
    return parts >= (1, 1, 0)


def _segments_from_words_doc(doc: dict) -> list:
    """Convert on-disk ``.words`` segments (startTime/endTime/body) into the
    internal renderer shape (start/end/text), seeding ``speaker_id`` from the
    existing id or, pre-backfill, the resolved ``speaker``. The resolved
    ``speaker`` is preserved verbatim — this is the degraded fallback when the
    raw ASR dump is gone, so there is no distinct base to recover."""
    segments = []
    for seg in doc.get('segments', []):
        sid = seg.get('speaker_id') or seg.get('speaker')
        words = []
        for w in seg.get('words', []):
            wid = w.get('speaker_id') or w.get('speaker')
            words.append({
                'word':       w.get('word', ''),
                'start':      w.get('startTime'),
                'end':        w.get('endTime'),
                'score':      w.get('score'),
                'speaker_id': wid,
                'speaker':    w.get('speaker') if w.get('speaker') is not None else wid,
            })
        segments.append({
            'start':      seg.get('startTime'),
            'end':        seg.get('endTime'),
            'text':       seg.get('body', ''),
            'speaker_id': sid,
            'speaker':    seg.get('speaker') if seg.get('speaker') is not None else sid,
            'words':      words,
        })
    return segments


def _resolve_from_base(segments: list, mapping: dict) -> None:
    """Set each segment/word ``speaker`` = ``mapping.get(speaker_id, speaker_id)``
    in place (the speaker_id base is never rewritten). Used by the whisper_raw
    path so the recovered pristine labels resolve through the approved chain."""
    for seg in segments:
        sid = seg.get('speaker_id')
        if sid is not None:
            seg['speaker'] = mapping.get(sid, sid)
        for w in seg.get('words', None) or []:
            wid = w.get('speaker_id')
            if wid is not None:
                w['speaker'] = mapping.get(wid, wid)


def _distinct_names(segments: list) -> list:
    return sorted({s.get('speaker') for s in segments if s.get('speaker')})


class Command(BaseCommand):
    help = "Backfill speaker_id onto existing transcripts (+ retroactive edits_speakers)."

    def add_arguments(self, parser):
        parser.add_argument('--all', action='store_true', help='Every completed transcript.')
        parser.add_argument('--network', help='Restrict to a network slug.')
        parser.add_argument('--podcast', help='Restrict to a podcast slug.')
        parser.add_argument('--episode', type=int, help='Restrict to a single episode id.')
        parser.add_argument('--limit', type=int, default=None, help='Process at most N transcripts.')
        parser.add_argument('--apply', action='store_true',
                            help='Perform the rewrite + counter recompute (default previews only).')
        parser.add_argument('--skip-recompute', action='store_true',
                            help='Skip the retroactive edits_speakers recompute pass.')

    def handle(self, *args, **options):
        if not (options['all'] or options['network'] or options['podcast'] or options['episode']):
            raise CommandError("Specify a scope: --all / --network=<slug> / --podcast=<slug> / --episode=<id>.")
        apply = options['apply']
        # R2 is only touched for transcripts already resident there (version >= 1)
        # AND only when R2 is enabled; the local-first majority needs nothing.
        if apply and settings.R2_MEDIA_ENABLED:
            self.stdout.write(self.style.WARNING(
                "R2_MEDIA_ENABLED=True — already-mirrored transcripts (version>=1) "
                "will be hash-checked + PUT in place; local ones stay on disk."
            ))

        qs = self._select(options)
        totals = {'backfilled': 0, 'seeded': 0, 'skipped': 0, 'failed': 0}
        name_changes = []
        count = 0
        for t in qs:
            if options['limit'] is not None and count >= options['limit']:
                break
            count += 1
            res, change = self._backfill_one(t, apply)
            totals[res] = totals.get(res, 0) + 1
            if change:
                name_changes.append(change)

        self.stdout.write(self.style.SUCCESS(
            f"\nTranscripts: {totals['backfilled']} backfilled (raw), "
            f"{totals['seeded']} seeded (fallback), {totals['skipped']} skipped, "
            f"{totals['failed']} failed{' (preview)' if not apply else ''}."
        ))
        if name_changes:
            self.stdout.write(self.style.WARNING(
                f"\n{len(name_changes)} episode(s) changed resolved names "
                f"(ROLLED_BACK reconciliation — review):"
            ))
            for ep_id, before, after in name_changes:
                self.stdout.write(f"  ep {ep_id}: {before} -> {after}")

        recomputed = 0
        if not options['skip_recompute']:
            recomputed = self._recompute_edits_speakers(apply, options)

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "mode": "backfill_speaker_ids",
            "applied": apply,
            "backfilled": totals['backfilled'],
            "seeded": totals['seeded'],
            "skipped": totals['skipped'],
            "failed": totals['failed'],
            "name_changes": len(name_changes),
            "memberships_recomputed": recomputed,
        })

    # ------------------------------------------------------------------
    def _backfill_one(self, t, apply):
        """Returns (result, name_change_or_None) where result is one of
        'backfilled' | 'seeded' | 'skipped' | 'failed' and name_change is
        (episode_id, before_names, after_names) when the resolved names move."""
        episode_id = t.episode_id

        # Read the current .words (local for the majority, R2 for version>=1).
        try:
            current_bytes = read_transcript_bytes(episode_id, 'words', t.version)
            current_doc = json.loads(current_bytes.decode('utf-8'))
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"  ep {episode_id}: .words unreadable — skipping ({exc})"))
            return 'failed', None

        if _schema_at_least_110(current_doc):
            return 'skipped', None

        # Preserve the existing header verbatim; only speaker_id is added below.
        header = {k: v for k, v in current_doc.items() if k not in ('segments', 'version')}
        fallback_language = header.get('language') or t.language or 'en'

        raw_path = transcript_path(episode_id, 'vtt').parent / f'{episode_id}.whisper_raw.txt'
        if raw_path.exists():
            try:
                segments, _lang = _parse_whisper_response(
                    raw_path.read_text(encoding='utf-8'), fallback_language,
                )
            except Exception as exc:
                self.stdout.write(self.style.ERROR(
                    f"  ep {episode_id}: whisper_raw unparseable — skipping ({exc})"))
                return 'failed', None
            mapping = fold_speaker_mappings(episode_id)
            _resolve_from_base(segments, mapping)
            header['speaker_mappings'] = mapping  # refresh the resolved-mapping cache
            result = 'backfilled'
        else:
            # Degraded fallback: seed speaker_id from the current resolved speaker,
            # which is preserved as-is (no fold — its keys wouldn't match a name).
            segments = _segments_from_words_doc(current_doc)
            result = 'seeded'

        # Name-change detection (whisper_raw path only ever moves names): the fold
        # excludes ROLLED_BACK edits, so a file still baking in a reverted edit
        # reconciles here. Logged for post-run review (§6).
        before = _distinct_names(_segments_from_words_doc(current_doc))
        after = _distinct_names(segments)
        change = (episode_id, before, after) if before != after else None

        rendered = [
            ('vtt',   _to_vtt(segments)),
            ('json',  _to_podcast_index_json(segments)),
            ('srt',   _to_srt(segments)),
            ('html',  _to_html(segments)),
            ('words', _to_words_json(segments, metadata=header)),
        ]

        if not apply:
            self.stdout.write(
                f"  would {result} ep {episode_id} "
                f"({'raw' if result == 'backfilled' else 'fallback'})"
                + (f"  names {change[1]} -> {change[2]}" if change else "")
            )
            return result, change

        try:
            self._write_back(t, rendered)
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"  FAILED ep {episode_id}: {exc}"))
            return 'failed', None

        self.stdout.write(self.style.SUCCESS(
            f"  {result} ep {episode_id}"
            + (self.style.WARNING(f"  names {change[1]} -> {change[2]}") if change else "")
        ))
        return result, change

    # ------------------------------------------------------------------
    def _write_back(self, t, rendered):
        """Write the rendered formats back to where the transcript lives.

        R2-resident (version>=1 + R2 enabled) → §4 idempotent hash-check
        (write_transcript_formats), bump version only on real change. Otherwise
        (the local-first majority) → write the MEDIA_ROOT files in place; do NOT
        push to R2 here (backfill_transcripts_to_r2 does that later) and do NOT
        bump version (version>=1 would misroute reads to a non-existent R2 object).
        """
        episode_id = t.episode_id
        is_r2_resident = settings.R2_MEDIA_ENABLED and (t.version or 0) >= 1
        if is_r2_resident:
            written, changed = write_transcript_formats(episode_id, rendered)
            if changed:
                for ext in _FORMATS:
                    setattr(t, _MARKER[ext], written[ext])
                t.version = (t.version or 0) + 1
                t.save(update_fields=[*( _MARKER[e] for e in _FORMATS), 'version'])
        else:
            for ext, content in rendered:
                p = transcript_path(episode_id, ext)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(content)

    # ------------------------------------------------------------------
    def _recompute_edits_speakers(self, apply, options):
        """Retroactively SET each NetworkMembership.edits_speakers from the
        historical approved speaker-edit chain (§6). Idempotent (set, not
        increment); trust score untouched. Replays each user's APPROVED
        (non-SUPERSEDED, non-ROLLED_BACK — those are distinct statuses) speaker
        edits per episode in resolved_at order, folding the user's own deltas as
        it goes and summing speaker_edit_points against the prior fold."""
        memberships = NetworkMembership.objects.select_related('user', 'network')
        if options['network']:
            memberships = memberships.filter(network__slug=options['network'])
        elif options['podcast']:
            memberships = memberships.filter(network__podcasts__slug=options['podcast']).distinct()

        changed = 0
        for m in memberships.iterator():
            edits = (EpisodeEditSuggestion.objects
                     .filter(user=m.user,
                             episode__podcast__network=m.network,
                             status=EpisodeEditSuggestion.Status.APPROVED,
                             suggested_data__has_key='speaker_mappings')
                     .order_by('episode_id', 'resolved_at', 'id'))
            folds = defaultdict(dict)  # per-episode running fold of THIS user's deltas
            total = 0
            for e in edits:
                delta = (e.suggested_data or {}).get('speaker_mappings') or {}
                prior = folds[e.episode_id]
                pts, _newly = speaker_edit_points(delta, prior)
                total += pts
                prior.update(delta)
            if m.edits_speakers != total:
                self.stdout.write(
                    f"  membership {m.user.username}@{m.network.slug}: "
                    f"edits_speakers {m.edits_speakers} -> {total}"
                )
                if apply:
                    m.edits_speakers = total
                    m.save(update_fields=['edits_speakers'])
                changed += 1

        verb = 'recomputed' if apply else 'would recompute'
        self.stdout.write(self.style.SUCCESS(
            f"\nedits_speakers: {verb} {changed} membership(s){' (preview)' if not apply else ''}."
        ))
        return changed

    # ------------------------------------------------------------------
    def _select(self, options):
        qs = Transcript.objects.filter(status=Transcript.Status.COMPLETED).select_related('episode')
        if options['episode']:
            qs = qs.filter(episode_id=options['episode'])
        if options['network']:
            qs = qs.filter(episode__podcast__network__slug=options['network'])
        if options['podcast']:
            qs = qs.filter(episode__podcast__slug=options['podcast'])
        return qs.order_by('episode_id')
