"""Backfill `points` + `counter_deltas` onto historical APPROVED edits.

These two fields (added with the unified trust model) let a rollback reverse the
EXACT trust + per-counter award of an edit. Edits approved before they existed
have `points = 0` and an empty `counter_deltas`, so today they restore their data
on rollback but reverse no trust/counters. This one-time pass reconstructs the
award each APPROVED edit WOULD earn under the current scorer and banks it, so
legacy edits become fully reversible going forward.

Idempotent (SET, not increment) and previews unless `--apply`:

  * Metadata edits → ``metadata_changes(suggested, original)`` then
    ``score_contribution(...)`` (same scorer the live paths use). cross-publish is
    not scored; the multi-field bonus + first-responder are included exactly as a
    live approval would award them.
  * Speaker edits → per episode, replay the APPROVED speaker chain in resolved_at
    order and bank each edit's ``speaker_edit_points`` against the running fold
    (mirrors what the live approve handler computed from ``fold_speaker_mappings``).

It does NOT touch NetworkMembership aggregates — historical trust/counter totals
stand (re-crediting would double-count). It only records per-edit deltas so future
rollbacks are exact.

    python manage.py backfill_edit_points --all            # preview
    python manage.py backfill_edit_points --all --apply
    python manage.py backfill_edit_points --network baldmove --apply
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import EpisodeEditSuggestion
from pod_manager.services.edits import metadata_changes, score_contribution


class Command(BaseCommand):
    help = "Backfill points + counter_deltas onto historical APPROVED edits (exact rollback)."

    def add_arguments(self, parser):
        parser.add_argument('--all', action='store_true', help='Every network.')
        parser.add_argument('--network', help='Restrict to a network slug.')
        parser.add_argument('--podcast', help='Restrict to a podcast slug.')
        parser.add_argument('--apply', action='store_true',
                            help='Write the banked values (default previews only).')

    def handle(self, *args, **options):
        if not (options['all'] or options['network'] or options['podcast']):
            raise CommandError("Specify a scope: --all / --network=<slug> / --podcast=<slug>.")
        apply = options['apply']

        qs = EpisodeEditSuggestion.objects.filter(status=EpisodeEditSuggestion.Status.APPROVED)
        if options['network']:
            qs = qs.filter(episode__podcast__network__slug=options['network'])
        if options['podcast']:
            qs = qs.filter(episode__podcast__slug=options['podcast'])

        speaker = qs.filter(suggested_data__has_key='speaker_mappings')
        metadata = qs.exclude(suggested_data__has_key='speaker_mappings')

        changed = 0
        changed += self._backfill_metadata(metadata, apply)
        changed += self._backfill_speaker(speaker, apply)

        self.stdout.write(self.style.SUCCESS(
            f"\nbackfill_edit_points: {'updated' if apply else 'would update'} "
            f"{changed} edit(s){' (preview)' if not apply else ''}."
        ))
        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "mode": "backfill_edit_points", "applied": apply, "updated": changed,
        })

    # ------------------------------------------------------------------
    def _bank(self, edit, points, deltas, apply):
        """SET the edit's banked award if it differs. Returns 1 if changed."""
        if (edit.points or 0) == points and (edit.counter_deltas or {}) == deltas:
            return 0
        self.stdout.write(
            f"  edit #{edit.id} (ep {edit.episode_id}): "
            f"points {edit.points or 0}->{points}, counter_deltas {edit.counter_deltas or {}}->{deltas}"
        )
        if apply:
            edit.points = points
            edit.counter_deltas = deltas
            edit.save(update_fields=['points', 'counter_deltas'])
        return 1

    # ------------------------------------------------------------------
    def _backfill_metadata(self, qs, apply):
        changed = 0
        for edit in qs.iterator():
            changes = metadata_changes(edit.suggested_data or {}, edit.original_data or {})
            points, deltas = score_contribution(changes, is_first=edit.is_first_responder)
            changed += self._bank(edit, points, deltas, apply)
        return changed

    # ------------------------------------------------------------------
    def _backfill_speaker(self, qs, apply):
        from pod_manager.services.transcription import speaker_edit_points

        changed = 0
        prior = {}
        current_ep = None
        # Per episode, fold the APPROVED speaker chain in resolved_at order and bank
        # each edit's points against the fold BEFORE it (mirrors fold_speaker_mappings
        # at live approval time).
        for edit in qs.order_by('episode_id', 'resolved_at', 'id').iterator():
            if edit.episode_id != current_ep:
                current_ep = edit.episode_id
                prior = {}
            delta = (edit.suggested_data or {}).get('speaker_mappings') or {}
            points, _newly = speaker_edit_points(delta, prior)
            deltas = {'edits_speakers': points} if points else {}
            changed += self._bank(edit, points, deltas, apply)
            prior.update(delta)
        return changed
