"""Backfill existing local transcript files to Cloudflare R2 (vecto-cdn).

Phase 3 of the user-asset CDN feature (see planned_features.txt, section F).
For each completed Transcript, this uploads the on-disk formats (vtt/json/srt/
html/words) to R2 at transcripts/{episode_id}.{ext} (ContentType + immutable
cache) and sets Transcript.version so the serve view + feed switch to the cdn.

The R2 key is DERIVED from id+ext, so no URL column is needed; the *_file
existence markers are kept as-is. Idempotent: a transcript already at version
>= 1 whose objects are present in R2 is skipped (--only-missing default).

    # rehearse — list what would move, change nothing (preview is the default)
    python manage.py backfill_transcripts_to_r2 --all

    # upload everything
    python manage.py backfill_transcripts_to_r2 --all --apply

    # scope + sample
    python manage.py backfill_transcripts_to_r2 --network baldmove --limit 10 --apply

    # re-upload rows already in R2 (bumps version)
    python manage.py backfill_transcripts_to_r2 --all --apply --force

    # MIGRATE -> VERIFY -> PRUNE gate: HEAD every expected object in R2 (read-only)
    python manage.py backfill_transcripts_to_r2 --all --verify

    # prune local files once verified — irreversible deletion, needs --apply --yes
    python manage.py backfill_transcripts_to_r2 --all --prune              # preview the prune
    python manage.py backfill_transcripts_to_r2 --all --prune --apply --yes

Requires R2_MEDIA_ENABLED=True (it lands objects in the cdn bucket).
"""

import json

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import Transcript
from pod_manager.services.transcription import (episode_recovery_metadata,
                                                transcript_path,
                                                transcript_r2_key)

_FORMATS = ['vtt', 'json', 'srt', 'html', 'words']
# Transcript.*_file field name per extension.
_MARKER = {ext: ('words_json_file' if ext == 'words' else f'{ext}_file') for ext in _FORMATS}


class Command(BaseCommand):
    help = "Backfill local transcript files to R2 (set Transcript.version)."

    def add_arguments(self, parser):
        parser.add_argument('--all', action='store_true', help='Every completed transcript.')
        parser.add_argument('--network', help='Restrict to a network slug.')
        parser.add_argument('--podcast', help='Restrict to a podcast slug.')
        parser.add_argument('--force', action='store_true',
                            help='Re-upload transcripts already in R2 (bumps version).')
        parser.add_argument('--only-missing', action='store_true',
                            help='Skip transcripts already present in R2 (this is the default).')
        parser.add_argument('--limit', type=int, default=None, help='Process at most N (sample latch).')
        parser.add_argument('--apply', action='store_true',
                            help='Perform the upload/prune (default is a preview that changes nothing).')
        parser.add_argument('--yes', action='store_true',
                            help='Confirm irreversible local deletion; required with --prune --apply.')
        parser.add_argument('--verify', action='store_true',
                            help='HEAD every expected object in R2 and report any missing (read-only prune gate).')
        parser.add_argument('--prune', action='store_true',
                            help='Delete local transcript files whose R2 objects are confirmed present '
                                 '(re-HEADs first; leaves the whisper_raw.txt). Previews unless --apply; '
                                 'deletion needs --apply --yes.')

    def handle(self, *args, **options):
        if not (options['all'] or options['network'] or options['podcast']):
            raise CommandError("Specify a scope: --all / --network=<slug> / --podcast=<slug>.")
        apply = options['apply']
        if options['prune'] and apply and not options['yes']:
            raise CommandError(
                "--prune --apply deletes local transcript files irreversibly. Re-run with --prune --apply --yes."
            )
        # R2 is touched whenever we upload (apply) or HEAD objects (verify/prune); a
        # plain preview lists targets only and needs nothing.
        needs_r2 = apply or options['verify'] or options['prune']
        if not settings.R2_MEDIA_ENABLED and needs_r2:
            raise CommandError("R2_MEDIA_ENABLED is False — enable it (and restart) before backfilling to R2.")

        qs = self._select(options)
        if options['verify']:
            return self._verify(qs)
        if options['prune']:
            return self._prune(qs, options)

        force, dry_run, limit = options['force'], not apply, options['limit']
        totals = {'migrated': 0, 'skipped': 0, 'failed': 0}
        count = 0
        for t in qs:
            if limit is not None and count >= limit:
                break
            count += 1
            res = self._migrate_one(t, force, dry_run)
            totals[res] = totals.get(res, 0) + 1

        self.stdout.write(self.style.SUCCESS(
            f"\nDone: {totals['migrated']} migrated, {totals['skipped']} skipped, "
            f"{totals['failed']} failed{' (dry-run)' if dry_run else ''}."
        ))

    # ------------------------------------------------------------------
    def _migrate_one(self, t, force, dry_run):
        from pod_manager.services.r2_storage import (media_object_exists,
                                                     put_media_object)
        from pod_manager.services.transcription import CONTENT_TYPES

        exts = [e for e in _FORMATS if getattr(t, _MARKER[e], None)]
        if not exts:
            self.stdout.write(self.style.WARNING(f"  ep {t.episode_id}: no format markers — skipping"))
            return 'skipped'

        # Already in R2 -> skip (presence-checked, not just version, so a row
        # written locally during a flag flip still gets pushed).
        if not force and (t.version or 0) >= 1:
            if all(media_object_exists(transcript_r2_key(t.episode_id, e)) for e in exts):
                return 'skipped'

        if dry_run:
            self.stdout.write(f"  would upload ep {t.episode_id}: {', '.join(exts)} -> transcripts/{t.episode_id}.*")
            return 'migrated'

        try:
            for ext in exts:
                # Read straight off local disk (version may already be >= 1, which
                # would route read_transcript_bytes at R2 where nothing exists).
                data = transcript_path(t.episode_id, ext).read_bytes()
                if ext == 'words':
                    data = self._enrich_words(data, t.episode)
                put_media_object(transcript_r2_key(t.episode_id, ext), data, CONTENT_TYPES[ext])
            t.version = (t.version or 0) + 1
            t.save(update_fields=['version'])
            self.stdout.write(self.style.SUCCESS(f"  uploaded ep {t.episode_id}: {', '.join(exts)} (v{t.version})"))
            return 'migrated'
        except FileNotFoundError as exc:
            self.stdout.write(self.style.ERROR(f"  FAILED ep {t.episode_id}: local file missing ({exc})"))
            return 'failed'
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"  FAILED ep {t.episode_id}: {exc}"))
            return 'failed'

    # ------------------------------------------------------------------
    def _enrich_words(self, data: bytes, episode) -> bytes:
        """Merge recovery metadata (title / GUIDs / audio_url) into a legacy
        .words file as it's uploaded, so existing transcripts get the same
        recovery header new ones write — no future second PUT needed."""
        try:
            doc = json.loads(data.decode('utf-8'))
        except Exception:
            return data  # not the JSON we recognize — upload as-is
        segments = doc.pop('segments', None)
        doc.update(episode_recovery_metadata(episode))
        if segments is not None:
            doc['segments'] = segments
        return json.dumps(doc, ensure_ascii=False, indent=2).encode('utf-8')

    # ------------------------------------------------------------------
    def _verify(self, qs):
        from pod_manager.services.r2_storage import media_object_exists
        present = missing = unmigrated = 0
        for t in qs:
            exts = [e for e in _FORMATS if getattr(t, _MARKER[e], None)]
            if (t.version or 0) < 1:
                unmigrated += 1
                self.stdout.write(self.style.WARNING(f"  not yet migrated: ep {t.episode_id} (version 0)"))
                continue
            for ext in exts:
                if media_object_exists(transcript_r2_key(t.episode_id, ext)):
                    present += 1
                else:
                    missing += 1
                    self.stdout.write(self.style.ERROR(f"  MISSING in R2: ep {t.episode_id}.{ext}"))
        style = self.style.SUCCESS if (missing == 0 and unmigrated == 0) else self.style.ERROR
        self.stdout.write(style(f"\nVerify: {present} present, {missing} missing, {unmigrated} not-yet-migrated."))
        if missing or unmigrated:
            raise CommandError("Verification failed — do NOT prune local transcript files yet.")

    # ------------------------------------------------------------------
    def _prune(self, qs, options):
        """Delete local transcript files only after re-confirming every format is
        in R2. Per-episode all-or-nothing: a partially-present episode is left
        intact. Leaves {id}.whisper_raw.txt for manual cleanup."""
        from pod_manager.services.r2_storage import media_object_exists
        dry_run = not options['apply']
        limit = options['limit']
        pruned = refused = skipped = 0
        count = 0
        for t in qs:
            if limit is not None and count >= limit:
                break
            count += 1
            exts = [e for e in _FORMATS if getattr(t, _MARKER[e], None)]
            if (t.version or 0) < 1:
                skipped += 1
                continue  # not migrated — nothing to prune against
            if not all(media_object_exists(transcript_r2_key(t.episode_id, e)) for e in exts):
                refused += 1
                self.stdout.write(self.style.ERROR(
                    f"  ep {t.episode_id}: not fully in R2 — refusing to prune"))
                continue
            for ext in exts:
                p = transcript_path(t.episode_id, ext)
                if not p.exists():
                    continue
                if dry_run:
                    self.stdout.write(f"  would prune {p}")
                else:
                    p.unlink()
                pruned += 1
        verb = 'would prune' if dry_run else 'pruned'
        self.stdout.write(self.style.SUCCESS(
            f"\nPrune: {verb} {pruned} local file(s); {refused} episode(s) refused (not fully in R2); "
            f"{skipped} not-migrated. whisper_raw.txt left in place."))

    # ------------------------------------------------------------------
    def _select(self, options):
        qs = Transcript.objects.filter(status=Transcript.Status.COMPLETED).select_related('episode')
        if options['network']:
            qs = qs.filter(episode__podcast__network__slug=options['network'])
        if options['podcast']:
            qs = qs.filter(episode__podcast__slug=options['podcast'])
        return qs.order_by('episode_id')
