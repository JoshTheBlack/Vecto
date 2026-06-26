"""Backfill existing local images (avatars + mix covers) to Cloudflare R2.

Phase 2 of the user-asset CDN feature (see planned_features.txt, section F).
For each row whose image field still points at a legacy LOCAL key, this:

    1. reads the existing local bytes,
    2. processes them to WebP (so backfilled images match the live save() path),
    3. writes ONCE to R2 at the new stable key (avatars/{user}-{network}.webp,
       covers/{uuid}.webp) via the field's storage,
    4. updates the DB: sets the field .name to the stable key and image_version=1
       so display_* resolves to "<cdn>/<key>?v=1".

It is a RE-KEY, so the DB must change; the OLD local file is left in place until
the post-cutover prune. Idempotent: rows already at a stable webp key are skipped
(--only-missing, the default), which makes a partial run safely resumable.

    # rehearse — list what would move, change nothing
    python manage.py backfill_media_to_r2 --all --dry-run

    # migrate everything (avatars + covers)
    python manage.py backfill_media_to_r2 --all

    # one asset class, small sample first
    python manage.py backfill_media_to_r2 --avatars --limit 5

    # re-process rows already migrated (bumps image_version)
    python manage.py backfill_media_to_r2 --all --force

    # MIGRATE -> VERIFY -> PRUNE gate: HEAD every migrated object in R2
    python manage.py backfill_media_to_r2 --all --verify

R2 must be reachable (R2_MEDIA_ENABLED=True) to land objects in the cdn bucket;
with it disabled the same re-key runs against the local OverwriteStorage, which
is a useful dev rehearsal.
"""

from io import BytesIO
from pathlib import Path

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import NetworkMembership, NetworkMix, UserMix
from pod_manager.services.images import process_image_field

# (model, field name, processed max px, stable-key prefix, human label).
_ASSET_SPECS = {
    'avatars': (NetworkMembership, 'custom_image_upload', 256, 'avatars/', 'avatar'),
    'covers_user': (UserMix, 'image_upload', 500, 'covers/', 'user-mix cover'),
    'covers_network': (NetworkMix, 'image_upload', 500, 'covers/', 'network-mix cover'),
}


class Command(BaseCommand):
    help = "Backfill local avatars + mix covers to R2 (re-key to stable WebP keys)."

    def add_arguments(self, parser):
        parser.add_argument('--avatars', action='store_true', help='Include custom avatars.')
        parser.add_argument('--covers', action='store_true', help='Include user + network mix covers.')
        parser.add_argument('--all', action='store_true', help='Include every asset class (default if none chosen).')
        parser.add_argument('--force', action='store_true',
                            help='Re-process + re-upload rows already at a stable key (bumps image_version).')
        parser.add_argument('--only-missing', action='store_true',
                            help='Skip rows already re-keyed to a stable webp key (this is the default).')
        parser.add_argument('--limit', type=int, default=None,
                            help='Process at most N rows per asset class (a sample latch).')
        parser.add_argument('--dry-run', action='store_true', help='List targets; change nothing.')
        parser.add_argument('--verify', action='store_true',
                            help='HEAD every migrated object in R2 and report any missing (prune gate).')
        parser.add_argument('--prune', action='store_true',
                            help='Delete the local copy of a stable-key image whose R2 object is confirmed '
                                 'present (re-HEADs first). Skips legacy-keyed rows (still served from /media '
                                 'via the transition branch). Honors --dry-run.')

    def handle(self, *args, **options):
        which = self._selected_specs(options)
        if options['verify']:
            return self._verify(which, options)
        if options['prune']:
            return self._prune(which, options)

        force = options['force']
        dry_run = options['dry_run']
        limit = options['limit']

        totals = {'migrated': 0, 'skipped': 0, 'failed': 0}
        for model, field, max_px, prefix, label in which:
            self.stdout.write(self.style.MIGRATE_HEADING(f"\n{label}s ({model.__name__}.{field}):"))
            count = 0
            for instance in self._rows_with_image(model, field):
                if limit is not None and count >= limit:
                    break
                count += 1
                res = self._migrate_one(instance, field, max_px, prefix, force, dry_run)
                totals[res] = totals.get(res, 0) + 1

        self.stdout.write(self.style.SUCCESS(
            f"\nDone: {totals['migrated']} migrated, {totals['skipped']} skipped, "
            f"{totals['failed']} failed{' (dry-run)' if dry_run else ''}."
        ))

    # ------------------------------------------------------------------
    def _migrate_one(self, instance, field, max_px, prefix, force, dry_run):
        ff = getattr(instance, field)
        name = ff.name or ''

        # Already re-keyed AND actually present in storage -> skip unless forced.
        # A stable-key NAME alone is not proof the bytes reached R2: a row written
        # to local disk while R2 was disabled, then re-enabled, holds a stable
        # name with no R2 object. So verify the object exists before skipping;
        # otherwise fall through and (re)upload it from the local bytes.
        if name.startswith(prefix) and not force:
            try:
                if ff.storage.exists(name):
                    return 'skipped'
            except Exception:
                pass  # existence check failed -> be safe and (re)upload

        # Read the source bytes straight off local disk: the field's storage is
        # now R2, so ff.open()/read() would hit R2 (where nothing exists yet).
        local_path = Path(settings.MEDIA_ROOT) / name
        if not local_path.exists():
            self.stdout.write(self.style.WARNING(
                f"  MISSING local file for {instance!r} ({name or '<empty>'}) - skipping"))
            return 'failed'

        if dry_run:
            self.stdout.write(f"  would migrate {instance!r}: {name} -> {prefix}<stable>.webp")
            return 'migrated'

        try:
            data = local_path.read_bytes()
            webp = process_image_field(BytesIO(data), max_px)
            # upload_to computes the stable key from the instance, ignoring this
            # filename; storage.file_overwrite replaces any existing object.
            ff.save('img.webp', ContentFile(webp), save=False)
            instance.image_version = max(instance.image_version or 0, 0) + 1
            instance.save(update_fields=[field, 'image_version'])
            self.stdout.write(self.style.SUCCESS(f"  migrated {instance!r}: {name} -> {ff.name} (v{instance.image_version})"))
            return 'migrated'
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"  FAILED {instance!r} ({name}): {exc}"))
            return 'failed'

    # ------------------------------------------------------------------
    def _verify(self, which, options):
        present = missing = unmigrated = 0
        for model, field, _max_px, prefix, label in which:
            for instance in self._rows_with_image(model, field):
                ff = getattr(instance, field)
                name = ff.name or ''
                if not name.startswith(prefix):
                    unmigrated += 1
                    self.stdout.write(self.style.WARNING(f"  not yet migrated: {instance!r} ({name})"))
                    continue
                # storage.exists() HEADs the object (location prefix added by it).
                if ff.storage.exists(name):
                    present += 1
                else:
                    missing += 1
                    self.stdout.write(self.style.ERROR(f"  MISSING in R2: {instance!r} ({name})"))
        style = self.style.SUCCESS if (missing == 0 and unmigrated == 0) else self.style.ERROR
        self.stdout.write(style(
            f"\nVerify: {present} present, {missing} missing, {unmigrated} not-yet-migrated."
        ))
        if missing or unmigrated:
            raise CommandError("Verification failed — do NOT prune local files yet.")

    # ------------------------------------------------------------------
    def _prune(self, which, options):
        """Delete the local copy of a stable-key image once its R2 object is
        re-confirmed present. Legacy-keyed rows are skipped — they're still
        served from /media via the transition branch until it's removed."""
        dry_run = options['dry_run']
        pruned = refused = skipped = 0
        for model, field, _max_px, prefix, label in which:
            for instance in self._rows_with_image(model, field):
                ff = getattr(instance, field)
                name = ff.name or ''
                if not name.startswith(prefix):
                    skipped += 1   # legacy key — leave it for the transition branch
                    continue
                if not ff.storage.exists(name):
                    refused += 1
                    self.stdout.write(self.style.ERROR(
                        f"  NOT in R2 — refusing to prune {instance!r} ({name})"))
                    continue
                local = Path(settings.MEDIA_ROOT) / name
                if not local.exists():
                    continue   # nothing local to prune (e.g. prod wrote straight to R2)
                if dry_run:
                    self.stdout.write(f"  would prune {local}")
                else:
                    local.unlink()
                pruned += 1
        verb = 'would prune' if dry_run else 'pruned'
        self.stdout.write(self.style.SUCCESS(
            f"\nPrune: {verb} {pruned} local file(s); {refused} refused (not in R2); "
            f"{skipped} legacy-keyed (left for the transition branch)."))

    # ------------------------------------------------------------------
    def _rows_with_image(self, model, field):
        return model.objects.exclude(**{f'{field}': ''}).exclude(**{f'{field}__isnull': True}).iterator()

    def _selected_specs(self, options):
        if options['all'] or not (options['avatars'] or options['covers']):
            keys = ['avatars', 'covers_user', 'covers_network']
        else:
            keys = []
            if options['avatars']:
                keys.append('avatars')
            if options['covers']:
                keys += ['covers_user', 'covers_network']
        return [_ASSET_SPECS[k] for k in keys]
