"""Sweep MEDIA_ROOT/mix_covers and delete orphaned UserMix cover files.

A cover file is an orphan when no UserMix row references it. Destructive and
irreversible (files are removed from disk), so preview is the default and an
actual delete requires --apply --yes.

    python manage.py clean_mix_images                # preview: list orphans
    python manage.py clean_mix_images --apply --yes  # delete orphans
"""

import logging
import os

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import UserMix

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = ('Delete orphaned UserMix cover images not attached to the database '
            '(preview by default; --apply --yes to delete).')

    def add_arguments(self, parser):
        parser.add_argument('--apply', action='store_true',
                            help='Perform the deletion (default: preview). Requires --yes.')
        parser.add_argument('--yes', action='store_true',
                            help='Confirm the irreversible deletion (required with --apply).')

    def handle(self, *args, **options):
        apply = options['apply']
        if apply and not options['yes']:
            raise CommandError('This permanently deletes orphaned image files. '
                               'Re-run with --apply --yes to confirm.')

        media_root = settings.MEDIA_ROOT
        mix_covers_dir = os.path.join(media_root, 'mix_covers')

        if not os.path.exists(mix_covers_dir):
            self.stdout.write(self.style.WARNING("No mix_covers directory found. Nothing to clean."))
            from pod_manager.admin_console.summary import emit_summary
            emit_summary(self.stdout, {"applied": apply, "deleted": 0})
            return

        # 1. Ask the database what files ACTUALLY exist right now
        valid_files = set()
        for mix in UserMix.objects.exclude(image_upload=''):
            if mix.image_upload:
                # Get just the filename (e.g., '1234-5678.jpg')
                valid_files.add(os.path.basename(mix.image_upload.name))

        # 2. Iterate through the hard drive and delete anything not in the database
        deleted_count = 0
        for filename in os.listdir(mix_covers_dir):
            if filename in valid_files:
                continue
            file_path = os.path.join(mix_covers_dir, filename)
            if apply:
                try:
                    os.remove(file_path)
                    self.stdout.write(self.style.SUCCESS(f"  [DELETED] Orphaned file: {filename}"))
                    deleted_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  [ERROR] Failed to delete {filename}: {e}"))
            else:
                self.stdout.write(f"  [would delete] Orphaned file: {filename}")
                deleted_count += 1

        if deleted_count == 0:
            self.stdout.write(self.style.SUCCESS("Filesystem is perfectly clean. No orphaned images found."))
        elif apply:
            logger.info("clean_mix_images applied: deleted=%d", deleted_count)
            self.stdout.write(self.style.WARNING(f"Cleanup complete. Removed {deleted_count} orphaned images."))
        else:
            self.stdout.write(self.style.WARNING(
                f"Preview: {deleted_count} orphaned image(s) would be deleted. Re-run with --apply --yes."))

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {"applied": apply, "deleted": deleted_count})
