"""Hard-delete everything under the dev/ prefix in the vecto-cdn (media) bucket —
disposable test avatars / covers / transcripts.

The sibling of purge_r2_dev (which targets the audio bucket). Destructive and
irreversible, so an actual purge requires --yes; --dry-run lists what would go
and removes nothing. Only ever touches the dev/ namespace (the prefix is
hardcoded, never R2_MEDIA_KEY_PREFIX which is "" in prod), so prod keys are never
listed or deleted.

    python manage.py purge_r2_media_dev --dry-run
    python manage.py purge_r2_media_dev --yes
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import DEV_PREFIX, purge_media_dev_prefix


class Command(BaseCommand):
    help = f"Delete every object under '{DEV_PREFIX}' in the vecto-cdn bucket. Requires --yes (or --dry-run)."

    def add_arguments(self, parser):
        parser.add_argument("--yes", action="store_true", help="Confirm the destructive purge.")
        parser.add_argument("--dry-run", action="store_true", help="List what would be deleted; remove nothing.")

    def handle(self, *args, **options):
        dry_run = options["dry_run"]
        if not (options["yes"] or dry_run):
            raise CommandError(
                f"This deletes ALL objects under '{DEV_PREFIX}' in the media bucket. "
                "Re-run with --dry-run to preview or --yes to confirm."
            )

        result = purge_media_dev_prefix(dry_run=dry_run)
        keys = result["keys"]
        if dry_run:
            for k in keys:
                self.stdout.write(f"  would delete: {k}")
            self.stdout.write(self.style.WARNING(
                f"DRY RUN: {len(keys)} object(s) under '{DEV_PREFIX}' would be deleted. Nothing removed."
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f"Purged {result['deleted']} object(s) under '{DEV_PREFIX}' in the media bucket."
            ))
