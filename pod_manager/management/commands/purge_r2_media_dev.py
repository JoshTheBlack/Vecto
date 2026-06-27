"""Hard-delete everything under the dev/ prefix in the vecto-cdn (media) bucket —
disposable test avatars / covers / transcripts.

The sibling of purge_r2_dev (which targets the audio bucket). Destructive and
irreversible, so preview is the default (lists what would go) and an actual purge
requires --apply --yes. Only ever touches the dev/ namespace (the prefix is
hardcoded, never R2_MEDIA_KEY_PREFIX which is "" in prod), so prod keys are never
listed or deleted.

    python manage.py purge_r2_media_dev                # preview
    python manage.py purge_r2_media_dev --apply --yes  # delete
"""

import logging

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import DEV_PREFIX, purge_media_dev_prefix

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (f"Delete every object under '{DEV_PREFIX}' in the vecto-cdn bucket "
            "(preview by default; --apply --yes to delete).")

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Perform the purge (default: preview). Requires --yes.")
        parser.add_argument("--yes", action="store_true",
                            help="Confirm the irreversible purge (required with --apply).")

    def handle(self, *args, **options):
        apply = options["apply"]
        if apply and not options["yes"]:
            raise CommandError(
                f"This deletes ALL objects under '{DEV_PREFIX}' in the media bucket. "
                "Re-run with --apply --yes to confirm."
            )

        result = purge_media_dev_prefix(dry_run=not apply)
        keys = result["keys"]
        if apply:
            logger.info("purge_r2_media_dev applied: deleted=%d", result["deleted"])
            self.stdout.write(self.style.SUCCESS(
                f"Purged {result['deleted']} object(s) under '{DEV_PREFIX}' in the media bucket."
            ))
        else:
            for k in keys:
                self.stdout.write(f"  would delete: {k}")
            self.stdout.write(self.style.WARNING(
                f"Preview: {len(keys)} object(s) under '{DEV_PREFIX}' would be deleted. "
                "Re-run with --apply --yes."
            ))

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "applied": apply,
            "deleted": result["deleted"] if apply else len(keys),
        })
