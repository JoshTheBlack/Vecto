"""Hard-delete everything under the R2 dev/ prefix (disposable test data).

Destructive and irreversible. Preview is the default (lists what would go); pass
--apply --yes to actually delete. Only touches the dev/ namespace; prod keys are
never listed or deleted.

    python manage.py purge_r2_dev                # preview: list what would be deleted
    python manage.py purge_r2_dev --apply --yes  # delete
"""

import logging

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import DEV_PREFIX, purge_dev_prefix

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (f"Delete every object under the R2 '{DEV_PREFIX}' prefix "
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
                f"This deletes ALL objects under '{DEV_PREFIX}'. Re-run with --apply --yes to confirm."
            )
        result = purge_dev_prefix(dry_run=not apply)
        if apply:
            logger.info("purge_r2_dev applied: deleted=%d", result["deleted"])
            self.stdout.write(self.style.SUCCESS(
                f"Purged {result['deleted']} object(s) under '{DEV_PREFIX}'."))
        else:
            for k in result["keys"]:
                self.stdout.write(f"  would delete: {k}")
            self.stdout.write(self.style.WARNING(
                f"Preview: {len(result['keys'])} object(s) under '{DEV_PREFIX}' would be deleted. "
                "Re-run with --apply --yes."))
