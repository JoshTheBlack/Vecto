"""Hard-delete everything under the R2 dev/ prefix (disposable test data).

Destructive and irreversible, so it requires --yes. Only touches the dev/
namespace; prod keys are never listed or deleted.

    python manage.py purge_r2_dev --yes
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import DEV_PREFIX, purge_dev_prefix


class Command(BaseCommand):
    help = f"Delete every object under the R2 '{DEV_PREFIX}' prefix. Requires --yes."

    def add_arguments(self, parser):
        parser.add_argument("--yes", action="store_true", help="Confirm the destructive purge.")

    def handle(self, *args, **options):
        if not options["yes"]:
            raise CommandError(f"This deletes ALL objects under '{DEV_PREFIX}'. Re-run with --yes to confirm.")
        result = purge_dev_prefix()
        self.stdout.write(self.style.SUCCESS(f"Purged {result['deleted']} object(s) under '{DEV_PREFIX}'."))
