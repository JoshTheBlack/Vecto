"""Delete LogEntry rows older than the retention window.

Trims the in-app log store (LogEntry) of rows older than LOG_RETENTION_DAYS
(default 30), or --days when given. Destructive and irreversible, so preview is
the default and an actual delete requires --apply --yes.

Note: the nightly Celery cleanup (task_prune_logs) deletes via the ORM directly,
not this command, so changing this command does not affect the scheduled prune.

    python manage.py prune_logs                  # preview rows older than retention
    python manage.py prune_logs --days 7         # preview with a 7-day window
    python manage.py prune_logs --apply --yes    # delete
"""

import logging
from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from pod_manager.models import LogEntry

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = ('Delete LogEntry rows older than LOG_RETENTION_DAYS (default 30) '
            '(preview by default; --apply --yes to delete).')

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=None,
            help='Override the retention window in days (ignores LOG_RETENTION_DAYS).',
        )
        parser.add_argument(
            '--apply', action='store_true',
            help='Perform the deletion (default: preview). Requires --yes.',
        )
        parser.add_argument(
            '--yes', action='store_true',
            help='Confirm the irreversible deletion (required with --apply).',
        )

    def handle(self, *args, **options):
        apply = options['apply']
        if apply and not options['yes']:
            raise CommandError('This permanently deletes log rows. Re-run with --apply --yes to confirm.')

        retention_days = options['days'] or getattr(settings, 'LOG_RETENTION_DAYS', 30)
        cutoff = timezone.now() - timedelta(days=retention_days)
        stale = LogEntry.objects.filter(created_at__lt=cutoff)

        if not apply:
            count = stale.count()
            self.stdout.write(
                f"Preview: {count} log entr{'y' if count == 1 else 'ies'} older than "
                f"{retention_days} days (before {cutoff:%Y-%m-%d %H:%M:%S} UTC) would be deleted."
            )
            if count:
                self.stdout.write("Re-run with --apply --yes to delete.")
            from pod_manager.admin_console.summary import emit_summary
            emit_summary(self.stdout, {"applied": False, "deleted": count, "retention_days": retention_days})
            return

        self.stdout.write(
            f"Pruning log entries older than {retention_days} days "
            f"(before {cutoff:%Y-%m-%d %H:%M:%S} UTC)…"
        )
        deleted, _ = stale.delete()

        if deleted:
            logger.info("prune_logs applied: deleted=%d retention_days=%d", deleted, retention_days)
            self.stdout.write(self.style.SUCCESS(f"Removed {deleted} log entries."))
        else:
            self.stdout.write(self.style.SUCCESS("Nothing to prune — no entries older than the cutoff."))

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {"applied": True, "deleted": deleted, "retention_days": retention_days})
