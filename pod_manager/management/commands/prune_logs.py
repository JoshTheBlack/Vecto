from datetime import timedelta

from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import timezone

from pod_manager.models import LogEntry


class Command(BaseCommand):
    help = 'Deletes LogEntry rows older than LOG_RETENTION_DAYS (default 30).'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=None,
            help='Override the retention window in days (ignores LOG_RETENTION_DAYS).',
        )

    def handle(self, *args, **options):
        retention_days = options['days'] or getattr(settings, 'LOG_RETENTION_DAYS', 30)
        cutoff = timezone.now() - timedelta(days=retention_days)

        self.stdout.write(f"Pruning log entries older than {retention_days} days (before {cutoff:%Y-%m-%d %H:%M:%S} UTC)…")
        deleted, _ = LogEntry.objects.filter(created_at__lt=cutoff).delete()

        if deleted:
            self.stdout.write(self.style.SUCCESS(f"Removed {deleted} log entries."))
        else:
            self.stdout.write(self.style.SUCCESS("Nothing to prune — no entries older than the cutoff."))
