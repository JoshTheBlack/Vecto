"""Purge the Celery 'transcription' queue and delete every PENDING Transcript row.

Destructive and irreversible (queued tasks and pending rows are dropped), so
preview is the default and an actual purge requires --apply --yes. Other queues
and non-pending transcripts are never touched.

    python manage.py clear_transcription_queue                # preview: count what would go
    python manage.py clear_transcription_queue --apply --yes  # purge
"""

import logging

from django.core.management.base import BaseCommand, CommandError

from pod_manager.models import Transcript
from pod_manager.services.transcription import purge_transcription_queue

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        "Purge the Celery 'transcription' queue on the broker and delete every "
        "PENDING Transcript row (preview by default; --apply --yes to purge). "
        "Other queues and non-pending transcripts are left untouched."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--apply', action='store_true',
            help='Perform the purge (default: preview). Requires --yes.',
        )
        parser.add_argument(
            '--yes', action='store_true',
            help='Confirm the irreversible purge (required with --apply).',
        )

    def handle(self, *args, **options):
        apply = options['apply']
        pending = Transcript.objects.filter(status=Transcript.Status.PENDING).count()

        if not apply:
            self.stdout.write(self.style.WARNING(
                f"Preview: would purge the transcription queue and delete {pending} pending "
                "transcript(s). Re-run with --apply --yes to perform the purge."
            ))
            from pod_manager.admin_console.summary import emit_summary
            emit_summary(self.stdout, {"applied": False, "pending_deleted": pending})
            return

        if not options['yes']:
            raise CommandError(
                f"This purges the transcription queue and deletes {pending} pending transcript(s) "
                "irreversibly. Re-run with --apply --yes to confirm."
            )

        result = purge_transcription_queue()
        purged = result['purged']
        purged_str = 'n/a (eager mode — no broker)' if purged is None else str(purged)
        logger.info("clear_transcription_queue applied: purged=%s deleted=%d",
                    purged_str, result['deleted'])
        self.stdout.write(self.style.SUCCESS(
            f"Purged {purged_str} queued task(s); "
            f"deleted {result['deleted']} pending transcript(s)."
        ))
        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "applied": True,
            "queued_purged": purged,
            "pending_deleted": result['deleted'],
        })
