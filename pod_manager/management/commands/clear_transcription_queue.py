from django.core.management.base import BaseCommand

from pod_manager.models import Transcript
from pod_manager.services.transcription import purge_transcription_queue


class Command(BaseCommand):
    help = (
        "Purge the Celery 'transcription' queue on the broker and delete every "
        "PENDING Transcript row. Other queues and non-pending transcripts are "
        "left untouched."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--yes', action='store_true',
            help='Skip the confirmation prompt (for non-interactive use).',
        )

    def handle(self, *args, **options):
        pending = Transcript.objects.filter(status=Transcript.Status.PENDING).count()

        if not options['yes']:
            confirm = input(
                f"This purges the transcription queue and deletes {pending} pending "
                f"transcript(s). Type 'yes' to continue: "
            )
            if confirm.strip().lower() != 'yes':
                self.stdout.write(self.style.WARNING('Aborted — nothing changed.'))
                return

        result = purge_transcription_queue()
        purged = result['purged']
        purged_str = 'n/a (eager mode — no broker)' if purged is None else str(purged)
        self.stdout.write(self.style.SUCCESS(
            f"Purged {purged_str} queued task(s); "
            f"deleted {result['deleted']} pending transcript(s)."
        ))
