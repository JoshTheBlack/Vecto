"""Transcribe a single episode synchronously (no Celery, IDE-safe).

Runs the full transcription pipeline inline for one episode and reports success or
the error. A single-target action — running it *is* the intent, so it takes no
--apply/--yes flag.

    python manage.py transcribe_episode 1234
"""

from django.core.management.base import BaseCommand

from pod_manager.services.transcription import run_transcription


class Command(BaseCommand):
    help = 'Transcribe a single episode synchronously. Works without Celery (IDE-safe).'

    def add_arguments(self, parser):
        parser.add_argument('episode_id', type=int, help='Primary key of the episode to transcribe')

    def handle(self, *args, **options):
        episode_id = options['episode_id']
        self.stdout.write(f"Starting transcription for episode {episode_id}…")
        try:
            run_transcription(episode_id)
            self.stdout.write(self.style.SUCCESS(f"Episode {episode_id} transcribed successfully."))
        except Exception as exc:
            self.stdout.write(self.style.ERROR(f"Transcription failed: {exc}"))
