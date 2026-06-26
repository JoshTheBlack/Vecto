"""Queue (or preview) transcription for every eligible episode in bulk.

Eligible = has subscriber audio AND has no transcript yet, or only a FAILED one.
Already-queued/completed episodes are skipped, so the command is safe to re-run.
Preview is the default: with no flags it lists what it *would* queue and dispatches
nothing; pass --apply to actually dispatch. In IDE mode (settings.IS_IDE) --apply
runs each transcription synchronously instead of via Celery.

    python manage.py backfill_transcripts                       # preview everything eligible
    python manage.py backfill_transcripts --apply               # queue everything eligible
    python manage.py backfill_transcripts --podcast watchmen --apply
    python manage.py backfill_transcripts --apply --stagger 60  # 60s Celery countdown between dispatches
    python manage.py backfill_transcripts --apply --model large --language es

Whisper overrides (--model / --language / --initial-prompt / --*-speakers) fall
back to the network/podcast setting when omitted.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

from pod_manager.models import Episode, Podcast, Transcript


class Command(BaseCommand):
    help = (
        'Queue transcription for all eligible episodes '
        '(has subscriber audio, no completed/pending/processing transcript). '
        'Preview by default; pass --apply to dispatch. Safe to re-run.'
    )

    def add_arguments(self, parser):
        parser.add_argument(
            '--podcast', dest='podcasts', action='append', metavar='SLUG',
            help='Podcast slug to backfill (repeat for multiple). Omit to process all.',
        )
        parser.add_argument(
            '--stagger', type=int, default=30, metavar='SECONDS',
            help='Celery countdown seconds between dispatches (default: 30). Ignored in IDE mode.',
        )
        parser.add_argument(
            '--apply', action='store_true',
            help='Queue the transcription tasks (default is a preview that dispatches nothing).',
        )
        parser.add_argument(
            '--model', default=None, metavar='MODEL',
            help='Whisper model override (e.g. large). Default: use network/podcast setting.',
        )
        parser.add_argument(
            '--language', default=None, metavar='LANG',
            help='Language code override (e.g. es). Default: use network/podcast setting.',
        )
        parser.add_argument(
            '--initial-prompt', dest='initial_prompt', default=None, metavar='TEXT',
            help='Vocabulary hint override for Whisper. Default: use network/podcast setting.',
        )
        parser.add_argument(
            '--min-speakers', dest='min_speakers', type=int, default=None,
            help='Min speaker count override. Default: use network/podcast setting.',
        )
        parser.add_argument(
            '--num-speakers', dest='num_speakers', type=int, default=None,
            help='Expected speaker count override. Default: use network/podcast setting.',
        )
        parser.add_argument(
            '--max-speakers', dest='max_speakers', type=int, default=None,
            help='Max speaker count override. Default: use network/podcast setting.',
        )

    def handle(self, *args, **options):
        from django.conf import settings
        from pod_manager.services.transcription import run_transcription, route_transcription
        from pod_manager.tasks import transcribe_episode

        podcast_slugs = options['podcasts'] or []
        stagger       = options['stagger']
        dry_run       = not options['apply']

        transcription_kwargs = {}
        for key in ('model', 'language', 'initial_prompt'):
            val = options.get(key)
            if val:
                transcription_kwargs[key] = val
        for key in ('min_speakers', 'num_speakers', 'max_speakers'):
            val = options.get(key)
            if val is not None:
                transcription_kwargs[key] = val

        if podcast_slugs:
            found   = set(Podcast.objects.filter(slug__in=podcast_slugs).values_list('slug', flat=True))
            missing = set(podcast_slugs) - found
            if missing:
                raise CommandError(f"Unknown podcast slug(s): {', '.join(sorted(missing))}")

        # Episodes eligible: has subscriber audio AND no transcript OR only a failed one
        episodes = (
            Episode.objects
            .filter(audio_url_subscriber__isnull=False)
            .exclude(audio_url_subscriber='')
            .filter(
                Q(transcript__isnull=True) | Q(transcript__status=Transcript.Status.FAILED)
            )
            .select_related('podcast__network')
            .order_by('pub_date')
        )

        if podcast_slugs:
            episodes = episodes.filter(podcast__slug__in=podcast_slugs)

        total = episodes.count()

        if total == 0:
            self.stdout.write(self.style.SUCCESS(
                'Nothing to do — all eligible episodes already have completed or in-progress transcripts.'
            ))
            return

        noun = 'episode' if total == 1 else 'episodes'
        self.stdout.write(f"Found {total} {noun} to {'preview' if dry_run else 'queue'}.\n")

        for i, ep in enumerate(episodes, 1):
            label = f"[{i}/{total}] {ep.podcast.slug} / ep {ep.pk}: {ep.title}"
            if dry_run:
                self.stdout.write(f"  [DRY RUN] {label}")
                continue

            if settings.IS_IDE:
                self.stdout.write(f"  Transcribing {label}…")
                try:
                    run_transcription(ep.pk, **transcription_kwargs)
                    self.stdout.write(self.style.SUCCESS(f"  ✓ Done"))
                except Exception as exc:
                    self.stdout.write(self.style.ERROR(f"  ✗ Failed: {exc}"))
            else:
                countdown = (i - 1) * stagger
                queue, priority = route_transcription(ep, model=transcription_kwargs.get('model'))
                transcribe_episode.apply_async(args=[ep.pk], kwargs=transcription_kwargs, countdown=countdown, queue=queue, priority=priority)
                self.stdout.write(f"  Queued {label} (countdown={countdown}s, queue={queue})")

        if not dry_run:
            if settings.IS_IDE:
                self.stdout.write(self.style.SUCCESS(f'\nDone. {total} episode(s) transcribed synchronously.'))
            else:
                self.stdout.write(self.style.SUCCESS(
                    f'\nDone. {total} episode(s) queued with {stagger}s stagger '
                    f'(total spread: ~{(total - 1) * stagger}s).'
                ))
