"""Ingest a single podcast feed using its network's configured strategy.

Loads the network's ingester module (Network.ingester_module, falling back to
'default') and runs it against the given podcast, importing new episodes. A
single-target operational action — running it *is* the intent, so it takes no
--apply/--yes flag.

    python manage.py ingest_feed 42
"""

import importlib
from django.core.management.base import BaseCommand
from pod_manager.models import Podcast

class Command(BaseCommand):
    help = 'Ingests a podcast feed using the network-specific strategy.'

    def add_arguments(self, parser):
        parser.add_argument('podcast_id', type=int, help='The ID of the Podcast to ingest')
        parser.add_argument(
            '--force',
            action='store_true',
            help='Ingest even if the feed ETag/Last-Modified shows no change '
                 '(bypasses the conditional-request 304 skip).',
        )

    def handle(self, *args, **options):
        podcast_id = options['podcast_id']
        force = options['force']
        try:
            podcast = Podcast.objects.get(id=podcast_id)
        except Podcast.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Podcast ID {podcast_id} not found."))
            return

        module_name = podcast.network.ingester_module or 'default'

        try:
            # Dynamically load the script defined on the Network model
            ingester = importlib.import_module(f'pod_manager.ingesters.{module_name}')
            self.stdout.write(f"Using ingester strategy: '{module_name}'")
            ingester.run_ingest(podcast, self.stdout, force=force)
        except ImportError:
            self.stdout.write(self.style.WARNING(f"Ingester '{module_name}' not found. Falling back to 'default'."))
            ingester = importlib.import_module('pod_manager.ingesters.default')
            ingester.run_ingest(podcast, self.stdout, force=force)