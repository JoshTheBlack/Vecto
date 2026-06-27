"""Queue WordPress post IDs for parallel discovery/ingest via Celery.

Walks a contiguous range of baldmove.com WordPress post IDs and, for any not
already ingested, dispatches ingest_wp_post_task to crawl and import it into the
"Unsorted Ingest" bin. Authentication cookies are required to reach gated posts.

WARNING — very heavy: the default range queues on the order of 100k Celery
tasks. Run it from a shell deliberately, with a scoped --start/--end range.

    python manage.py crawl_by_id --cookie_name <name> --cookie_value <value>
    python manage.py crawl_by_id --start 34 --end 500 --cookie_name <n> --cookie_value <v>
"""

from django.core.management.base import BaseCommand
from pod_manager.models import Podcast, Network, Episode
from pod_manager.tasks import ingest_wp_post_task

class Command(BaseCommand):
    help = 'Queue WP IDs for parallel discovery via Celery (Postgres optimized)'

    def add_arguments(self, parser):
        parser.add_argument('--start', type=int, default=34,
                            help='First WordPress post ID to crawl (inclusive). Default: 34.')
        parser.add_argument('--end', type=int, default=109880,
                            help='Last WordPress post ID to crawl (inclusive). Default: 109880.')
        parser.add_argument('--cookie_name', type=str, required=True,
                            help='Name of the auth cookie used to fetch gated WordPress posts.')
        parser.add_argument('--cookie_value', type=str, required=True,
                            help='Value of the auth cookie (a credential) used to fetch gated posts.')

    def handle(self, *args, **options):
        # Assign to the first available network
        default_network = Network.objects.first()
        if not default_network:
            self.stdout.write(self.style.ERROR("No Network found."))
            return

        # Setup "Unsorted Ingest" bin
        ingest_podcast, _ = Podcast.objects.get_or_create(
            title="Unsorted Ingest",
            network=default_network,
            defaults={'slug': 'unsorted-ingest'}
        )

        self.stdout.write(f"Queueing IDs {options['start']} to {options['end']}...")
        
        count = 0
        for post_id in range(options['start'], options['end'] + 1):
            guid_url = f"https://baldmove.com/?p={post_id}"
            
            # Quick local check to save Redis/RabbitMQ space
            if not Episode.objects.filter(guid_private=guid_url).exists():
                ingest_wp_post_task.delay(
                    post_id, 
                    options['cookie_name'], 
                    options['cookie_value'], 
                    ingest_podcast.id
                )
                count += 1
        
        self.stdout.write(self.style.SUCCESS(f"Queued {count} tasks. Processing in background."))