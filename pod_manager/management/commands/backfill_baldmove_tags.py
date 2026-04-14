import time
from django.core.management.base import BaseCommand
from django.db.models import Q
from pod_manager.models import Episode, Network

# Import the robust scraper directly from the ingester
from pod_manager.ingesters.baldmove import scrape_tags_from_wp

class Command(BaseCommand):
    help = 'Backfills tags for episodes by scraping their WordPress links.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--network', 
            type=str, 
            default='baldmove', 
            help='The slug of the network to process (default: baldmove)'
        )
        parser.add_argument(
            '--force', 
            action='store_true', 
            help='Process ALL episodes, even if they already have tags.'
        )

    def handle(self, *args, **options):
        network_slug = options['network']
        force = options['force']
        
        try:
            network = Network.objects.get(slug=network_slug)
        except Network.DoesNotExist:
            self.stderr.write(self.style.ERROR(f"Network '{network_slug}' not found."))
            return

        # Safely get episodes with a valid link (excluding both NULL and empty strings)
        episodes = Episode.objects.filter(podcast__network=network).exclude(link__isnull=True).exclude(link__exact='')
        
        if not force:
            # Safely check for empty tags across all DB backends (SQLite/Postgres)
            episodes = episodes.filter(Q(tags=[]) | Q(tags__isnull=True) | Q(tags=''))

        total = episodes.count()
        
        if total == 0:
            self.stdout.write(self.style.WARNING(f"Found 0 episodes to process. Try running with --force to see if tags are already populated."))
            return
            
        self.stdout.write(self.style.WARNING(f"Found {total} episodes to process for {network.name}..."))
        
        updated_count = 0

        for i, ep in enumerate(episodes, 1):
            if "baldmove.com" not in ep.link:
                continue

            # Pass the link and stdout to the imported scraper
            tags = scrape_tags_from_wp(ep.link, self.stdout)
            
            if tags:
                ep.tags = tags
                ep.save()
                updated_count += 1
                self.stdout.write(self.style.SUCCESS(f"[{i}/{total}] Updated '{ep.title}': {tags}"))
            else:
                self.stdout.write(f"[{i}/{total}] No tags found for '{ep.title}'")

            # Polite delay to avoid getting blocked by Cloudflare/WP
            time.sleep(0.3)

        self.stdout.write(self.style.SUCCESS(f"Finished backfilling. Successfully updated {updated_count} out of {total} episodes."))