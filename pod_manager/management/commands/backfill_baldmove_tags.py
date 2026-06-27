"""Backfill episode tags by scraping each episode's WordPress (baldmove.com) link.

For every episode in the chosen network that has a baldmove.com link, this fetches
the post and scrapes its tags. By default only episodes missing tags are processed;
--force re-scrapes every episode. Preview is the default: it scrapes and reports the
tags it *would* write but saves nothing — pass --apply to persist them.

    python manage.py backfill_baldmove_tags                     # preview baldmove network
    python manage.py backfill_baldmove_tags --apply             # save scraped tags
    python manage.py backfill_baldmove_tags --network=baldmove --force --apply

Note: scraping is rate-limited (a short sleep per episode) to avoid Cloudflare/WP
blocks, so a full run is slow. Preview still scrapes, so it is just as slow as a
real run — it only skips the database write.
"""

import time
from django.core.management.base import BaseCommand
from django.db.models import Q
from pod_manager.models import Episode, Network

# Import the robust scraper directly from the ingester
from pod_manager.ingesters.baldmove import scrape_tags_from_wp

class Command(BaseCommand):
    help = ('Backfill episode tags by scraping their WordPress links. '
            'Preview by default; pass --apply to save.')

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
        parser.add_argument(
            '--apply',
            action='store_true',
            help='Save the scraped tags (default is a preview that writes nothing).'
        )

    def handle(self, *args, **options):
        network_slug = options['network']
        force = options['force']
        apply = options['apply']

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
        
        from pod_manager.admin_console.summary import emit_summary
        if total == 0:
            self.stdout.write(self.style.WARNING(f"Found 0 episodes to process. Try running with --force to see if tags are already populated."))
            emit_summary(self.stdout, {"applied": apply, "processed": 0, "updated": 0})
            return
            
        self.stdout.write(self.style.WARNING(f"Found {total} episodes to process for {network.name}..."))
        
        updated_count = 0

        for i, ep in enumerate(episodes, 1):
            if "baldmove.com" not in ep.link:
                continue

            # Pass the link and stdout to the imported scraper
            tags = scrape_tags_from_wp(ep.link, self.stdout)
            
            if tags:
                if apply:
                    ep.tags = tags
                    ep.save()
                updated_count += 1
                verb = "Updated" if apply else "Would update"
                self.stdout.write(self.style.SUCCESS(f"[{i}/{total}] {verb} '{ep.title}': {tags}"))
            else:
                self.stdout.write(f"[{i}/{total}] No tags found for '{ep.title}'")

            # Polite delay to avoid getting blocked by Cloudflare/WP
            time.sleep(0.3)

        verb = "updated" if apply else "would update"
        self.stdout.write(self.style.SUCCESS(
            f"Finished. {verb.capitalize()} {updated_count} out of {total} episodes."))
        if not apply and updated_count:
            self.stdout.write("Re-run with --apply to save the scraped tags.")

        emit_summary(self.stdout, {
            "applied": apply,
            "processed": total,
            "updated": updated_count,
        })