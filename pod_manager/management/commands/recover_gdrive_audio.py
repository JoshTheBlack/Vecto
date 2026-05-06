import csv
import re
import os
import logging
from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand
from pod_manager.models import Podcast, Episode

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = (
        'Recovers Google Drive audio links and generates a verification report, '
        'targeting S3 subscriber URLs. Pass an optional podcast title to restrict '
        'to one show; omit it to run across every podcast in the database.'
    )

    def add_arguments(self, parser):
        parser.add_argument('csv_path', type=str, help='Path to the Vecto Recovery Links CSV')
        parser.add_argument(
            'podcast_title', nargs='?', default=None,
            help='Partial title of the podcast to target. Omit to run across all podcasts.',
        )

    def normalize_string(self, text):
        """Strips all non-alphanumeric characters and lowercases for bulletproof matching."""
        if not text:
            return ""
        return re.sub(r'[^a-z0-9]', '', text.lower().replace('.mp3', ''))

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        target_title = options['podcast_title']

        # Resolve podcast queryset
        if target_title:
            try:
                podcast_qs = [Podcast.objects.get(title__icontains=target_title)]
            except Podcast.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"No podcast found matching '{target_title}'."))
                return
            except Podcast.MultipleObjectsReturned:
                self.stdout.write(self.style.ERROR(
                    f"Multiple podcasts match '{target_title}'. Please be more specific."
                ))
                return
        else:
            podcast_qs = list(Podcast.objects.all().order_by('title'))
            self.stdout.write(f"No podcast filter — targeting all {len(podcast_qs)} podcasts.")

        # Build the recovery map once from the CSV
        recovery_map = {}
        try:
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                for row in csv.DictReader(csvfile):
                    norm_name = self.normalize_string(row['Filename'])
                    recovery_map[norm_name] = row
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"CSV file not found: {csv_path}"))
            return

        self.stdout.write(f"Loaded {len(recovery_map)} entries from CSV.\n")

        total_updated = 0
        all_report_data = []

        for podcast in podcast_qs:
            self.stdout.write(f"── {podcast.title} (ID: {podcast.id})")
            pod_updated = 0

            for episode in Episode.objects.filter(podcast=podcast):
                norm_title = self.normalize_string(episode.title)
                if norm_title not in recovery_map:
                    continue

                csv_row = recovery_map[norm_title]
                old_subscriber_url = episode.audio_url_subscriber

                if not (old_subscriber_url and 's3.amazonaws.com' in old_subscriber_url):
                    self.stdout.write(
                        self.style.WARNING(f"   Skipped (not S3): {episode.title}")
                    )
                    continue

                gdrive_link = csv_row['DirectDownload']
                file_id = csv_row['FileID']

                episode.audio_url_public = old_subscriber_url
                episode.audio_url_subscriber = gdrive_link
                episode.match_reason = f"GDrive Recovery"[:100]
                episode.save()
                cache.delete(f"ep_frag_public_{episode.id}")
                cache.delete(f"ep_frag_private_{episode.id}")

                pod_updated += 1
                total_updated += 1

                all_report_data.append({
                    'Podcast': podcast.title,
                    'Episode ID': episode.id,
                    'Title': episode.title,
                    'Verification Link': f"https://drive.google.com/file/d/{file_id}/view",
                    'Patreon Direct Link': gdrive_link,
                })
                self.stdout.write(self.style.SUCCESS(f"   Updated: {episode.title}"))

            if pod_updated == 0:
                self.stdout.write("   (no matches)\n")
            else:
                self.stdout.write(self.style.SUCCESS(f"   {pod_updated} episode(s) updated.\n"))

        # Write consolidated report
        if total_updated > 0:
            report_name = "recovery_report_all.csv" if not target_title else \
                f"recovery_report_{podcast_qs[0].slug}.csv"
            output_dir = settings.MEDIA_ROOT
            os.makedirs(output_dir, exist_ok=True)
            report_filename = os.path.join(output_dir, report_name)
            with open(report_filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['Podcast', 'Episode ID', 'Title', 'Verification Link', 'Patreon Direct Link']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_report_data)

            self.stdout.write(self.style.SUCCESS(f"Finished. {total_updated} episode(s) updated total."))
            self.stdout.write(self.style.SUCCESS(f"Report saved to: {os.path.abspath(report_filename)}"))
        else:
            self.stdout.write(self.style.WARNING("No matches found, or no S3 URLs remaining to process."))
