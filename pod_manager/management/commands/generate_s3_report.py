import os
import csv
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db.models import Count, Q
from pod_manager.models import Podcast, Episode

class Command(BaseCommand):
    help = 'Generates TXT and CSV reports of episodes hosted on S3 and saves them to the media folder'

    def handle(self, *args, **options):
        self.stdout.write("Fetching data from the database... please wait.")

        # Let Django dictate where the media folder is!
        output_dir = settings.MEDIA_ROOT  
        
        txt_path = os.path.join(output_dir, 's3_hosting_report.txt')
        csv_path = os.path.join(output_dir, 's3_hosted_episodes.csv')

        os.makedirs(output_dir, exist_ok=True)

        # Define separate filters for the related query vs the direct query
        pod_s3_filter = Q(episodes__audio_url_subscriber__icontains='s3.amazonaws.com')
        ep_s3_filter = Q(audio_url_subscriber__icontains='s3.amazonaws.com')

        # Fetch data into memory
        all_podcasts = list(Podcast.objects.annotate(
            s3_count=Count('episodes', filter=pod_s3_filter),
            other_count=Count('episodes', filter=~pod_s3_filter)
        ).order_by('title'))

        s3_episodes = list(Episode.objects.filter(
            ep_s3_filter
        ).select_related('podcast').order_by('podcast__title', '-pub_date'))

        self.stdout.write(f"-> Found {len(all_podcasts)} total Podcasts.")
        self.stdout.write(f"-> Found {len(s3_episodes)} total S3 Hosted Episodes.")

        if not s3_episodes:
            self.stdout.write(self.style.WARNING("No S3 episodes found. Skipping file generation."))
            return

        # Write TXT (Using 'w' automatically overwrites existing files)
        with open(txt_path, 'w', encoding='utf-8') as txt_file:
            txt_file.write("--- REPORT 1: S3 HOSTING SUMMARY ---\n\n")
            for pod in all_podcasts:
                txt_file.write(f"{pod.title} - S3: {pod.s3_count} - Other: {pod.other_count}\n")

            txt_file.write("\n\n--- REPORT 2: DETAILED S3 EPISODE LIST ---\n")
            current_podcast = None
            for ep in s3_episodes:
                if ep.podcast.title != current_podcast:
                    txt_file.write(f"\n{ep.podcast.title}\n")
                    current_podcast = ep.podcast.title
                txt_file.write(f"  {ep.id} - {ep.title} - {ep.audio_url_subscriber}\n")

        # Write CSV
        with open(csv_path, 'w', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['Podcast Title', 'Episode ID', 'Episode Title', 'S3 URL'])
            for ep in s3_episodes:
                writer.writerow([ep.podcast.title, ep.id, ep.title, ep.audio_url_subscriber])

        self.stdout.write(self.style.SUCCESS(f"Successfully generated reports in {output_dir}!"))