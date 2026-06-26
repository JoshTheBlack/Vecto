"""Rewind a GDrive recovery run: restore the original S3 subscriber URL and mark
the episode 'Missing Audio'.

The inverse of recover_gdrive_audio. For every Episode ID in the given recovery
CSV(s) whose public slot still holds the stashed S3 link, this swaps that link
back into the subscriber field, clears the stash, unlocks the audio, and sets the
match reason to 'Missing Audio'. Preview is the default; pass --apply to save.

    python manage.py rewind_gdrive_audio run.csv                  # preview
    python manage.py rewind_gdrive_audio run.csv another.csv --apply
"""

import csv
import logging

from django.core.cache import cache
from django.core.management.base import BaseCommand

from pod_manager.models import Episode

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = ('Rewind a GDrive recovery by restoring the original S3 URL and marking '
            'episodes Missing Audio (preview by default; --apply to save).')

    def add_arguments(self, parser):
        # Accepts one or more CSV file paths
        parser.add_argument('csv_paths', nargs='+', type=str, help='Paths to the recovery CSVs to revert')
        parser.add_argument('--apply', action='store_true',
                            help='Save the reversions (default is a preview that writes nothing).')

    def handle(self, *args, **options):
        csv_paths = options['csv_paths']
        apply = options['apply']
        episode_ids = set()

        # 1. Collect all unique Episode IDs from the provided CSVs
        for path in csv_paths:
            try:
                with open(path, newline='', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    for row in reader:
                        if 'Episode ID' in row:
                            episode_ids.add(row['Episode ID'])
            except FileNotFoundError:
                self.stdout.write(self.style.ERROR(f"CSV not found: {path}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error reading {path}: {e}"))

        self.stdout.write(f"Found {len(episode_ids)} unique episodes to revert across {len(csv_paths)} file(s).")

        restored_count = 0

        # 2. Iterate through the exact episodes modified and swap the URLs back
        for ep_id in episode_ids:
            try:
                episode = Episode.objects.get(id=ep_id)

                # Verify the public URL slot contains the stashed S3 link
                if episode.audio_url_public and 's3.amazonaws.com' in episode.audio_url_public:
                    if apply:
                        # Move the S3 link back to the subscriber field
                        episode.audio_url_subscriber = episode.audio_url_public
                        # Clear out the temporary public stash
                        episode.audio_url_public = ""
                        # Unlock the audio and set the specific match reason
                        episode.audio_locked = False
                        episode.match_reason = "Missing Audio"
                        episode.save()
                        # Bust the caches just like the original script
                        cache.delete(f"ep_frag_public_{episode.id}")
                        cache.delete(f"ep_frag_private_{episode.id}")

                    restored_count += 1
                    verb = "Reverted & Unlocked" if apply else "Would revert"
                    self.stdout.write(self.style.SUCCESS(f" ↺ {verb}: {episode.title}"))
                else:
                    self.stdout.write(self.style.WARNING(f" ⚠ Skipped (No S3 link found in public slot): {episode.title}"))

            except Episode.DoesNotExist:
                self.stdout.write(self.style.ERROR(f" ✖ Episode ID {ep_id} not found in database."))

        verb = "restored" if apply else "would be restored"
        self.stdout.write(self.style.SUCCESS(f"\nRewind complete. {restored_count} episode(s) {verb}."))
        if not apply and restored_count:
            self.stdout.write("Re-run with --apply to perform the rewind.")
        if apply:
            logger.info("rewind_gdrive_audio applied: restored=%d", restored_count)
