import csv
import re
import os
import logging
from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand
from pod_manager.models import Podcast, Episode

logger = logging.getLogger(__name__)

CONFIDENCE_HIGH = 'HIGH'
CONFIDENCE_MEDIUM = 'MEDIUM'
CONFIDENCE_LOW = 'LOW'
CONFIDENCE_RANK = {CONFIDENCE_HIGH: 3, CONFIDENCE_MEDIUM: 2, CONFIDENCE_LOW: 1}

_STOP_WORDS = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for', 'is', 'it'}


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
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Show what would be matched without saving any changes.',
        )
        parser.add_argument(
            '--min-confidence', choices=[CONFIDENCE_HIGH, CONFIDENCE_MEDIUM, CONFIDENCE_LOW],
            default=CONFIDENCE_HIGH,
            help='Minimum confidence level to auto-apply a match (default: HIGH).',
        )

    def normalize_string(self, text):
        """Strips all non-alphanumeric characters and lowercases for bulletproof matching."""
        if not text:
            return ""
        return re.sub(r'[^a-z0-9]', '', text.lower().replace('.mp3', ''))

    def tokenize(self, text):
        """Splits normalized text into meaningful tokens, removing stop words and short tokens."""
        tokens = re.findall(r'[a-z0-9]+', text.lower().replace('.mp3', ''))
        return {t for t in tokens if t not in _STOP_WORDS and len(t) > 2}

    def jaccard(self, set_a, set_b):
        if not set_a or not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union else 0.0

    def find_match(self, norm_title, title_tokens, recovery_map, recovery_tokens):
        """
        Tries multiple strategies to find the best CSV match for an episode title.
        Returns (csv_key, strategy_label, confidence) or (None, None, None).

        Strategies tried in order (first match wins at highest possible confidence):
          1. EXACT      — normalized title == normalized CSV filename
          2. SUFFIX     — normalized title ends with normalized CSV filename (handles brand prefixes)
          3. PREFIX     — normalized CSV filename ends with normalized title
          4. CONTAINS   — one string is a substring of the other (min length 20)
          5. TOKEN      — Jaccard similarity on word tokens
        """
        best = (None, None, None)
        best_rank = 0

        for norm_csv, csv_row in recovery_map.items():
            # 1. EXACT
            if norm_title == norm_csv:
                return norm_csv, 'EXACT', CONFIDENCE_HIGH

            # 2. SUFFIX — title has a brand prefix, CSV starts at the content name
            if len(norm_csv) >= 15 and norm_title.endswith(norm_csv):
                candidate = (norm_csv, 'SUFFIX', CONFIDENCE_HIGH)
                if CONFIDENCE_RANK[CONFIDENCE_HIGH] > best_rank:
                    best = candidate
                    best_rank = CONFIDENCE_RANK[CONFIDENCE_HIGH]
                continue

            # 3. PREFIX — CSV has extra suffix noise
            if len(norm_title) >= 15 and norm_csv.endswith(norm_title):
                candidate = (norm_csv, 'PREFIX', CONFIDENCE_HIGH)
                if CONFIDENCE_RANK[CONFIDENCE_HIGH] > best_rank:
                    best = candidate
                    best_rank = CONFIDENCE_RANK[CONFIDENCE_HIGH]
                continue

            # 4. CONTAINS
            if len(norm_csv) >= 20 and len(norm_title) >= 20:
                if norm_title in norm_csv or norm_csv in norm_title:
                    candidate = (norm_csv, 'CONTAINS', CONFIDENCE_MEDIUM)
                    if CONFIDENCE_RANK[CONFIDENCE_MEDIUM] > best_rank:
                        best = candidate
                        best_rank = CONFIDENCE_RANK[CONFIDENCE_MEDIUM]
                    continue

            # 5. TOKEN (Jaccard)
            csv_tok = recovery_tokens.get(norm_csv)
            if csv_tok is None:
                continue
            score = self.jaccard(title_tokens, csv_tok)
            if score >= 0.80:
                confidence = CONFIDENCE_HIGH
            elif score >= 0.60:
                confidence = CONFIDENCE_MEDIUM
            elif score >= 0.45:
                confidence = CONFIDENCE_LOW
            else:
                continue

            label = f'TOKEN:{score:.2f}'
            if CONFIDENCE_RANK[confidence] > best_rank:
                best = (norm_csv, label, confidence)
                best_rank = CONFIDENCE_RANK[confidence]

        return best

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        target_title = options['podcast_title']
        dry_run = options['dry_run']
        min_confidence = options['min_confidence']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN — no changes will be saved.\n'))

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

        # Build recovery map and token index once from the CSV
        recovery_map = {}
        recovery_tokens = {}
        try:
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                for row in csv.DictReader(csvfile):
                    norm_name = self.normalize_string(row['Filename'])
                    recovery_map[norm_name] = row
                    recovery_tokens[norm_name] = self.tokenize(row['Filename'])
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"CSV file not found: {csv_path}"))
            return

        self.stdout.write(f"Loaded {len(recovery_map)} entries from CSV.\n")

        total_updated = 0
        total_skipped_locked = 0
        total_below_threshold = 0
        all_report_data = []

        for podcast in podcast_qs:
            self.stdout.write(f"── {podcast.title} (ID: {podcast.id})")
            pod_updated = 0

            if hasattr(podcast, 'network') and podcast.network and podcast.network.custom_domain:
                domain = podcast.network.custom_domain.rstrip('/')
            else:
                domain = "vecto.joshtheblack.com"

            for episode in Episode.objects.filter(podcast=podcast):
                old_subscriber_url = episode.audio_url_subscriber
                if not (old_subscriber_url and 's3.amazonaws.com' in old_subscriber_url):
                    continue

                if episode.audio_locked:
                    self.stdout.write(
                        self.style.WARNING(f"   Locked (skipped): {episode.title}")
                    )
                    total_skipped_locked += 1
                    continue

                norm_title = self.normalize_string(episode.title)
                title_tokens = self.tokenize(episode.title)
                norm_csv, strategy, confidence = self.find_match(
                    norm_title, title_tokens, recovery_map, recovery_tokens
                )

                if norm_csv is None:
                    continue

                csv_row = recovery_map[norm_csv]
                gdrive_link = csv_row['DirectDownload']
                file_id = csv_row['FileID']
                vecto_link = f"https://{domain}/episode/{episode.id}"
                match_reason = f"GDrive Recovery ({strategy})"[:100]

                above_threshold = CONFIDENCE_RANK[confidence] >= CONFIDENCE_RANK[min_confidence]

                if not above_threshold:
                    total_below_threshold += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f"   Below threshold [{confidence}] ({strategy}): {episode.title}"
                            f"\n      → CSV: {csv_row['Filename']}"
                        )
                    )
                    continue

                if not dry_run:
                    episode.audio_url_public = old_subscriber_url
                    episode.audio_url_subscriber = gdrive_link
                    episode.match_reason = match_reason
                    episode.audio_locked = True
                    episode.save()
                    cache.delete(f"ep_frag_public_{episode.id}")
                    cache.delete(f"ep_frag_private_{episode.id}")

                pod_updated += 1
                total_updated += 1

                label = f"[{confidence}] ({strategy})"
                self.stdout.write(
                    self.style.SUCCESS(f"   {'Would update' if dry_run else 'Updated'} {label}: {episode.title}")
                )

                all_report_data.append({
                    'Podcast': podcast.title,
                    'Episode ID': episode.id,
                    'Title': episode.title,
                    'Vecto Link': vecto_link,
                    'Match Strategy': strategy,
                    'Confidence': confidence,
                    'Verification Link': f"https://drive.google.com/file/d/{file_id}/view",
                    'Patreon Direct Link': gdrive_link,
                })

            if pod_updated == 0:
                self.stdout.write("   (no matches)\n")
            else:
                self.stdout.write(
                    self.style.SUCCESS(f"   {pod_updated} episode(s) {'would be ' if dry_run else ''}updated.\n")
                )

        # Summary
        self.stdout.write(f"\nSummary:")
        self.stdout.write(f"  {'Would update' if dry_run else 'Updated'}:       {total_updated}")
        self.stdout.write(f"  Skipped (locked): {total_skipped_locked}")
        self.stdout.write(f"  Below threshold:  {total_below_threshold}")

        # Write consolidated report
        if total_updated > 0 and not dry_run:
            report_name = "recovery_report_all.csv" if not target_title else \
                f"recovery_report_{podcast_qs[0].slug}.csv"
            output_dir = settings.MEDIA_ROOT
            os.makedirs(output_dir, exist_ok=True)
            report_filename = os.path.join(output_dir, report_name)
            fieldnames = ['Podcast', 'Episode ID', 'Title', 'Vecto Link', 'Match Strategy', 'Confidence', 'Verification Link', 'Patreon Direct Link']
            with open(report_filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(all_report_data)

            self.stdout.write(self.style.SUCCESS(f"\nReport saved to: {os.path.abspath(report_filename)}"))
        elif total_updated == 0 and not dry_run:
            self.stdout.write(self.style.WARNING("No matches found, or no S3 URLs remaining to process."))
