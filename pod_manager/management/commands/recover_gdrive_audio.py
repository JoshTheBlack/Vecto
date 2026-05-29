import csv
import json
import re
import os
import logging
from datetime import datetime
from django.conf import settings
from django.core.cache import cache
from django.core.management.base import BaseCommand
from pod_manager.models import Podcast, Episode

logger = logging.getLogger(__name__)

CONFIDENCE_EXACT = 'EXACT'
CONFIDENCE_HIGH = 'HIGH'
CONFIDENCE_MEDIUM = 'MEDIUM'
CONFIDENCE_LOW = 'LOW'
CONFIDENCE_RANK = {CONFIDENCE_EXACT: 4, CONFIDENCE_HIGH: 3, CONFIDENCE_MEDIUM: 2, CONFIDENCE_LOW: 1}

_STOP_WORDS = {'the', 'a', 'an', 'and', 'or', 'of', 'in', 'on', 'at', 'to', 'for', 'is', 'it'}

# Episode-structure words that appear across every show — not show-specific identifiers.
# Titles composed *only* of these words (plus bare numbers) are too generic to drive PREFIX.
_GENERIC_EPISODE_VOCAB = {
    'season', 'episode', 'preview', 'wrapup', 'wrap', 'finale', 'premiere',
    'recap', 'series', 'mid', 'midseason', 'spoiler', 'spoilers', 'spoilore',
    'rewatch', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight',
    'nine', 'ten', 'first', 'second', 'third', 'half', 'part',
}

# Feed/brand names that get prepended to episode titles on RSS import (e.g. "Bald Move Prestige – Title").
# These are stripped during the live run. podcast.title is also checked automatically.
_BRAND_PREFIXES = frozenset({
    'bald move prestige',
    'bald move pulp',
})

# Separators to split on when looking for a brand prefix. Tried in order; first match wins.
_TITLE_SEPARATORS = [' – ', ' — ', ' - ']  # en dash, em dash, ASCII hyphen-minus


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
        parser.add_argument(
            '--output', type=str, default=None,
            help='Path to write the dry-run analysis CSV. Defaults to dry_run_<timestamp>.csv in the current directory.',
        )
        parser.add_argument(
            '--prefix-map', type=str, default=None,
            help='Path to a JSON file mapping filename prefixes to podcast slugs. '
                 'Matching prefix boosts confidence by one level (LOW→MEDIUM, MEDIUM→HIGH).',
        )

    def normalize_string(self, text):
        """Strips all non-alphanumeric characters and lowercases for bulletproof matching."""
        if not text:
            return ""
        return re.sub(r'[^a-z0-9]', '', text.lower().replace('.mp3', ''))

    def tokenize(self, text):
        """Splits into meaningful tokens. Numeric tokens are kept regardless of length
        because season/episode numbers are high-specificity identifiers."""
        tokens = re.findall(r'[a-z0-9]+', text.lower().replace('.mp3', ''))
        return {t for t in tokens if (t not in _STOP_WORDS) and (t.isnumeric() or len(t) > 2)}

    def extract_numbers(self, text):
        """Returns all digit sequences found in text."""
        return set(re.findall(r'\d+', text))

    def has_number_conflict(self, title_numbers, csv_filename):
        """True if both strings have numbers but share none — a hard veto for wrong-match detection.
        e.g. 'Season 5 Preview' vs 'season_3_preview.mp3' would conflict."""
        csv_numbers = self.extract_numbers(csv_filename)
        return bool(title_numbers) and bool(csv_numbers) and not (title_numbers & csv_numbers)

    def is_all_generic_tokens(self, tokens):
        """True when every token is generic episode-structure vocabulary or a bare number.
        Used to gate PREFIX — 'Season One Wrap Up' must not drive a prefix match."""
        return bool(tokens) and all(
            t in _GENERIC_EPISODE_VOCAB or t.isnumeric() for t in tokens
        )

    def jaccard(self, set_a, set_b):
        if not set_a or not set_b:
            return 0.0
        intersection = len(set_a & set_b)
        union = len(set_a | set_b)
        return intersection / union if union else 0.0

    def clean_title(self, title, podcast):
        """Strip a leading 'Brand – ' or 'PodcastTitle – ' prefix from episode titles.

        Matches known Bald Move brand names (see _BRAND_PREFIXES) and the podcast's own
        title. Deliberately does NOT strip content separators like 'Deliberations 1 – '
        because those prefixes won't match any known brand or podcast name.
        """
        podcast_lower = podcast.title.lower()
        for sep in _TITLE_SEPARATORS:
            if sep not in title:
                continue
            prefix, remainder = title.split(sep, 1)
            prefix_lower = prefix.strip().lower()
            # Match exact brand names, exact podcast title, or podcast title that starts
            # with the prefix (e.g. prefix "Fargo" matches podcast "Fargo - An Unofficial Podcast")
            is_match = (
                prefix_lower in _BRAND_PREFIXES
                or prefix_lower == podcast_lower
                or podcast_lower.startswith(prefix_lower + ' ')
            )
            if is_match:
                return remainder.strip()
        return title

    def find_match(self, norm_title, title_tokens, title_numbers, recovery_map, recovery_tokens, used_keys):
        """
        Tries multiple strategies to find the best CSV match for an episode title.
        Returns (csv_key, strategy_label, confidence) or (None, None, None).

        used_keys: set of norm_csv keys already claimed by a previous episode in this run.

        Strategies tried in order (first EXACT wins immediately; others find global best):
          1. EXACT    — normalized strings are equal
          2. SUFFIX   — title ends with CSV name (handles brand prefixes like "Bald Move Pulp – ")
          3. PREFIX   — CSV name ends with title
          4. CONTAINS — one is a substring of the other (min length 20)
          5. TOKEN    — Jaccard similarity on word+number tokens

        Number conflict veto: if both strings have numbers that are completely disjoint,
        the match is rejected. This prevents "Season 5" from matching "Season 3" via TOKEN.

        Tiebreaker: within the same confidence tier, the candidate with the higher Jaccard
        score wins. This prevents iteration order from determining the winner when two CSV
        files both satisfy a structural strategy (e.g. two csv_full matches at HIGH).
        """
        best = (None, None, None)
        best_key = (0, 0.0)  # (confidence_rank, jaccard_score) — higher tuple wins

        for norm_csv, csv_row in recovery_map.items():
            if norm_csv in used_keys:
                continue

            # 1. EXACT — numbers can't conflict if strings are equal
            if norm_title == norm_csv:
                return norm_csv, 'EXACT', CONFIDENCE_HIGH

            # Hard veto for all fuzzy strategies: disjoint numbers = wrong episode
            if self.has_number_conflict(title_numbers, csv_row['Filename']):
                continue

            # Compute Jaccard upfront — used as tiebreaker across all strategies
            csv_tok = recovery_tokens.get(norm_csv)
            score = self.jaccard(title_tokens, csv_tok) if csv_tok else 0.0

            # 2. SUFFIX — title has a brand prefix, CSV starts at the content name
            if len(norm_csv) >= 15 and norm_title.endswith(norm_csv):
                candidate_key = (CONFIDENCE_RANK[CONFIDENCE_HIGH], score)
                if candidate_key > best_key:
                    best = (norm_csv, 'SUFFIX', CONFIDENCE_HIGH)
                    best_key = candidate_key
                continue

            # 3. PREFIX — CSV filename ends with the episode title (show prefix in filename only)
            # Guard: if every title token is generic episode-structure vocab (e.g. "Season One
            # Wrap Up"), this fires falsely across every show that has a wrapup file.
            if len(norm_title) >= 15 and norm_csv.endswith(norm_title):
                if not self.is_all_generic_tokens(title_tokens):
                    candidate_key = (CONFIDENCE_RANK[CONFIDENCE_HIGH], score)
                    if candidate_key > best_key:
                        best = (norm_csv, 'PREFIX', CONFIDENCE_HIGH)
                        best_key = candidate_key
                continue  # always skip TOKEN for this entry — PREFIX is the right lens here

            # 4. CONTAINS — one string is a literal substring of the other
            if len(norm_csv) >= 20 and len(norm_title) >= 20:
                if norm_csv in norm_title:
                    # CSV content fully inside title → title has extra noise (e.g. "- Livewatch")
                    # This direction is reliable: HIGH
                    candidate_key = (CONFIDENCE_RANK[CONFIDENCE_HIGH], score)
                    if candidate_key > best_key:
                        best = (norm_csv, 'CONTAINS+', CONFIDENCE_HIGH)
                        best_key = candidate_key
                    continue
                if norm_title in norm_csv:
                    # Title inside CSV → CSV has extra context we can't verify: MEDIUM
                    candidate_key = (CONFIDENCE_RANK[CONFIDENCE_MEDIUM], score)
                    if candidate_key > best_key:
                        best = (norm_csv, 'CONTAINS', CONFIDENCE_MEDIUM)
                        best_key = candidate_key
                    continue

            # 5. TOKEN (Jaccard) — shared vocabulary after removing stop words
            if not csv_tok:
                continue

            # CSV-coverage boost: strip generic episode vocab from the CSV token set, then
            # check whether every remaining (show-specific) token appears in the title.
            # Handles "Title - Livewatch" vs "title.mp3": all specific CSV tokens are present
            # even though Jaccard is diluted by the extra title tokens.
            meaningful_csv_tok = csv_tok - _GENERIC_EPISODE_VOCAB
            if len(meaningful_csv_tok) >= 2 and meaningful_csv_tok <= title_tokens:
                confidence = CONFIDENCE_HIGH
                label = 'TOKEN:csv_full'
            else:
                if score >= 0.80:
                    confidence = CONFIDENCE_HIGH
                    label = f'TOKEN:{score:.2f}'
                elif score >= 0.60:
                    confidence = CONFIDENCE_MEDIUM
                    label = f'TOKEN:{score:.2f}'
                elif score >= 0.45:
                    confidence = CONFIDENCE_LOW
                    label = f'TOKEN:{score:.2f}'
                else:
                    continue

            candidate_key = (CONFIDENCE_RANK[confidence], score)
            if candidate_key > best_key:
                best = (norm_csv, label, confidence)
                best_key = candidate_key

        return best

    def handle(self, *args, **options):
        csv_path = options['csv_path']
        target_title = options['podcast_title']
        dry_run = options['dry_run']
        min_confidence = options['min_confidence']
        output_path = options['output']
        prefix_map_path = options['prefix_map']

        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN - no changes will be saved.\n'))

        # Load optional prefix→slug affinity map
        prefix_map = {}
        if prefix_map_path:
            try:
                with open(prefix_map_path, encoding='utf-8') as f:
                    prefix_map = json.load(f)
                self.stdout.write(f"Loaded {len(prefix_map)} prefix affinity entries.\n")
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Failed to load prefix map: {e}"))
                return

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
            self.stdout.write(f"No podcast filter - targeting all {len(podcast_qs)} podcasts.")

        # Build recovery map, token index, and number index once from the CSV
        recovery_map = {}
        recovery_tokens = {}
        filename_index = {}  # raw Filename → norm_key, for exact S3 filename matching
        try:
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                for row in csv.DictReader(csvfile):
                    norm_name = self.normalize_string(row['Filename'])
                    recovery_map[norm_name] = row
                    recovery_tokens[norm_name] = self.tokenize(row['Filename'])
                    filename_index[row['Filename']] = norm_name
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"CSV file not found: {csv_path}"))
            return

        self.stdout.write(f"Loaded {len(recovery_map)} entries from CSV.\n")

        total_updated = 0
        total_skipped_locked = 0
        total_below_threshold = 0
        all_report_data = []
        dry_run_rows = []

        # Tracks which CSV entries have been claimed, preventing one file from
        # matching multiple episodes across the entire run.
        used_csv_keys = set()

        for podcast in podcast_qs:
            self.stdout.write(f"-- {podcast.title} (ID: {podcast.id})")
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
                    self.stdout.write(self.style.WARNING(f"   Locked (skipped): {episode.title}"))
                    total_skipped_locked += 1
                    continue

                norm_title = self.normalize_string(episode.title)
                title_tokens = self.tokenize(episode.title)
                title_numbers = self.extract_numbers(episode.title)

                # Layer 0: exact S3 filename match — strongest signal, checked first.
                # Extract the filename from the S3 URL (last path segment after splitting on /).
                s3_filename = old_subscriber_url.rstrip('/').rsplit('/', 1)[-1]
                filename_norm_key = filename_index.get(s3_filename)
                if filename_norm_key and filename_norm_key not in used_csv_keys:
                    norm_csv, strategy, confidence = filename_norm_key, 'FILENAME', CONFIDENCE_EXACT
                else:
                    norm_csv, strategy, confidence = self.find_match(
                        norm_title, title_tokens, title_numbers,
                        recovery_map, recovery_tokens, used_csv_keys,
                    )

                if norm_csv is None:
                    continue

                # Apply prefix-affinity confidence boost when the CSV filename's
                # show prefix matches the episode's own podcast slug.
                if prefix_map:
                    csv_filename_lower = recovery_map[norm_csv]['Filename'].lower()
                    for prefix, mapped_slug in prefix_map.items():
                        if csv_filename_lower.startswith(prefix.lower()):
                            if mapped_slug == podcast.slug:
                                if confidence == CONFIDENCE_LOW:
                                    confidence = CONFIDENCE_MEDIUM
                                    strategy = f"{strategy}+prefix"
                                elif confidence == CONFIDENCE_MEDIUM:
                                    confidence = CONFIDENCE_HIGH
                                    strategy = f"{strategy}+prefix"
                            break

                csv_row = recovery_map[norm_csv]
                gdrive_link = csv_row['DirectDownload']
                file_id = csv_row['FileID']
                vecto_link = f"https://{domain}/episode/{episode.id}"
                match_reason = f"GDrive Recovery ({strategy})"[:100]
                above_threshold = CONFIDENCE_RANK[confidence] >= CONFIDENCE_RANK[min_confidence]

                cleaned_title = self.clean_title(episode.title, podcast)

                if dry_run:
                    dry_run_rows.append({
                        'Podcast': podcast.title,
                        'Episode ID': episode.id,
                        'Episode Title': episode.title,
                        'Cleaned Title': cleaned_title if cleaned_title != episode.title else '',
                        'CSV Filename': csv_row['Filename'],
                        'Strategy': strategy,
                        'Confidence': confidence,
                        'Would Apply': 'YES' if above_threshold else f'NO (below {min_confidence})',
                        'Vecto Link': vecto_link,
                        'Verification Link': f"https://drive.google.com/file/d/{file_id}/view",
                        'Patreon Direct Link': gdrive_link,
                    })

                if not above_threshold:
                    total_below_threshold += 1
                    self.stdout.write(self.style.WARNING(
                        f"   Below threshold [{confidence}] ({strategy}): {episode.title}"
                        f"\n      -> CSV: {csv_row['Filename']}"
                    ))
                    continue

                # Claim this CSV entry so no other episode can match it
                used_csv_keys.add(norm_csv)

                if not dry_run:
                    episode.audio_url_public = old_subscriber_url
                    episode.audio_url_subscriber = gdrive_link
                    episode.match_reason = match_reason
                    episode.audio_locked = True
                    if cleaned_title != episode.title:
                        episode.title = cleaned_title
                    episode.save()
                    cache.delete(f"ep_frag_public_{episode.id}")
                    cache.delete(f"ep_frag_private_{episode.id}")

                pod_updated += 1
                total_updated += 1

                label = f"[{confidence}] ({strategy})"
                title_display = episode.title if cleaned_title == episode.title else f"{episode.title} -> {cleaned_title}"
                self.stdout.write(self.style.SUCCESS(
                    f"   {'Would update' if dry_run else 'Updated'} {label}: {title_display}"
                ))

                if not dry_run:
                    all_report_data.append({
                        'Podcast': podcast.title,
                        'Episode ID': episode.id,
                        'Title': cleaned_title,
                        'Vecto Link': vecto_link,
                        'Match Strategy': strategy,
                        'Confidence': confidence,
                        'Verification Link': f"https://drive.google.com/file/d/{file_id}/view",
                        'Patreon Direct Link': gdrive_link,
                    })

            if pod_updated == 0:
                self.stdout.write("   (no matches)\n")
            else:
                self.stdout.write(self.style.SUCCESS(
                    f"   {pod_updated} episode(s) {'would be ' if dry_run else ''}updated.\n"
                ))

        # Summary
        self.stdout.write(f"\nSummary:")
        self.stdout.write(f"  {'Would update' if dry_run else 'Updated'}:       {total_updated}")
        self.stdout.write(f"  Skipped (locked): {total_skipped_locked}")
        self.stdout.write(f"  Below threshold:  {total_below_threshold}")

        # Write dry-run analysis CSV (includes all candidates, above and below threshold)
        if dry_run and dry_run_rows:
            if not output_path:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_path = f"dry_run_{timestamp}.csv"
            fieldnames = [
                'Podcast', 'Episode ID', 'Episode Title', 'Cleaned Title', 'CSV Filename',
                'Strategy', 'Confidence', 'Would Apply',
                'Vecto Link', 'Verification Link', 'Patreon Direct Link',
            ]
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(dry_run_rows)
            self.stdout.write(self.style.SUCCESS(f"\nDry-run report: {os.path.abspath(output_path)}"))

        # Write live recovery reports
        if not dry_run:
            if total_updated > 0:
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                slug_suffix = f"_{podcast_qs[0].slug}" if target_title else "_all"
                output_dir = settings.MEDIA_ROOT
                os.makedirs(output_dir, exist_ok=True)

                # --- CSV report ---
                csv_filename = os.path.join(output_dir, f"recovery_report{slug_suffix}_{timestamp}.csv")
                fieldnames = [
                    'Podcast', 'Episode ID', 'Title', 'Vecto Link',
                    'Match Strategy', 'Confidence', 'Verification Link', 'Patreon Direct Link',
                ]
                with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(all_report_data)
                self.stdout.write(self.style.SUCCESS(f"\nCSV report:     {os.path.abspath(csv_filename)}"))

                # --- Discord report ---
                _CONF_ORDER = {CONFIDENCE_LOW: 0, CONFIDENCE_MEDIUM: 1, CONFIDENCE_HIGH: 2}
                by_podcast = {}
                for row in all_report_data:
                    by_podcast.setdefault(row['Podcast'], []).append(row)

                discord_lines = []
                for pod_name in sorted(by_podcast):
                    entries = sorted(
                        by_podcast[pod_name],
                        key=lambda r: (_CONF_ORDER.get(r['Confidence'], 0), r['Title'])
                    )
                    count = len(entries)
                    discord_lines.append(f"**{pod_name} ({count} episode{'s' if count != 1 else ''})**")
                    for entry in entries:
                        discord_lines.append(
                            f"- {entry['Confidence']} | [{entry['Title']}]({entry['Vecto Link']}) | "
                            f"[Drive Audio]({entry['Verification Link']})"
                        )
                    discord_lines.append("")
                    discord_lines.append("----------------------------------------")
                    discord_lines.append("")

                discord_filename = os.path.join(output_dir, f"discord_links{slug_suffix}_{timestamp}.txt")
                with open(discord_filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(discord_lines))
                self.stdout.write(self.style.SUCCESS(f"Discord report: {os.path.abspath(discord_filename)}"))
            else:
                self.stdout.write(self.style.WARNING("No matches found, or no S3 URLs remaining to process."))
