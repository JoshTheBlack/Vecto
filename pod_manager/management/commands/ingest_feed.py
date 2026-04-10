import feedparser
import time
import re
from datetime import datetime
from urllib.parse import urlparse
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware
from django.conf import settings
from pod_manager.models import Podcast, Episode
import os
import requests
import hashlib
from difflib import SequenceMatcher
from bs4 import BeautifulSoup

class Command(BaseCommand):
    help = 'Ingests episodes and prints debug logs for unmatched items'

    def add_arguments(self, parser):
        parser.add_argument('podcast_id', type=int)

    def get_slug(self, url):
        if not url or "?" in url:
            return None
        try:
            path = urlparse(url).path
            return path.strip('/')
        except Exception:
            return None

    def get_fingerprint(self, title, network):
        if not title:
            return ""
        title_lower = title.lower()
        if network.ignored_title_tags:
            tags = [t.strip().lower() for t in network.ignored_title_tags.split(',') if t.strip()]
            for tag in tags:
                title_lower = title_lower.replace(tag, '')
        return re.sub(r'[^a-z0-9]', '', title_lower)

    def is_robust_title_match(self, public_title, private_title, network):
        """
        Returns a tuple: (is_match: bool, reason: str)
        """
        if not public_title or not private_title:
            return False, ""
            
        pub_fp = self.get_fingerprint(public_title, network)
        priv_fp = self.get_fingerprint(private_title, network)
        
        # 1. Exact Fingerprint Match
        if pub_fp == priv_fp and len(pub_fp) > 0:
            return True, "Fuzzy Match (Exact Fingerprint)"
            
        # 2. Substring Match (Catches Megaphone prefixes/suffixes)
        if len(priv_fp) > 8 and priv_fp in pub_fp:
            return True, "Fuzzy Match (Substring - Private in Public)"
        if len(pub_fp) > 8 and pub_fp in priv_fp:
            return True, "Fuzzy Match (Substring - Public in Private)"
            
        # 3. Token/Word Overlap Match (Catches rearranged or extra words)
        pub_words = set(re.sub(r'[^a-z0-9\s]', '', public_title.lower()).split())
        priv_words = set(re.sub(r'[^a-z0-9\s]', '', private_title.lower()).split())
        
        stop_words = {'the', 'a', 'an', 'and', 'or', 'in', 'on', 'of', 'to', 'for', 'with', 'part', 'episode'}
        pub_words -= stop_words
        priv_words -= stop_words
        
        if pub_words and priv_words:
            overlap = priv_words.intersection(pub_words)
            if len(overlap) / float(len(priv_words)) >= 0.8:
                return True, "Fuzzy Match (Word Overlap >= 80%)"

        # 4. Fallback: Fuzzy Sequence Match (for typos)
        if len(pub_fp) >= 5 and len(priv_fp) >= 5:
            ratio = SequenceMatcher(None, pub_fp, priv_fp).ratio()
            if ratio > 0.80:  
                return True, f"Fuzzy Match (Sequence Ratio: {ratio:.2f})"
                
        return False, ""
    
    def get_enclosure(self, entry):
        if hasattr(entry, 'enclosures') and entry.enclosures:
            return entry.enclosures[0].href
        if hasattr(entry, 'links'):
            for link in entry.links:
                if link.get('rel') == 'enclosure':
                    return link.href
        return ""

    def clean_html_description(self, html_content, network):
        if not html_content:
            return ""
        html_content = re.sub(r'\n{2,}', '</p><p>', html_content)
        html_content = re.sub(r'(<br\s*/?>\s*){2,}', '</p><p>', html_content)
        html_content = f"<p>{html_content}</p>"
        
        soup = BeautifulSoup(html_content, "html.parser")
        
        if network.description_cut_triggers:
            triggers = [t.strip().lower() for t in network.description_cut_triggers.split(',') if t.strip()]
            for element in soup.find_all(['p', 'div', 'li', 'em', 'strong']):
                text = element.get_text().lower()
                if any(trigger in text for trigger in triggers):
                    element.decompose()
        
        for empty in soup.find_all(lambda tag: not tag.contents and not tag.get_text(strip=True)):
            empty.decompose()
            
        final_html = str(soup).strip()
        final_html = final_html.replace('\n', '<br>')
        return final_html

    def get_cached_feed(self, url, feed_type):
        use_cache = os.getenv('USE_LOCAL_FEED_CACHE', 'False') == 'True'
        parsed_url = urlparse(url)
        auth = (parsed_url.username, parsed_url.password) if parsed_url.username else None
        clean_url = parsed_url._replace(netloc=parsed_url.hostname).geturl()

        def fetch_live():
            self.stdout.write(f"  [LIVE FETCH] Downloading {feed_type} feed...")
            response = requests.get(clean_url, auth=auth, timeout=30, headers={'User-Agent': 'Vecto/1.0'})
            response.raise_for_status()
            return response.content

        if not use_cache:
            return feedparser.parse(fetch_live())

        cache_dir = os.path.join(settings.BASE_DIR, '.feed_cache')
        if not os.path.exists(cache_dir): os.makedirs(cache_dir)
        url_hash = hashlib.md5(clean_url.encode('utf-8')).hexdigest()
        cache_file = os.path.join(cache_dir, f"{feed_type}_{url_hash}.xml")

        if os.path.exists(cache_file):
            self.stdout.write(self.style.WARNING(f"  [CACHE HIT] Loading {feed_type} from disk..."))
            return feedparser.parse(cache_file)

        content = fetch_live()
        with open(cache_file, 'wb') as f:
            f.write(content)
        return feedparser.parse(content)
        
    def handle(self, *args, **options):
        try:
            podcast = Podcast.objects.get(pk=options['podcast_id'])
        except Podcast.DoesNotExist:
            self.stderr.write(self.style.ERROR(f"Podcast ID {options['podcast_id']} not found."))
            return

        self.stdout.write(f"--- Harvesting: {podcast.title} ---")

        sub_data = self.get_cached_feed(podcast.subscriber_feed_url, "PRIVATE")
        if hasattr(sub_data, 'status') and sub_data.status == 401:
            self.stderr.write(self.style.ERROR("Auth Failed on Private Feed."))
            return

        public_data = self.get_cached_feed(podcast.public_feed_url, "PUBLIC")

        # Capture Artwork
        feed_image = ""
        if 'image' in public_data.feed:
            feed_image = public_data.feed.image.get('href') or public_data.feed.image.get('url', '')
        if feed_image and podcast.image_url != feed_image:
            podcast.image_url = feed_image
            podcast.save()
            self.stdout.write(self.style.SUCCESS(f"  [Artwork Captured]: {feed_image}"))

        # ==========================================
        # MULTI-PASS MATCHING ALGORITHM
        # ==========================================
        
        # 1. Pool all private entries by their unique audio URL
        private_pool = {}
        for entry in sub_data.entries:
            audio_url = self.get_enclosure(entry)
            if audio_url:
                private_pool[audio_url] = entry
                
        unmatched_private_audios = set(private_pool.keys())
        
        # 2. Pool all public entries by their index
        public_entries_list = list(public_data.entries)
        unmatched_public_indices = set(range(len(public_entries_list)))
        
        # This dict will hold our final pairings: pub_index -> (priv_audio_url, match_reason)
        matched_pairs = {} 

        # --- PASS 1: Exact Link (Slug) or GUID ---
        for i in list(unmatched_public_indices):
            pub_entry = public_entries_list[i]
            p_link = getattr(pub_entry, 'link', None)
            p_slug = self.get_slug(p_link)
            p_id = getattr(pub_entry, 'id', None)
            
            for priv_audio in list(unmatched_private_audios):
                priv_entry = private_pool[priv_audio]
                s_link = getattr(priv_entry, 'link', None)
                s_slug = self.get_slug(s_link)
                s_id = getattr(priv_entry, 'id', None)
                
                if p_slug and s_slug and p_slug == s_slug:
                    matched_pairs[i] = (priv_audio, "Link Match")
                    unmatched_public_indices.remove(i)
                    unmatched_private_audios.remove(priv_audio)
                    break
                elif p_id and s_id and p_id == s_id:
                    matched_pairs[i] = (priv_audio, "GUID Match")
                    unmatched_public_indices.remove(i)
                    unmatched_private_audios.remove(priv_audio)
                    break

        # --- PASS 2: Exact Title Fingerprint ---
        for i in list(unmatched_public_indices):
            pub_entry = public_entries_list[i]
            p_title = getattr(pub_entry, 'title', '')
            p_fp = self.get_fingerprint(p_title, podcast.network)
            
            for priv_audio in list(unmatched_private_audios):
                priv_entry = private_pool[priv_audio]
                s_title = getattr(priv_entry, 'title', '')
                s_fp = self.get_fingerprint(s_title, podcast.network)
                
                if p_fp and s_fp and p_fp == s_fp:
                    matched_pairs[i] = (priv_audio, "Exact Title Match")
                    unmatched_public_indices.remove(i)
                    unmatched_private_audios.remove(priv_audio)
                    break

        # --- PASS 3: Robust Fuzzy Match ---
        for i in list(unmatched_public_indices):
            pub_entry = public_entries_list[i]
            p_title = getattr(pub_entry, 'title', '')
            
            for priv_audio in list(unmatched_private_audios):
                priv_entry = private_pool[priv_audio]
                s_title = getattr(priv_entry, 'title', '')
                
                is_match, reason = self.is_robust_title_match(p_title, s_title, podcast.network)
                if is_match:
                    matched_pairs[i] = (priv_audio, reason)
                    unmatched_public_indices.remove(i)
                    unmatched_private_audios.remove(priv_audio)
                    break

        # ==========================================
        # DATABASE INGESTION
        # ==========================================
        count = 0
        matches = 0
        exclusive_count = 0

        # Step A: Ingest Public Episodes (with or without matched private audio)
        for i, entry in enumerate(public_entries_list):
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                dt = make_aware(datetime.fromtimestamp(time.mktime(entry.published_parsed)))
            else:
                dt = make_aware(datetime.now())

            public_audio = self.get_enclosure(entry)
            entry_title = getattr(entry, 'title', 'Untitled Episode')
            raw_desc = getattr(entry, 'description', '')
            duration_val = entry.get('itunes_duration', '')
            
            entry_guid = getattr(entry, 'id', None)
            if not entry_guid:
                fallback_string = f"{entry_title}-{dt.timestamp()}-{public_audio}"
                entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

            # Grab the private audio and reason if it was matched in our algorithm
            match_data = matched_pairs.get(i)
            if match_data:
                sub_audio, reason = match_data
                matches += 1
                self.stdout.write(self.style.SUCCESS(f"  [Matched: {reason}] {entry_title}"))
            else:
                sub_audio = None
                reason = "Public Only (No Match)"  # Default reason if no match
                self.stdout.write(self.style.WARNING(f"  [Public Only] {entry_title}"))

            final_sub_audio = sub_audio if sub_audio else public_audio

            Episode.objects.update_or_create(
                podcast=podcast, guid=entry_guid,
                defaults={
                    'title': entry_title,
                    'pub_date': dt,
                    'audio_url_public': public_audio,
                    'audio_url_subscriber': final_sub_audio,
                    'raw_description': raw_desc,
                    'clean_description': self.clean_html_description(raw_desc, podcast.network), 
                    'duration': duration_val,
                    'match_reason': reason, # <-- NEW: Save the audit trail!
                }
            )
            count += 1

        # Step B: Ingest Remaining Unmatched Private Episodes (True Exclusives)
        for priv_audio in unmatched_private_audios:
            entry = private_pool[priv_audio]
            
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                dt = make_aware(datetime.fromtimestamp(time.mktime(entry.published_parsed)))
            else:
                dt = make_aware(datetime.now())

            entry_title = getattr(entry, 'title', 'Untitled Episode')
            raw_desc = getattr(entry, 'description', '')
            duration_val = entry.get('itunes_duration', '')
            
            entry_guid = getattr(entry, 'id', None)
            if not entry_guid:
                fallback_string = f"{entry_title}-{dt.timestamp()}-{priv_audio}"
                entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

            Episode.objects.update_or_create(
                podcast=podcast, guid=entry_guid,
                defaults={
                    'title': entry_title,
                    'pub_date': dt,
                    'audio_url_public': '', 
                    'audio_url_subscriber': priv_audio,
                    'raw_description': raw_desc,
                    'clean_description': self.clean_html_description(raw_desc, podcast.network), 
                    'duration': duration_val,
                    'match_reason': 'Private Exclusive',
                }
            )
            count += 1
            exclusive_count += 1
            self.stdout.write(self.style.SUCCESS(f"  [Private Exclusive Captured] {entry_title}"))

        self.stdout.write(self.style.SUCCESS(f"Finished. Total: {count} | Matches: {matches} | Premium Exclusives: {exclusive_count}"))