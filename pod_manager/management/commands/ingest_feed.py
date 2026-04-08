import feedparser
import time
import re
from datetime import datetime
from urllib.parse import urlparse
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware
from django.conf import settings
from pod_manager.models import Podcast, Episode
import nh3
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
        
        # Pull custom tags from the DB, split by comma, and clean up whitespace
        if network.ignored_title_tags:
            tags = [t.strip().lower() for t in network.ignored_title_tags.split(',') if t.strip()]
            for tag in tags:
                title_lower = title_lower.replace(tag, '')
                
        # The Nuclear Strip: Remove absolutely everything that isn't a lowercase letter or number
        return re.sub(r'[^a-z0-9]', '', title_lower)

    def is_fuzzy_match(self, public_fp, private_fp):
        """Compares two alphanumeric fingerprints and returns True if they are 95%+ identical."""
        if not public_fp or not private_fp:
            return False
        ratio = SequenceMatcher(None, public_fp, private_fp).ratio()
        return ratio >= 0.95
    
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
            
        soup = BeautifulSoup(html_content, "html.parser")
        
        # FIX: Point to the new, dedicated description triggers
        if network.description_cut_triggers:
            triggers = [t.strip().lower() for t in network.description_cut_triggers.split(',') if t.strip()]
            
            # Find all common text containers
            for element in soup.find_all(['p', 'div', 'li', 'em', 'strong']):
                text = element.get_text().lower()
                if any(trigger in text for trigger in triggers):
                    element.decompose()
        
        # Clean up empty tags left behind
        for empty in soup.find_all(lambda tag: not tag.contents and not tag.get_text(strip=True)):
            empty.decompose()
            
        return str(soup).strip()

    def get_cached_feed(self, url, feed_type):
        """
        Fetches the raw XML. Can cache to disk to prevent hammering servers during dev.
        Controlled via the USE_LOCAL_FEED_CACHE environment variable.
        """
        # 1. Check the toggle (Defaults to False in production)
        use_cache = os.getenv('USE_LOCAL_FEED_CACHE', 'False') == 'True'
        
        # Requests handles URL parsing and Auth automatically
        parsed_url = urlparse(url)
        # Extract credentials if they exist in the URL
        auth = (parsed_url.username, parsed_url.password) if parsed_url.username else None
        # Create a clean URL without credentials for logging/caching
        clean_url = parsed_url._replace(netloc=parsed_url.hostname).geturl()

        # --- PRODUCTION MODE / CACHE MISS ---
        def fetch_live():
            self.stdout.write(f"  [LIVE FETCH] Downloading {feed_type} feed...")
            # 'auth' parameter handles redirects correctly!
            response = requests.get(clean_url, auth=auth, timeout=30, headers={'User-Agent': 'Vecto/1.0'})
            response.raise_for_status()
            return response.content

        if not use_cache:
            return feedparser.parse(fetch_live())

        # --- DEV MODE: DISK CACHING ---
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

        # 1. Fetch Subscriber Feed
        sub_data = self.get_cached_feed(podcast.subscriber_feed_url, "PRIVATE")
        if hasattr(sub_data, 'status') and sub_data.status == 401:
            self.stderr.write(self.style.ERROR("Auth Failed on Private Feed."))
            return

        slug_map = {}
        fingerprint_map = {}
        date_title_map = {}
        sub_guid_map = {}

        # Capture raw titles for debugging
        debug_private_titles = {}

        for entry in sub_data.entries:
            audio_url = self.get_enclosure(entry)
            if not audio_url:
                continue

            sub_guid_map[entry.id] = audio_url

            s_link = getattr(entry, 'link', None)
            slug = self.get_slug(s_link)
            if slug:
                slug_map[slug] = audio_url

            raw_title = getattr(entry, 'title', '')
            fp = self.get_fingerprint(raw_title, podcast.network)
            if fp:
                fingerprint_map[fp] = audio_url
                debug_private_titles[fp] = raw_title

            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                date_str = time.strftime('%Y-%m-%d', entry.published_parsed)
                date_title_map[(date_str, fp)] = audio_url

        # 2. Fetch Public Feed
        public_data = self.get_cached_feed(podcast.public_feed_url, "PUBLIC")

        feed_image = ""
        # Feedparser is a little weird with images, so we check both common locations
        if 'image' in public_data.feed:
            feed_image = public_data.feed.image.get('href') or public_data.feed.image.get('url', '')
            
        # If we found an image and it's not currently saved, update the Podcast
        if feed_image and podcast.image_url != feed_image:
            podcast.image_url = feed_image
            podcast.save()
            self.stdout.write(self.style.SUCCESS(f"  [Artwork Captured]: {feed_image}"))

        count = 0
        matches = 0
        for entry in public_data.entries:
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                dt = make_aware(datetime.fromtimestamp(time.mktime(entry.published_parsed)))
            else:
                dt = make_aware(datetime.now())

            public_audio = self.get_enclosure(entry)
            entry_title = getattr(entry, 'title', 'Untitled Episode')
            
            sub_audio = None
            
            p_link = getattr(entry, 'link', None)
            p_slug = self.get_slug(p_link)
            if p_slug and p_slug in slug_map:
                sub_audio = slug_map[p_slug]

            # 1. Exact Match Check
            p_fp = self.get_fingerprint(entry_title, podcast.network)
            if not sub_audio and p_fp in fingerprint_map:
                sub_audio = fingerprint_map[p_fp]

            # 2. Substring Match Check
            if not sub_audio and len(p_fp) >= 5:
                for sub_fp, audio in fingerprint_map.items():
                    if len(sub_fp) >= 5 and (sub_fp in p_fp or p_fp in sub_fp):
                        sub_audio = audio
                        break

            # 3. NEW: Fuzzy Match Check
            if not sub_audio and len(p_fp) >= 5:
                for sub_fp, audio in fingerprint_map.items():
                    if self.is_fuzzy_match(p_fp, sub_fp):
                        sub_audio = audio
                        break

            # 4. Guid fallback
            if not sub_audio and hasattr(entry, 'id') and entry.id in sub_guid_map:
                sub_audio = sub_guid_map[entry.id]

            if not sub_audio:
                d_str = dt.strftime('%Y-%m-%d')
                if (d_str, p_fp) in date_title_map:
                    sub_audio = date_title_map[(d_str, p_fp)]

            final_sub_audio = sub_audio if sub_audio else public_audio
            if sub_audio:
                matches += 1
            else:
                self.stdout.write(self.style.WARNING(f"  [Public Only] {entry_title}"))
                self.stdout.write(f"      -> Public Fingerprint generated: '{p_fp}'")
                
                # If this is the specific episode we are debugging, dump the private list
                if "S02E12" in entry_title or "6:00" in entry_title:
                    self.stdout.write("      -> CHECKING PRIVATE FEED CONTENTS:")
                    for priv_fp, priv_raw in debug_private_titles.items():
                        self.stdout.write(f"           Raw: '{priv_raw}' | FP: '{priv_fp}'")
            raw_desc = getattr(entry, 'description', '')
            duration_val = entry.get('itunes_duration', '')

            entry_guid = getattr(entry, 'id', None)
            if not entry_guid:
                # Hash the title, timestamp, and audio URL to guarantee a unique, repeatable ID
                fallback_string = f"{entry_title}-{dt.timestamp()}-{public_audio}"
                entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()
                self.stdout.write(self.style.WARNING(f"  [Warning] No GUID found for '{entry_title}'. Generated fallback: {entry_guid}"))

            Episode.objects.update_or_create(
                podcast=podcast,
                guid=entry_guid,
                defaults={
                    'title': entry_title,
                    'pub_date': dt,
                    'audio_url_public': public_audio,
                    'audio_url_subscriber': final_sub_audio,
                    'raw_description': raw_desc,
                    'clean_description': self.clean_html_description(raw_desc, podcast.network), 
                    'duration': duration_val,
                    }
                )   
            count += 1

        self.stdout.write(self.style.SUCCESS(f"Finished. Total: {count} | Matches: {matches}"))