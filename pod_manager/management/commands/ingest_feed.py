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
        if network.ignored_title_tags:
            tags = [t.strip().lower() for t in network.ignored_title_tags.split(',') if t.strip()]
            for tag in tags:
                title_lower = title_lower.replace(tag, '')
        return re.sub(r'[^a-z0-9]', '', title_lower)

    def is_fuzzy_match(self, public_fp, private_fp):
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
        import re 
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

        slug_map = {}
        fingerprint_map = {}
        date_title_map = {}
        sub_guid_map = {}

        for entry in sub_data.entries:
            audio_url = self.get_enclosure(entry)
            if not audio_url: continue

            sub_guid_map[entry.id] = audio_url
            s_link = getattr(entry, 'link', None)
            slug = self.get_slug(s_link)
            if slug: slug_map[slug] = audio_url

            raw_title = getattr(entry, 'title', '')
            fp = self.get_fingerprint(raw_title, podcast.network)
            if fp: fingerprint_map[fp] = audio_url

            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                date_str = time.strftime('%Y-%m-%d', entry.published_parsed)
                date_title_map[(date_str, fp)] = audio_url

        public_data = self.get_cached_feed(podcast.public_feed_url, "PUBLIC")

        feed_image = ""
        if 'image' in public_data.feed:
            feed_image = public_data.feed.image.get('href') or public_data.feed.image.get('url', '')
        if feed_image and podcast.image_url != feed_image:
            podcast.image_url = feed_image
            podcast.save()
            self.stdout.write(self.style.SUCCESS(f"  [Artwork Captured]: {feed_image}"))

        count = 0
        matches = 0
        # NEW: Keep track of private URLs that have been successfully paired
        matched_sub_urls = set()

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
            if p_slug and p_slug in slug_map: sub_audio = slug_map[p_slug]

            p_fp = self.get_fingerprint(entry_title, podcast.network)
            if not sub_audio and p_fp in fingerprint_map: sub_audio = fingerprint_map[p_fp]
            
            if not sub_audio and len(p_fp) >= 5:
                for sub_fp, audio in fingerprint_map.items():
                    if len(sub_fp) >= 5 and (sub_fp in p_fp or p_fp in sub_fp):
                        sub_audio = audio
                        break

            if not sub_audio and len(p_fp) >= 5:
                for sub_fp, audio in fingerprint_map.items():
                    if self.is_fuzzy_match(p_fp, sub_fp):
                        sub_audio = audio
                        break

            if not sub_audio and hasattr(entry, 'id') and entry.id in sub_guid_map:
                sub_audio = sub_guid_map[entry.id]

            if not sub_audio:
                d_str = dt.strftime('%Y-%m-%d')
                if (d_str, p_fp) in date_title_map: sub_audio = date_title_map[(d_str, p_fp)]

            final_sub_audio = sub_audio if sub_audio else public_audio
            if sub_audio:
                matches += 1
                matched_sub_urls.add(sub_audio) # Track it!
            else:
                self.stdout.write(self.style.WARNING(f"  [Public Only] {entry_title}"))
                
            raw_desc = getattr(entry, 'description', '')
            duration_val = entry.get('itunes_duration', '')
            entry_guid = getattr(entry, 'id', None)
            if not entry_guid:
                fallback_string = f"{entry_title}-{dt.timestamp()}-{public_audio}"
                entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

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
                }
            )   
            count += 1

        # ==========================================
        # NEW: SECOND PASS - PRIVATE EXCLUSIVES
        # ==========================================
        exclusive_count = 0
        for entry in sub_data.entries:
            audio_url = self.get_enclosure(entry)
            
            # Skip if it has no audio, or we already matched it to a public episode above
            if not audio_url or audio_url in matched_sub_urls:
                continue
                
            # It's an exclusive! Let's ingest it.
            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                dt = make_aware(datetime.fromtimestamp(time.mktime(entry.published_parsed)))
            else:
                dt = make_aware(datetime.now())

            entry_title = getattr(entry, 'title', 'Untitled Episode')
            raw_desc = getattr(entry, 'description', '')
            duration_val = entry.get('itunes_duration', '')
            
            entry_guid = getattr(entry, 'id', None)
            if not entry_guid:
                fallback_string = f"{entry_title}-{dt.timestamp()}-{audio_url}"
                entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

            Episode.objects.update_or_create(
                podcast=podcast, guid=entry_guid,
                defaults={
                    'title': entry_title,
                    'pub_date': dt,
                    'audio_url_public': '', # Blank intentionally! The UI will handle this.
                    'audio_url_subscriber': audio_url,
                    'raw_description': raw_desc,
                    'clean_description': self.clean_html_description(raw_desc, podcast.network), 
                    'duration': duration_val,
                }
            )
            count += 1
            exclusive_count += 1
            self.stdout.write(self.style.SUCCESS(f"  [Private Exclusive Captured] {entry_title}"))

        self.stdout.write(self.style.SUCCESS(f"Finished. Total: {count} | Matches: {matches} | Premium Exclusives: {exclusive_count}"))