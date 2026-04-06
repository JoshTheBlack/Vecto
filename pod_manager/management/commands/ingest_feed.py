import feedparser
import time
import re
from datetime import datetime
from urllib.parse import urlparse
from django.core.management.base import BaseCommand
from django.utils.timezone import make_aware
from pod_manager.models import Podcast, Episode
import nh3
import os
import urllib.request
import hashlib
import base64

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

    def get_fingerprint(self, title):
        if not title:
            return ""
        
        # 1. CHARACTER NORMALIZATION
        title = title.replace('–', '-').replace('—', '-') 
        title = title.replace('“', '"').replace('”', '"') 
        title = title.replace('‘', "'").replace('’', "'") 
        
        title_lower = title.lower()
        
        # 2. TAG STRIPPING
        tags = [
            '(ad-free)', '(premium)', '(private)', 
            '(instant)', 'instant take', 'instant talk',
            '- public premiere', 'public premiere',
            'off the clock'
        ]
        for tag in tags:
            title_lower = title_lower.replace(tag, '')
            
        # 3. KNOWN PREFIX STRIPPING
        title_lower = re.sub(r'^(ahs:\d{4}|american horror story podcast|bald move pulp|pulp|the pitt|into the pitt)\s*-\s*', '', title_lower)

        # 4. FINAL CLEANUP
        return re.sub(r'[^a-zA-Z0-9]', '', title_lower).strip()

    def get_enclosure(self, entry):
        if hasattr(entry, 'enclosures') and entry.enclosures:
            return entry.enclosures[0].href
        if hasattr(entry, 'links'):
            for link in entry.links:
                if link.get('rel') == 'enclosure':
                    return link.href
        return ""

    def clean_html_description(self, raw_text):
        if not raw_text:
            return ""
            
        # 1. The Guillotine: Chop off the entire boilerplate footer
        # This regex looks for the start of the boilerplate and splits the string.
        # We only keep the first half (parts[0]).
        boilerplate_triggers = r'(Hey there!\s*Check out|Join the discussion:|Follow us:|Leave Us A Review)'
        text = re.split(boilerplate_triggers, raw_text, flags=re.IGNORECASE)[0]
        
        # 2. Remove any lingering Megaphone Ad Choices that might have been ABOVE the guillotine
        text = re.sub(r'<p>\s*Learn more about your ad choices.*?adchoices.*?</a>\s*</p>', '', text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r'Learn more about your ad choices.*?adchoices\.?', '', text, flags=re.IGNORECASE | re.DOTALL)
        
        # 3. Normalize spaces
        text = text.replace('\xa0', '&nbsp;')
        
        # 4. Clean with nh3
        allowed_tags = {'a', 'p', 'b', 'i', 'em', 'strong', 'br', 'ul', 'ol', 'li'}
        allowed_attributes = {'a': {'href'}} 
        text = nh3.clean(text, tags=allowed_tags, attributes=allowed_attributes)
        
        # 5. Final cleanup
        text = text.replace('<p></p>', '').replace('<p> </p>', '').strip()
        
        return text

    def get_cached_feed(self, url, feed_type):
        """
        [DEV HACK] Caches the raw XML to disk to prevent hammering servers.
        Includes Basic Auth handling for private feeds.
        """
        cache_dir = '.feed_cache'
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
            
        # Parse URL and create a clean version without credentials
        parsed_url = urlparse(url)
        clean_url = parsed_url._replace(netloc=parsed_url.hostname).geturl()
        
        # Create a unique filename based on the clean URL
        url_hash = hashlib.md5(clean_url.encode('utf-8')).hexdigest()
        cache_file = os.path.join(cache_dir, f"{feed_type}_{url_hash}.xml")
        
        if os.path.exists(cache_file):
            self.stdout.write(self.style.WARNING(f"  [CACHE HIT] Loading {feed_type} feed from local disk..."))
            return feedparser.parse(cache_file)
            
        self.stdout.write(f"  [CACHE MISS] Downloading {feed_type} feed from web...")
        
        # Setup Request headers USING THE CLEAN URL to avoid the port error
        req = urllib.request.Request(clean_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        
        # Inject Basic Authentication if credentials exist in the original URL
        if parsed_url.username and parsed_url.password:
            auth_str = f"{parsed_url.username}:{parsed_url.password}"
            b64_auth_str = base64.b64encode(auth_str.encode('utf-8')).decode('utf-8')
            req.add_header('Authorization', f'Basic {b64_auth_str}')

        try:
            with urllib.request.urlopen(req) as response:
                raw_xml = response.read()
                with open(cache_file, 'wb') as f:
                    f.write(raw_xml)
                return feedparser.parse(raw_xml)
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"  [WARNING] Caching failed ({e}). Falling back to live fetch."))
            return feedparser.parse(url)
        
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
            fp = self.get_fingerprint(raw_title)
            if fp:
                fingerprint_map[fp] = audio_url
                debug_private_titles[fp] = raw_title

            if hasattr(entry, 'published_parsed') and entry.published_parsed:
                date_str = time.strftime('%Y-%m-%d', entry.published_parsed)
                date_title_map[(date_str, fp)] = audio_url

        # 2. Fetch Public Feed
        public_data = self.get_cached_feed(podcast.public_feed_url, "PUBLIC")

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

            p_fp = self.get_fingerprint(entry_title)
            if not sub_audio and p_fp in fingerprint_map:
                sub_audio = fingerprint_map[p_fp]

            if not sub_audio and len(p_fp) >= 5:
                for sub_fp, audio in fingerprint_map.items():
                    if len(sub_fp) >= 5 and (sub_fp in p_fp or p_fp in sub_fp):
                        sub_audio = audio
                        break

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

            Episode.objects.update_or_create(
                podcast=podcast,
                guid=getattr(entry, 'id', entry_title),
                defaults={
                    'title': entry_title,
                    'pub_date': dt,
                    'audio_url_public': public_audio,
                    'audio_url_subscriber': final_sub_audio,
                    'raw_description': raw_desc,
                    'clean_description': self.clean_html_description(raw_desc), 
                }
            )
            count += 1

        self.stdout.write(self.style.SUCCESS(f"Finished. Total: {count} | Matches: {matches}"))