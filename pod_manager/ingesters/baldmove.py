import time
import re
import hashlib
import requests
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from django.utils.timezone import make_aware
from pod_manager.models import Episode

from .default import (
    get_slug,
    get_fingerprint,
    is_robust_title_match,
    get_enclosure,
    clean_html_description,
    get_cached_feed,
    extract_rss_chapters
)

logger = logging.getLogger(__name__)

def scrape_tags_from_wp(url, stdout):
    """Fetches the WP page and scrapes tags using a robust global search with heavy debugging."""
    if not url or "baldmove.com" not in url:
        return []

    def _fetch_and_extract(target_url):
        logger.debug(f"Scraping Bald Move WP tags from URL: {target_url}")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
            }
            resp = requests.get(target_url, timeout=10, headers=headers)
            
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                
                title = soup.title.string if soup.title else "No Title"
                if "Just a moment" in title or "Cloudflare" in title:
                    logger.warning(f"Cloudflare blocked scrape attempt on {target_url}")
                    stdout.write(f"  [WARN] Blocked by Cloudflare on {target_url}")
                    return None
                
                tag_elements = soup.find_all('a', rel=True)
                tags = []
                
                for a in tag_elements:
                    rels = a.get('rel', [])
                    if isinstance(rels, str):
                        rels = rels.split()
                        
                    if 'tag' in rels and 'category' not in rels:
                        tag_text = a.text.strip()
                        if tag_text:
                            tags.append(tag_text)
                
                if tags:
                    unique_tags = list(dict.fromkeys(tags))
                    logger.debug(f"Successfully scraped tags: {unique_tags}")
                    return unique_tags
            else:
                logger.warning(f"Scrape returned HTTP {resp.status_code} for {target_url}")
                
        except Exception as e:
            logger.error(f"Scrape request failed for {target_url}: {str(e)}", exc_info=True)
            stdout.write(f"  [WARN] Request failed for {target_url}: {str(e)}")
        return None

    tags = _fetch_and_extract(url)
    if tags: return tags

    rewritten_url = None
    if "patreon.baldmove.com" in url:
        rewritten_url = url.replace("patreon.baldmove.com", "baldmove.com")
    elif "baldmove.com" in url and "patreon" not in url:
        rewritten_url = url.replace("https://baldmove.com", "https://patreon.baldmove.com")
        
    if rewritten_url:
        logger.info(f"Retrying web scrape with rewritten URL: {rewritten_url}")
        stdout.write(f"  [INFO] Retrying with rewritten URL: {rewritten_url}")
        tags = _fetch_and_extract(rewritten_url)
        if tags: return tags

    logger.debug(f"No tags found on web page for {url}")
    return []

def get_feed_tags(entry):
    """Extracts category tags directly from the RSS feedparser entry."""
    if hasattr(entry, 'tags'):
        return [t.get('term').strip() for t in entry.tags if t.get('term')]
    return []

def parse_html_chapters(html_description):
    """Scrapes Bald Move's specific <ul><li>HH:MM:SS format."""
    if not html_description:
        return None
        
    soup = BeautifulSoup(html_description, 'html.parser')
    for ul in soup.find_all('ul'):
        chapters = []
        li_tags = ul.find_all('li')
        
        # A true chapter list usually has multiple entries
        if len(li_tags) < 2:
            continue
            
        is_valid_chapter_list = True
        for li in li_tags:
            text = li.get_text().strip()
            # Match formats like "00:00:30 - Title" or "01:20 — Title"
            match = re.match(r'^(\d{1,2}:\d{2}(?::\d{2})?)\s*[-—–]+\s*(.+)$', text)
            
            if match:
                time_str = match.group(1)
                title = match.group(2).strip()
                
                parts = time_str.split(':')
                seconds = 0
                if len(parts) == 3:
                    seconds = int(parts[0])*3600 + int(parts[1])*60 + int(parts[2])
                elif len(parts) == 2:
                    seconds = int(parts[0])*60 + int(parts[1])
                    
                chapters.append({"startTime": seconds, "title": title})
            else:
                # If any item in the UL fails the regex, it's probably just a normal list
                is_valid_chapter_list = False
                break 

        if is_valid_chapter_list and chapters:
            return {"version": "1.2.0", "chapters": chapters}
            
    return None

def run_ingest(podcast, stdout):
    logger.info(f"Starting Ingest Strategy for podcast: {podcast.title} (ID: {podcast.id})")
    stdout.write(f"--- Harvesting: {podcast.title} ---")

    sub_data = None
    if podcast.subscriber_feed_url:
        sub_data = get_cached_feed(podcast.subscriber_feed_url, "PRIVATE", stdout)
        if hasattr(sub_data, 'status') and sub_data.status == 401:
            logger.error(f"Auth Failed on Private Feed for podcast: {podcast.title}")
            stdout.write("[ERROR] Auth Failed on Private Feed.")
            return

    public_data = None
    if podcast.public_feed_url:
        public_data = get_cached_feed(podcast.public_feed_url, "PUBLIC", stdout)

    # 3. Dynamic Title & Artwork Extraction
    feed_image = ""
    feed_title = ""
    source_data = public_data if public_data else sub_data
    
    if source_data and hasattr(source_data, 'feed'):
        # Image extraction
        if 'image' in source_data.feed:
            feed_image = source_data.feed.image.get('href') or source_data.feed.image.get('url', '')
        if not feed_image and 'itunes_image' in source_data.feed:
            feed_image = source_data.feed.itunes_image.get('href', '')
            
        # Title extraction
        feed_title = source_data.feed.get('title', '')

    needs_save = False

    # Process and Update Title
    if feed_title:
        cleaned_title = feed_title
        if podcast.network.ignored_title_tags:
            tags = [t.strip() for t in podcast.network.ignored_title_tags.split(',') if t.strip()]
            for tag in tags:
                # Case-insensitive removal of the ignored tag
                pattern = re.compile(re.escape(tag), re.IGNORECASE)
                cleaned_title = pattern.sub('', cleaned_title)
        
        # Clean up any leftover double-spaces or trailing spaces
        cleaned_title = " ".join(cleaned_title.split())
        
        if cleaned_title and podcast.title != cleaned_title:
            logger.info(f"Updating podcast title from '{podcast.title}' to '{cleaned_title}'")
            podcast.title = cleaned_title
            stdout.write(f"  [Title Updated]: {cleaned_title}")
            needs_save = True

    # Process and Update Artwork
    if feed_image and podcast.image_url != feed_image:
        logger.info(f"Updating podcast artwork for {podcast.title} to {feed_image}")
        podcast.image_url = feed_image
        stdout.write(f"  [Artwork Captured]: {feed_image}")
        needs_save = True

    # Save the database record only once if anything changed
    if needs_save:
        podcast.save()

    private_pool = {}
    unmatched_private_audios = set()
    if sub_data:
        for entry in sub_data.entries:
            audio_url = get_enclosure(entry)
            if audio_url: 
                private_pool[audio_url] = entry
        unmatched_private_audios = set(private_pool.keys())
            
    public_entries_list = list(public_data.entries) if public_data else []
    unmatched_public_indices = set(range(len(public_entries_list)))
    matched_pairs = {} 

    for i in list(unmatched_public_indices):
        pub_entry = public_entries_list[i]
        p_slug = get_slug(getattr(pub_entry, 'link', None))
        p_id = getattr(pub_entry, 'id', None)
        
        for priv_audio in list(unmatched_private_audios):
            priv_entry = private_pool[priv_audio]
            s_slug = get_slug(getattr(priv_entry, 'link', None))
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

    for i in list(unmatched_public_indices):
        pub_entry = public_entries_list[i]
        p_fp = get_fingerprint(getattr(pub_entry, 'title', ''), podcast.network)
        
        for priv_audio in list(unmatched_private_audios):
            priv_entry = private_pool[priv_audio]
            s_fp = get_fingerprint(getattr(priv_entry, 'title', ''), podcast.network)
            
            if p_fp and s_fp and p_fp == s_fp:
                matched_pairs[i] = (priv_audio, "Exact Title Match")
                unmatched_public_indices.remove(i)
                unmatched_private_audios.remove(priv_audio)
                break

    for i in list(unmatched_public_indices):
        pub_entry = public_entries_list[i]
        p_title = getattr(pub_entry, 'title', '')
        
        for priv_audio in list(unmatched_private_audios):
            priv_entry = private_pool[priv_audio]
            s_title = getattr(priv_entry, 'title', '')
            
            is_match, reason = is_robust_title_match(p_title, s_title, podcast.network)
            if is_match:
                matched_pairs[i] = (priv_audio, reason)
                unmatched_public_indices.remove(i)
                unmatched_private_audios.remove(priv_audio)
                break

    count = 0
    matches = 0
    exclusive_count = 0

    for i, entry in enumerate(public_entries_list):
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            dt = make_aware(datetime.fromtimestamp(time.mktime(entry.published_parsed)))
        else:
            dt = make_aware(datetime.now())

        public_audio = get_enclosure(entry)
        entry_title = getattr(entry, 'title', 'Untitled Episode')
        raw_desc = getattr(entry, 'description', '')
        duration_val = entry.get('itunes_duration', '')
        pub_link = getattr(entry, 'link', '')
        
        pub_tags = get_feed_tags(entry)
        priv_tags = []
        
        entry_guid = getattr(entry, 'id', None)
        if not entry_guid:
            fallback_string = f"{entry_title}-{dt.timestamp()}-{public_audio}"
            entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

        # --- EXTRACT PUBLIC CHAPTERS ---
        pub_chapters = extract_rss_chapters(entry)
        if not pub_chapters:
            pub_chapters = parse_html_chapters(raw_desc)
            if pub_chapters: stdout.write("  -> Scraped HTML Chapters (Public)")

        if match_data := matched_pairs.get(i):
            sub_audio, reason = match_data
            matches += 1
            stdout.write(f"  [Matched: {reason}] {entry_title}")
            logger.debug(f"Episode Match: '{entry_title}' -> {reason}")
            
            priv_entry = private_pool[sub_audio]
            priv_link = getattr(priv_entry, 'link', '')
            final_link = priv_link if priv_link else pub_link
            
            priv_tags = get_feed_tags(priv_entry)

            # --- EXTRACT PRIVATE CHAPTERS ---
            priv_raw_desc = getattr(priv_entry, 'description', '')
            priv_chapters = extract_rss_chapters(priv_entry)
            if not priv_chapters:
                priv_chapters = parse_html_chapters(priv_raw_desc)
                if priv_chapters: stdout.write("  -> Scraped HTML Chapters (Private)")

        else:
            sub_audio = None
            reason = "Public Only (No Match)"
            stdout.write(f"  [Public Only] {entry_title}")
            logger.debug(f"Episode Unmatched (Public Only): '{entry_title}'")
            final_link = pub_link
            priv_chapters = None

        final_sub_audio = sub_audio if sub_audio else public_audio
        
        combined_tags = pub_tags + priv_tags
        merged_tags = list({t.lower(): t for t in combined_tags}.values())
        
        episode_exists = Episode.objects.filter(guid=entry_guid).exists()
        
        if not episode_exists and not merged_tags and final_link:
            merged_tags = scrape_tags_from_wp(final_link, stdout)
            if merged_tags:
                stdout.write(f"  -> Web Scraped Tags: {merged_tags}")
            time.sleep(0.2) 
        elif not episode_exists and merged_tags:
            stdout.write(f"  -> RSS Tags Extracted: {merged_tags}")

        Episode.objects.update_or_create(
            podcast=podcast, guid=entry_guid,
            defaults={
                'title': entry_title,
                'pub_date': dt,
                'link': final_link,
                'audio_url_public': public_audio,
                'audio_url_subscriber': final_sub_audio,
                'raw_description': raw_desc,
                'clean_description': clean_html_description(raw_desc, podcast.network), 
                'duration': duration_val,
                'match_reason': reason,
                'tags': merged_tags,
                'chapters_public': pub_chapters,
                'chapters_private': priv_chapters,
            }
        )
        count += 1

    for priv_audio in unmatched_private_audios:
        entry = private_pool[priv_audio]
        
        if hasattr(entry, 'published_parsed') and entry.published_parsed:
            dt = make_aware(datetime.fromtimestamp(time.mktime(entry.published_parsed)))
        else:
            dt = make_aware(datetime.now())

        entry_title = getattr(entry, 'title', 'Untitled Episode')
        raw_desc = getattr(entry, 'description', '')
        duration_val = entry.get('itunes_duration', '')
        link = getattr(entry, 'link', '')
        
        priv_tags = get_feed_tags(entry)
        merged_tags = list({t.lower(): t for t in priv_tags}.values())
        
        entry_guid = getattr(entry, 'id', None)
        if not entry_guid:
            fallback_string = f"{entry_title}-{dt.timestamp()}-{priv_audio}"
            entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

        episode_exists = Episode.objects.filter(guid=entry_guid).exists()
        
        if not episode_exists and not merged_tags and link:
            merged_tags = scrape_tags_from_wp(link, stdout)
            if merged_tags:
                stdout.write(f"  -> Web Scraped Tags: {merged_tags}")
            time.sleep(0.2)
        elif not episode_exists and merged_tags:
            stdout.write(f"  -> RSS Tags Extracted: {merged_tags}")

        # --- EXTRACT PRIVATE EXCLUSIVE CHAPTERS ---
        priv_chapters = extract_rss_chapters(entry)
        if not priv_chapters:
            priv_chapters = parse_html_chapters(raw_desc)
            if priv_chapters: stdout.write("  -> Scraped HTML Chapters (Private Exclusive)")

        Episode.objects.update_or_create(
            podcast=podcast, guid=entry_guid,
            defaults={
                'title': entry_title,
                'pub_date': dt,
                'link': link,
                'audio_url_public': '', 
                'audio_url_subscriber': priv_audio,
                'raw_description': raw_desc,
                'clean_description': clean_html_description(raw_desc, podcast.network), 
                'duration': duration_val,
                'match_reason': 'Private Exclusive',
                'tags': merged_tags,
                'chapters_public': None,
                'chapters_private': priv_chapters,
            }
        )
        count += 1
        exclusive_count += 1
        stdout.write(f"  [Private Exclusive Captured] {entry_title}")
        logger.debug(f"Episode Unmatched (Private Exclusive): '{entry_title}'")

    summary = f"Finished. Total: {count} | Matches: {matches} | Premium Exclusives: {exclusive_count}"
    stdout.write(summary)
    logger.info(f"Ingestion complete for {podcast.title}. Total: {count}, Matches: {matches}, Exclusives: {exclusive_count}")