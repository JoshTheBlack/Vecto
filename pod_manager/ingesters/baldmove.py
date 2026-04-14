import time
import hashlib
import requests
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
    get_cached_feed
)

def scrape_tags_from_wp(url, stdout):
    """Fetches the WP page and scrapes tags using a robust global search with heavy debugging."""
    if not url or "baldmove.com" not in url:
        return []

    def _fetch_and_extract(target_url):
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
                    return list(dict.fromkeys(tags))
        except Exception as e:
            stdout.write(f"  [WARN] Request failed for {target_url}: {str(e)}")
        return None

    # Attempt 1: The original URL
    tags = _fetch_and_extract(url)
    if tags: return tags

    # Attempt 2: URL Rewrite Fallback
    rewritten_url = None
    if "patreon.baldmove.com" in url:
        rewritten_url = url.replace("patreon.baldmove.com", "baldmove.com")
    elif "baldmove.com" in url and "patreon" not in url:
        rewritten_url = url.replace("https://baldmove.com", "https://patreon.baldmove.com")
        
    if rewritten_url:
        stdout.write(f"  [INFO] Retrying with rewritten URL: {rewritten_url}")
        tags = _fetch_and_extract(rewritten_url)
        if tags: return tags

    return []

def get_feed_tags(entry):
    """Extracts category tags directly from the RSS feedparser entry."""
    if hasattr(entry, 'tags'):
        # feedparser puts <category> data inside entry.tags as a list of dicts with a 'term' key
        return [t.get('term').strip() for t in entry.tags if t.get('term')]
    return []

def run_ingest(podcast, stdout):
    stdout.write(f"--- Harvesting (Bald Move Strategy): {podcast.title} ---")

    sub_data = get_cached_feed(podcast.subscriber_feed_url, "PRIVATE", stdout)
    if hasattr(sub_data, 'status') and sub_data.status == 401:
        stdout.write("[ERROR] Auth Failed on Private Feed.")
        return

    public_data = get_cached_feed(podcast.public_feed_url, "PUBLIC", stdout)

    feed_image = ""
    if 'image' in public_data.feed:
        feed_image = public_data.feed.image.get('href') or public_data.feed.image.get('url', '')
    if feed_image and podcast.image_url != feed_image:
        podcast.image_url = feed_image
        podcast.save()
        stdout.write(f"  [Artwork Captured]: {feed_image}")

    private_pool = {}
    for entry in sub_data.entries:
        audio_url = get_enclosure(entry)
        if audio_url: private_pool[audio_url] = entry
            
    unmatched_private_audios = set(private_pool.keys())
    public_entries_list = list(public_data.entries)
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
        
        # Grab public tags from the RSS feed
        pub_tags = get_feed_tags(entry)
        priv_tags = []
        
        entry_guid = getattr(entry, 'id', None)
        if not entry_guid:
            fallback_string = f"{entry_title}-{dt.timestamp()}-{public_audio}"
            entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

        match_data = matched_pairs.get(i)
        if match_data:
            sub_audio, reason = match_data
            matches += 1
            stdout.write(f"  [Matched: {reason}] {entry_title}")
            
            priv_entry = private_pool[sub_audio]
            priv_link = getattr(priv_entry, 'link', '')
            final_link = priv_link if priv_link else pub_link
            
            # Grab private tags from the matched RSS feed
            priv_tags = get_feed_tags(priv_entry)
        else:
            sub_audio = None
            reason = "Public Only (No Match)"
            stdout.write(f"  [Public Only] {entry_title}")
            final_link = pub_link

        final_sub_audio = sub_audio if sub_audio else public_audio
        
        # Merge public and private tags, deduplicating case-insensitively
        combined_tags = pub_tags + priv_tags
        merged_tags = list({t.lower(): t for t in combined_tags}.values())
        
        episode_exists = Episode.objects.filter(guid=entry_guid).exists()
        
        # Only fallback to the web scraper if NO tags were found in the RSS feeds
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
        
        # Grab private tags from the RSS feed
        priv_tags = get_feed_tags(entry)
        merged_tags = list({t.lower(): t for t in priv_tags}.values())
        
        entry_guid = getattr(entry, 'id', None)
        if not entry_guid:
            fallback_string = f"{entry_title}-{dt.timestamp()}-{priv_audio}"
            entry_guid = hashlib.md5(fallback_string.encode('utf-8')).hexdigest()

        episode_exists = Episode.objects.filter(guid=entry_guid).exists()
        
        # Fallback to web scraper if no RSS tags found
        if not episode_exists and not merged_tags and link:
            merged_tags = scrape_tags_from_wp(link, stdout)
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
                'link': link,
                'audio_url_public': '', 
                'audio_url_subscriber': priv_audio,
                'raw_description': raw_desc,
                'clean_description': clean_html_description(raw_desc, podcast.network), 
                'duration': duration_val,
                'match_reason': 'Private Exclusive',
                'tags': merged_tags,
            }
        )
        count += 1
        exclusive_count += 1
        stdout.write(f"  [Private Exclusive Captured] {entry_title}")

    stdout.write(f"Finished. Total: {count} | Matches: {matches} | Premium Exclusives: {exclusive_count}")