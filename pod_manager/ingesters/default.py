import feedparser
import time
import re
import os
import requests
import hashlib
import logging
from datetime import datetime
from urllib.parse import urlparse
from django.utils.timezone import make_aware
from django.conf import settings
from pod_manager.models import Podcast, Episode
from pod_manager.tasks import task_rebuild_episode_fragments, task_rebuild_podcast_fragments
from difflib import SequenceMatcher
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def extract_rss_chapters(entry):
    """Attempts to extract chapters from Podcast Index namespace or Podlove Simple Chapters."""
    
    # 1. Check for Podcast Index <podcast:chapters> tag
    if hasattr(entry, 'podcast_chapters') and isinstance(entry.podcast_chapters, dict):
        url = entry.podcast_chapters.get('url')
        if url:
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code == 200:
                    return resp.json()
            except Exception:
                pass

    # 2. Check for Podlove <psc:chapters>
    if hasattr(entry, 'psc_chapters') and hasattr(entry.psc_chapters, 'chapters'):
        chapters = []
        for ch in entry.psc_chapters.chapters:
            start_str = ch.get('start', '0')
            title = ch.get('title', '')
            
            seconds = 0
            if ':' in start_str:
                parts = start_str.split(':')
                if len(parts) == 3:
                    seconds = int(parts[0])*3600 + int(parts[1])*60 + float(parts[2])
                elif len(parts) == 2:
                    seconds = int(parts[0])*60 + float(parts[1])
            else:
                try: seconds = float(start_str)
                except: pass
                    
            chapters.append({
                "startTime": int(seconds),
                "title": title
            })
        if chapters:
            return {"version": "1.2.0", "chapters": chapters}
            
    return None

def get_slug(url):
    if not url or "?" in url:
        return None
    try:
        path = urlparse(url).path
        return path.strip('/')
    except Exception:
        return None

def get_fingerprint(title, network):
    if not title:
        return ""
    title_lower = title.lower()
    if network.ignored_title_tags:
        tags = [t.strip().lower() for t in network.ignored_title_tags.split(',') if t.strip()]
        for tag in tags:
            title_lower = title_lower.replace(tag, '')
    return re.sub(r'[^a-z0-9]', '', title_lower)

def is_robust_title_match(public_title, private_title, network):
    if not public_title or not private_title:
        return False, ""
        
    pub_fp = get_fingerprint(public_title, network)
    priv_fp = get_fingerprint(private_title, network)
    
    if pub_fp == priv_fp and len(pub_fp) > 0:
        return True, "Fuzzy Match (Exact Fingerprint)"
        
    if len(priv_fp) > 8 and priv_fp in pub_fp:
        return True, "Fuzzy Match (Substring - Private in Public)"
    if len(pub_fp) > 8 and pub_fp in priv_fp:
        return True, "Fuzzy Match (Substring - Public in Private)"
        
    pub_words = set(re.sub(r'[^a-z0-9\s]', '', public_title.lower()).split())
    priv_words = set(re.sub(r'[^a-z0-9\s]', '', private_title.lower()).split())
    
    stop_words = {'the', 'a', 'an', 'and', 'or', 'in', 'on', 'of', 'to', 'for', 'with', 'part', 'episode'}
    pub_words -= stop_words
    priv_words -= stop_words
    
    if pub_words and priv_words:
        overlap = priv_words.intersection(pub_words)
        if len(overlap) / float(len(priv_words)) >= 0.8:
            return True, "Fuzzy Match (Word Overlap >= 80%)"

    if len(pub_fp) >= 5 and len(priv_fp) >= 5:
        ratio = SequenceMatcher(None, pub_fp, priv_fp).ratio()
        if ratio > 0.80:  
            return True, f"Fuzzy Match (Sequence Ratio: {ratio:.2f})"
            
    return False, ""

def get_enclosure(entry):
    if hasattr(entry, 'enclosures') and entry.enclosures:
        return entry.enclosures[0].href
    if hasattr(entry, 'links'):
        for link in entry.links:
            if link.get('rel') == 'enclosure':
                return link.href
    return ""

def clean_html_description(html_content, network):
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

def get_cached_feed(url, feed_type, stdout):
    use_cache = os.getenv('USE_LOCAL_FEED_CACHE', 'False') == 'True'
    parsed_url = urlparse(url)
    auth = (parsed_url.username, parsed_url.password) if parsed_url.username else None
    clean_url = parsed_url._replace(netloc=parsed_url.hostname).geturl()

    def fetch_live():
        stdout.write(f"  [LIVE FETCH] Downloading {feed_type} feed...")
        logger.debug(f"Fetching live {feed_type} feed from {clean_url}")
        try:
            response = requests.get(clean_url, auth=auth, timeout=30, headers={'User-Agent': 'Vecto/1.0'})
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP Error fetching {feed_type} feed from {clean_url}: {e}", exc_info=True)
            raise

    if not use_cache:
        return feedparser.parse(fetch_live())

    cache_dir = os.path.join(settings.BASE_DIR, '.feed_cache')
    if not os.path.exists(cache_dir): os.makedirs(cache_dir)
    url_hash = hashlib.md5(clean_url.encode('utf-8')).hexdigest()
    cache_file = os.path.join(cache_dir, f"{feed_type}_{url_hash}.xml")

    if os.path.exists(cache_file):
        stdout.write(f"  [CACHE HIT] Loading {feed_type} from disk...")
        logger.debug(f"Loaded {feed_type} feed from local disk cache: {cache_file}")
        return feedparser.parse(cache_file)

    content = fetch_live()
    with open(cache_file, 'wb') as f:
        f.write(content)
    logger.debug(f"Saved {feed_type} feed to local disk cache: {cache_file}")
    return feedparser.parse(content)

def commit_episode(podcast, pub_entry, sub_entry, match_reason, stdout, enhancer=None):
    """
    Intelligently creates or updates an episode.
    - Prevents duplicates by checking BOTH guid_public and guid_private.
    - Prevents overwriting manual edits if is_metadata_locked is True.
    - NEW: Rejects algorithmic matches if an episode was manually unpaired.
    """
    pub_guid = getattr(pub_entry, 'id', None) if pub_entry else None
    sub_guid = getattr(sub_entry, 'id', None) if sub_entry else None
    
    # 1. Independent Lookup: Find the exact database records for these GUIDs
    ep_pub = Episode.objects.filter(podcast=podcast, guid_public=pub_guid).first() if pub_guid else None
    ep_priv = Episode.objects.filter(podcast=podcast, guid_private=sub_guid).first() if sub_guid else None
        
    # ==========================================================
    # THE DIVORCE CLAUSE (Anti-Match Protection)
    # ==========================================================
    # If the fuzzy algorithm attempted to merge them, but the database says 
    # one (or both) of them were Manually Unpaired, we reject the match.
    if pub_entry and sub_entry:
        is_pub_unpaired = ep_pub and ep_pub.match_reason == 'Manually Unpaired'
        is_priv_unpaired = ep_priv and ep_priv.match_reason == 'Manually Unpaired'
        
        if is_pub_unpaired or is_priv_unpaired:
            stdout.write(f"  [Split Enforced] Preventing algorithmic re-merge of unpaired episode: {getattr(pub_entry, 'title', '')}")
            
            # Recursively save them as two distinct orphans, preserving their unpaired status
            commit_episode(podcast, pub_entry, None, "Manually Unpaired", stdout, enhancer)
            commit_episode(podcast, None, sub_entry, "Manually Unpaired", stdout, enhancer)
            return None # Exit this joint commit
    # ==========================================================

    # 2. Standard Anti-Zombie Lookup
    episode = ep_pub or ep_priv
    is_new = False
    
    if not episode:
        episode = Episode(podcast=podcast)
        is_new = True
        
    # 3. Always update the routing identifiers
    if pub_guid: episode.guid_public = pub_guid
    if sub_guid: episode.guid_private = sub_guid
    
    # Protect manual audit trails from being overwritten by the algorithm
    protected_reasons = ['Manually Unpaired', 'Manual Merge (Merge Desk)']
    if episode.match_reason not in protected_reasons:
        episode.match_reason = match_reason
        
    # 4. Always update audio URLs (Hosts frequently rotate CDNs or ad-tracking prefixes)
    if pub_entry:
        episode.audio_url_public = get_enclosure(pub_entry) 
    if sub_entry:
        episode.audio_url_subscriber = get_enclosure(sub_entry)

    # 5. THE METADATA LOCK
    if not episode.is_metadata_locked:
        source_entry = pub_entry if pub_entry else sub_entry
        
        episode.title = getattr(source_entry, 'title', 'Untitled Episode')
        raw_desc = getattr(source_entry, 'description', '')
        episode.raw_description = raw_desc
        episode.clean_description = clean_html_description(raw_desc, podcast.network)
        
        # Prefer private link, fallback to public
        priv_link = getattr(sub_entry, 'link', '') if sub_entry else ''
        pub_link = getattr(pub_entry, 'link', '') if pub_entry else ''
        episode.link = priv_link if priv_link else pub_link
        
        episode.duration = source_entry.get('itunes_duration', '') if hasattr(source_entry, 'get') else getattr(source_entry, 'itunes_duration', '')
        
        if hasattr(source_entry, 'published_parsed') and source_entry.published_parsed:
            episode.pub_date = make_aware(datetime.fromtimestamp(time.mktime(source_entry.published_parsed)))
        elif is_new:
            episode.pub_date = make_aware(datetime.now())
        
        if pub_entry:
            episode.chapters_public = extract_rss_chapters(pub_entry)
        if sub_entry:
            episode.chapters_private = extract_rss_chapters(sub_entry)

    # 6. Execute custom network enhancements (like HTML chapters or WP scraping)
    if enhancer:
        enhancer(episode, pub_entry, sub_entry, is_new, stdout)

    episode.save()

    network = podcast.network
    if network.custom_domain:
        base_url = f"https://{network.custom_domain}".rstrip('/')
    else:
        # Fallback for local dev or networks without a custom domain
        base_url = getattr(settings, 'SITE_URL', 'http://localhost:8000').rstrip('/')

    task_rebuild_episode_fragments.delay(episode.id, base_url)
    
    status = "Created" if is_new else ("Updated [LOCKED]" if episode.is_metadata_locked else "Updated")
    stdout.write(f"  -> [{status}] {episode.title}")
    return episode

def run_ingest(podcast, stdout, enhancer=None):
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
    feed_description = ""
    source_data = public_data if public_data else sub_data
    
    if source_data and hasattr(source_data, 'feed'):
        # Image extraction
        if 'image' in source_data.feed:
            feed_image = source_data.feed.image.get('href') or source_data.feed.image.get('url', '')
        if not feed_image and 'itunes_image' in source_data.feed:
            feed_image = source_data.feed.itunes_image.get('href', '')
            
        # Title extraction
        feed_title = source_data.feed.get('title', '')

        # Description extraction
        feed_description = source_data.feed.get('description', '')
        if not feed_description:
            feed_description = source_data.feed.get('summary', '')

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

    # Process and Update Description
    if feed_description:
        clean_desc = feed_description.strip()
        if clean_desc and podcast.description != clean_desc:
            logger.info(f"Updating podcast description for {podcast.title}")
            podcast.description = clean_desc
            stdout.write("  [Description Updated]")
            needs_save = True

    # Save the database record only once if anything changed
    if needs_save:
        podcast.save()
        network = podcast.network
        if network.custom_domain:
            base_url = f"https://{network.custom_domain}".rstrip('/')
        else:
            base_url = getattr(settings, 'SITE_URL', 'http://localhost:8000').rstrip('/')
            
        task_rebuild_podcast_fragments.delay(podcast.id, base_url)

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
        p_link = getattr(pub_entry, 'link', None)
        p_slug = get_slug(p_link)
        p_id = getattr(pub_entry, 'id', None)
        
        for priv_audio in list(unmatched_private_audios):
            priv_entry = private_pool[priv_audio]
            s_link = getattr(priv_entry, 'link', None)
            s_slug = get_slug(s_link)
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
        p_title = getattr(pub_entry, 'title', '')
        p_fp = get_fingerprint(p_title, podcast.network)
        
        for priv_audio in list(unmatched_private_audios):
            priv_entry = private_pool[priv_audio]
            s_title = getattr(priv_entry, 'title', '')
            s_fp = get_fingerprint(s_title, podcast.network)
            
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

    # 1. Process all Public Entries (Matched and Unmatched)
    for i, pub_entry in enumerate(public_entries_list):
        sub_entry = None
        reason = "Public Only (No Match)"
        
        if match_data := matched_pairs.get(i):
            sub_audio, reason = match_data
            sub_entry = private_pool[sub_audio]
            matches += 1
            stdout.write(f"  [Matched: {reason}] {getattr(pub_entry, 'title', 'Untitled Episode')}")
            logger.debug(f"Episode Match: '{getattr(pub_entry, 'title', '')}' -> {reason}")
        else:
            stdout.write(f"  [Public Only] {getattr(pub_entry, 'title', 'Untitled Episode')}")
            logger.debug(f"Episode Unmatched (Public Only): '{getattr(pub_entry, 'title', '')}'")

        commit_episode(podcast, pub_entry, sub_entry, reason, stdout, enhancer)
        count += 1

    # 2. Process all Private Orphans
    for priv_audio in unmatched_private_audios:
        sub_entry = private_pool[priv_audio]
        stdout.write(f"  [Private Exclusive Captured] {getattr(sub_entry, 'title', 'Untitled Episode')}")
        logger.debug(f"Episode Unmatched (Private Exclusive): '{getattr(sub_entry, 'title', '')}'")
        
        commit_episode(podcast, None, sub_entry, "Private Exclusive", stdout, enhancer)
        count += 1
        exclusive_count += 1

    summary = f"Finished. Total: {count} | Matches: {matches} | Premium Exclusives: {exclusive_count}"
    stdout.write(summary)
    logger.info(f"Ingestion complete for {podcast.title}. Total: {count}, Matches: {matches}, Exclusives: {exclusive_count}")