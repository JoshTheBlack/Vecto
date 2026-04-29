import time
import re
import requests
import logging
from bs4 import BeautifulSoup

from .default import run_ingest as default_run_ingest

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

def _get_rich_description(entry):
    if not entry: return ''
    if hasattr(entry, 'content') and entry.content:
        return entry.content[0].get('value', '')
    return getattr(entry, 'description', '')

def baldmove_enhancer(episode, pub_entry, sub_entry, is_new, stdout):
    """
    Custom network logic executed during commit_episode, just before saving to the DB.
    """
    # 1. Chapters fallback to HTML scraping
    if pub_entry and not episode.chapters_public:
        html_chaps = parse_html_chapters(_get_rich_description(pub_entry))
        if html_chaps:
            episode.chapters_public = html_chaps
            stdout.write("  -> Scraped HTML Chapters (Public)")
            
    if sub_entry and not episode.chapters_private:
        html_chaps = parse_html_chapters(_get_rich_description(sub_entry))
        if html_chaps:
            episode.chapters_private = html_chaps
            stdout.write("  -> Scraped HTML Chapters (Private)")
            
    # 2. Tags extraction
    pub_tags = get_feed_tags(pub_entry) if pub_entry else []
    priv_tags = get_feed_tags(sub_entry) if sub_entry else []
    
    combined_tags = pub_tags + priv_tags
    merged_tags = list({t.lower(): t for t in combined_tags}.values())
    
    # Only scrape tags from web if we don't have them, AND it's a brand new episode
    if is_new and not merged_tags and episode.link:
        scraped = scrape_tags_from_wp(episode.link, stdout)
        if scraped:
            merged_tags = scraped
            stdout.write(f"  -> Web Scraped Tags: {merged_tags}")
        time.sleep(0.2)
    elif is_new and merged_tags:
        stdout.write(f"  -> RSS Tags Extracted: {merged_tags}")
        
    if merged_tags:
        episode.tags = merged_tags

def run_ingest(podcast, stdout):
    """Delegate entirely to the default engine, passing our custom enhancer."""
    default_run_ingest(podcast, stdout, enhancer=baldmove_enhancer)