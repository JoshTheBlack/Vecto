import time
import re
import json
import requests
import logging
from bs4 import BeautifulSoup

from .default import run_ingest as default_run_ingest

logger = logging.getLogger(__name__)

def scrape_tags_from_patreon(url, stdout):
    if not url or "patreon.com" not in url:
        stdout.write(f"  [PATREON SKIP] URL does not contain patreon.com: {url}")
        return []
        
    stdout.write(f"  [PATREON SCRAPE] Attempting to fetch: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5'
        }
        resp = requests.get(url, timeout=10, headers=headers)
        
        stdout.write(f"  [PATREON SCRAPE] HTTP Status: {resp.status_code}")
        
        if resp.status_code == 200:
            html_text = resp.text
            
            if "Just a moment..." in html_text or "Enable JavaScript" in html_text:
                stdout.write("  [PATREON SCRAPE ERROR] Blocked by anti-bot page (Cloudflare/Datadome).")
                return []

            soup = BeautifulSoup(html_text, 'html.parser')
            
            # STRATEGY 1: __NEXT_DATA__
            script_tag = soup.find('script', id='__NEXT_DATA__')
            if script_tag and script_tag.string:
                stdout.write("  [PATREON SCRAPE] Found __NEXT_DATA__ script block.")
                try:
                    data = json.loads(script_tag.string)
                    
                    def find_tags(obj):
                        if isinstance(obj, dict):
                            # NEW LOGIC based on user JSON: "id": "user_defined;A.Ron", "type": "post_tag"
                            if obj.get('type') == 'post_tag' and 'id' in obj:
                                tag_id = obj['id']
                                if tag_id.startswith('user_defined;'):
                                    yield tag_id.split('user_defined;', 1)[1]
                            for k, v in obj.items():
                                yield from find_tags(v)
                        elif isinstance(obj, list):
                            for item in obj:
                                yield from find_tags(item)
                                
                    found_tags = list(find_tags(data))
                    if found_tags:
                        unique = list(dict.fromkeys(found_tags))
                        stdout.write(f"  [PATREON SCRAPE SUCCESS] JSON Extracted: {unique}")
                        return unique
                    else:
                        stdout.write("  [PATREON SCRAPE FAIL] JSON parsed successfully, but no 'post_tag' objects found.")
                except json.JSONDecodeError:
                    stdout.write("  [PATREON SCRAPE ERROR] Found __NEXT_DATA__ but failed to decode JSON.")
            else:
                stdout.write("  [PATREON SCRAPE FAIL] __NEXT_DATA__ script block not found on page.")

            # STRATEGY 2: REGEX FALLBACK
            stdout.write("  [PATREON SCRAPE] Attempting Regex Fallback...")
            matches = re.findall(r'"id":\s*"user_defined;([^"]+)",\s*"type":\s*"post_tag"', html_text)
            if matches:
                unique = list(dict.fromkeys(matches))
                stdout.write(f"  [PATREON SCRAPE SUCCESS] Regex Extracted: {unique}")
                return unique
            else:
                stdout.write("  [PATREON SCRAPE FAIL] Regex fallback found nothing.")

        else:
            stdout.write(f"  [PATREON SCRAPE ERROR] Bad HTTP Status: {resp.status_code}")
            
    except Exception as e:
        stdout.write(f"  [PATREON SCRAPE EXCEPTION] {str(e)}")
        
    return []

def scrape_tags_from_wp(url, stdout):
    if not url or "baldmove.com" not in url:
        return []

    def _fetch_and_extract(target_url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(target_url, timeout=10, headers=headers)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.content, 'html.parser')
                tags = [a.text.strip() for a in soup.find_all('a', rel=True) if 'tag' in (a.get('rel', []) if isinstance(a.get('rel'), list) else a.get('rel').split()) and 'category' not in a.get('rel')]
                if tags: return list(dict.fromkeys(tags))
        except Exception:
            pass
        return None

    tags = _fetch_and_extract(url)
    if tags: return tags

    rewritten_url = None
    if "patreon.baldmove.com" in url: rewritten_url = url.replace("patreon.baldmove.com", "baldmove.com")
    elif "baldmove.com" in url and "patreon" not in url: rewritten_url = url.replace("https://baldmove.com", "https://patreon.baldmove.com")
        
    if rewritten_url: return _fetch_and_extract(rewritten_url) or []
    return []

def get_feed_tags(entry):
    if hasattr(entry, 'tags'): return [t.get('term').strip() for t in entry.tags if t.get('term')]
    return []

def parse_html_chapters(html_description):
    if not html_description: return None
    soup = BeautifulSoup(html_description, 'html.parser')
    for ul in soup.find_all('ul'):
        chapters = []
        li_tags = ul.find_all('li')
        if len(li_tags) < 2: continue
            
        is_valid = True
        for li in li_tags:
            match = re.match(r'^(\d{1,2}:\d{2}(?::\d{2})?)\s*[-—–]+\s*(.+)$', li.get_text().strip())
            if match:
                parts = match.group(1).split(':')
                seconds = int(parts[0])*3600 + int(parts[1])*60 + int(parts[2]) if len(parts)==3 else int(parts[0])*60 + int(parts[1])
                chapters.append({"startTime": seconds, "title": match.group(2).strip()})
            else:
                is_valid = False
                break 

        if is_valid and chapters: return {"version": "1.2.0", "chapters": chapters}
    return None

def _get_rich_description(entry):
    if not entry: return ''
    if hasattr(entry, 'content') and entry.content: return entry.content[0].get('value', '')
    return getattr(entry, 'description', '')

def baldmove_enhancer(episode, pub_entry, sub_entry, is_new, stdout):
    stdout.write(f"\n[ENHANCER] Running for: {episode.title}")
    stdout.write(f"[ENHANCER] Episode Link property: {episode.link}")
    stdout.write(f"[ENHANCER] Is New? {is_new}")
    
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
            
    pub_tags = get_feed_tags(pub_entry) if pub_entry else []
    priv_tags = get_feed_tags(sub_entry) if sub_entry else []
    
    combined_tags = pub_tags + priv_tags
    merged_tags = list({t.lower(): t for t in combined_tags}.values())
    
    if merged_tags:
        stdout.write(f"  -> Initial RSS Tags Extracted: {merged_tags}")
    else:
        stdout.write("  -> No tags found natively in RSS feeds.")

    # CHANGED: Gather all possible links and try them in order until tags are found
    if not merged_tags:
        possible_links = []
        
        # 1. Grab the raw public link
        if pub_entry and hasattr(pub_entry, 'link') and pub_entry.link:
            possible_links.append(pub_entry.link)
            
        # 2. Grab the raw private link
        if sub_entry and hasattr(sub_entry, 'link') and sub_entry.link:
            possible_links.append(sub_entry.link)
            
        # 3. Include the finalized episode link just in case
        if episode.link:
            possible_links.append(episode.link)
            
        # Remove duplicates while preserving order
        unique_links = list(dict.fromkeys(possible_links))
        
        for url in unique_links:
            scraped = []
            if "patreon.com" in url:
                scraped = scrape_tags_from_patreon(url, stdout)
            elif "baldmove.com" in url:
                scraped = scrape_tags_from_wp(url, stdout)
            else:
                stdout.write(f"  -> Link domain not recognized for scraping: {url}")
                
            if scraped:
                merged_tags = scraped
                stdout.write(f"  -> Successfully Web Scraped Tags from {url}. Final list: {merged_tags}")
                break  # Stop checking URLs once we find tags!
                
        time.sleep(0.5) # Gentle rate limit
        
    if merged_tags:
        episode.tags = merged_tags

def run_ingest(podcast, stdout):
    default_run_ingest(podcast, stdout, enhancer=baldmove_enhancer)