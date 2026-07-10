import feedparser
import time
import re
import requests
import logging
from datetime import datetime
from urllib.parse import urlparse
from django.utils.timezone import make_aware
from django.conf import settings
from django.core.cache import cache
from pod_manager.models import Episode
from pod_manager.tasks import task_rebuild_episode_fragments, task_rebuild_podcast_shell
from pod_manager.utils import validate_public_url, sanitize_user_html
from difflib import SequenceMatcher
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def extract_season_episode(entry):
    """Read itunes:season / itunes:episode / itunes:episodeType off a parsed
    feedparser entry. Returns (season: int|None, episode: int|None, type: str).
    Shared by commit_episode() and the backfill_season_episode_tags command so
    both apply feed values the same way."""
    def _get(key, default=None):
        return entry.get(key, default) if hasattr(entry, 'get') else getattr(entry, key, default)

    def _int_or_none(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    return (
        _int_or_none(_get('itunes_season')),
        _int_or_none(_get('itunes_episode')),
        (_get('itunes_episodetype', '') or '').strip()[:50],
    )

def extract_explicit(entry):
    """Read itunes:explicit off a parsed feedparser entry. Returns True/False,
    or None when the feed doesn't specify it (so callers can leave a manual
    value untouched rather than clobbering it). Accepts the modern true/false
    and the legacy yes/no/clean/explicit spellings; feedparser may also hand
    back a real bool."""
    def _get(key, default=None):
        return entry.get(key, default) if hasattr(entry, 'get') else getattr(entry, key, default)

    raw = _get('itunes_explicit')
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return None
    val = str(raw).strip().lower()
    if val in ('yes', 'true', 'explicit'):
        return True
    if val in ('no', 'false', 'clean'):
        return False
    return None


def extract_feed_tags(entry):
    """Category/keyword tags from a parsed feedparser entry. feedparser folds
    <category>, <itunes:keywords>, and <media:keyword> into entry.tags as
    {term: ...} dicts. Returns a de-duplicated list preserving first-seen order
    and casing."""
    if not entry or not hasattr(entry, 'tags'):
        return []
    terms = [t.get('term').strip() for t in entry.tags if t.get('term') and t.get('term').strip()]
    return merge_tags(terms)


def merge_tags(*tag_lists):
    """Concatenate tag lists into one, de-duplicated case-insensitively while
    preserving first-seen order and the first casing encountered."""
    merged = {}
    for tags in tag_lists:
        for t in tags or []:
            key = t.lower()
            if key not in merged:
                merged[key] = t
    return list(merged.values())


def extract_rss_chapters(entry):
    """Attempts to extract chapters from Podcast Index namespace or Podlove Simple Chapters."""
    
    # 1. Check for Podcast Index <podcast:chapters> tag
    if hasattr(entry, 'podcast_chapters') and isinstance(entry.podcast_chapters, dict):
        url = entry.podcast_chapters.get('url')
        if url:
            # SSRF guard: chapter URL is publisher-controlled. Reject anything
            # that resolves to a non-public address (cloud metadata, internal
            # services, etc) before issuing the HTTP request.
            ok, reason = validate_public_url(url)
            if not ok:
                logger.warning(f"Rejected chapter URL '{url}': {reason}")
            else:
                try:
                    resp = requests.get(url, timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        # --- FIX: Force standard if remote host returns a legacy flat array ---
                        if isinstance(data, list):
                            return {"version": "1.2.0", "chapters": data}
                        return data
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
        
    # 1. Normalize line breaks to paragraphs BEFORE parsing
    html_content = re.sub(r'(<br\s*/?>\s*){2,}', '</p><p>', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'\n{2,}', '</p><p>', html_content)
    
    # Prevent creating invalid nested <p><p> wrappers if the content is already HTML
    if not html_content.strip().startswith('<'):
        html_content = f"<p>{html_content}</p>"
        
    soup = BeautifulSoup(html_content, "html.parser")
    
    # 2. Description Cut Triggers (with Parent Protection)
    if network.description_cut_triggers:
        triggers = [t.strip().lower() for t in network.description_cut_triggers.split(',') if t.strip()]
        for element in soup.find_all(['p', 'div', 'li', 'em', 'strong']):
            
            if element.name == 'div' and element.find(['p', 'li', 'div']):
                continue
                
            text = element.get_text().lower()
            if any(trigger in text for trigger in triggers):
                element.decompose()
    
    # 3. Clean up empty tags (Fixes the surviving <p></p> spacing)
    for empty in soup.find_all(['p', 'div', 'span', 'li']):
        # Use get_text(strip=True) so whitespace/breaks aren't counted as real content
        if not empty.get_text(strip=True) and not empty.find(['img', 'iframe', 'a']):
            empty.decompose()
            
    # 4. Final Polish
    final_html = str(soup).strip()
    final_html = final_html.replace('\n', ' ')

    # 5. Strip scripts / event handlers / disallowed tags. Feed publishers are
    # presumptively trusted but ad insertion or a compromised host can poison
    # an episode description, and the home/episode pages render this with
    # `|safe`. Sanitize on the way in so the DB stays clean.
    return sanitize_user_html(final_html)

def get_feed(url, feed_type, podcast_id, stdout, force_fetch=False):
    """Fetch and parse an RSS feed, utilizing ETags via Redis to save bandwidth."""
    parsed_url = urlparse(url)
    auth = (parsed_url.username, parsed_url.password) if parsed_url.username else None
    clean_url = parsed_url._replace(netloc=parsed_url.hostname).geturl()

    # Define Redis keys based on the podcast ID
    etag_key = f"inbound_etag_{feed_type}_{podcast_id}"
    mod_key = f"inbound_modified_{feed_type}_{podcast_id}"
    
    headers = {'User-Agent': 'Vecto/1.0'}
    
    # Inject cache headers unless we are forced to download the body for merging
    if not force_fetch:
        cached_etag = cache.get(etag_key)
        cached_mod = cache.get(mod_key)
        if cached_etag: headers['If-None-Match'] = cached_etag
        if cached_mod: headers['If-Modified-Since'] = cached_mod

    stdout.write(f"  [LIVE FETCH] Downloading {feed_type} feed...")
    logger.debug(f"Fetching live {feed_type} feed from {clean_url}")
    
    try:
        response = requests.get(clean_url, auth=auth, timeout=30, headers=headers)
        
        # The HTTP 304 Check (Bandwidth Saver)
        if response.status_code == 304:
            stdout.write(f"  [{feed_type} FEED] 304 Not Modified.")
            return 304  # Return a specific integer to signal a cached skip
            
        response.raise_for_status()
        
        # Save the new cache headers to Redis (7 Days)
        if response.headers.get('ETag'):
            cache.set(etag_key, response.headers.get('ETag'), timeout=604800)
        if response.headers.get('Last-Modified'):
            cache.set(mod_key, response.headers.get('Last-Modified'), timeout=604800)

        return feedparser.parse(response.content)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP Error fetching {feed_type} feed from {clean_url}: {e}", exc_info=True)
        raise

def _network_base_url(network):
    """Absolute base URL for a network — custom domain when set, else the
    SITE_URL fallback for local dev / networks without one."""
    if network.custom_domain:
        return f"https://{network.custom_domain}".rstrip('/')
    return getattr(settings, 'SITE_URL', 'http://localhost:8000').rstrip('/')

def commit_episode(podcast, pub_entry, sub_entry, match_reason, stdout, enhancer=None):
    """
    Intelligently creates or updates an episode.
    - Prevents duplicates by checking BOTH guid_public and guid_private.
    - Prevents overwriting manual edits if is_metadata_locked is True.
    - NEW: Rejects algorithmic matches if an episode was manually unpaired.
    """
    pub_guid = getattr(pub_entry, 'id', None) if pub_entry else None
    sub_guid = getattr(sub_entry, 'id', None) if sub_entry else None
    
    # 1. Independent Lookup: Find the exact database records for these GUIDs anywhere in the network
    ep_pub = Episode.objects.filter(podcast__network=podcast.network, guid_public=pub_guid).first() if pub_guid else None
    ep_priv = Episode.objects.filter(podcast__network=podcast.network, guid_private=sub_guid).first() if sub_guid else None
        
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

    # GUID auto-migration: an episode parented to a low-priority feed moves to
    # a normal-priority feed that ingests the same GUID — exactly as if someone
    # had used the bulk mover, minus the pin (a null pin is what lets this fire
    # at all; a manual move permanently opts an episode out).
    if not is_new and episode.podcast_id != podcast.id:
        existing_owner = episode.podcast
        if (existing_owner.is_low_priority and not podcast.is_low_priority
                and not episode.podcast_pinned_at):
            guids_diverge = bool(ep_pub and ep_priv and ep_pub.pk != ep_priv.pk)
            if guids_diverge:
                # The pub/priv lookups landed on two DIFFERENT rows — the fuzzy
                # matcher attempting an algorithmic cross-row merge. Ownership
                # of the pair is genuinely ambiguous; never gamble a move on
                # it. After a Merge Desk resolution the next ingest sees a
                # clean single-row match and migrates normally.
                logger.info(
                    f"[ingest] Skipping auto-migration: pub/priv GUIDs resolve "
                    f"to different episodes (ids {ep_pub.id}/{ep_priv.id}) — "
                    f"resolve via Merge Desk"
                )
                stdout.write(
                    f"  [SKIP MIGRATE] pub/priv GUIDs resolve to different "
                    f"episodes (ids {ep_pub.id}/{ep_priv.id}) — resolve via "
                    f"Merge Desk"
                )
            else:
                from pod_manager.services.episode_move import move_episodes
                move_episodes([episode.id], podcast,
                              base_url=_network_base_url(podcast.network),
                              pin=False, rebuild_fragments=False)
                episode.podcast = podcast  # keep the in-memory object
                                           # consistent for the rest of this
                                           # function
                logger.info(
                    f"[ingest] Auto-migrated episode {episode.id} '{episode.title}' "
                    f"from low-priority '{existing_owner.title}' (id={existing_owner.id}) "
                    f"to '{podcast.title}' (id={podcast.id})"
                )
                stdout.write(
                    f"  [AUTO-MIGRATE] '{episode.title}': "
                    f"{existing_owner.title} -> {podcast.title}"
                )  # logger alone never reaches the creator-facing import log —
                   # CommandLogStream only sees stdout

    # 3. Always update the routing identifiers
    if pub_guid: episode.guid_public = pub_guid
    if sub_guid: episode.guid_private = sub_guid
    
    # Protect manual audit trails from being overwritten by the algorithm
    protected_reasons = ['Manually Unpaired', 'Manual Merge (Merge Desk)']
    if episode.match_reason not in protected_reasons:
        episode.match_reason = match_reason

    # A low-priority ingester never owns metadata for an episode parented to
    # another feed — the poll stagger makes it ingest LAST each cycle, so
    # without this its copy would systematically overwrite the owning feed's
    # fields. GUIDs above are the only thing it may update; the audio,
    # metadata, and enhancer sections below are all skipped.
    guid_update_only = podcast.is_low_priority and episode.podcast_id != podcast.id

    if not guid_update_only:
        # 4. Always update audio URLs (Hosts frequently rotate CDNs or ad-tracking prefixes)
        # Skip if audio_locked (e.g. set by GDrive recovery to protect manually mapped URLs)
        if not episode.audio_locked:
            if pub_entry:
                episode.audio_url_public = get_enclosure(pub_entry)
            if sub_entry:
                episode.audio_url_subscriber = get_enclosure(sub_entry)

        # 5. THE METADATA LOCK
        # Season/episode/type are read from the feed regardless of the lock, purely
        # so a locked episode's would-be values can be logged below (with the
        # episode ID) for later targeting via backfill_season_episode_tags
        # --bypass-lock. Everything else in this section stays lock-gated as before.
        new_season, new_episode_num, new_episode_type = extract_season_episode(pub_entry if pub_entry else sub_entry)

        if not episode.is_metadata_locked:
            episode.season_number = new_season
            episode.episode_number = new_episode_num
            if new_episode_type:
                episode.episode_type = new_episode_type
        elif (new_season or new_episode_num) and (new_season, new_episode_num) != (episode.season_number, episode.episode_number):
            logger.info(
                f"[ingest] episode {episode.id} '{episode.title}' is metadata-locked; "
                f"season {episode.season_number}->{new_season}, episode {episode.episode_number}->{new_episode_num} "
                f"— not applied. Use "
                f"'manage.py backfill_season_episode_tags --bypass-lock --episode={episode.id}' to force."
            )

        # itunes:explicit is a content-rating fact, not curated metadata — it applies
        # even on a locked episode. Only overwrite when the feed actually states it,
        # so an unset feed value can't wipe a manually-set rating.
        new_explicit = extract_explicit(pub_entry if pub_entry else sub_entry)
        if new_explicit is not None:
            episode.explicit = new_explicit

        if not episode.is_metadata_locked:
            source_entry = pub_entry if pub_entry else sub_entry

            episode.title = getattr(source_entry, 'title', 'Untitled Episode')
            raw_desc = ""
            if hasattr(source_entry, 'content') and source_entry.content:
                raw_desc = source_entry.content[0].get('value', '')
            if not raw_desc:
                raw_desc = getattr(source_entry, 'description', '')
            if not raw_desc:
                raw_desc = getattr(source_entry, 'summary', '')

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

            # Category/keyword tags straight from the feed. Enhancers (e.g. baldmove)
            # merge additional scraped tags onto these — see baldmove_enhancer.
            pub_tags = extract_feed_tags(pub_entry) if pub_entry else []
            priv_tags = extract_feed_tags(sub_entry) if sub_entry else []
            episode.tags = merge_tags(pub_tags, priv_tags)

        # 6. Execute custom network enhancements (like HTML chapters or WP scraping)
        if enhancer:
            enhancer(episode, pub_entry, sub_entry, is_new, stdout)

    episode.save()

    if is_new:
        # Link-only calendar reconciliation (never auto-creates — see
        # services.release_calendar): a pre-planned CalendarEntry for an
        # RSS-sourced show would otherwise never reconcile, since ingested
        # episodes are born published and never pass through publish.py.
        from pod_manager.services.release_calendar import link_calendar_entry_for_new_episode
        link_calendar_entry_for_new_episode(episode, stdout=stdout)

    task_rebuild_episode_fragments.delay(episode.id, _network_base_url(podcast.network))
    
    status = "Created" if is_new else ("Updated [LOCKED]" if episode.is_metadata_locked else "Updated")
    stdout.write(f"  -> [{status}] {episode.title}")
    return episode

def run_ingest(podcast, stdout, enhancer=None):
    logger.info(f"Starting Ingest Strategy for podcast: {podcast.title} (ID: {podcast.id})")
    stdout.write(f"--- Harvesting: {podcast.title} ---")

    sub_data = None
    public_data = None
    
    # 1. Initial Conditional Fetch
    if podcast.subscriber_feed_url:
        sub_data = get_feed(podcast.subscriber_feed_url, "PRIVATE", podcast.id, stdout)
        if hasattr(sub_data, 'status') and sub_data.status == 401:
            logger.error(f"Auth Failed on Private Feed for podcast: {podcast.title}")
            stdout.write("[ERROR] Auth Failed on Private Feed.")
            return

    if podcast.public_feed_url:
        public_data = get_feed(podcast.public_feed_url, "PUBLIC", podcast.id, stdout)

    # 2. Evaluate 304 Statuses
    is_sub_304 = (sub_data == 304)
    is_pub_304 = (public_data == 304)

    # Exit entirely if all requested feeds returned 304 Not Modified
    if (not podcast.subscriber_feed_url or is_sub_304) and (not podcast.public_feed_url or is_pub_304):
        stdout.write("  [CACHE HIT] All feeds are unmodified. Skipping ingestion.")
        logger.info(f"Ingestion skipped for {podcast.title} (304 Not Modified).")
        return

    # If one feed updated but the other didn't, we MUST force-fetch the 304 feed
    # so we have both sets of XML entries in memory to execute the merge logic.
    if is_sub_304 and podcast.subscriber_feed_url:
        stdout.write("  [FORCE FETCH] Public feed updated. Forcing Private feed download for merge logic.")
        sub_data = get_feed(podcast.subscriber_feed_url, "PRIVATE", podcast.id, stdout, force_fetch=True)
        
    if is_pub_304 and podcast.public_feed_url:
        stdout.write("  [FORCE FETCH] Private feed updated. Forcing Public feed download for merge logic.")
        public_data = get_feed(podcast.public_feed_url, "PUBLIC", podcast.id, stdout, force_fetch=True)

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
        task_rebuild_podcast_shell.delay(podcast.id, _network_base_url(podcast.network))

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