import logging
from celery import shared_task
from collections import defaultdict
from django.core.cache import cache
from django.core.management import call_command
from pod_manager.models import Network
from django.utils import timezone
from datetime import timedelta
from django.core.files.base import ContentFile
from django.template.loader import render_to_string
import pdfkit
import platform
from .models import Network, PatronProfile, Invoice, Podcast, Episode
import io
from datetime import timedelta


logger = logging.getLogger(__name__)

class CacheLogStream(io.StringIO):
    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id
        self.buffer = ""

    def write(self, s):
        super().write(s)
        self.buffer += s
        formatted_chunk = "".join([f"data: {line}\n\n" for line in s.splitlines() if line])
        current_logs = cache.get(self.task_id, "")
        cache.set(self.task_id, current_logs + formatted_chunk, timeout=3600)

@shared_task
def task_smart_poll_feeds():
    logger.info("Starting smart feed poll...")
    podcasts = Podcast.objects.all()
    now = timezone.now()
    
    for podcast in podcasts:
        latest_ep = podcast.episodes.order_by('-pub_date').first()
        is_active = False
        
        # If the newest episode is less than 14 days old, it is "Active"
        if latest_ep and latest_ep.pub_date >= now - timedelta(days=14):
            is_active = True
            
        # If Active: queue it every time this runs (every 15 mins).
        # If Inactive: only queue it if we are in the first 15 mins of the hour (once an hour).
        if is_active or now.minute < 15:
            logger.info(f"Queuing update for {podcast.title} (Active: {is_active})")
            task_ingest_feed.delay(podcast.id)

@shared_task
def task_ingest_feed(show_id):
    from pod_manager.views import invalidate_show_cache
    task_id = f"import_logs_{show_id}"
    stream = CacheLogStream(task_id)
    try:
        stream.write("\n[SYSTEM] Celery worker acquired task. Starting ingestion...\n")
        call_command('ingest_feed', show_id, stdout=stream, stderr=stream, no_color=True)
    except Exception as e:
        stream.write(f"\n[ERROR] {str(e)}\n")
    finally:
        stream.write("[DONE]")

@shared_task
def task_generate_monthly_invoices():
    logger.info("Starting monthly invoice generation...")
    networks = Network.objects.filter(patreon_sync_enabled=True)
    thirty_days_ago = timezone.now() - timedelta(days=30)

    for network in networks:
        # Dynamically count users (Active + Lapsed bandwidth users)
        kwargs = {
            f"active_pledges__has_key": str(network.patreon_campaign_id),
            "last_active__gte": thirty_days_ago
        }
        active_count = PatronProfile.objects.filter(**kwargs).count()

        active_user_cost = network.per_user_cost * active_count
        total_due = network.base_cost + active_user_cost

        if total_due <= 0:
            logger.info(f"Skipping invoice for {network.name} (Total Due: $0.00)")
            continue

        html_string = render_to_string('pod_manager/invoice_template.html', {
            'network': network,
            'active_count': active_count,
            'active_user_cost': active_user_cost,
            'total_due': total_due,
            'date': timezone.now()
        })

        if platform.system() == 'Windows':
            path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
            config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
            pdf_bytes = pdfkit.from_string(html_string, False, configuration=config)
        else:
            pdf_bytes = pdfkit.from_string(html_string, False)

        invoice = Invoice(network=network, amount_due=total_due, active_user_count=active_count)
        filename = f"{network.slug}_invoice_{timezone.now().strftime('%Y_%m')}.pdf"
        invoice.pdf_file.save(filename, ContentFile(pdf_bytes))
        
        logger.info(f"Generated invoice for {network.name}: ${total_due}")

@shared_task
def task_sync_network_patrons(network_id):
    from pod_manager.views import sync_network_patrons
    try:
        network = Network.objects.get(id=network_id)
        count, error = sync_network_patrons(network)
        if error:
            logger.error(f"Task sync failed for network {network.name}: {error}")
        else:
            logger.info(f"Task sync complete for {network.name}. Updated: {count}")
    except Network.DoesNotExist:
        logger.error(f"Network ID {network_id} not found for sync task.")

@shared_task
def task_sync_all_networks():
    """Scheduled task to sync all enabled networks."""
    networks = Network.objects.filter(patreon_sync_enabled=True)
    for network in networks:
        task_sync_network_patrons.delay(network.id)

@shared_task
def task_clean_mix_images():
    logger.info("Running nightly sweep of orphaned mix images.")
    call_command('clean_mix_images')

@shared_task
def task_sync_last_active_timestamps():
    """
    Sweeps Redis for buffered 'last_active' timestamps and bulk updates PatronProfiles.
    """
    import redis
    from django.conf import settings
    
    # 1. Gracefully skip if running IDE tests with LocMemCache
    if 'locmem' in settings.CACHES['default']['BACKEND'].lower():
        logger.info("Local memory cache detected, skipping Redis sweep.")
        return

    # 2. Connect directly to Redis using the URL from settings
    redis_url = settings.CACHES['default']['LOCATION']
    redis_client = redis.from_url(redis_url)
    
    # Find all keys matching our buffer pattern
    keys = redis_client.keys("*buffer_last_active_*")
    
    if not keys:
        logger.info("No active timestamps to sync.")
        return

    profiles_to_update = []
    keys_to_delete = []

    for key_bytes in keys:
        key_str = key_bytes.decode('utf-8')
        # Extract the profile ID regardless of Django's internal cache prefixes (e.g. ':1:')
        clean_key = key_str.split('buffer_last_active_')[-1]
        
        try:
            profile_id = int(clean_key)
            # Retrieve the timestamp (we still use cache.get so it handles the prefixing natively)
            last_active_time = cache.get(f"buffer_last_active_{profile_id}")
            
            if last_active_time:
                profile = PatronProfile(id=profile_id)
                profile.last_active = last_active_time
                profiles_to_update.append(profile)
                keys_to_delete.append(key_str) # Save the raw Redis key for deletion
                
        except (ValueError, TypeError):
            continue

    if profiles_to_update:
        PatronProfile.objects.bulk_update(profiles_to_update, ['last_active'])
        logger.info(f"Successfully bulk-updated {len(profiles_to_update)} user activity timestamps.")

    if keys_to_delete:
        # Pass the unpacked list of raw keys directly to Redis to delete them
        redis_client.delete(*keys_to_delete)

def parse_duration_to_hours(duration_str):
    """Helper to convert the HH:MM:SS string to fractional hours."""
    if not duration_str:
        return 0.0
    try:
        parts = duration_str.split(':')
        if len(parts) == 3:
            return int(parts[0]) + (int(parts[1]) / 60.0) + (float(parts[2]) / 3600.0)
        elif len(parts) == 2:
            return (int(parts[0]) / 60.0) + (float(parts[1]) / 3600.0)
        else:
            return float(parts[0]) / 3600.0
    except ValueError:
        return 0.0

@shared_task
def sweep_analytics_buffer():
    """
    Sweeps Redis for playback and RSS hits, aggregates them, 
    and applies the data to the Patron profiles.
    """
    logger.info("Starting Analytics Buffer Sweep...")
    
    # 1. Gather all analytics keys from Redis
    play_keys = cache.keys("analytics:play:*")
    rss_keys = cache.keys("analytics:rss:*")
    
    if not play_keys and not rss_keys:
        logger.info("No analytics keys found. Sweep complete.")
        return

    # Data structures to aggregate hits before hitting the DB
    profile_updates = defaultdict(lambda: {
        'play_hits': 0,
        'episodes_played': set(),
        'podcast_hits': defaultdict(int),
        'rss_hits': 0
    })

    # 2. Sweep Playback Keys
    for key in play_keys:
        hits = cache.get(key)
        if hits:
            parts = key.split(':')
            if len(parts) == 5:
                p_id, e_id, pod_id = int(parts[2]), int(parts[3]), int(parts[4])
                profile_updates[p_id]['play_hits'] += hits
                profile_updates[p_id]['episodes_played'].add(e_id)
                profile_updates[p_id]['podcast_hits'][pod_id] += hits
        cache.delete(key) 

    # 3. Sweep RSS Keys
    for key in rss_keys:
        hits = cache.get(key)
        if hits:
            p_id = int(key.split(':')[2])
            profile_updates[p_id]['rss_hits'] += hits
        cache.delete(key)

    # 4. Fetch necessary DB objects in bulk
    all_profile_ids = list(profile_updates.keys())
    profiles = {p.id: p for p in PatronProfile.objects.filter(id__in=all_profile_ids)}
    
    all_episode_ids = {e_id for data in profile_updates.values() for e_id in data['episodes_played']}
    episodes = {e.id: e for e in Episode.objects.filter(id__in=all_episode_ids)}

    today = timezone.now().date()
    current_iso_week = today.isocalendar()[1]
    profiles_to_save = []

    # 5. Apply Gamification Math
    for p_id, data in profile_updates.items():
        if p_id not in profiles:
            continue
            
        profile = profiles[p_id]
        
        # A. Apply Raw Hits (Activity Score)
        profile.total_playback_hits += data['play_hits']
        
        # B. Apply Endurance (Hours Accessed)
        hours_gained = 0.0
        for e_id in data['episodes_played']:
            if e_id in episodes:
                hours_gained += parse_duration_to_hours(episodes[e_id].duration)
        profile.total_hours_accessed += hours_gained
        
        # C. Calculate Streaks
        if data['play_hits'] > 0:
            # Daily Streak
            if profile.last_playback_date == today - timedelta(days=1):
                profile.streak_days += 1
            elif profile.last_playback_date != today:
                profile.streak_days = 1 
            profile.last_playback_date = today
            
            # Weekly Streak
            if profile.last_play_week == current_iso_week - 1:
                profile.streak_weeks += 1
            elif profile.last_play_week != current_iso_week:
                profile.streak_weeks = 1 
            profile.last_play_week = current_iso_week

        # D. Determine Current Obsession
        if data['podcast_hits']:
            top_podcast_id = max(data['podcast_hits'], key=data['podcast_hits'].get)
            profile.current_obsession_id = top_podcast_id

        profiles_to_save.append(profile)

    # 6. Bulk write back to Postgres
    if profiles_to_save:
        PatronProfile.objects.bulk_update(
            profiles_to_save, 
            ['total_playback_hits', 'total_hours_accessed', 'streak_days', 'streak_weeks', 'last_playback_date', 'last_play_week', 'current_obsession_id']
        )
        
    logger.info(f"Sweep complete. Updated {len(profiles_to_save)} profiles.")