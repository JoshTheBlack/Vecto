import logging
import io
import platform
import redis
import pdfkit
from datetime import timedelta
from collections import defaultdict

from celery import shared_task
from django.conf import settings
from django.core.cache import cache
from django.core.management import call_command
from django.utils import timezone
from django.core.files.base import ContentFile
from django.template.loader import render_to_string

from .models import Network, PatronProfile, Invoice, Podcast, Episode, NetworkMembership

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
def task_smart_poll_feeds():
    logger.info("Starting smart feed poll...")
    podcasts = Podcast.objects.all()
    now = timezone.now()
    
    for podcast in podcasts:
        latest_ep = podcast.episodes.order_by('-pub_date').first()
        is_active = False
        
        if latest_ep and latest_ep.pub_date >= now - timedelta(days=14):
            is_active = True
            
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
        active_count = NetworkMembership.objects.filter(
            network=network,
            is_active_patron=True,
            user__patron_profile__last_active__gte=thirty_days_ago
        ).count()

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
def sweep_analytics_buffer():
    logger.info("Starting Multi-Tenant Analytics Buffer Sweep...")
    
    cache_backend = settings.CACHES['default'].get('BACKEND', '').lower()
    if 'locmem' in cache_backend or 'dummy' in cache_backend:
        logger.info("Local memory cache detected, skipping Redis sweep.")
        return

    redis_url = settings.CACHES['default']['LOCATION']
    redis_client = redis.from_url(redis_url)
    
    play_keys = redis_client.keys("*analytics:play:*")
    
    if not play_keys:
        logger.info("No analytics keys found. Sweep complete.")
        return

    raw_play_data = []
    profile_ids = set()
    podcast_ids = set()
    episode_ids = set()
    keys_to_delete = []

    for key_bytes in play_keys:
        key_str = key_bytes.decode('utf-8')
        hits = redis_client.get(key_bytes)
        
        if hits:
            clean_key = key_str.split('analytics:play:')[-1]
            parts = clean_key.split(':')
            if len(parts) == 3:
                p_id, e_id, pod_id = int(parts[0]), int(parts[1]), int(parts[2])
                raw_play_data.append({'p_id': p_id, 'e_id': e_id, 'pod_id': pod_id, 'hits': int(hits)})
                profile_ids.add(p_id)
                episode_ids.add(e_id)
                podcast_ids.add(pod_id)
                
        keys_to_delete.append(key_bytes)

    profile_to_user = dict(PatronProfile.objects.filter(id__in=profile_ids).values_list('id', 'user_id'))
    podcast_to_network = dict(Podcast.objects.filter(id__in=podcast_ids).values_list('id', 'network_id'))
    episodes = {e.id: e for e in Episode.objects.filter(id__in=episode_ids)}

    membership_updates = defaultdict(lambda: {
        'play_hits': 0, 'episodes_played': set(), 'podcast_hits': defaultdict(int)
    })

    for data in raw_play_data:
        user_id = profile_to_user.get(data['p_id'])
        network_id = podcast_to_network.get(data['pod_id'])
        
        if user_id and network_id:
            mem_key = (user_id, network_id)
            membership_updates[mem_key]['play_hits'] += data['hits']
            membership_updates[mem_key]['episodes_played'].add(data['e_id'])
            membership_updates[mem_key]['podcast_hits'][data['pod_id']] += data['hits']

    today = timezone.now().date()
    current_iso_week = today.isocalendar()[1]
    memberships_to_save = []

    for (user_id, network_id), data in membership_updates.items():
        membership, _ = NetworkMembership.objects.get_or_create(user_id=user_id, network_id=network_id)
        
        membership.total_playback_hits = (membership.total_playback_hits or 0) + data['play_hits']
        
        hours_gained = 0.0
        for e_id in data['episodes_played']:
            if e_id in episodes:
                hours_gained += parse_duration_to_hours(episodes[e_id].duration)
        membership.total_hours_accessed = (membership.total_hours_accessed or 0.0) + hours_gained
        
        if data['play_hits'] > 0:
            if membership.last_playback_date == today - timedelta(days=1):
                membership.streak_days = (membership.streak_days or 0) + 1
            elif membership.last_playback_date != today:
                membership.streak_days = 1 
            membership.last_playback_date = today
            
            if membership.last_play_week == current_iso_week - 1:
                membership.streak_weeks = (membership.streak_weeks or 0) + 1
            elif membership.last_play_week != current_iso_week:
                membership.streak_weeks = 1 
            membership.last_play_week = current_iso_week

        if data['podcast_hits']:
            top_podcast_id = max(data['podcast_hits'], key=data['podcast_hits'].get)
            membership.current_obsession_id = top_podcast_id

        memberships_to_save.append(membership)

    if memberships_to_save:
        NetworkMembership.objects.bulk_update(
            memberships_to_save, 
            ['total_playback_hits', 'total_hours_accessed', 'streak_days', 'streak_weeks', 'last_playback_date', 'last_play_week', 'current_obsession_id']
        )

    if keys_to_delete:
        redis_client.delete(*keys_to_delete)
        
    logger.info(f"Sweep complete. Updated {len(memberships_to_save)} Network Memberships.")
    
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
    networks = Network.objects.filter(patreon_sync_enabled=True)
    for network in networks:
        task_sync_network_patrons.delay(network.id)

@shared_task
def task_clean_mix_images():
    logger.info("Running nightly sweep of orphaned mix images.")
    call_command('clean_mix_images')

@shared_task
def task_sync_last_active_timestamps():
    if 'locmem' in settings.CACHES['default']['BACKEND'].lower():
        logger.info("Local memory cache detected, skipping Redis sweep.")
        return

    redis_url = settings.CACHES['default']['LOCATION']
    redis_client = redis.from_url(redis_url)
    
    keys = redis_client.keys("*buffer_last_active_*")
    if not keys:
        logger.info("No active timestamps to sync.")
        return

    profiles_to_update = []
    keys_to_delete = []

    for key_bytes in keys:
        key_str = key_bytes.decode('utf-8')
        clean_key = key_str.split('buffer_last_active_')[-1]
        
        try:
            profile_id = int(clean_key)
            last_active_time = cache.get(f"buffer_last_active_{profile_id}")
            
            if last_active_time:
                profile = PatronProfile(id=profile_id)
                profile.last_active = last_active_time
                profiles_to_update.append(profile)
                keys_to_delete.append(key_str) 
                
        except (ValueError, TypeError):
            continue

    if profiles_to_update:
        PatronProfile.objects.bulk_update(profiles_to_update, ['last_active'])
        logger.info(f"Successfully bulk-updated {len(profiles_to_update)} user activity timestamps.")

    
    if keys_to_delete:
        redis_client.delete(*keys_to_delete)

@shared_task
def task_rebuild_episode_fragments(episode_id, base_url):
    """Rebuilds just a single episode (Fast). Used for Inbox Approvals."""
    from pod_manager.views import get_or_build_episode_fragment
    try:
        ep = Episode.objects.get(id=episode_id)
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        
        # Pre-warm the cache immediately
        get_or_build_episode_fragment(ep, base_url, False)
        if ep.is_premium:
            get_or_build_episode_fragment(ep, base_url, True)
    except Episode.DoesNotExist:
        pass

@shared_task
def task_rebuild_podcast_fragments(podcast_id, base_url):
    """Rebuilds all fragments for a show (Heavy). Used for Footer Updates."""
    from pod_manager.views import get_or_build_feed_shell, get_or_build_episode_fragment
    try:
        pod = Podcast.objects.get(id=podcast_id)
        
        # Rebuild Shell
        cache.delete(f"feed_shell_public_{pod.id}")
        cache.delete(f"feed_shell_private_{pod.id}")
        get_or_build_feed_shell(pod, base_url, False)
        get_or_build_feed_shell(pod, base_url, True)
        
        # Rebuild All Episode Fragments
        for ep in pod.episodes.all():
            cache.delete(f"ep_frag_public_{ep.id}")
            cache.delete(f"ep_frag_private_{ep.id}")
            get_or_build_episode_fragment(ep, base_url, False)
            if ep.is_premium:
                get_or_build_episode_fragment(ep, base_url, True)
                
    except Podcast.DoesNotExist:
        pass

@shared_task
def task_rebuild_podcast_shell(podcast_id, base_url):
    """Rebuilds ONLY the <channel> shell. Used for title/desc/art updates."""
    from pod_manager.views import get_or_build_feed_shell
    try:
        pod = Podcast.objects.get(id=podcast_id)
        
        # Rebuild Shell only (Do not touch episode fragments)
        cache.delete(f"feed_shell_public_{pod.id}")
        cache.delete(f"feed_shell_private_{pod.id}")
        get_or_build_feed_shell(pod, base_url, False)
        get_or_build_feed_shell(pod, base_url, True)
    except Podcast.DoesNotExist:
        pass