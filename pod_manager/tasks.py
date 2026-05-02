import logging
import io
import platform
import redis
import pdfkit
from datetime import timedelta
from collections import defaultdict

from celery import shared_task
from django.core.mail import send_mail
from django.conf import settings
from django.core.cache import cache
from django.core.management import call_command
from django.utils import timezone
from django.utils.html import strip_tags
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
    thirty_days_ago_date = (timezone.now() - timedelta(days=30)).date()

    for network in networks:
        # 1. Query Active Patrons
        active_patrons_count = NetworkMembership.objects.filter(
            network=network,
            is_active_patron=True,
            last_active_date__gte=thirty_days_ago_date
        ).count()

        # 2. Query Active Former/Free Listeners
        active_former_count = NetworkMembership.objects.filter(
            network=network,
            is_active_patron=False,
            last_active_date__gte=thirty_days_ago_date
        ).count()

        total_active_count = active_patrons_count + active_former_count

        # Future-proof math: Currently both use the same multiplier
        active_patron_cost = network.per_user_cost * active_patrons_count
        active_former_cost = network.per_user_cost * active_former_count
        
        total_due = network.base_cost + active_patron_cost + active_former_cost

        if total_due <= 0:
            logger.info(f"Skipping invoice for {network.name} (Total Due: $0.00)")
            continue

        # Pass the separated counts and costs to the HTML template
        html_string = render_to_string('pod_manager/invoice_template.html', {
            'network': network,
            'active_patrons_count': active_patrons_count,
            'active_former_count': active_former_count,
            'total_active_count': total_active_count,
            'active_patron_cost': active_patron_cost,
            'active_former_cost': active_former_cost,
            'total_due': total_due,
            'date': timezone.now()
        })

        if platform.system() == 'Windows':
            path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
            config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
            pdf_bytes = pdfkit.from_string(html_string, False, configuration=config)
        else:
            pdf_bytes = pdfkit.from_string(html_string, False)

        # Record the combined total in the database for tracking
        invoice = Invoice(network=network, amount_due=total_due, active_user_count=total_active_count)
        filename = f"{network.slug}_invoice_{timezone.now().strftime('%Y_%m')}.pdf"
        invoice.pdf_file.save(filename, ContentFile(pdf_bytes))
        
        logger.info(f"Generated invoice for {network.name}: ${total_due} ({active_patrons_count} Patrons, {active_former_count} Former)")

def _scan_keys(redis_client, pattern, batch=500):
    """
    Cursor-based wrapper around Redis SCAN. Use this instead of KEYS:
    SCAN returns chunks of ~batch keys per call so Redis can interleave
    other clients' commands; KEYS would block the entire instance for the
    full keyspace walk.
    """
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=batch)
        for k in keys:
            yield k
        if cursor == 0:
            break


@shared_task
def sweep_analytics_buffer():
    logger.info("Starting Multi-Tenant Analytics Buffer Sweep...")

    cache_backend = settings.CACHES['default'].get('BACKEND', '').lower()
    if 'locmem' in cache_backend or 'dummy' in cache_backend:
        logger.info("Local memory cache detected, skipping Redis sweep.")
        return

    redis_url = settings.CACHES['default']['LOCATION']
    redis_client = redis.from_url(redis_url)

    # Don't early-return when there are no play keys — the billing and
    # analytics:rss sweeps below also need to run (a quiet day with web
    # visits and RSS polls but no playback would otherwise leave
    # last_active_date unchanged and undercount active users).
    play_keys = list(_scan_keys(redis_client, "analytics:play:*"))

    raw_play_data = []
    profile_ids = set()
    podcast_ids = set()
    episode_ids = set()
    keys_to_delete = []

    if not play_keys:
        logger.info("No analytics:play:* keys to sweep.")
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

    # ==========================================
    # ACTIVE-USER PRESENCE SWEEP
    # ==========================================
    # Two key shapes feed last_active_date:
    #   billing:active:{net_id}:{user_id}:{date}      <- web visits (middleware)
    #   analytics:rss:{net_id}:{user_id}:{date}       <- RSS polls (feed views)
    # Both encode the activity date in the key itself, so we can drain them
    # together. Deduplicate across (net_id, user_id) and write the most
    # recent date we saw for each.
    presence_by_member = {}  # (net_id, user_id) -> latest date_str seen
    presence_keys_to_delete = []

    for pattern in ("billing:active:*", "analytics:rss:*"):
        for key_bytes in _scan_keys(redis_client, pattern):
            key_str = key_bytes.decode('utf-8')
            parts = key_str.split(':')
            if len(parts) != 5:
                presence_keys_to_delete.append(key_bytes)
                continue
            try:
                net_id = int(parts[2])
                user_id = int(parts[3])
            except ValueError:
                presence_keys_to_delete.append(key_bytes)
                continue
            date_str = parts[4]
            mem_key = (net_id, user_id)
            existing = presence_by_member.get(mem_key)
            # Keep the most recent date if a member has multiple keys.
            if existing is None or date_str > existing:
                presence_by_member[mem_key] = date_str
            presence_keys_to_delete.append(key_bytes)

    if presence_by_member:
        member_keys = list(presence_by_member.keys())
        memberships = NetworkMembership.objects.filter(
            network_id__in={m[0] for m in member_keys},
            user_id__in={m[1] for m in member_keys},
        )
        billing_updates = []
        for mem in memberships:
            new_date = presence_by_member.get((mem.network_id, mem.user_id))
            if new_date and (mem.last_active_date is None or str(mem.last_active_date) < new_date):
                mem.last_active_date = new_date
                billing_updates.append(mem)

        if billing_updates:
            NetworkMembership.objects.bulk_update(billing_updates, ['last_active_date'])
            logger.info(f"Updated {len(billing_updates)} active-user timestamps.")

    if presence_keys_to_delete:
        redis_client.delete(*presence_keys_to_delete)

    # Drain the play-analytics keys we processed earlier in the function.
    if keys_to_delete:
        redis_client.delete(*keys_to_delete)

    logger.info("Sweep complete.")
    
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

@shared_task
def task_send_otp_email(email, otp_code, network_name="Vecto", theme_config=None):
    subject = f"Your {network_name} Login Code"
    
    # Failsafe in case a network doesn't have a theme config saved yet
    if not theme_config:
        theme_config = {}
        
    html_message = render_to_string('pod_manager/email/otp_email.html', {
        'otp_code': otp_code,
        'network_name': network_name,
        'theme': theme_config
    })
    
    plain_message = strip_tags(html_message)
    
    send_mail(
        subject=subject,
        message=plain_message,
        from_email=settings.DEFAULT_FROM_EMAIL,
        recipient_list=[email],
        html_message=html_message,
        fail_silently=False,
    )

@shared_task
def task_generate_s3_reports():
    """Runs the S3 Report generation script in the background."""
    logger.info("\n\n=======================================================")
    logger.info("⚙️ [CELERY WORKER] Task Picked Up! Running S3 Script...")
    try:
        call_command('generate_s3_report')
        logger.info("✅ [CELERY WORKER] Task Completed Successfully! Files saved to /media.")
        logger.info("=======================================================\n")
        return "Success"
    except Exception as e:
        logger.error(f"❌ [CELERY WORKER] Task FAILED: {e}")
        logger.info("=======================================================\n")
        return f"Failed: {e}"
    
@shared_task
def task_sync_discord_avatar(discord_id, membership_id):
    import requests
    
    bot_token = settings.DISCORD_BOT_TOKEN
    if not bot_token:
        logger.warning("DISCORD_BOT_TOKEN not set. Skipping Discord avatar sync.")
        return

    url = f"https://discord.com/api/v10/users/{discord_id}"
    headers = {"Authorization": f"Bot {bot_token}"}
    
    try:
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            data = res.json()
            avatar_hash = data.get('avatar')
            
            if avatar_hash:
                # Handle animated GIFs vs standard PNGs
                ext = "gif" if avatar_hash.startswith("a_") else "png"
                avatar_url = f"https://cdn.discordapp.com/avatars/{discord_id}/{avatar_hash}.{ext}?size=256"
                
                NetworkMembership.objects.filter(id=membership_id).update(discord_image_url=avatar_url)
                logger.info(f"Synced Discord avatar for user {discord_id}.")
            else:
                logger.info(f"Discord user {discord_id} has no custom avatar.")
        else:
            logger.error(f"Failed to fetch Discord user {discord_id}: {res.status_code} - {res.text}")
    except Exception as e:
        logger.error(f"Error fetching Discord avatar for {discord_id}: {e}", exc_info=True)