import json
import logging
import os
import platform
import redis
import pdfkit
import requests, re
from datetime import timedelta, datetime
from bs4 import BeautifulSoup
from collections import defaultdict

from celery import shared_task
from django.core.mail import send_mail
from django.conf import settings
from django.core.cache import cache
from django.core.management import call_command
from django.utils import timezone
from django.utils.timezone import make_aware
from django.utils.html import strip_tags
from django.core.files.base import ContentFile
from django.template.loader import render_to_string

from .models import Network, PatronProfile, Invoice, Podcast, Episode, NetworkMembership, LogEntry
from .admin_console.log_stream import CommandLogStream

logger = logging.getLogger(__name__)


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
            if podcast.is_low_priority:
                task_ingest_feed.apply_async(args=[podcast.id], countdown=600)
            else:
                task_ingest_feed.delay(podcast.id)

@shared_task
def task_ingest_feed(show_id):
    task_id = f"import_logs_{show_id}"
    stream = CommandLogStream(task_id)
    logger.info("task_ingest_feed streaming to cache key %s for show %s", task_id, show_id)
    try:
        stream.write("\n[SYSTEM] Celery worker acquired task. Starting ingestion...\n")
        call_command('ingest_feed', show_id, stdout=stream, stderr=stream, no_color=True)
    except Exception as e:
        stream.write(f"\n[ERROR] {str(e)}\n")
        raise  # Let Celery record the failure; finally still writes [DONE]
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


def _sweep_play_analytics(redis_client):
    """Drains analytics:play:* keys and writes playback stats to NetworkMembership rows."""
    # Don't early-return on empty play keys — _sweep_presence_keys must still run.
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
            ['total_playback_hits', 'total_hours_accessed', 'streak_days', 'streak_weeks',
             'last_playback_date', 'last_play_week', 'current_obsession_id']
        )

    if keys_to_delete:
        redis_client.delete(*keys_to_delete)


def _sweep_presence_keys(redis_client):
    """Drains billing:active:* and analytics:rss:* keys and updates last_active_date.

    Key shapes:
      billing:active:{net_id}:{user_id}:{date}   <- web visits (middleware)
      analytics:rss:{net_id}:{user_id}:{date}    <- RSS polls (feed views)
    The activity date is encoded in the key; we keep only the most recent per member.
    """
    presence_by_member = {}  # (net_id, user_id) -> latest date_str seen
    keys_to_delete = []

    for pattern in ("billing:active:*", "analytics:rss:*"):
        for key_bytes in _scan_keys(redis_client, pattern):
            key_str = key_bytes.decode('utf-8')
            parts = key_str.split(':')
            if len(parts) != 5:
                keys_to_delete.append(key_bytes)
                continue
            try:
                net_id = int(parts[2])
                user_id = int(parts[3])
            except ValueError:
                keys_to_delete.append(key_bytes)
                continue
            date_str = parts[4]
            mem_key = (net_id, user_id)
            existing = presence_by_member.get(mem_key)
            if existing is None or date_str > existing:
                presence_by_member[mem_key] = date_str
            keys_to_delete.append(key_bytes)

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

    if keys_to_delete:
        redis_client.delete(*keys_to_delete)


@shared_task
def sweep_analytics_buffer():
    logger.info("Starting Multi-Tenant Analytics Buffer Sweep...")

    cache_backend = settings.CACHES['default'].get('BACKEND', '').lower()
    if 'locmem' in cache_backend or 'dummy' in cache_backend:
        logger.info("Local memory cache detected, skipping Redis sweep.")
        return

    redis_url = settings.CACHES['default']['LOCATION']
    redis_client = redis.from_url(redis_url)

    _sweep_play_analytics(redis_client)
    _sweep_presence_keys(redis_client)

    logger.info("Sweep complete.")
    
@shared_task
def task_sync_network_patrons(network_id):
    from pod_manager.services.patreon import sync_network_patrons
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
    # clean_mix_images now previews by default; the nightly sweep must actually delete.
    call_command('clean_mix_images', apply=True, yes=True)

@shared_task
def task_rebuild_episode_fragments(episode_id, base_url):
    """Rebuilds just a single episode (Fast). Used for Inbox Approvals.

    Invariant: ep_frag_* keys are shared by every feed that carries the
    episode — its parent podcast, podcasts it's cross-published into, and
    mixes — so this single rebuild propagates everywhere. If per-target
    fragment keys are ever introduced, they must be invalidated here too."""
    from pod_manager.views.feeds import get_or_build_episode_fragment
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
    from pod_manager.views.feeds import get_or_build_feed_shell, get_or_build_episode_fragment
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
    from pod_manager.views.feeds import get_or_build_feed_shell
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


@shared_task
def task_publish_scheduled_episodes():
    """Publishes episodes whose scheduled_at has passed."""
    now = timezone.now()
    episodes = list(
        Episode.objects.filter(is_published=False, scheduled_at__lte=now)
        .select_related('podcast')
    )
    if not episodes:
        return
    for ep in episodes:
        ep.is_published = True
        ep.pub_date = ep.scheduled_at
        ep.scheduled_at = None
        ep.save(update_fields=['is_published', 'pub_date', 'scheduled_at'])
        cache.delete(f"ep_frag_public_{ep.id}")
        cache.delete(f"ep_frag_private_{ep.id}")
        cache.delete(f"feed_shell_public_{ep.podcast_id}")
        cache.delete(f"feed_shell_private_{ep.podcast_id}")
        # Cross-published targets carry this episode too — flush their shells
        # so lastBuildDate/ETag move on the next fetch.
        for target_id in ep.cross_publications.values_list('podcast_id', flat=True):
            cache.delete(f"feed_shell_public_{target_id}")
            cache.delete(f"feed_shell_private_{target_id}")
    logger.info(f"Published {len(episodes)} scheduled episode(s).")


@shared_task
def task_prune_logs():
    retention_days = getattr(settings, 'LOG_RETENTION_DAYS', 30)
    cutoff = timezone.now() - timedelta(days=retention_days)
    deleted, _ = LogEntry.objects.filter(created_at__lt=cutoff).delete()
    logger.info(f"Log pruning complete: removed {deleted} entries older than {retention_days} days.")


@shared_task
def task_sync_bot_avatar():
    """Check if any linked Discord server's icon has changed and update the
    bot's avatar via the REST API. Runs hourly via Celery Beat.
    Uses a cached icon hash to skip the upload when nothing changed, staying
    well within Discord's ~2 avatar edits per hour rate limit."""
    import base64
    import requests

    bot_token = settings.DISCORD_BOT_TOKEN
    if not bot_token:
        logger.warning("[BotAvatarSync] DISCORD_BOT_TOKEN not set, skipping.")
        return

    network = Network.objects.exclude(
        discord_server_id__isnull=True
    ).exclude(discord_server_id__exact='').first()
    if not network:
        logger.info("[BotAvatarSync] No network with a Discord server ID found, skipping.")
        return

    guild_id = network.discord_server_id
    headers = {'Authorization': f'Bot {bot_token}'}

    try:
        guild_resp = requests.get(
            f'https://discord.com/api/v10/guilds/{guild_id}',
            headers=headers, timeout=10,
        )
    except Exception as e:
        logger.error(f"[BotAvatarSync] Failed to reach Discord API: {e}")
        return

    if guild_resp.status_code != 200:
        logger.warning(f"[BotAvatarSync] Guild fetch returned {guild_resp.status_code}.")
        return

    icon_hash = guild_resp.json().get('icon')
    if not icon_hash:
        logger.info("[BotAvatarSync] Guild has no server icon, skipping.")
        return

    cache_key = 'bot_avatar_icon_hash'
    if cache.get(cache_key) == icon_hash:
        logger.debug("[BotAvatarSync] Icon unchanged, no upload needed.")
        return

    ext = 'gif' if icon_hash.startswith('a_') else 'png'
    icon_url = f'https://cdn.discordapp.com/icons/{guild_id}/{icon_hash}.{ext}?size=256'
    try:
        icon_resp = requests.get(icon_url, timeout=10)
        icon_resp.raise_for_status()
    except Exception as e:
        logger.error(f"[BotAvatarSync] Failed to download guild icon: {e}")
        return

    mime = 'image/gif' if ext == 'gif' else 'image/png'
    avatar_data_uri = f"data:{mime};base64,{base64.b64encode(icon_resp.content).decode()}"

    try:
        patch_resp = requests.patch(
            'https://discord.com/api/v10/users/@me',
            headers={**headers, 'Content-Type': 'application/json'},
            json={'avatar': avatar_data_uri},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"[BotAvatarSync] Failed to reach Discord API for avatar update: {e}")
        return

    if patch_resp.status_code == 200:
        cache.set(cache_key, icon_hash, timeout=None)
        logger.info(f"[BotAvatarSync] Bot avatar updated to icon hash {icon_hash[:8]}…")
    elif patch_resp.status_code == 429:
        logger.warning("[BotAvatarSync] Rate-limited by Discord, will retry next hour.")
    else:
        logger.error(f"[BotAvatarSync] Avatar update failed: {patch_resp.status_code} {patch_resp.text}")

@shared_task(bind=True, max_retries=3)
def ingest_wp_post_task(self, post_id, cookie_name, cookie_value, ingest_podcast_id):
    guid_url = f"https://baldmove.com/?p={post_id}"
    
    # Check again inside worker to prevent duplicates during race conditions
    if Episode.objects.filter(guid_private=guid_url).exists():
        return f"ID {post_id} already exists."

    try:
        session = requests.Session()
        session.headers.update({'User-Agent': 'VectoDiscoveryBot/1.0'})
        session.cookies.set(cookie_name, cookie_value, domain='baldmove.com')

        # WordPress will redirect ?p=ID to the readable permalink
        response = session.get(guid_url, timeout=15, allow_redirects=True)
        
        # Standard WordPress skips (404s or Login Redirects)
        if response.status_code != 200 or "wp-login.php" in response.url:
            return f"ID {post_id} skipped (Status {response.status_code})"

        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Tiered Audio Extraction
        audio_url = None
        tag = soup.select_one('.powerpress_links_mp3 a.powerpress_link_d')
        if tag:
            audio_url = tag.get('href')
        else:
            source = soup.select_one('audio source')
            if source:
                audio_url = source.get('src')
        
        if not audio_url:
            # Regex fallback for generic .mp3 links
            link = soup.find('a', href=re.compile(r'\.mp3(\?.*)?$'))
            audio_url = link['href'] if link else None

        if audio_url:
            title_tag = soup.select_one('h1.entry-title')
            title = title_tag.get_text(strip=True) if title_tag else f"Post {post_id}"
            
            # Metadata: Clean description of player UI
            content = soup.select_one('.entry-content')
            description = ""
            if content:
                for p in content.select('.powerpress_player, .episode_player, .powerpress_links'):
                    p.decompose()
                description = content.get_text(separator="\n", strip=True)

            # Metadata: Tags and Date
            tags = [t.get_text(strip=True) for t in soup.select('footer.entry-meta a[rel="tag"]')]
            
            pub_date = make_aware(datetime.now())
            date_tag = soup.select_one('.entry-date')
            if date_tag:
                try:
                    naive_date = datetime.strptime(date_tag.get_text(strip=True), "%B %d, %Y")
                    pub_date = make_aware(naive_date)
                except (ValueError, TypeError):
                    pass

            # Create entry using fields matching your models.py
            Episode.objects.create(
                podcast_id=ingest_podcast_id,
                title=title,
                raw_description=description,
                audio_url_subscriber=audio_url, # Store as private/subscriber
                guid_private=guid_url,
                pub_date=pub_date,
                tags=tags,
                link=response.url, # Resolves to the pretty permalink
                match_reason='celery_brute_discovery'
            )
            return f"Successfully ingested ID {post_id}: {title}"
        
        return f"ID {post_id} skipped: No audio found"

    except Exception as exc:
        # Retry for transient network issues or timeouts
        raise self.retry(exc=exc, countdown=10)


# ---------------------------------------------------------------------------
# GDrive Audio Recovery
# ---------------------------------------------------------------------------

def _s3_count_for_podcast(podcast_title):
    """Count episodes with S3 subscriber URLs, optionally scoped to one podcast."""
    qs = Episode.objects.filter(audio_url_subscriber__icontains='s3.amazonaws.com')
    if podcast_title and podcast_title not in ('all', ''):
        qs = qs.filter(podcast__title=podcast_title)
    return qs.count()


def _save_recovery_run(run_id, meta):
    runs_dir = os.path.join(settings.MEDIA_ROOT, 'Recovery', 'runs')
    os.makedirs(runs_dir, exist_ok=True)
    with open(os.path.join(runs_dir, f"{run_id}.json"), 'w', encoding='utf-8') as f:
        json.dump(meta, f, indent=2)


def _abs_path_to_media_url(abs_path):
    try:
        rel = os.path.relpath(abs_path, settings.MEDIA_ROOT).replace('\\', '/')
        return f"{settings.MEDIA_URL}{rel}"
    except ValueError:
        return None


@shared_task
def task_run_gdrive_recovery(run_id, csv_path, podcast_title, dry_run, min_confidence='HIGH'):
    task_id = f"gdrive_recovery_{run_id}"
    stream = CommandLogStream(task_id)
    logger.info("task_run_gdrive_recovery streaming to cache key %s (run %s)", task_id, run_id)
    meta = {
        'run_id': run_id,
        'csv_filename': os.path.basename(csv_path),
        'podcast_title': podcast_title or 'all',
        'mode': 'dry-run' if dry_run else 'live',
        'min_confidence': min_confidence,
        'started_at': datetime.utcnow().isoformat(),
        'status': 'running',
        'recovery_csv_url': None,
        'discord_txt_url': None,
        's3_before': _s3_count_for_podcast(podcast_title),
    }
    _save_recovery_run(run_id, meta)
    try:
        args = [csv_path]
        if podcast_title:
            args.append(podcast_title)
        # recover_gdrive_audio now previews by default; the UI's "dry run" toggle
        # maps to the inverse of --apply at this task boundary.
        call_command('recover_gdrive_audio', *args,
                     apply=not dry_run, min_confidence=min_confidence,
                     stdout=stream, stderr=stream, no_color=True)
        meta['status'] = 'completed'
    except Exception as e:
        stream.write(f"\n[ERROR] {str(e)}\n")
        meta['status'] = 'failed'
    finally:
        captured = stream.captured()
        csv_m = re.search(r'CSV report:\s+(\S+)', captured)
        disc_m = re.search(r'Discord report:\s+(\S+)', captured)
        if csv_m:
            meta['recovery_csv_url'] = _abs_path_to_media_url(csv_m.group(1))
        if disc_m:
            meta['discord_txt_url'] = _abs_path_to_media_url(disc_m.group(1))
        if dry_run:
            m = re.search(r'Would update:\s+(\d+)', captured)
            meta['would_recover'] = int(m.group(1)) if m else None
        else:
            meta['s3_after'] = _s3_count_for_podcast(podcast_title)
        meta['log'] = captured
        _save_recovery_run(run_id, meta)
        stream.write('[DONE]')


@shared_task
def task_run_gdrive_rewind(run_id, csv_path):
    task_id = f"gdrive_recovery_{run_id}"
    stream = CommandLogStream(task_id)
    logger.info("task_run_gdrive_rewind streaming to cache key %s (run %s)", task_id, run_id)
    meta = {
        'run_id': run_id,
        'csv_filename': os.path.basename(csv_path),
        'podcast_title': 'all',
        'mode': 'rewind',
        'started_at': datetime.utcnow().isoformat(),
        'status': 'running',
        'recovery_csv_url': None,
        'discord_txt_url': None,
    }
    _save_recovery_run(run_id, meta)
    try:
        # rewind_gdrive_audio now previews by default; the Creator UI rewind has no
        # preview toggle, so always apply.
        call_command('rewind_gdrive_audio', csv_path, apply=True,
                     stdout=stream, stderr=stream, no_color=True)
        meta['status'] = 'completed'
    except Exception as e:
        stream.write(f"\n[ERROR] {str(e)}\n")
        meta['status'] = 'failed'
    finally:
        meta['log'] = stream.captured()
        _save_recovery_run(run_id, meta)
        stream.write('[DONE]')


# ---------------------------------------------------------------------------
# Admin Command Console — generic management-command runner (design §7)
# ---------------------------------------------------------------------------

@shared_task(queue='admin')
def task_run_management_command(run_id, name, args, options):
    """Run any console-dispatched management command off the request thread.

    Routed to the dedicated ``admin`` queue (its own idle ``celery-admin`` worker)
    so an operator waiting on output isn't stuck behind a bulk backlog on the default
    queue — e.g. hundreds of ``ensure_source_audio`` jobs (design §15.3 / §7). The
    main worker also drains ``admin`` as a fallback if ``celery-admin`` is down.

    One generic task replaces the per-command wrappers: it streams live output to
    the ``admin_cmd_{run_id}`` cache key (polled by the console, §8) and records the
    lifecycle + full log on the pre-created ``CommandRun`` row (§8a). ``args`` /
    ``options`` are the *real* (un-redacted) invocation; the [SYSTEM] echo uses the
    row's already-redacted ``command_line`` so secrets never reach the log (§15.6).
    """
    from .models import CommandRun

    task_id = f"admin_cmd_{run_id}"
    stream = CommandLogStream(task_id)
    logger.info("task_run_management_command streaming to cache key %s (run %s, command %s)",
                task_id, run_id, name)
    run = CommandRun.objects.get(run_id=run_id)
    run.mark_running()
    try:
        stream.write(f"[SYSTEM] Running: {run.command_line}\n")
        call_command(name, *args, stdout=stream, stderr=stream, no_color=True, **options)
        run.mark_finished(CommandRun.Status.COMPLETED)
    except Exception as e:
        stream.write(f"\n[ERROR] {str(e)}\n")
        run.mark_finished(CommandRun.Status.FAILED, error=str(e))
        raise  # Let Celery record the failure; finally still persists log + [DONE]
    finally:
        from .admin_console.summary import extract_summary
        captured = stream.captured()
        run.log = captured
        # Pull a command-emitted [SUMMARY] line into result_summary, if any (§8a).
        # Fault-tolerant: a missing/malformed marker just leaves it null.
        try:
            summary = extract_summary(captured)
        except Exception:  # noqa: BLE001 - summary extraction must never fail the run
            summary = None
        if summary is not None:
            run.result_summary = summary
            run.save(update_fields=['log', 'result_summary'])
        else:
            run.save(update_fields=['log'])
        stream.write("[DONE]")


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------

@shared_task(bind=True, max_retries=3, time_limit=600)
def task_ensure_source_audio(self, episode_id: int):
    """Download and persist subscriber audio for a completed episode if missing on disk."""
    from pod_manager.services.transcription import ensure_source_audio
    try:
        ensure_source_audio(episode_id)
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@shared_task(
    bind=True,
    max_retries=3,
    queue='transcription',
    time_limit=7200,
    soft_time_limit=6900,
    acks_late=True,
    reject_on_worker_lost=True,
)
def transcribe_episode(self, episode_id: int, **kwargs):
    """Celery wrapper around run_transcription(). Retries up to 3 times.

    Accepts optional transcription override kwargs (model, language,
    initial_prompt, min_speakers, num_speakers, max_speakers) which are
    passed through to run_transcription().

    Timeout ladder: WHISPER_TIMEOUT (requests) < soft_time_limit (SoftTimeLimitExceeded
    exception → retry path) < time_limit (SIGKILL, last resort). WHISPER_TIMEOUT must be
    the first to fire so the transcript is never left stuck in PROCESSING by a hard kill.
    """
    from celery.exceptions import SoftTimeLimitExceeded
    from pod_manager.services.transcription import run_transcription
    try:
        run_transcription(episode_id, **kwargs)
    except SoftTimeLimitExceeded as exc:
        # Celery soft limit fired before requests.post timed out — mark stuck record and retry.
        from pod_manager.models import Transcript
        Transcript.objects.filter(
            episode_id=episode_id,
            status=Transcript.Status.PROCESSING,
        ).update(status=Transcript.Status.FAILED, error_message=str(exc))
        countdown = 60 * (self.request.retries + 1)
        raise self.retry(exc=exc, countdown=countdown)
    except Exception as exc:
        # Exponential-ish back-off: 60s, 120s, 180s
        countdown = 60 * (self.request.retries + 1)
        raise self.retry(exc=exc, countdown=countdown)


@shared_task(
    bind=True,
    max_retries=3,
    time_limit=1800,
    acks_late=True,
    reject_on_worker_lost=True,
)
def task_mirror_episode_audio(self, episode_id: int, force: bool = False):
    """Mirror an episode's subscriber audio to R2 independently of transcription.

    Two callers:
      - the standalone save signal, when transcription is disabled/not configured
        (so run_transcription's inline mirror never runs), and
      - the Phase 5 backfill.

    Idempotent + best-effort: MirrorSkipped is a normal no-op (public/dead-S3/
    not-premium), not a failure. Real errors retry with back-off. Runs on the
    default queue, not 'transcription', so it works even when no transcription
    workers are running."""
    from pod_manager.services.r2_mirror import MirrorSkipped, mirror_episode_audio
    try:
        result = mirror_episode_audio(episode_id, force=force)
        logger.info(
            "task_mirror_episode_audio: ep %d -> %s (%s)",
            episode_id, result.get('status'), result.get('key'),
        )
    except MirrorSkipped as exc:
        logger.info("task_mirror_episode_audio: skipped ep %d — %s", episode_id, exc)
    except Exception as exc:
        countdown = 60 * (self.request.retries + 1)
        raise self.retry(exc=exc, countdown=countdown)


@shared_task(bind=True, max_retries=3, time_limit=1800, acks_late=True, reject_on_worker_lost=True)
def task_rekey_episode_audio(self, episode_id: int):
    """Relocate an episode's R2 object to its current-parent key after a move.
    Idempotent (no-op when the key already matches); retries on transient errors."""
    from pod_manager.services.r2_maintenance import rekey_episode_audio
    try:
        result = rekey_episode_audio(episode_id)
        logger.info("task_rekey_episode_audio: ep %d -> %s", episode_id, result.get("status"))
    except Exception as exc:
        raise self.retry(exc=exc, countdown=60 * (self.request.retries + 1))


@shared_task
def task_r2_reconcile():
    """Weekly: record partial-failure orphans (section I, Layer 2)."""
    from pod_manager.services.r2_maintenance import reconcile_orphans
    reconcile_orphans(apply=True)


@shared_task
def task_r2_orphan_cleanup():
    """Daily: hard-delete expired, still-unreferenced orphan objects (section I)."""
    from pod_manager.services.r2_maintenance import cleanup_orphans
    cleanup_orphans(apply=True)