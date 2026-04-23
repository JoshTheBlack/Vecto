import logging
from celery import shared_task
from django.core.cache import cache
from django.core.management import call_command
from pod_manager.models import Network
from django.utils import timezone
from datetime import timedelta
from django.core.files.base import ContentFile
from django.template.loader import render_to_string
import pdfkit
import platform
from .models import Network, PatronProfile, Invoice, Podcast
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
    # 1. We use the lower-level Redis client to efficiently find all matching keys
    # (Django's default cache framework doesn't have a great way to search keys by pattern)
    redis_client = cache.client.get_client()
    
    # Find all keys matching our buffer pattern
    # Note: If you are using django-redis, keys might have a prefix like ":1:buffer_last_active_*"
    keys = redis_client.keys("*buffer_last_active_*")
    
    if not keys:
        logger.info("No active timestamps to sync.")
        return

    profiles_to_update = []
    keys_to_delete = []

    for key_bytes in keys:
        # Decode the key and extract the profile ID
        key_str = key_bytes.decode('utf-8')
        # Strip any django-redis prefixes (usually ':1:')
        clean_key = key_str.split('buffer_last_active_')[-1]
        
        try:
            profile_id = int(clean_key)
            # Retrieve the timestamp we saved in views.py
            last_active_time = cache.get(f"buffer_last_active_{profile_id}")
            
            if last_active_time:
                # Prepare the object for the bulk update
                profile = PatronProfile(id=profile_id)
                profile.last_active = last_active_time
                profiles_to_update.append(profile)
                keys_to_delete.append(key_str)
                
        except (ValueError, TypeError):
            continue

    # 2. Execute a single massive database write!
    if profiles_to_update:
        PatronProfile.objects.bulk_update(profiles_to_update, ['last_active'])
        logger.info(f"Successfully bulk-updated {len(profiles_to_update)} user activity timestamps.")

    # 3. Clean up Redis so we don't process them again
    if keys_to_delete:
        redis_client.delete(*keys_to_delete)