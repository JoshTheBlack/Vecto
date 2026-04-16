import logging
from celery import shared_task
from django.core.management import call_command
from pod_manager.models import Network
from pod_manager.views import sync_network_patrons, invalidate_show_cache
from django.utils import timezone
from datetime import timedelta
from django.core.files.base import ContentFile
from django.template.loader import render_to_string
import pdfkit

from .models import Network, PatronProfile, Invoice

logger = logging.getLogger(__name__)

@shared_task
def task_generate_monthly_invoices():
    logger.info("Starting monthly invoice generation...")
    networks = Network.objects.filter(patreon_sync_enabled=True)
    thirty_days_ago = timezone.now() - timedelta(days=30)

    # --- Windows Configuration for pdfkit ---
    # Update this path if you installed it somewhere else!
    path_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
    config = pdfkit.configuration(wkhtmltopdf=path_wkhtmltopdf)
    # ----------------------------------------

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

        # Generate PDF using pdfkit
        pdf_bytes = pdfkit.from_string(html_string, False, configuration=config)

        invoice = Invoice(network=network, amount_due=total_due, active_user_count=active_count)
        filename = f"{network.slug}_invoice_{timezone.now().strftime('%Y_%m')}.pdf"
        invoice.pdf_file.save(filename, ContentFile(pdf_bytes))
        
        logger.info(f"Generated invoice for {network.name}: ${total_due}")

@shared_task
def task_sync_network_patrons(network_id):
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
def task_ingest_feed(show_id):
    logger.info(f"Starting background ingestion for show_id={show_id}")
    try:
        call_command('ingest_feed', show_id, no_color=True)
        invalidate_show_cache(show_id)
        logger.info(f"Successfully ingested show_id={show_id}")
    except Exception as e:
        logger.error(f"Ingestion failed for show_id={show_id}: {str(e)}", exc_info=True)

@shared_task
def task_clean_mix_images():
    logger.info("Running nightly sweep of orphaned mix images.")
    call_command('clean_mix_images')