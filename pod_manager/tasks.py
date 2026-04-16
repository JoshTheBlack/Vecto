import logging
from celery import shared_task
from django.core.management import call_command
from pod_manager.models import Network
from pod_manager.views import sync_network_patrons, invalidate_show_cache

logger = logging.getLogger(__name__)

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