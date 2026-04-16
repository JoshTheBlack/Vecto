import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('vecto')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# Celery Beat Schedule
app.conf.beat_schedule = {
    'sync-all-patrons-hourly': {
        'task': 'pod_manager.tasks.task_sync_all_networks',
        'schedule': crontab(minute=0), # Run every hour at minute 0
    },
    'clean-mix-images-nightly': {
        'task': 'pod_manager.tasks.task_clean_mix_images',
        'schedule': crontab(hour=3, minute=0), # Run at 3:00 AM
    },
}