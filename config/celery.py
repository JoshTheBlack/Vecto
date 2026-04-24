import os
from celery import Celery
from celery.schedules import crontab

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('vecto')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# Celery Beat Schedule
app.conf.beat_schedule = {
    'smart-feed-polling': {
        'task': 'pod_manager.tasks.task_smart_poll_feeds',
        'schedule': crontab(minute='*/15'),
    },
    'sync-patreon-daily': {
        'task': 'pod_manager.tasks.task_sync_all_networks',
        'schedule': crontab(hour=2, minute=0),
    },
    'generate-invoices-first-of-month': {
        'task': 'pod_manager.tasks.task_generate_monthly_invoices',
        'schedule': crontab(day_of_month='1', hour=0, minute=0),
    },
    'sync-active-timestamps-hourly': {
        'task': 'pod_manager.tasks.task_sync_last_active_timestamps',
        'schedule': crontab(minute=0), 
    },
    'sweep-analytics-hourly': {
        'task': 'pod_manager.tasks.sweep_analytics_buffer',
        'schedule': crontab(minute=0),
    },
}