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
    # R2 orphan lifecycle (planned_features.txt section I). Reconcile weekly to
    # record partial-failure orphans; cleanup daily to delete expired ones (the
    # 90-day / 7-day retention windows make the exact cadence non-critical).
    'r2-reconcile-weekly': {
        'task': 'pod_manager.tasks.task_r2_reconcile',
        'schedule': crontab(day_of_week=1, hour=3, minute=30),
    },
    'r2-orphan-cleanup-daily': {
        'task': 'pod_manager.tasks.task_r2_orphan_cleanup',
        'schedule': crontab(hour=4, minute=0),
    },
}