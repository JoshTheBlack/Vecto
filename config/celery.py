import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')

app = Celery('vecto')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()

# Beat schedule: defined in settings.CELERY_BEAT_SCHEDULE, NOT here. With
# namespace='CELERY' above, the settings value is looked up before anything assigned to
# app.conf.beat_schedule, so a schedule set here would be silently ignored.
