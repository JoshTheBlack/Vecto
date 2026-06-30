from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0089_episodeeditsuggestion_counter_deltas'),
    ]

    operations = [
        migrations.AddField(
            model_name='commandrun',
            name='celery_task_id',
            field=models.CharField(blank=True, db_index=True, max_length=255),
        ),
    ]
