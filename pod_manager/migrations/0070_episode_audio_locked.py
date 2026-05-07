from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0069_episode_type_free_text'),
    ]

    operations = [
        migrations.AddField(
            model_name='episode',
            name='audio_locked',
            field=models.BooleanField(default=False, help_text='If checked, future feed ingests will NOT overwrite audio URLs. Set automatically by the GDrive recovery script.'),
        ),
    ]
