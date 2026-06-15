from django.db import migrations, models


def null_out_default_num_speakers(apps, schema_editor):
    Network = apps.get_model('pod_manager', 'Network')
    Network.objects.all().update(whisper_num_speakers=None)


def restore_default_num_speakers(apps, schema_editor):
    Network = apps.get_model('pod_manager', 'Network')
    Network.objects.filter(whisper_num_speakers__isnull=True).update(whisper_num_speakers=2)


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0075_podcast_whisper_overrides'),
    ]

    operations = [
        migrations.AlterField(
            model_name='network',
            name='whisper_num_speakers',
            field=models.IntegerField(
                blank=True,
                null=True,
                help_text='Expected speaker count hint for diarization. Null = auto-detect.',
            ),
        ),
        migrations.RunPython(null_out_default_num_speakers, restore_default_num_speakers),
    ]
