from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0074_network_whisper_defaults'),
    ]

    operations = [
        migrations.AddField(
            model_name='podcast',
            name='whisper_initial_prompt',
            field=models.TextField(blank=True, null=True, help_text='Override network initial_prompt for this podcast. Null = inherit.'),
        ),
        migrations.AddField(
            model_name='podcast',
            name='whisper_model',
            field=models.CharField(blank=True, null=True, help_text='Override network whisper model for this podcast. Null = inherit.', max_length=50),
        ),
        migrations.AddField(
            model_name='podcast',
            name='whisper_language',
            field=models.CharField(blank=True, null=True, help_text='Override network language for this podcast. Null = inherit.', max_length=10),
        ),
        migrations.AddField(
            model_name='podcast',
            name='whisper_min_speakers',
            field=models.IntegerField(blank=True, null=True, help_text='Override min speakers. Null = inherit.'),
        ),
        migrations.AddField(
            model_name='podcast',
            name='whisper_num_speakers',
            field=models.IntegerField(blank=True, null=True, help_text='Override default speakers. Null = inherit.'),
        ),
        migrations.AddField(
            model_name='podcast',
            name='whisper_max_speakers',
            field=models.IntegerField(blank=True, null=True, help_text='Override max speakers. Null = inherit.'),
        ),
    ]
