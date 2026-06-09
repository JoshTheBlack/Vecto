from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0073_transcript_words_json_file'),
    ]

    operations = [
        migrations.AddField(
            model_name='network',
            name='whisper_initial_prompt',
            field=models.TextField(blank=True, help_text='Vocabulary hint passed to Whisper. E.g. host names, show titles, proper nouns. Leave blank to omit.'),
        ),
        migrations.AddField(
            model_name='network',
            name='whisper_model',
            field=models.CharField(default='medium.en', help_text='Whisper model size: tiny/base/small/medium/large or language-specific (e.g. medium.en).', max_length=50),
        ),
        migrations.AddField(
            model_name='network',
            name='whisper_language',
            field=models.CharField(default='en', help_text="BCP-47 language code passed to Whisper. E.g. 'en', 'es', 'fr'.", max_length=10),
        ),
        migrations.AddField(
            model_name='network',
            name='whisper_min_speakers',
            field=models.IntegerField(default=1, help_text='Minimum expected speakers for diarization.'),
        ),
        migrations.AddField(
            model_name='network',
            name='whisper_num_speakers',
            field=models.IntegerField(default=2, help_text='Expected speaker count hint for diarization.'),
        ),
        migrations.AddField(
            model_name='network',
            name='whisper_max_speakers',
            field=models.IntegerField(default=4, help_text='Maximum expected speakers for diarization.'),
        ),
    ]
