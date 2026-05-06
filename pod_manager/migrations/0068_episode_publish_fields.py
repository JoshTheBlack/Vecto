from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0067_logentry_add_user'),
    ]

    operations = [
        migrations.AddField(
            model_name='episode',
            name='is_published',
            field=models.BooleanField(
                default=True,
                db_index=True,
                help_text='Uncheck to hide this episode from all RSS feeds.',
            ),
        ),
        migrations.AddField(
            model_name='episode',
            name='scheduled_at',
            field=models.DateTimeField(
                null=True,
                blank=True,
                db_index=True,
                help_text='If set and is_published=False, Celery will publish at this time.',
            ),
        ),
        migrations.AddField(
            model_name='episode',
            name='season_number',
            field=models.PositiveSmallIntegerField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='episode',
            name='episode_number',
            field=models.PositiveSmallIntegerField(null=True, blank=True),
        ),
        migrations.AddField(
            model_name='episode',
            name='episode_type',
            field=models.CharField(
                max_length=10,
                choices=[('full', 'Full'), ('trailer', 'Trailer'), ('bonus', 'Bonus')],
                default='full',
                blank=True,
            ),
        ),
    ]
