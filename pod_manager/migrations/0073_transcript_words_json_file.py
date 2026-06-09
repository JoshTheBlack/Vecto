from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0072_rename_subgen_model_used'),
    ]

    operations = [
        migrations.AddField(
            model_name='transcript',
            name='words_json_file',
            field=models.CharField(blank=True, max_length=500, null=True),
        ),
    ]
