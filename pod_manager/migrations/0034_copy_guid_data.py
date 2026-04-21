from django.db import migrations

def copy_old_guids(apps, schema_editor):
    Episode = apps.get_model('pod_manager', 'Episode')
    
    for ep in Episode.objects.all():
        if ep.audio_url_public:
            ep.guid_public = ep.guid

            if ep.audio_url_subscriber:
                ep.guid_private = ep.guid
        
        elif ep.audio_url_subscriber:
            ep.guid_private = ep.guid
            
        ep.save()

def reverse_copy(apps, schema_editor):
    pass

class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0033_episode_guid_private_episode_guid_public_and_more'),
    ]

    operations = [
        migrations.RunPython(copy_old_guids, reverse_copy),
    ]