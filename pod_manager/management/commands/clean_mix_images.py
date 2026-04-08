import os
from django.core.management.base import BaseCommand
from django.conf import settings
from pod_manager.models import UserMix

class Command(BaseCommand):
    help = 'Sweeps the media directory and deletes orphaned UserMix images not attached to the database.'

    def handle(self, *args, **kwargs):
        media_root = settings.MEDIA_ROOT
        mix_covers_dir = os.path.join(media_root, 'mix_covers')
        
        if not os.path.exists(mix_covers_dir):
            self.stdout.write(self.style.WARNING("No mix_covers directory found. Nothing to clean."))
            return

        # 1. Ask the database what files ACTUALLY exist right now
        valid_files = set()
        for mix in UserMix.objects.exclude(image_upload=''):
            if mix.image_upload:
                # Get just the filename (e.g., '1234-5678.jpg')
                valid_files.add(os.path.basename(mix.image_upload.name))

        # 2. Iterate through the hard drive and delete anything not in the database
        deleted_count = 0
        for filename in os.listdir(mix_covers_dir):
            if filename not in valid_files:
                file_path = os.path.join(mix_covers_dir, filename)
                try:
                    os.remove(file_path)
                    self.stdout.write(self.style.SUCCESS(f"  [DELETED] Orphaned file: {filename}"))
                    deleted_count += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  [ERROR] Failed to delete {filename}: {e}"))

        if deleted_count == 0:
            self.stdout.write(self.style.SUCCESS("Filesystem is perfectly clean. No orphaned images found."))
        else:
            self.stdout.write(self.style.WARNING(f"Cleanup complete. Removed {deleted_count} orphaned images."))