"""Confirm every uploaded file the site serves from vecto-cdn really exists there.

Checks every file field backed by the media storage (avatars, mix covers,
network logos, fallback images and fonts, 404 images, and any added later),
not just the asset classes the backfill commands migrated. A row whose object is
missing renders as a broken image: typically a file uploaded while R2 media was
disabled, which landed on local disk under a name the site now looks up on the
CDN. Fix one by re-uploading it in the UI.

One HEAD per stored file; read-only. Exits with an error when anything is
missing, so it doubles as a gate before deleting local-storage fallbacks.

    python manage.py verify_r2_media
"""

from django.apps import apps
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db.models import FileField

from pod_manager.admin_console.summary import emit_summary
from pod_manager.services.r2_storage import select_media_storage


def media_file_fields():
    """(model, field name) for every FileField whose storage is the media
    storage callable, in a stable order."""
    found = []
    for model in apps.get_app_config('pod_manager').get_models():
        for field in model._meta.get_fields():
            if (isinstance(field, FileField)
                    and getattr(field, '_storage_callable', None) is select_media_storage):
                found.append((model, field.name))
    return sorted(found, key=lambda pair: (pair[0].__name__, pair[1]))


class Command(BaseCommand):
    help = ("HEAD every uploaded file served from the R2 media bucket (all image and "
            "font fields) and report any that are missing.")

    def handle(self, *args, **options):
        if not settings.R2_MEDIA_ENABLED:
            raise CommandError("R2_MEDIA_ENABLED is off, so there is no R2 media bucket to check.")

        present = missing = 0
        for model, field in media_file_fields():
            self.stdout.write(self.style.MIGRATE_HEADING(f"\n{model.__name__}.{field}:"))
            for instance in model.objects.exclude(**{field: ''}).exclude(**{f'{field}__isnull': True}):
                stored = getattr(instance, field)
                if stored.storage.exists(stored.name):
                    present += 1
                else:
                    missing += 1
                    self.stdout.write(self.style.ERROR(f"  MISSING in R2: {instance!r} ({stored.name})"))

        style = self.style.SUCCESS if missing == 0 else self.style.ERROR
        self.stdout.write(style(f"\nVerify: {present} present, {missing} missing."))
        emit_summary(self.stdout, {"present": present, "missing": missing})
        if missing:
            raise CommandError("Some uploaded files are missing from R2; see the list above.")
