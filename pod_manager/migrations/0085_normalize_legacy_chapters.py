"""Convert any legacy bare-list chapters into the canonical Podcast Index dict
{"version": "1.2.0", "chapters": [...]} (empty list -> NULL), so no episode
holds chapters as a legacy list. Going forward the Episode pre_save signal keeps
new writes in the canonical shape; this backfills existing rows.

Lossless: it only re-shapes a list into the wrapping dict, never drops chapters.
"""

from django.db import migrations
from django.db.models import Q


def wrap_legacy_chapters(apps, schema_editor):
    Episode = apps.get_model('pod_manager', 'Episode')
    qs = Episode.objects.filter(Q(chapters_public__isnull=False) | Q(chapters_private__isnull=False))
    for ep in qs.iterator():
        changed = False
        for field in ('chapters_public', 'chapters_private'):
            val = getattr(ep, field)
            if isinstance(val, list):
                setattr(ep, field, {"version": "1.2.0", "chapters": val} if val else None)
                changed = True
        if changed:
            ep.save(update_fields=['chapters_public', 'chapters_private'])


class Migration(migrations.Migration):

    dependencies = [
        ('pod_manager', '0084_transcript_version'),
    ]

    operations = [
        # Reverse is a noop — we never want to re-introduce the legacy list shape.
        migrations.RunPython(wrap_legacy_chapters, migrations.RunPython.noop),
    ]
