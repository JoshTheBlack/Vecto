"""Orphan cleanup — the only hard-delete (planned_features.txt section I, CLEANUP).

Deletes R2 objects for orphan rows past their per-reason retention (move_rekey ->
R2_REKEY_GRACE_DAYS, else R2_ORPHAN_RETENTION_DAYS), re-validating against live
Episode.r2_url first so a re-adopted key is never deleted. Default DRY RUN.

    python manage.py r2_cleanup_orphans            # dry run: report only
    python manage.py r2_cleanup_orphans --apply    # delete expired, unreferenced
"""

from django.core.management.base import BaseCommand

from pod_manager.services.r2_maintenance import cleanup_orphans


class Command(BaseCommand):
    help = "Delete expired, still-unreferenced orphan objects from R2 (dry run by default)."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Actually delete (default: dry run).")

    def handle(self, *args, **options):
        result = cleanup_orphans(apply=options["apply"])
        self.stdout.write(
            f"{len(result['deleted'])} object(s) eligible for deletion; "
            f"{result['readopted']} re-adopted (row dropped, object kept)."
        )
        for key in result["deleted"][:50]:
            self.stdout.write(f"  delete: {key}")
        if len(result["deleted"]) > 50:
            self.stdout.write(f"  ... and {len(result['deleted']) - 50} more")
        if options["apply"]:
            self.stdout.write(self.style.SUCCESS(f"Deleted {len(result['deleted'])} object(s)."))
        else:
            self.stdout.write(self.style.WARNING("Dry run — nothing deleted. Re-run with --apply."))
