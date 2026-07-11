"""Orphan cleanup — the only hard-delete in the R2 object lifecycle.

Deletes R2 objects for orphan rows past their per-reason retention (move_rekey ->
R2_REKEY_GRACE_DAYS, else R2_ORPHAN_RETENTION_DAYS), re-validating against live
Episode.r2_url first so a re-adopted key is never deleted. Default DRY RUN.

    python manage.py r2_cleanup_orphans                # dry run: report only
    python manage.py r2_cleanup_orphans --apply --yes  # delete expired, unreferenced
"""

import logging

from django.core.management.base import BaseCommand, CommandError

from pod_manager.services.r2_maintenance import cleanup_orphans

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Delete expired, still-unreferenced orphan objects from R2 (dry run by default; --apply --yes to delete)."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true",
                            help="Actually delete (default: dry run). Requires --yes.")
        parser.add_argument("--yes", action="store_true",
                            help="Confirm irreversible deletion (required with --apply).")

    def handle(self, *args, **options):
        apply = options["apply"]
        if apply and not options["yes"]:
            raise CommandError("This permanently deletes R2 objects. Re-run with --apply --yes to confirm.")
        result = cleanup_orphans(apply=apply)
        self.stdout.write(
            f"{len(result['deleted'])} audio object(s) eligible for deletion; "
            f"{len(result['transcripts'])} transcript object(s) (media bucket, "
            f"delete + CDN purge); "
            f"{result['readopted']} re-adopted (row dropped, object kept)."
        )
        for key in result["deleted"][:50]:
            self.stdout.write(f"  delete: {key}")
        if len(result["deleted"]) > 50:
            self.stdout.write(f"  ... and {len(result['deleted']) - 50} more")
        for key in result["transcripts"][:50]:
            self.stdout.write(f"  delete+purge (media): {key}")
        if len(result["transcripts"]) > 50:
            self.stdout.write(f"  ... and {len(result['transcripts']) - 50} more")
        if apply and result["transcripts_retained"]:
            self.stdout.write(self.style.WARNING(
                f"{result['transcripts_retained']} transcript row(s) retained "
                f"(delete or CDN purge failed — will retry next run)."
            ))
        if apply:
            logger.info("r2_cleanup_orphans applied: deleted=%d transcripts=%d retained=%d readopted=%d",
                        len(result["deleted"]), len(result["transcripts"]),
                        result["transcripts_retained"], result["readopted"])
            cleared_tx = len(result["transcripts"]) - result["transcripts_retained"]
            self.stdout.write(self.style.SUCCESS(
                f"Deleted {len(result['deleted'])} audio + {cleared_tx} transcript object(s)."))
        else:
            self.stdout.write(self.style.WARNING("Dry run — nothing deleted. Re-run with --apply --yes."))

        from pod_manager.admin_console.summary import emit_summary
        emit_summary(self.stdout, {
            "applied": apply,
            "deleted": len(result["deleted"]),
            "transcripts": len(result["transcripts"]),
            "transcripts_retained": result["transcripts_retained"],
            "readopted": result["readopted"],
        })
