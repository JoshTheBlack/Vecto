"""Reconciliation sweep over the production R2 keyspace.

Lists the entire prod keyspace and RECORDS objects that no Episode.r2_url points
at (partial-failure orphans) into R2OrphanedObject. Never deletes — that's
r2_cleanup_orphans. Default is a DRY RUN; pass --apply to write rows.

    python manage.py r2_gc                 # dry run: report only
    python manage.py r2_gc --apply         # record orphans
    python manage.py r2_gc --age-days=14   # ignore objects newer than 14 days
"""

from django.core.management.base import BaseCommand

from pod_manager.services.r2_maintenance import reconcile_orphans


class Command(BaseCommand):
    help = "Sweep the R2 bucket and record unreferenced objects as orphans (dry run by default)."

    def add_arguments(self, parser):
        parser.add_argument("--apply", action="store_true", help="Write R2OrphanedObject rows (default: dry run).")
        parser.add_argument("--age-days", type=int, default=7,
                            help="Skip objects modified more recently than this (mid-pipeline guard).")

    def handle(self, *args, **options):
        result = reconcile_orphans(apply=options["apply"], age_days=options["age_days"])
        self.stdout.write(f"Scanned {result['scanned']} object(s); {len(result['orphans'])} unreferenced.")
        for key in result["orphans"][:50]:
            self.stdout.write(f"  orphan: {key}")
        if len(result["orphans"]) > 50:
            self.stdout.write(f"  ... and {len(result['orphans']) - 50} more")
        if options["apply"]:
            self.stdout.write(self.style.SUCCESS(f"Recorded {len(result['orphans'])} orphan row(s)."))
        else:
            self.stdout.write(self.style.WARNING("Dry run — nothing recorded. Re-run with --apply."))
