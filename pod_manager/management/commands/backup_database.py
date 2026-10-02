"""Take an encrypted database backup to the private R2 backup bucket right now.

The weekly Celery task runs the same pipeline (services/db_backup). Use this for a backup
before a risky change, or to prove the setup works after configuring it:

    python manage.py backup_database                  # dump, verify, encrypt, upload, prune
    python manage.py backup_database --list           # what is in the bucket
    python manage.py backup_database --generate-key   # a new DB_BACKUP_KEY (prints, saves nothing)

Restoring: see restore_database_backup and docs/database-backups.md.
"""

from django.core.management.base import BaseCommand, CommandError

from pod_manager.admin_console.summary import emit_summary
from pod_manager.services import db_backup


class Command(BaseCommand):
    help = "Encrypted pg_dump to the private R2 backup bucket (also runs weekly via Celery)."

    def add_arguments(self, parser):
        parser.add_argument("--list", action="store_true", help="List existing backups and exit.")
        parser.add_argument("--generate-key", action="store_true",
                            help="Print a fresh DB_BACKUP_KEY and exit. Nothing is saved: put it in .env "
                                 "AND somewhere off the server.")

    def handle(self, *args, **options):
        if options["generate_key"]:
            self.stdout.write(db_backup.generate_key())
            self.stdout.write("Set this as DB_BACKUP_KEY, and keep a copy off the server: "
                              "a backup cannot be opened without it.")
            return

        ok, reason = db_backup.is_configured()
        if not ok:
            raise CommandError(f"Backups are not configured: {reason}.")

        try:
            if options["list"]:
                backups = db_backup.list_backups()
                for b in backups:
                    self.stdout.write(f"{b['key']}  {b['size'] / 1_048_576:.1f} MiB  {b['modified']:%Y-%m-%d %H:%M} UTC")
                self.stdout.write(f"{len(backups)} backup(s).")
                emit_summary(self.stdout, {"backups": len(backups)})
                return

            self.stdout.write("Dumping, verifying, encrypting and uploading...")
            result = db_backup.run_backup()
        except db_backup.BackupError as exc:
            raise CommandError(str(exc))

        self.stdout.write(self.style.SUCCESS(
            f"Uploaded {result['key']} ({result['bytes'] / 1_048_576:.1f} MiB encrypted, "
            f"{result['tables']} tables); pruned {len(result['pruned'])} old backup(s)."))
        emit_summary(self.stdout, {"key": result["key"], "bytes": result["bytes"],
                                   "tables": result["tables"], "pruned": len(result["pruned"])})
