"""Get a database backup back out of R2: download, decrypt, verify, and optionally load it.

    python manage.py restore_database_backup --latest --out /tmp/vecto.dump
        # download + decrypt + verify to a plain pg_dump file (no database touched)
    python manage.py restore_database_backup --latest --into vecto_check            # preview
    python manage.py restore_database_backup --latest --into vecto_check --apply    # restore
    python manage.py restore_database_backup --key db-backups/vecto-2026-10-04-023000.dump.enc ...

--into restores into a NEW database and is refused for the live one: restoring over production
is an outage you should choose deliberately. Restore to a scratch name, check the row counts,
then swap by hand (docs/database-backups.md). --into needs a database user with CREATEDB.
"""

import tempfile
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from pod_manager.admin_console.summary import emit_summary
from pod_manager.services import db_backup


class Command(BaseCommand):
    help = "Download + decrypt + verify a backup; optionally restore it into a NEW database."

    def add_arguments(self, parser):
        which = parser.add_mutually_exclusive_group()
        which.add_argument("--latest", action="store_true", help="Use the newest backup.")
        which.add_argument("--key", help="Use this backup's object key (see backup_database --list).")
        parser.add_argument("--out", help="Write the decrypted pg_dump file here.")
        parser.add_argument("--into", help="Restore into this NEW database name (never the live one).")
        parser.add_argument("--apply", action="store_true",
                            help="With --into: actually create the database and load the backup "
                                 "(default is a preview).")

    def handle(self, *args, **options):
        ok, reason = db_backup.is_configured()
        if not ok:
            raise CommandError(f"Backups are not configured: {reason}.")
        if not (options["latest"] or options["key"]):
            raise CommandError("Choose a backup: --latest or --key=<object key>.")
        if not (options["out"] or options["into"]):
            raise CommandError("Say what to do with it: --out=<file> and/or --into=<new database>.")

        try:
            object_key = options["key"]
            if options["latest"]:
                backups = db_backup.list_backups()
                if not backups:
                    raise CommandError("There are no backups in the bucket.")
                object_key = backups[0]["key"]
            self.stdout.write(f"Backup: {object_key}")

            if options["into"] and not options["apply"]:
                self.stdout.write(f"Would download, decrypt and restore into a new database '{options['into']}'. "
                                  "Re-run with --apply to do it.")
                if not options["out"]:
                    emit_summary(self.stdout, {"applied": False, "key": object_key})
                    return

            if options["out"]:
                dest = Path(options["out"])
                db_backup.download_backup(object_key, dest)
                self.stdout.write(self.style.SUCCESS(f"Decrypted and verified: {dest} ({dest.stat().st_size / 1_048_576:.1f} MiB)."))
                dump = dest
                tmp = None
            else:
                tmp = tempfile.TemporaryDirectory(prefix="vecto-restore-")
                dump = db_backup.download_backup(object_key, Path(tmp.name) / "vecto.dump")
                self.stdout.write("Decrypted and verified.")

            if options["into"] and options["apply"]:
                self.stdout.write(f"Restoring into new database '{options['into']}'...")
                db_backup.restore_into(dump, options["into"])
                self.stdout.write(self.style.SUCCESS(
                    f"Restored into '{options['into']}'. Compare its row counts with production before "
                    "relying on it."))
            if tmp is not None:
                tmp.cleanup()
        except db_backup.BackupError as exc:
            raise CommandError(str(exc))

        emit_summary(self.stdout, {"applied": bool(options["apply"]), "key": object_key,
                                   "into": options["into"] or "", "out": options["out"] or ""})
