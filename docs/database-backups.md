# Database backups

A daily Celery task takes an encrypted `pg_dump` and uploads it to a **private** R2
bucket. Code: `pod_manager/services/db_backup.py`. Commands: `backup_database`,
`restore_database_backup`.

## What happens each night at 02:30 (`db-backup-daily`)

1. `pg_dump -Fc` straight to Postgres (`POSTGRES_HOST_DIRECT`, not PgBouncer).
2. The dump is **verified** with `pg_restore -l` (it must be readable and hold Django table
   data). A failed or empty dump is an error, not a backup: nothing is uploaded and the task
   fails loudly in the worker log.
3. Encrypted with AES-256-GCM using `DB_BACKUP_KEY`. The file is chunked and authenticated, so
   a wrong key, a damaged byte, a truncated or reordered file all fail to open rather than
   yielding a bad dump.
4. Uploaded to `DB_BACKUP_BUCKET` as `db-backups/vecto-<UTC timestamp>.dump.enc`.
5. Old backups are thinned by retention (below).

If `DB_BACKUP_BUCKET` or `DB_BACKUP_KEY` is unset the task logs a warning and does nothing.

## Retention

Grandfather-father-son: after each upload, the newest backup of each of the last N days, ISO
weeks and months **that have one** is kept, and everything else is deleted. Defaults:

| Setting | Default | Keeps |
|---|---|---|
| `DB_BACKUP_KEEP_DAILY` | 14 | every night for two weeks |
| `DB_BACKUP_KEEP_WEEKLY` | 13 | one per week for about three months |
| `DB_BACKUP_KEEP_MONTHLY` | 12 | one per month for a year |

That is roughly 35 backups at steady state. The newest backup is always kept, and a gap in
the schedule never shortens the history (it counts days/weeks/months that have a backup, not
calendar windows). Set a tier to `0` to switch it off.

## One-time setup

1. **Create a private R2 bucket** (e.g. `vecto-backups`). Do **not** attach a public custom
   domain or enable the `r2.dev` URL: a dump contains user emails and credentials. (The audio
   and media buckets are public, so don't reuse them.)
2. **Make sure the R2 API token covers it.** The backup reuses `R2_ENDPOINT`,
   `R2_ACCESS_KEY_ID` and `R2_SECRET_ACCESS_KEY`. A token scoped to specific buckets needs the
   new one added (Object Read & Write).
3. **Generate the key:** `python manage.py backup_database --generate-key`. Put it in `.env`
   as `DB_BACKUP_KEY` **and in your password manager.** A backup cannot be opened without the
   key, and if the server is lost the `.env` goes with it.
4. Set `DB_BACKUP_BUCKET` in `.env`, rebuild the image (it now ships the PostgreSQL 15 client
   tools), and restart the web, worker and beat containers.
5. Prove it: `python manage.py backup_database`, then `python manage.py backup_database --list`.
   Then do a test restore (below). A backup you have never restored is a hope, not a backup.
6. Optional: lifecycle rule on the bucket as a second line of retention.

The Admin Command Console lists both commands under Maintenance.

## Restoring

Never restore over the live database. Restore into a new one, check it, then swap on purpose.

    # decrypt + verify only (touches no database)
    python manage.py restore_database_backup --latest --out /tmp/vecto.dump

    # restore into a NEW database (preview first, then --apply). Needs a DB user with CREATEDB.
    python manage.py restore_database_backup --latest --into vecto_check
    python manage.py restore_database_backup --latest --into vecto_check --apply

A specific backup: `--key db-backups/vecto-2026-10-04-023000.dump.enc` (see `--list`).
`--into` refuses the live database name.

Check the result before relying on it: compare row counts (`pod_manager_episode`,
`auth_user`, `pod_manager_podcast`, ...) with production. To make it live, stop the app, rename
the databases (`ALTER DATABASE ... RENAME TO ...`), and start again.

A decrypted `.dump` is a normal PostgreSQL custom-format file, so you can also use plain tools
(`pg_restore -l`, `pg_restore -d`). Keep it version 15: restore with the `postgres:15` tools.

## Lost the key?

The encrypted backups cannot be recovered. Generate a new key, set it, and take a fresh
backup; the old ones are unreadable and will age out under the retention tiers.

## Not covered

- The R2 audio/media buckets themselves (they have their own reconcile/GC jobs).
- A copy outside Cloudflare. Backups share the R2 account with the production audio; for a
  truly separate copy, download one now and then (`--out`) and keep it elsewhere.

## Testing it without production

`scripts/test_db_backup_e2e.sh` runs the real pipeline in throwaway containers: Postgres 15,
an S3-compatible server standing in for R2, and the project image. It checks backup, the
encrypted object, retention, a wrong database password (nothing uploaded), restore into a new
database with matching row counts, refusal to restore over the live database, and a wrong key.

    docker build -t vecto-e2e .
    bash scripts/test_db_backup_e2e.sh vecto-e2e

SQLite (the IDE setup) is not supported by the backup and the scheduled task skips itself there:
`pg_dump` is PostgreSQL-specific, and a separate SQLite path would not test the real one.
