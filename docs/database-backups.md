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

Never restore over a live database that has data: `--into` refuses it. Restore into a new name, check it, then swap on purpose.

    # decrypt + verify only (touches no database)
    python manage.py restore_database_backup --latest --out /tmp/vecto.dump

    # restore into a NEW database (preview first, then --apply). Needs a DB user with CREATEDB.
    python manage.py restore_database_backup --latest --into vecto_check
    python manage.py restore_database_backup --latest --into vecto_check --apply

A specific backup: `--key db-backups/vecto-2026-10-04-023000.dump.enc` (see `--list`).
`--into` accepts the configured database name only while it is empty (a fresh server).

Check the result before relying on it: compare row counts (`pod_manager_episode`,
`auth_user`, `pod_manager_podcast`, ...) with production. To make it live, stop the app, rename
the databases (`ALTER DATABASE ... RENAME TO ...`), and start again.

A decrypted `.dump` is a normal PostgreSQL custom-format file, so you can also use plain tools
(`pg_restore -l`, `pg_restore -d`). Keep it version 15: restore with the `postgres:15` tools.

## Recovering onto a new server (disaster recovery)

The database is the only thing the backup holds, so a replacement server needs three
things: the **code/image**, the **`.env`**, and the **latest backup**. Audio, transcripts and
media live in R2 and are untouched by losing the server.

**Keep a copy of `.env` in your password manager, not only on the server.** The dump cannot
replace it. In particular:

| `.env` value | If it is lost |
|---|---|
| `DB_BACKUP_KEY` | the backups cannot be opened at all |
| `DJANGO_CRYPTOGRAPHY_KEY` | encrypted fields (e.g. Patreon tokens) decrypt to empty; users must reconnect |
| `R2_ENDPOINT` / `R2_ACCESS_KEY_ID` / `R2_SECRET_ACCESS_KEY` | the app cannot fetch the backup (create a new token in Cloudflare) |
| `POSTGRES_*`, `DJANGO_SECRET_KEY`, Patreon/Discord/Mailgun keys | regenerate or re-enter; a new `DJANGO_SECRET_KEY` only signs everyone out |

Steps:

1. Provision a host with Docker, check out the repository, restore `.env`, build the image.
2. Start **only Postgres**: `docker compose up -d db`. The compose file creates an empty
   database named `POSTGRES_DB`.
3. Restore into it. The configured database name is accepted here because it is empty:

        docker compose run --rm web python manage.py restore_database_backup --latest --into <POSTGRES_DB>
        docker compose run --rm web python manage.py restore_database_backup --latest --into <POSTGRES_DB> --apply

   It lists the newest backup, downloads and decrypts it, verifies it, and loads it. A
   database that already has tables is refused.
4. `docker compose run --rm web python manage.py migrate` (applies any migrations newer than
   the backup; a no-op if the backup is current).
5. `docker compose up -d`, then log in and check an episode page, the feed XML and the Celery
   beat schedule (Admin: Periodic tasks).

The Admin Command Console cannot do this restore: it needs a working database to log in. It is
for routine use: taking a backup now, listing backups, or a test restore into a scratch name.

Tested end to end by `scripts/test_db_backup_e2e.sh` (the "disaster recovery" section).

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
