#!/usr/bin/env bash
# End-to-end test of the encrypted database backup against REAL Postgres 15 and an
# S3-compatible store (moto standing in for R2), using the project's own image.
# Everything runs in throwaway containers on a private Docker network: no production
# access, no touching your dev database.
#
#     docker build -t vecto-e2e .
#     bash scripts/test_db_backup_e2e.sh vecto-e2e
#
# Proves: migrate -> backup (real pg_dump, verify, encrypt, upload) -> prune -> download +
# decrypt -> restore into a NEW database with matching row counts; and that a wrong DB
# password uploads nothing, a wrong key cannot open a backup, and the live DB is refused.
set -u
IMAGE="${1:-vecto-e2e}"
NET="vbk-e2e-$$"
export MSYS_NO_PATHCONV=1

pass=0; fail=0
ok()   { echo "  PASS  $1"; pass=$((pass+1)); }
bad()  { echo "  FAIL  $1"; fail=$((fail+1)); }
check() { if eval "$2"; then ok "$1"; else bad "$1"; fi; }

cleanup() { docker rm -f "$NET-db" "$NET-minio" "$NET-redis" >/dev/null 2>&1; docker network rm "$NET" >/dev/null 2>&1; }
trap cleanup EXIT

docker network create "$NET" >/dev/null
docker run -d --name "$NET-db" --network "$NET" --network-alias db \
    -e POSTGRES_USER=vecto_user -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=vecto postgres:15 >/dev/null
# moto's S3 server stands in for R2 (same S3 API; installed from pip so no registry login is needed)
docker run -d --name "$NET-minio" --network "$NET" --network-alias minio python:3.12-slim \
    sh -c "pip install -q 'moto[server]' && moto_server -H 0.0.0.0 -p 9000" >/dev/null
docker run -d --name "$NET-redis" --network "$NET" --network-alias redis redis:7 >/dev/null
until docker exec "$NET-db" pg_isready -U vecto_user -d vecto >/dev/null 2>&1; do sleep 1; done
until docker run --rm --network "$NET" "$IMAGE" python -c "import urllib.request;urllib.request.urlopen('http://minio:9000/moto-api/')" >/dev/null 2>&1; do sleep 3; done

KEY=$(docker run --rm "$IMAGE" python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
WRONG_KEY=$(docker run --rm "$IMAGE" python -c "import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())")

# run <image-env overrides...> -- <command...>
app() {
    docker run --rm --network "$NET" \
        -e DEBUG=False -e DJANGO_SECRET_KEY=e2e-not-a-secret -e ALLOWED_HOSTS='*' \
        -e POSTGRES_DB=vecto -e POSTGRES_USER=vecto_user -e POSTGRES_PASSWORD=pw \
        -e POSTGRES_HOST=db -e POSTGRES_PORT=5432 -e POSTGRES_HOST_DIRECT=db \
        -e REDIS_URL=redis://redis:6379/0 \
        -e R2_ENDPOINT=http://minio:9000 -e R2_ACCESS_KEY_ID=minioadmin -e R2_SECRET_ACCESS_KEY=minioadmin123 \
        -e R2_MIRROR_ENABLED=False -e R2_MEDIA_ENABLED=False \
        -e DB_BACKUP_BUCKET=vecto-backups -e DB_BACKUP_KEY="$KEY" -e DB_BACKUP_KEEP_DAILY=1 -e DB_BACKUP_KEEP_WEEKLY=0 -e DB_BACKUP_KEEP_MONTHLY=0 \
        "$@"
}
count() { docker exec "$NET-db" psql -U vecto_user -d "$1" -At -c "$2" 2>/dev/null; }

echo "== set up: real schema, some data, an empty private bucket"
app "$IMAGE" python manage.py migrate --noinput >/dev/null 2>&1 || { echo "migrate failed"; app "$IMAGE" python manage.py migrate --noinput 2>&1 | tail -5; exit 1; }
app "$IMAGE" python manage.py shell -c "
from django.contrib.auth.models import User
for i in range(5): User.objects.create_user(f'e2e{i}', f'e2e{i}@example.test', 'pw')
" >/dev/null 2>&1
app "$IMAGE" python -c "
import boto3, os
c = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123', region_name='us-east-1')
c.create_bucket(Bucket='vecto-backups')
" >/dev/null 2>&1
USERS=$(count vecto "select count(*) from auth_user"); MIGS=$(count vecto "select count(*) from django_migrations")
check "source database has data ($USERS users, $MIGS migrations)" '[ "${USERS:-0}" -ge 5 ]'

echo "== backup"
OUT=$(app "$IMAGE" python manage.py backup_database 2>&1); echo "$OUT" | tail -2
check "backup_database reports an upload" 'echo "$OUT" | grep -q "Uploaded db-backups/vecto-"'
LIST=$(app "$IMAGE" python manage.py backup_database --list 2>&1)
check "the backup is listed in the bucket" 'echo "$LIST" | grep -q "1 backup(s)"'

echo "== the stored object is encrypted, not a readable dump"
app "$IMAGE" python -c "
import boto3
c = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin123', region_name='us-east-1')
k = c.list_objects_v2(Bucket='vecto-backups', Prefix='db-backups/')['Contents'][0]['Key']
body = c.get_object(Bucket='vecto-backups', Key=k)['Body'].read()
assert body[:4] == b'VBK1', 'bad header'
assert b'PGDMP' not in body[:64] and b'e2e0@example.test' not in body, 'plaintext leaked'
print('encrypted ok', len(body))
" 2>&1 | tail -1 | grep -q "encrypted ok"; check "object starts with the VBK1 header and contains no plaintext" '[ $? -eq 0 ]'

echo "== retention (keep=1)"
sleep 1
app "$IMAGE" python manage.py backup_database >/dev/null 2>&1
LIST=$(app "$IMAGE" python manage.py backup_database --list 2>&1)
check "a second backup prunes down to one" 'echo "$LIST" | grep -q "1 backup(s)"'

echo "== failure: wrong database password uploads nothing"
BEFORE=$(app "$IMAGE" python manage.py backup_database --list 2>&1 | grep -c "dump.enc")
docker run --rm --network "$NET" -e DEBUG=False -e DJANGO_SECRET_KEY=x -e ALLOWED_HOSTS='*' \
    -e POSTGRES_DB=vecto -e POSTGRES_USER=vecto_user -e POSTGRES_PASSWORD=WRONG \
    -e POSTGRES_HOST=db -e POSTGRES_PORT=5432 -e POSTGRES_HOST_DIRECT=db -e REDIS_URL=redis://redis:6379/0 \
    -e R2_ENDPOINT=http://minio:9000 -e R2_ACCESS_KEY_ID=minioadmin -e R2_SECRET_ACCESS_KEY=minioadmin123 \
    -e DB_BACKUP_BUCKET=vecto-backups -e DB_BACKUP_KEY="$KEY" "$IMAGE" python manage.py backup_database >/tmp/vbk-wrongpw.txt 2>&1
RC=$?
check "wrong password makes the command fail (exit $RC)" '[ "$RC" -ne 0 ]'
AFTER=$(app "$IMAGE" python manage.py backup_database --list 2>&1 | grep -c "dump.enc")
check "and nothing new was uploaded ($BEFORE -> $AFTER)" '[ "$BEFORE" = "$AFTER" ]'

echo "== restore into a NEW database"
PREVIEW=$(app "$IMAGE" python manage.py restore_database_backup --latest --into vecto_check 2>&1)
check "preview does not create anything" 'echo "$PREVIEW" | grep -q "Would download" && [ -z "$(count postgres "select 1 from pg_database where datname=\$\$vecto_check\$\$")" ]'
APPLY=$(app "$IMAGE" python manage.py restore_database_backup --latest --into vecto_check --apply 2>&1); echo "$APPLY" | tail -2
R_USERS=$(count vecto_check "select count(*) from auth_user"); R_MIGS=$(count vecto_check "select count(*) from django_migrations")
check "restored auth_user count matches ($R_USERS = $USERS)" '[ "$R_USERS" = "$USERS" ]'
check "restored django_migrations count matches ($R_MIGS = $MIGS)" '[ "$R_MIGS" = "$MIGS" ]'
check "restored tables match" '[ "$(count vecto_check "select count(*) from information_schema.tables where table_schema=\$\$public\$\$")" = "$(count vecto "select count(*) from information_schema.tables where table_schema=\$\$public\$\$")" ]'

echo "== safety"
LIVE=$(app "$IMAGE" python manage.py restore_database_backup --latest --into vecto --apply 2>&1)
check "refuses to restore over the live database" 'echo "$LIVE" | grep -q "Refusing to restore over the live database"'
WRONG=$(docker run --rm --network "$NET" -e DEBUG=False -e DJANGO_SECRET_KEY=x -e ALLOWED_HOSTS='*' \
    -e POSTGRES_DB=vecto -e POSTGRES_USER=vecto_user -e POSTGRES_PASSWORD=pw -e POSTGRES_HOST=db -e POSTGRES_PORT=5432 \
    -e POSTGRES_HOST_DIRECT=db -e REDIS_URL=redis://redis:6379/0 \
    -e R2_ENDPOINT=http://minio:9000 -e R2_ACCESS_KEY_ID=minioadmin -e R2_SECRET_ACCESS_KEY=minioadmin123 \
    -e DB_BACKUP_BUCKET=vecto-backups -e DB_BACKUP_KEY="$WRONG_KEY" "$IMAGE" \
    python manage.py restore_database_backup --latest --out /tmp/x.dump 2>&1)
check "a wrong key cannot open the backup" 'echo "$WRONG" | grep -q "Decryption failed"'

echo
echo "$pass passed, $fail failed"
[ "$fail" -eq 0 ]
