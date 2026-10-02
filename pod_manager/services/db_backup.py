"""Encrypted PostgreSQL backups to a private R2 bucket, and the way back.

    task_backup_database (weekly)
        pg_dump -Fc  ->  verified readable (pg_restore -l)  ->  AES-256-GCM encrypted
        ->  uploaded to DB_BACKUP_BUCKET  ->  old backups pruned to DB_BACKUP_KEEP

A dump holds user emails and integration credentials, so it is ALWAYS encrypted before it
leaves the server, and the bucket must be a private one (never one fronted by a public
custom domain like the audio/media buckets). Without DB_BACKUP_KEY a backup is useless:
keep that key somewhere other than the server (see docs/database-backups.md).

File format (".dump.enc"): 12-byte header (b"VBK1" + 4 random nonce-prefix bytes + the chunk
size as uint32), then chunks of [uint32 length][AES-GCM ciphertext+tag]. The nonce is the
prefix + an 8-byte chunk counter, and the header plus a "final chunk" flag are authenticated
data, so reordering, truncating or editing any part makes decryption fail rather than
yielding a damaged dump. Chunked so a multi-gigabyte dump never has to fit in memory.
"""

import base64
import binascii
import logging
import os
import shutil
import struct
import subprocess
import tempfile
from datetime import datetime, timezone as dt_timezone
from pathlib import Path

from django.conf import settings

logger = logging.getLogger(__name__)

MAGIC = b"VBK1"
CHUNK = 1024 * 1024
_HEADER = struct.Struct(">4s4sI")
_LEN = struct.Struct(">I")
SUFFIX = ".dump.enc"


class BackupError(RuntimeError):
    """A backup/restore step failed in a way the operator needs to hear about."""


# --------------------------------------------------------------------------- key + crypto

def generate_key() -> str:
    """A fresh URL-safe base64 256-bit key, for DB_BACKUP_KEY."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode("ascii")


def load_key(value=None) -> bytes:
    raw = settings.DB_BACKUP_KEY if value is None else value
    if not raw:
        raise BackupError("DB_BACKUP_KEY is not set.")
    try:
        key = base64.urlsafe_b64decode(raw.strip().encode("ascii"))
    except (binascii.Error, ValueError, UnicodeEncodeError):
        raise BackupError("DB_BACKUP_KEY is not valid URL-safe base64.")
    if len(key) != 32:
        raise BackupError("DB_BACKUP_KEY must decode to exactly 32 bytes (generate one with --generate-key).")
    return key


def _nonce(prefix: bytes, counter: int) -> bytes:
    return prefix + counter.to_bytes(8, "big")


def encrypt_stream(src, dst, key: bytes, chunk_size: int = CHUNK) -> int:
    """Encrypt the binary file object ``src`` into ``dst``. Returns bytes written."""
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    aes = AESGCM(key)
    prefix = os.urandom(4)
    header = _HEADER.pack(MAGIC, prefix, chunk_size)
    dst.write(header)
    written = len(header)
    counter = 0
    block = src.read(chunk_size)
    while True:
        nxt = src.read(chunk_size)
        final = not nxt
        ct = aes.encrypt(_nonce(prefix, counter), block, header + (b"\x01" if final else b"\x00"))
        dst.write(_LEN.pack(len(ct)))
        dst.write(ct)
        written += _LEN.size + len(ct)
        if final:
            return written
        block, counter = nxt, counter + 1


def decrypt_stream(src, dst, key: bytes) -> int:
    """Decrypt ``src`` into ``dst``. Raises BackupError on a wrong key or any corruption,
    including a truncated file. Returns plaintext bytes written."""
    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    header = src.read(_HEADER.size)
    if len(header) != _HEADER.size:
        raise BackupError("Not a Vecto backup file (too short).")
    magic, prefix, chunk_size = _HEADER.unpack(header)
    if magic != MAGIC:
        raise BackupError("Not a Vecto backup file (bad header).")
    aes = AESGCM(key)
    counter = total = 0

    def read_len():
        raw = src.read(_LEN.size)
        if not raw:
            return None
        if len(raw) != _LEN.size:
            raise BackupError("Backup file is truncated.")
        return _LEN.unpack(raw)[0]

    length = read_len()
    if length is None:
        raise BackupError("Backup file is truncated (no data).")
    while length is not None:
        if length > chunk_size + 16:
            raise BackupError("Backup file is corrupt (oversized chunk).")
        ct = src.read(length)
        if len(ct) != length:
            raise BackupError("Backup file is truncated.")
        next_length = read_len()
        final = next_length is None
        try:
            pt = aes.decrypt(_nonce(prefix, counter), ct, header + (b"\x01" if final else b"\x00"))
        except InvalidTag:
            raise BackupError("Decryption failed: wrong DB_BACKUP_KEY, or the file is corrupt or truncated.")
        dst.write(pt)
        total += len(pt)
        counter += 1
        length = next_length
    return total


# --------------------------------------------------------------------------- pg_dump / pg_restore

def _pg_conn():
    """Connection settings for the dump: the DATABASES credentials, but straight to Postgres
    (POSTGRES_HOST_DIRECT) rather than through PgBouncer, which is not safe for pg_dump."""
    db = settings.DATABASES["default"]
    return {
        "host": os.getenv("POSTGRES_HOST_DIRECT", "db"),
        "port": os.getenv("POSTGRES_PORT_DIRECT", "5432"),
        "user": db.get("USER") or "",
        "password": db.get("PASSWORD") or "",
        "name": db.get("NAME") or "",
    }


def _require_tool(name: str) -> str:
    path = shutil.which(name)
    if not path:
        raise BackupError(f"{name} is not installed in this container (rebuild the image).")
    return path


def _run(cmd, env=None, timeout=3600):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise BackupError(f"{Path(cmd[0]).name} timed out after {timeout}s.")


def verify_dump(path) -> int:
    """Raise BackupError unless ``path`` is a readable custom-format dump that holds table
    data. Returns the number of table-data entries. This is what catches the failures that
    still leave a file behind (wrong credentials, a dump that died half way)."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        raise BackupError("The dump is empty.")
    result = _run([_require_tool("pg_restore"), "-l", str(path)], timeout=600)
    if result.returncode != 0:
        raise BackupError(f"pg_restore cannot read the dump: {result.stderr.strip()[:300]}")
    data_entries = [ln for ln in result.stdout.splitlines() if " TABLE DATA " in ln]
    if not data_entries or not any("django_migrations" in ln for ln in data_entries):
        raise BackupError("The dump has no Django table data; refusing to treat it as a backup.")
    return len(data_entries)


def run_pg_dump(dest) -> Path:
    conn = _pg_conn()
    env = {**os.environ, "PGPASSWORD": conn["password"]}
    cmd = [_require_tool("pg_dump"), "-h", conn["host"], "-p", str(conn["port"]), "-U", conn["user"],
           "-d", conn["name"], "-Fc", "-f", str(dest)]
    result = _run(cmd, env=env, timeout=3600)
    if result.returncode != 0:
        raise BackupError(f"pg_dump failed: {result.stderr.strip()[:500]}")
    return Path(dest)


# --------------------------------------------------------------------------- R2

def is_configured() -> tuple:
    """(ok, reason). Backups need a bucket and a key; the dump itself needs Postgres."""
    if "postgresql" not in settings.DATABASES["default"]["ENGINE"]:
        return False, "the database is not PostgreSQL"
    if not settings.DB_BACKUP_BUCKET:
        return False, "DB_BACKUP_BUCKET is not set"
    if not settings.DB_BACKUP_KEY:
        return False, "DB_BACKUP_KEY is not set"
    return True, ""


def _client():
    from pod_manager.services.r2_client import get_r2_client
    return get_r2_client()


def _prefix() -> str:
    p = (settings.DB_BACKUP_PREFIX or "").strip("/")
    return f"{p}/" if p else ""


def list_backups():
    """Backups in the bucket, newest first: [{key, size, modified}]."""
    client = _client()
    out, token = [], None
    while True:
        kwargs = {"Bucket": settings.DB_BACKUP_BUCKET, "Prefix": _prefix()}
        if token:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(SUFFIX):
                out.append({"key": obj["Key"], "size": obj["Size"], "modified": obj["LastModified"]})
        if not page.get("IsTruncated"):
            break
        token = page.get("NextContinuationToken")
    return sorted(out, key=lambda b: b["key"], reverse=True)


def prune_backups(keep=None) -> list:
    """Delete all but the newest ``keep`` backups. Returns the deleted keys."""
    keep = settings.DB_BACKUP_KEEP if keep is None else keep
    if keep < 1:
        raise BackupError("DB_BACKUP_KEEP must be at least 1.")
    stale = list_backups()[keep:]
    client = _client()
    for b in stale:
        client.delete_object(Bucket=settings.DB_BACKUP_BUCKET, Key=b["key"])
    return [b["key"] for b in stale]


def run_backup() -> dict:
    """The whole pipeline. Raises BackupError (nothing is uploaded) if any step fails;
    returns a summary dict, or {'skipped': reason} when backups aren't configured."""
    ok, reason = is_configured()
    if not ok:
        logger.warning("Database backup skipped: %s.", reason)
        return {"skipped": reason}
    key = load_key()
    stamp = datetime.now(dt_timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    object_key = f"{_prefix()}vecto-{stamp}{SUFFIX}"
    with tempfile.TemporaryDirectory(prefix="vecto-backup-") as tmp:
        dump = run_pg_dump(Path(tmp) / "vecto.dump")
        tables = verify_dump(dump)
        enc = Path(tmp) / "vecto.dump.enc"
        with open(dump, "rb") as src, open(enc, "wb") as dst:
            encrypt_stream(src, dst, key)
        plain_size, enc_size = dump.stat().st_size, enc.stat().st_size
        _client().upload_file(str(enc), settings.DB_BACKUP_BUCKET, object_key)
    pruned = prune_backups()
    logger.info("Database backup uploaded: %s (%d bytes encrypted, %d tables, pruned %d).",
                object_key, enc_size, tables, len(pruned))
    return {"key": object_key, "bytes": enc_size, "dump_bytes": plain_size, "tables": tables, "pruned": pruned}


def download_backup(object_key: str, dest) -> Path:
    """Fetch ``object_key`` and decrypt it to a plain pg_dump file at ``dest``, verified
    readable before returning. A wrong key fails here, before anything is restored."""
    key = load_key()
    with tempfile.TemporaryDirectory(prefix="vecto-restore-") as tmp:
        enc = Path(tmp) / "download.enc"
        _client().download_file(settings.DB_BACKUP_BUCKET, object_key, str(enc))
        dest = Path(dest)
        with open(enc, "rb") as src, open(dest, "wb") as dst:
            decrypt_stream(src, dst, key)
    verify_dump(dest)
    return dest


def restore_into(dump_path, target_db: str) -> None:
    """Create ``target_db`` and load the dump into it. Never the live database: restoring
    over production would be an outage, so that is a hard refusal."""
    conn = _pg_conn()
    if target_db == conn["name"]:
        raise BackupError(f"Refusing to restore over the live database '{conn['name']}'. Restore into a new name, "
                          "check it, then swap deliberately.")
    env = {**os.environ, "PGPASSWORD": conn["password"]}
    base = ["-h", conn["host"], "-p", str(conn["port"]), "-U", conn["user"]]
    created = _run([_require_tool("createdb"), *base, target_db], env=env, timeout=120)
    if created.returncode != 0:
        raise BackupError(f"createdb failed (the user needs CREATEDB): {created.stderr.strip()[:300]}")
    restored = _run([_require_tool("pg_restore"), *base, "-d", target_db, "--no-owner", "--exit-on-error",
                     str(dump_path)], env=env, timeout=3600)
    if restored.returncode != 0:
        raise BackupError(f"pg_restore failed: {restored.stderr.strip()[:500]}")
