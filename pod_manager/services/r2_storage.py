"""Cloudflare R2 storage backend for user assets (vecto-cdn).

See planned_features.txt (USER ASSETS TO CLOUDFLARE R2). A django-storages S3
backend pointed at the vecto-cdn bucket and served via cdn.joshtheblack.com,
attached PER FIELD to the uploaded avatar + mix-cover ImageFields. It is NEVER
wired up as the global `default` storage — invoices/PII and recovery runs must
stay on local disk.

Stable keys + a DB-side version int (?v=N, appended by the model URL helpers)
handle cache-busting, so objects are OVERWRITTEN in place: no content hashing,
no orphan table, no GC (unlike the audio mirror's content-addressed keys).
"""

import logging
from urllib.parse import urlparse

from django.conf import settings
from storages.backends.s3boto3 import S3Boto3Storage

from pod_manager.services.r2_client import _build_config, get_r2_client

logger = logging.getLogger(__name__)

# Immutable cache on every media object; ?v=N (built by callers) is the bust.
MEDIA_CACHE_CONTROL = "public, max-age=31536000, immutable"


def media_object_key(key: str) -> str:
    """Apply R2_MEDIA_KEY_PREFIX to a bare media key.

    Prod prefix is ""; IDE/dev is "dev/" so dev objects are namespaced and
    bulk-purgeable. Mirrors r2_client.prefixed_key for the media bucket, and
    matches the django-storages `location` the image fields use.
    """
    prefix = settings.R2_MEDIA_KEY_PREFIX or ""
    if prefix and key.startswith(prefix):
        return key
    return f"{prefix}{key}"


def media_public_url(key: str) -> str:
    """Public cdn URL for a bare media key (prefix applied)."""
    return f"{settings.R2_MEDIA_PUBLIC_HOST}/{media_object_key(key).lstrip('/')}"


def put_media_object(key: str, body, content_type: str, *, cache_control: str = MEDIA_CACHE_CONTROL) -> None:
    """PUT a non-FileField object (e.g. a transcript) to the vecto-cdn bucket.

    Uses the account R2 client directly (like the audio mirror) so Content-Type
    is set explicitly per object — R2 has no bucket-level default, and the
    transcript extensions (.words, .vtt, .srt) don't all map cleanly via
    mimetypes. file_overwrite is implicit: PUT replaces the stable key in place.
    """
    get_r2_client().put_object(
        Bucket=settings.R2_MEDIA_BUCKET,
        Key=media_object_key(key),
        Body=body,
        ContentType=content_type,
        CacheControl=cache_control,
    )


def get_media_object(key: str) -> tuple[bytes, str | None]:
    """GET a media object's bytes + Content-Type. Raises ClientError on miss."""
    obj = get_r2_client().get_object(Bucket=settings.R2_MEDIA_BUCKET, Key=media_object_key(key))
    return obj["Body"].read(), obj.get("ContentType")


def delete_media_object(key: str) -> None:
    """DELETE a media object (free op in R2). No error if already absent."""
    get_r2_client().delete_object(Bucket=settings.R2_MEDIA_BUCKET, Key=media_object_key(key))


def media_object_exists(key: str) -> bool:
    """True if the media object is present in R2 (a HEAD). Used by backfill/verify."""
    from botocore.exceptions import ClientError
    try:
        get_r2_client().head_object(Bucket=settings.R2_MEDIA_BUCKET, Key=media_object_key(key))
        return True
    except ClientError:
        return False


class R2MediaStorage(S3Boto3Storage):
    """S3 backend for the vecto-cdn bucket (public, edge-cached).

    file_overwrite=True is the crux of the no-GC design: re-uploading the same
    stable key replaces the object in place, so nothing is ever stranded.
    querystring_auth=False keeps URLs clean + public (the objects are public;
    the ?v=N cache-bust is appended by the model URL helpers, not boto signing).
    """
    bucket_name = settings.R2_MEDIA_BUCKET
    endpoint_url = settings.R2_ENDPOINT
    access_key = settings.R2_ACCESS_KEY_ID
    secret_key = settings.R2_SECRET_ACCESS_KEY
    region_name = "auto"
    # custom_domain wants a bare host (no scheme); R2_MEDIA_PUBLIC_HOST carries one.
    custom_domain = urlparse(settings.R2_MEDIA_PUBLIC_HOST).netloc or None
    # django-storages `location` is the key prefix ("dev/" in IDE, "" in prod).
    location = (settings.R2_MEDIA_KEY_PREFIX or "").rstrip("/")
    file_overwrite = True
    querystring_auth = False
    default_acl = None
    # Long-lived immutable cache on every object: the key is stable and the
    # ?v=N query string (appended by the model URL helpers) is what busts the
    # cache on re-upload, so the bytes themselves can be treated as immutable.
    # Required for edge caching — the Cloudflare Cache Rule bypasses cache when
    # an object carries no Cache-Control header.
    object_parameters = {"CacheControl": "public, max-age=31536000, immutable"}
    # Same checksum workaround r2_client uses, or R2 can 400 the PUT.
    client_config = _build_config()


# Module-level singleton: instantiating is cheap and does NOT open a connection
# (boto3 client is built lazily on first I/O), so this is safe even when the
# bucket/creds aren't configured yet.
r2_media_storage = R2MediaStorage()


def select_media_storage():
    """Per-field ``storage=`` callable for the R2-backed image fields.

    Returns the R2 backend when R2_MEDIA_ENABLED, else the local OverwriteStorage
    (``mix_storage``) — so the app (and the test suite, where the flag is forced
    off) keeps working without R2, and a dev re-upload OVERWRITES the stable key
    just like R2's file_overwrite does in prod. Django records the callable by
    reference in migrations, so the backend choice is never baked into a
    migration file.
    """
    if settings.R2_MEDIA_ENABLED:
        return r2_media_storage
    # Local import: models.py imports this callable at class-definition time, so
    # a module-level `from pod_manager.models import ...` would be circular. By
    # the time a field is constructed, mix_storage is already defined.
    from pod_manager.models import mix_storage
    return mix_storage
