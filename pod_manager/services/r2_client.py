"""Cloudflare R2 client factory and key helpers.

Phase 1 of the audio-mirror feature (see planned_features.txt): a thin,
configured boto3 S3 client plus the few pure helpers every later phase needs to
build keys and resolve public URLs. NO upload / mirror logic lives here.

R2 is S3-compatible but has a couple of mandatory quirks baked in below:
  - region_name must be "auto".
  - endpoint_url must be the account R2 endpoint.
  - recent boto3 (>=1.36) sends default integrity checksums that some
    S3-compatible providers reject with opaque 400s; we force them to
    "when_required" so plain put/get/delete and multipart uploads stay clean.
"""

import logging

import boto3
from botocore.config import Config
from django.conf import settings

logger = logging.getLogger(__name__)


def _build_config() -> Config:
    """botocore Config with R2's required region + the checksum workaround.

    The checksum kwargs only exist on botocore >=1.36; older releases don't send
    the problematic checksums anyway, so we fall back to a plain config there.
    """
    base = dict(
        region_name="auto",
        retries={"max_attempts": 3, "mode": "standard"},
    )
    try:
        return Config(
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            **base,
        )
    except TypeError:
        # botocore predates the checksum-calculation knobs.
        return Config(**base)


def get_r2_client():
    """Return a configured boto3 S3 client pointed at Cloudflare R2.

    Raises RuntimeError if the R2 credentials/endpoint aren't configured so
    callers fail loudly instead of getting an anonymous client.
    """
    if not (settings.R2_ENDPOINT and settings.R2_ACCESS_KEY_ID and settings.R2_SECRET_ACCESS_KEY):
        raise RuntimeError(
            "R2 is not configured: set R2_ENDPOINT, R2_ACCESS_KEY_ID and "
            "R2_SECRET_ACCESS_KEY in the environment."
        )
    return boto3.client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        config=_build_config(),
    )


def prefixed_key(key: str) -> str:
    """Apply R2_KEY_PREFIX to a bare key.

    Prod prefix is ""; IDE/dev is "dev/" so dev objects are namespaced and
    bulk-purgeable. Idempotent if the prefix is already present.
    """
    prefix = settings.R2_KEY_PREFIX or ""
    if prefix and key.startswith(prefix):
        return key
    return f"{prefix}{key}"


def public_url(key: str) -> str:
    """Full public URL for an object key, served via the R2 custom domain."""
    return f"{settings.R2_PUBLIC_HOST}/{key.lstrip('/')}"


def key_from_public_url(url: str) -> str:
    """Inverse of public_url(): strip R2_PUBLIC_HOST to recover the object key.

    The orphan tracker and GC store/compare host-stripped keys, so this is the
    single place that knows how to turn a stored Episode.r2_url back into a key.
    Returns the input unchanged if it doesn't carry the configured host.
    """
    host = settings.R2_PUBLIC_HOST
    if host and url.startswith(host):
        return url[len(host):].lstrip("/")
    return url
