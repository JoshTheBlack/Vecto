"""Cloudflare edge-cache purge client (planned_features.txt Section E4).

Deleting an R2 object at origin does NOT evict the year-long immutable edge
cache, so the transcript rekey churn purges every old CDN URL after deleting
the origin object. Cloudflare's cache key includes the query string, so callers
must pass every variant that was ever emitted (the bare URL plus ?v=1..N).

Failure semantics: purge_urls returns True only when EVERY batch succeeded.
Callers treat False as "keep the orphan row" — the row is the retry ledger, and
it is only cleared after delete AND purge both succeed. Unconfigured settings
fail closed the same way (a silent no-op purge would clear rows while the edge
kept serving the bytes).
"""

import logging

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

# Cloudflare's purge_cache endpoint accepts at most 30 URLs per call.
PURGE_BATCH = 30

_TIMEOUT = 15


def cloudflare_purge_configured() -> bool:
    return bool(settings.CLOUDFLARE_ZONE_ID and settings.CLOUDFLARE_PURGE_TOKEN)


def purge_urls(urls) -> bool:
    """Purge the given URLs from the Cloudflare edge cache, 30 per API call.

    Returns True only if every batch succeeded (empty input is trivially True).
    Unconfigured (no zone id / token) fails closed: logs and returns False so
    callers retain their retry ledger instead of assuming the edge was evicted.
    """
    urls = list(urls)
    if not urls:
        return True
    if not cloudflare_purge_configured():
        logger.warning(
            "cloudflare purge skipped — CLOUDFLARE_ZONE_ID/CLOUDFLARE_PURGE_TOKEN "
            "unset; %d url(s) NOT purged", len(urls),
        )
        return False

    endpoint = (
        f"https://api.cloudflare.com/client/v4/zones/"
        f"{settings.CLOUDFLARE_ZONE_ID}/purge_cache"
    )
    headers = {"Authorization": f"Bearer {settings.CLOUDFLARE_PURGE_TOKEN}"}
    all_ok = True
    for i in range(0, len(urls), PURGE_BATCH):
        chunk = urls[i:i + PURGE_BATCH]
        try:
            resp = requests.post(
                endpoint, json={"files": chunk}, headers=headers, timeout=_TIMEOUT,
            )
            ok = resp.status_code == 200 and resp.json().get("success") is True
            if not ok:
                logger.warning(
                    "cloudflare purge batch failed: status=%s body=%.500s",
                    resp.status_code, resp.text,
                )
        except Exception as exc:
            logger.warning("cloudflare purge batch errored: %s", exc)
            ok = False
        all_ok = all_ok and ok
    return all_ok
