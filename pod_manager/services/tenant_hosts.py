import logging

from django.core.cache import cache

logger = logging.getLogger(__name__)

_CACHE_KEY = 'tenant_custom_domains'
_CACHE_SECONDS = 60


def live_tenant_domains():
    """Every Network.custom_domain value, cached for _CACHE_SECONDS so
    NetworkMiddleware doesn't hit the DB just to learn "is this host even
    one of ours" on every single request. A domain added less than
    _CACHE_SECONDS ago may briefly read as unknown -- self-heals on the next
    refresh, never wrong beyond that window.

    A DB error returns an empty list WITHOUT caching it, so the next request
    retries rather than locking in a 60s outage of "no host is ever ours".
    """
    domains = cache.get(_CACHE_KEY)
    if domains is not None:
        return domains
    try:
        from ..models import Network
        domains = list(
            Network.objects.exclude(custom_domain__isnull=True)
            .exclude(custom_domain='')
            .values_list('custom_domain', flat=True)
        )
    except Exception:
        logger.error("Failed to refresh tenant domain cache", exc_info=True)
        return []
    cache.set(_CACHE_KEY, domains, _CACHE_SECONDS)
    return domains
