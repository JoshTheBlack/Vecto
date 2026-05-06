"""
Shared utility helpers used across views, tasks, and ingesters.

Right now the module is intentionally small — just the security primitives
that need to be called from more than one entry point (views + ingesters).
Don't dump unrelated helpers in here; if a function only has one caller, it
should live next to that caller.
"""
import functools
import ipaddress
import logging
import socket
import time
import urllib.parse

import nh3

_diag_logger = logging.getLogger('pod_manager.diagnostic')


def diagnostic_timer(label: str):
    """Decorator: logs *label*, runs the function, logs elapsed time."""
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            t = time.time()
            _diag_logger.info(f"[DIAGNOSTIC] {label}...")
            result = fn(*args, **kwargs)
            _diag_logger.info(f"[DIAGNOSTIC] {label} done in {time.time() - t:.2f}s")
            return result
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# SSRF guard
# ---------------------------------------------------------------------------
# Any URL we feed to `requests.get` from non-trusted input must resolve to a
# public, routable address. Blocks loopback, link-local, private, multicast,
# and reserved ranges (covers cloud metadata services, internal Redis, etc).

_ALLOWED_URL_SCHEMES = ('http', 'https')


def validate_public_url(url: str) -> tuple[bool, str]:
    """Return (ok, reason). reason is empty when ok=True."""
    if not url or not isinstance(url, str):
        return False, "URL is empty."
    parsed = urllib.parse.urlsplit(url.strip())
    if parsed.scheme.lower() not in _ALLOWED_URL_SCHEMES:
        return False, f"Scheme '{parsed.scheme}' not allowed."
    host = parsed.hostname
    if not host:
        return False, "URL has no host."
    try:
        # Resolve all addresses; reject if ANY resolution lands in a reserved
        # range (defends against DNS rebinding to a public-then-private answer).
        infos = socket.getaddrinfo(host, None)
    except socket.gaierror:
        return False, "Host could not be resolved."
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            return False, f"Resolved to invalid address: {addr}"
        if (ip.is_private or ip.is_loopback or ip.is_link_local or
                ip.is_reserved or ip.is_multicast or ip.is_unspecified):
            return False, f"Host resolves to a non-public address ({addr})."
    return True, ""


# ---------------------------------------------------------------------------
# HTML sanitizer for user- and feed-supplied content
# ---------------------------------------------------------------------------
# Strips scripts, event handlers, and any tag/attribute not on the allowlist.
# Used on submit (user edits) AND on ingest (RSS publisher content) so the DB
# stays clean and the downstream `|safe` template usages remain correct.

_ALLOWED_DESC_TAGS = {
    'p', 'br', 'em', 'strong', 'b', 'i', 'u', 's', 'a',
    'ul', 'ol', 'li', 'blockquote', 'code', 'pre',
    'h2', 'h3', 'h4', 'h5', 'h6', 'span', 'div', 'hr', 'img',
}
_ALLOWED_DESC_ATTRS = {
    'a': {'href', 'title', 'target'},
    'img': {'src', 'alt', 'title'},
}


def sanitize_user_html(html_str: str) -> str:
    if not html_str:
        return ''
    return nh3.clean(
        html_str,
        tags=_ALLOWED_DESC_TAGS,
        attributes=_ALLOWED_DESC_ATTRS,
        link_rel='noopener noreferrer nofollow',
    )


# ---------------------------------------------------------------------------
# Request-context shortcuts
# ---------------------------------------------------------------------------

def get_membership(request):
    """Return the NetworkMembership for (request.user, request.network), or None."""
    from .models import NetworkMembership
    return NetworkMembership.objects.filter(user=request.user, network=request.network).first()
