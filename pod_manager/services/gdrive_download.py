"""Follow Google Drive's "couldn't scan this file for viruses" confirmation.

Drive serves files over ~100 MB from behind an HTML interstitial — an HTTP **200**
warning page, not an error — instead of the bytes. Getting the real file needs a
confirm token:

  * **Modern Drive** embeds it in a ``<form id="download-form">`` that posts to
    ``drive.usercontent.google.com/download`` with hidden ``confirm`` + ``uuid``
    inputs.
  * **Older Drive** sets a ``download_warning*`` cookie whose value is the token,
    expecting it appended as ``&confirm=<token>``.

This module detects that page and re-requests with the confirmation so the actual
audio streams back. Dependency-light (requests + re) so both the transcription
downloader and the R2 mirror can share it without circular imports.
"""

import logging
import re
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# A browser-ish UA — Drive can vary its response for unknown clients.
USER_AGENT = "Mozilla/5.0 (compatible; VectoBot/1.0; +https://baldmove.com)"

_FORM_RE = re.compile(r'id="download-form"\s+action="([^"]+)"', re.IGNORECASE)
_INPUT_RE = re.compile(r'<input[^>]*\bname="([^"]+)"[^>]*\bvalue="([^"]*)"', re.IGNORECASE)
_CONFIRM_RE = re.compile(r'confirm=([0-9A-Za-z_\-]+)')


def is_gdrive_url(url: str) -> bool:
    """True for any Google Drive / Docs host (the only place this page appears)."""
    host = urlparse(url or '').netloc.lower()
    return host.endswith('google.com') and ('drive' in host or 'docs' in host or 'usercontent' in host)


def _is_google_host(url: str) -> bool:
    return urlparse(url or '').netloc.lower().endswith('google.com')


def looks_like_interstitial(resp) -> bool:
    """True if a 200 response is an HTML page (the virus-scan warning) rather
    than file bytes — i.e. worth trying to click through."""
    ctype = (resp.headers.get('Content-Type') or '').split(';')[0].strip().lower()
    return ctype in ('text/html', 'application/xhtml+xml')


def follow_confirmation(session, original_url, resp, timeout):
    """Given a first response that is a Drive interstitial, return a NEW streaming
    Response for the actual file.

    Returns ``resp`` unchanged if it can't resolve (the caller then detects the
    non-audio body and fails normally). Only ever follows ``*.google.com`` targets
    so a crafted page can't redirect the fetch elsewhere (SSRF).
    """
    try:
        html = resp.text
    except Exception:
        return resp

    # Modern flow: a <form id="download-form"> carrying the confirm token + uuid.
    m = _FORM_RE.search(html)
    if m:
        action = m.group(1).replace('&amp;', '&')
        params = {k: v for k, v in _INPUT_RE.findall(html)}
        if _is_google_host(action):
            logger.info("gdrive: following download-form confirmation -> %s", action)
            return session.get(action, params=params, stream=True, timeout=timeout)

    # Older flow: a download_warning* cookie (or an inline confirm=) holds the token.
    token = None
    for name, value in session.cookies.items():
        if name.startswith('download_warning'):
            token = value
            break
    if not token:
        m = _CONFIRM_RE.search(html)
        if m:
            token = m.group(1)
    if token and _is_google_host(original_url):
        sep = '&' if '?' in original_url else '?'
        logger.info("gdrive: following confirm token for %s", original_url)
        return session.get(f"{original_url}{sep}confirm={token}", stream=True, timeout=timeout)

    return resp
