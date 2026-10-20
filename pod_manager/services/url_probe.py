"""Cheap, read-only reachability checks for remote audio URLs.

A probe never downloads the file: it sends a HEAD, and only when the host refuses
HEAD outright (some CDNs answer 403/405/501) confirms with a single-byte ranged
GET so the real status is still learned.
"""

from dataclasses import dataclass

import requests

from pod_manager.utils import validate_public_url

# Statuses some hosts return for HEAD alone while serving GET normally.
_HEAD_REFUSED = frozenset({403, 405, 501})


@dataclass(frozen=True)
class ProbeResult:
    status: int | None = None   # final HTTP status; None when no response arrived
    content_type: str = ''
    error: str = ''              # why there is no status (unsafe URL, network failure)

    @property
    def reachable(self) -> bool:
        """The host answered with a success or redirect status."""
        return self.status is not None and 200 <= self.status < 400

    @property
    def serves_audio(self) -> bool:
        """Reachable and not an HTML page. GDrive and Patreon answer dead or
        quota-limited files with a 200 HTML interstitial, which must not count."""
        return self.reachable and not self.content_type.lower().startswith('text/html')


def probe_url(url: str, timeout: float = 30) -> ProbeResult:
    """HEAD ``url`` (following redirects) and report what came back."""
    ok, reason = validate_public_url(url)
    if not ok:
        return ProbeResult(error=f'unsafe URL: {reason}')
    try:
        resp = requests.head(url, timeout=timeout, allow_redirects=True)
        if resp.status_code in _HEAD_REFUSED:
            resp = requests.get(url, timeout=timeout, stream=True,
                                headers={'Range': 'bytes=0-0'}, allow_redirects=True)
            resp.close()
    except requests.RequestException as exc:
        return ProbeResult(error=str(exc))
    return ProbeResult(status=resp.status_code,
                       content_type=resp.headers.get('Content-Type', ''))
