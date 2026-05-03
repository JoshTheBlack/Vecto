"""
Security helpers shared across views and services: signed OAuth state,
rate limiting, and OTP attempt accounting.
"""
from django.core.cache import cache
from django.core.signing import TimestampSigner, SignatureExpired, BadSignature


# Signer for OAuth `state` so we can put a value (e.g. network_id) in the redirect
# without letting attackers forge it. Salt scopes the signature so a state for
# patreon_oauth can't be reused as a generic CSRF token elsewhere.
_OAUTH_STATE_SIGNER = TimestampSigner(salt='patreon-oauth-state')


def _sign_oauth_state(payload: str) -> str:
    return _OAUTH_STATE_SIGNER.sign(payload)


def _unsign_oauth_state(signed_value: str, max_age_seconds: int = 600):
    """Return the original payload, or None if the state is missing/forged/expired."""
    if not signed_value:
        return None
    try:
        return _OAUTH_STATE_SIGNER.unsign(signed_value, max_age=max_age_seconds)
    except (BadSignature, SignatureExpired):
        return None


# Rate-limit helper backed by the existing cache (Redis in prod, locmem in IDE).
# Increments a counter on a sliding window equal to `window_seconds`. Returns
# True when the action is over the limit (caller should reject).
def _is_rate_limited(bucket: str, limit: int, window_seconds: int) -> bool:
    key = f"ratelimit:{bucket}"
    try:
        current = cache.incr(key)
    except ValueError:
        cache.set(key, 1, timeout=window_seconds)
        current = 1
    return current > limit


def _client_ip(request) -> str:
    # Trust the leftmost X-Forwarded-For entry (Traefik/nginx already strip
    # client-supplied XFF before forwarding). Falls back to REMOTE_ADDR.
    xff = request.META.get('HTTP_X_FORWARDED_FOR', '')
    if xff:
        return xff.split(',')[0].strip()
    return request.META.get('REMOTE_ADDR', 'unknown')


# OTP attempt accounting. After MAX_OTP_ATTEMPTS bad tries the cached OTP is
# burned, forcing the user to request a new code.
MAX_OTP_ATTEMPTS = 5


def _record_otp_failure(email: str) -> int:
    """Increment failure counter for this email's OTP. Returns the new count."""
    key = f"recurly_otp_attempts:{email}"
    try:
        return cache.incr(key)
    except ValueError:
        cache.set(key, 1, timeout=600)
        return 1


def _clear_otp_state(email: str):
    cache.delete(f"recurly_otp_{email}")
    cache.delete(f"recurly_account_{email}")
    cache.delete(f"recurly_otp_attempts:{email}")
