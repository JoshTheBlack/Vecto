"""Transcript R2 key layout — pure functions, no model imports (so models.py can use
them for field validators without a cycle).

A transcript's objects live at ``{stem}.{ext}`` for ext in vtt/json/srt/html/words.
The *stem* is STORED on the row (``Transcript.r2_key_stem``) and is the source of
truth for where the objects are; nothing re-derives a key from the episode id on
the read/serve path any more. That is what lets a transcript change owner (a merge
repoints the row) without moving a single object, lets a restore re-attach objects
that are already in the bucket, and lets the layout change without breaking old
keys.

What *this module* decides is only how a NEW stem is generated (and what the rekey
command treats as "canonical"): the per-network ``Network.transcript_key_pattern``,
default ``DEFAULT_KEY_PATTERN``. The default equals the historical tokened layout,
so adopting stored stems moved nothing.

Stems are bare: no environment prefix (``R2_MEDIA_KEY_PREFIX`` is applied at I/O
time by media_object_key) and no extension.
"""
import re
import string

DEFAULT_KEY_PATTERN = "transcripts/{bucket}/{episode_id}.{token}"

# Every stem lives under this prefix: the orphan GC and the media-bucket sweeps
# classify objects as transcripts by it.
TRANSCRIPTS_PREFIX = "transcripts/"

# {bucket}        episode_id // 1000 — keeps each folder to ~1000 episodes
# {episode_id}    the owning episode's id when the stem was generated
# {token}         the row's random r2_key_token (REQUIRED — it is the secrecy)
# {network_slug}  / {podcast_slug}: informational folders. Mutable (an episode
#                 can move feeds), so a stem using them can drift from canonical;
#                 `rekey_transcripts --normalize` is what reconciles that.
KEY_PATTERN_FIELDS = frozenset({"bucket", "episode_id", "token", "network_slug", "podcast_slug"})

_SAFE_CHARS = re.compile(r"^[A-Za-z0-9_\-./{}]+$")
_MAX_PATTERN_LEN = 200


def validate_key_pattern(pattern: str) -> None:
    """Raise ValueError unless ``pattern`` is a safe, secret-preserving stem pattern.
    A blank pattern is valid (it means "use the default")."""
    if not pattern:
        return
    if len(pattern) > _MAX_PATTERN_LEN:
        raise ValueError(f"Pattern is longer than {_MAX_PATTERN_LEN} characters.")
    if not pattern.startswith(TRANSCRIPTS_PREFIX):
        raise ValueError(f"Pattern must start with '{TRANSCRIPTS_PREFIX}' (the cleanup jobs recognise transcript objects by it).")
    if not _SAFE_CHARS.match(pattern):
        raise ValueError("Pattern may only contain letters, digits, '_', '-', '.', '/' and {placeholders}.")
    if ".." in pattern or "//" in pattern or pattern.endswith(("/", ".")):
        raise ValueError("Pattern must not contain '..' or '//' or end with '/' or '.'.")
    try:
        fields = {name for _, name, _, _ in string.Formatter().parse(pattern) if name is not None}
    except ValueError as exc:                 # unbalanced braces
        raise ValueError(f"Malformed placeholder: {exc}") from exc
    unknown = fields - KEY_PATTERN_FIELDS
    if unknown:
        raise ValueError(
            f"Unknown placeholder(s) {sorted(unknown)}; allowed: {sorted(KEY_PATTERN_FIELDS)}.")
    if "token" not in fields:
        raise ValueError("Pattern must contain {token}: it is what makes the object keys non-guessable.")


def render_stem(pattern: str, *, episode_id: int, token: str,
                network_slug: str = "", podcast_slug: str = "") -> str:
    """The stem for a new/normalized transcript. ``pattern`` blank -> the default."""
    return (pattern or DEFAULT_KEY_PATTERN).format(
        bucket=episode_id // 1000, episode_id=episode_id, token=token,
        network_slug=network_slug, podcast_slug=podcast_slug,
    )


def legacy_stem(episode_id: int, token: str | None = None) -> str:
    """The pre-stored-stem derivation, kept for backfilling existing rows and as the
    fallback for a row that has no stored stem:
        token None -> transcripts/{id // 1000}/{id}          (untokened, fuzzable)
        token set  -> transcripts/{id // 1000}/{id}.{token}"""
    tail = f"{episode_id}.{token}" if token else str(episode_id)
    return f"{TRANSCRIPTS_PREFIX}{episode_id // 1000}/{tail}"
