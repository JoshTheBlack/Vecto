"""Shared audio-content detection — the single "is this really audio?" check.

Several paths can be handed bytes that are secretly *not* audio: an HTTP-200 HTML
error page from Google Drive ("file removed" / quota / "couldn't scan for
viruses"), a Patreon login wall, a truncated download. We confirm audio the
strongest way possible — **positive magic-byte detection**: the header must match
a known audio container. This is a whitelist by design, so an unrecognized header
is treated as NOT audio rather than assumed fine.

Kept dependency-free (no Django, no models, no requests) so both the R2 mirror
and the transcription pipeline can import it without circular imports.
"""

from pathlib import Path

# Enough leading bytes for every signature below (MP4 'ftyp' at offset 4, WAV
# 'WAVE' at offset 8). Cheap to read.
SNIFF_BYTES = 512


def looks_like_audio(head: bytes) -> bool:
    """True if ``head`` begins with a known audio-container signature.

    Covers every format we mirror: MP3 (ID3-tagged or raw MPEG frame sync),
    MP4/M4A/M4B, Ogg/Opus, FLAC, WAV. Anything else returns False.
    """
    if len(head) < 12:
        return False
    if head[:3] == b'ID3':                                    # ID3v2-tagged MP3
        return True
    if head[0] == 0xFF and (head[1] & 0xE0) == 0xE0:          # MPEG / AAC-ADTS frame sync
        return True
    if head[4:8] == b'ftyp':                                  # MP4 / M4A / M4B box
        return True
    if head[:4] == b'OggS':                                   # Ogg / Opus
        return True
    if head[:4] == b'fLaC':                                   # FLAC
        return True
    if head[:4] == b'RIFF' and head[8:12] == b'WAVE':         # WAV
        return True
    return False


def is_audio_file(path) -> bool:
    """True if the file at ``path`` begins with an audio signature.

    Reads only the first bytes; returns False on any read error (a file we can't
    inspect is not provably audio).
    """
    try:
        with Path(path).open('rb') as fp:
            return looks_like_audio(fp.read(SNIFF_BYTES))
    except OSError:
        return False
