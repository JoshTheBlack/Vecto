"""Extract Podcasting-2.0 chapters from an episode's free-text show notes.

Publishers frequently list chapter markers in the episode description instead of
(or in addition to) the structured ``<podcast:chapters>`` feed tag. This module
recognizes those textual chapter lists and converts them into the canonical
``{"version": "1.2.0", "chapters": [{"startTime": <seconds>, "title": <str>}, ...]}``
shape used everywhere else (see ``services.edits.parse_chapter_payload`` and
``models.normalize_chapters``).

It is deliberately *style-driven* and easy to extend: each known way a publisher
formats a chapter line is a :class:`ChapterStyle` with a regex that captures a
``time`` group and a ``title`` group. To teach the extractor a new layout, add one
entry to :data:`CHAPTER_STYLES` — nothing else changes.

Currently recognized line styles:

* ``trailing_paren`` — ``Title Of Thing (00:12:56)`` (time last, in parens/brackets).
  This is the Bald Move "What're We Watching" / Patreon notes style.
* ``leading_time``  — ``(00:12:56) - Title`` / ``00:12:56 Title`` (time first).
  Mirrors the original ingester scraper
  (``ingesters.baldmove.parse_html_chapters``).
"""

import logging
import re
from dataclasses import dataclass

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# A timecode is H:MM:SS / HH:MM:SS / M:SS / MM:SS. Re-used by every style.
_TIMECODE = r"\d{1,2}:\d{2}(?::\d{2})?"

# Minimum recognized lines before we trust a description as a real chapter list.
# Guards against a lone stray timestamp in prose ("...around 1:30 he says...").
MIN_CHAPTERS = 2


@dataclass(frozen=True)
class ChapterStyle:
    """One recognized way a publisher writes a chapter line. ``pattern`` must
    expose two named groups: ``time`` (a timecode) and ``title``."""
    name: str
    pattern: "re.Pattern"


# Order matters: the first style whose pattern matches a line wins for that line.
# leading_time is tried first because its anchored leading timecode is unambiguous;
# trailing_paren is the looser catch-all (its title may contain its own parens).
CHAPTER_STYLES = [
    ChapterStyle(
        name="leading_time",
        # "(00:12:56) - Title", "00:12:56 Title", "00:12:56 — Title"
        pattern=re.compile(
            rf"^\s*[\(\[]?\s*(?P<time>{_TIMECODE})\s*[\)\]]?\s*[-—–:|.)]*\s*(?P<title>.+\S)\s*$"
        ),
    ),
    ChapterStyle(
        name="trailing_paren",
        # "Title Of Thing (00:12:56)" / "Title [1:24:09]". The title may itself hold
        # parens (e.g. "One Battle After Another (2025) (00:39:38)") — a non-greedy
        # title plus an end-anchored timecode means the *last* parenthetical wins.
        pattern=re.compile(
            rf"^\s*(?P<title>.+?)\s*[\(\[]\s*(?P<time>{_TIMECODE})\s*[\)\]]\s*$"
        ),
    ),
]


def parse_timecode(value):
    """``"01:02:03"`` → ``3723`` seconds. ``MM:SS`` and ``H:MM:SS`` both supported.
    Returns ``None`` if the string isn't a well-formed timecode."""
    parts = value.split(":")
    try:
        nums = [int(p) for p in parts]
    except ValueError:
        return None
    if len(nums) == 3:
        h, m, s = nums
    elif len(nums) == 2:
        h, m, s = 0, nums[0], nums[1]
    else:
        return None
    return h * 3600 + m * 60 + s


def _match_line(line):
    """Return ``(seconds, title)`` for the first style that recognizes ``line``,
    else ``None``."""
    for style in CHAPTER_STYLES:
        m = style.pattern.match(line)
        if not m:
            continue
        seconds = parse_timecode(m.group("time"))
        title = m.group("title").strip()
        if seconds is None or not title:
            continue
        return seconds, title
    return None


def extract_chapters_from_text(text):
    """Scan plain text line-by-line and return a list of
    ``{"startTime": int, "title": str}`` chapters (chronological, de-duplicated by
    start time), or ``None`` if fewer than :data:`MIN_CHAPTERS` lines are recognized."""
    if not text:
        return None
    found = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        hit = _match_line(line)
        if hit:
            seconds, title = hit
            found.append({"startTime": seconds, "title": title})

    if len(found) < MIN_CHAPTERS:
        return None

    # Chronological order + drop exact-duplicate start times (keep the first title).
    seen = set()
    ordered = []
    for chap in sorted(found, key=lambda c: c["startTime"]):
        if chap["startTime"] in seen:
            continue
        seen.add(chap["startTime"])
        ordered.append(chap)
    return ordered


def extract_chapters_from_html(html, *, add_intro=True):
    """Parse an episode description (HTML or plain text) and return canonical
    chapters ``{"version": "1.2.0", "chapters": [...]}`` or ``None``.

    With ``add_intro`` (the default) a ``00:00:00`` "Intro" chapter is prepended
    when the first recognized chapter doesn't already start at zero, so players
    always have a marker covering the show's open.
    """
    if not html:
        return None
    # A newline separator turns <li>/<p>/<br> structure back into one chapter per
    # line, matching how publishers lay these lists out.
    text = BeautifulSoup(html, "html.parser").get_text(separator="\n")
    chapters = extract_chapters_from_text(text)
    if not chapters:
        return None
    if add_intro and chapters[0]["startTime"] != 0:
        chapters = [{"startTime": 0, "title": "Intro"}] + chapters
    return {"version": "1.2.0", "chapters": chapters}
