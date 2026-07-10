"""
Release calendar domain logic: reconciling pre-planned CalendarEntry rows
with real episodes. A planned entry and a later-scheduled episode should
merge into one calendar row instead of duplicating — matching uses
podcast + season/episode (numbered shows) or type+title (unnumbered shows)
as the natural key.

Callers own network scoping of any user-supplied episode; the explicit
calendar_entry_id path here re-checks network itself because that id comes
straight from request.POST.
"""
import html
import logging
from datetime import timedelta

from django.db.models import Q
from django.utils.html import strip_tags

from ..models import CalendarEntry

logger = logging.getLogger(__name__)

# A candidate whose planned time is further than this from the episode's
# actual release time is stale (e.g. an abandoned entry from a prior season)
# and never auto-matched.
MATCH_WINDOW = timedelta(days=60)


def match_calendar_entry(episode):
    """Find an unlinked CalendarEntry that plausibly represents `episode`,
    scoped to the same network+podcast. Returns None if no podcast is set
    on the episode, or no plausible match exists within ±60 days of the
    episode's target time."""
    if not episode.podcast_id:
        return None
    target_time = episode.scheduled_at or episode.pub_date
    if target_time is None:
        return None
    candidates = CalendarEntry.objects.filter(
        network_id=episode.podcast.network_id, podcast_id=episode.podcast_id,
        episode__isnull=True,
        scheduled_at__gte=target_time - MATCH_WINDOW,
        scheduled_at__lte=target_time + MATCH_WINDOW,
    )
    if episode.season_number is not None and episode.episode_number is not None:
        # Numbered shows (e.g. HOTD): season+episode is the natural key.
        candidates = candidates.filter(
            season_number=episode.season_number, episode_number=episode.episode_number)
    else:
        # Unnumbered shows (e.g. Bald Movies): episode_type+title is the key.
        # A planner's entry typically leaves episode_type blank while the real
        # episode arrives typed ('full'), so blank entries match any type.
        candidates = candidates.filter(title__iexact=episode.title)
        if episode.episode_type:
            candidates = candidates.filter(
                Q(episode_type='') | Q(episode_type__iexact=episode.episode_type))
    return candidates.order_by('scheduled_at').first()


def _sync_entry_from_episode(entry, episode):
    """Once an entry links to a real episode, the episode becomes the public
    source of truth: adopt its title, numbering/type, and description (as the
    entry's public notes). Episode values only overwrite when actually set, so
    a planned entry's numbering survives an unnumbered episode."""
    entry.episode = episode
    if episode.title:
        entry.title = episode.title
    if episode.season_number is not None:
        entry.season_number = episode.season_number
    if episode.episode_number is not None:
        entry.episode_number = episode.episode_number
    if episode.episode_type:
        entry.episode_type = episode.episode_type
    desc = html.unescape(strip_tags(episode.clean_description or '')).strip()
    if desc:
        entry.notes = desc


def ensure_calendar_entry_for_episode(episode, *, calendar_entry_id=None):
    """Called whenever an episode is scheduled or published. Links to an
    explicit calendar_entry_id if the publisher picked one; else tries
    match_calendar_entry(); else auto-creates a new entry. Always keeps
    the entry's scheduled_at in sync with the episode's actual time.
    Idempotent — safe to call again on every schedule/publish/edit."""
    target_time = episode.scheduled_at or episode.pub_date
    if target_time is None:
        return None
    entry = getattr(episode, 'calendar_entry', None)
    if not entry and calendar_entry_id:
        # Network-scoped: calendar_entry_id comes straight from request.POST,
        # so a forged id must not link another network's entry.
        entry = CalendarEntry.objects.filter(
            id=calendar_entry_id, network_id=episode.podcast.network_id,
            episode__isnull=True,
        ).first()
        if entry:
            # The publisher may pick a freeform entry that never had a podcast.
            entry.podcast = episode.podcast
    if not entry:
        entry = match_calendar_entry(episode)
    if not entry:
        entry = CalendarEntry(
            network_id=episode.podcast.network_id, podcast=episode.podcast,
            title=episode.title, season_number=episode.season_number,
            episode_number=episode.episode_number, episode_type=episode.episode_type,
        )
    _sync_entry_from_episode(entry, episode)
    entry.scheduled_at = target_time
    entry.save()
    return entry


def link_calendar_entry_for_new_episode(episode, stdout=None):
    """Ingest hook, LINK-ONLY: reconcile a pre-planned entry with a NEW
    ingested episode (ingested episodes are born published and never pass
    through publish.py). Never auto-creates — that would flood the calendar
    with every episode from ~88 feeds. Called from commit_episode's is_new
    branch only, after the episode has a PK."""
    entry = match_calendar_entry(episode)
    if not entry:
        return None
    _sync_entry_from_episode(entry, episode)
    entry.scheduled_at = episode.pub_date
    entry.save()
    logger.info(
        f"[calendar] Linked entry {entry.id} '{entry.title}' to ingested "
        f"episode {episode.id} '{episode.title}'")
    if stdout:
        stdout.write(
            f"  [CALENDAR LINK] '{episode.title}' -> planned entry "
            f"'{entry.title}' (id={entry.id})"
        )  # logger alone never reaches the creator-facing import log —
           # CommandLogStream only sees stdout
    return entry
