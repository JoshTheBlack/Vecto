"""
Cross-publication helpers: the single source of truth for which podcasts an
episode may be cross-published into, and for syncing the link set.

Two ownership domains share the EpisodeCrossPublication table:
  - MANUAL links (auto_created=False) — managed by per-episode editors and
    the bulk Cross-Publish tab, all of which route through
    sync_cross_publications.
  - AUTO links (auto_created=True) — owned exclusively by the feed-level
    engine (apply_auto_cross_publish / sync_feed_auto_targets /
    reeval_auto_cross_publish). Per-episode sync is blind to them; the only
    crossover is promotion: explicitly submitting a target that already has
    an auto link flips it to manual so it survives feed-level teardown.

The same-network rule lives ONLY in the validate_* functions — relaxing it
later (e.g. cross-network when the publisher owns both networks) is a change
to one place.
"""
import logging

from ..models import EpisodeCrossPublication, Podcast

logger = logging.getLogger(__name__)

BULK_CHUNK = 500


def _coerce_ids(raw_ids):
    """Raw form/JSON values -> set of ints; junk dropped silently."""
    ids = set()
    for raw in raw_ids or []:
        try:
            ids.add(int(raw))
        except (TypeError, ValueError):
            continue
    return ids


def _valid_targets(network, ids, exclude_podcast_id):
    if not ids:
        return Podcast.objects.none()
    return Podcast.objects.filter(network=network, id__in=ids).exclude(id=exclude_podcast_id)


def validate_cross_targets(episode, raw_ids, network):
    """Coerce raw ids into valid per-episode targets: same network, never the
    episode's own parent."""
    return _valid_targets(network, _coerce_ids(raw_ids), episode.podcast_id)


def validate_feed_cross_targets(source_feed, raw_ids, network):
    """Feed-level analogue: same network, never the source feed itself."""
    return _valid_targets(network, _coerce_ids(raw_ids), source_feed.id)


def current_target_ids(episode):
    """The episode's MANUAL targets — what per-episode editors manage. Auto
    links are the feed-level engine's business and are not reported here."""
    return sorted(
        episode.cross_publications.filter(auto_created=False)
        .values_list('podcast_id', flat=True)
    )


def _create_auto_links(episode_ids, target_ids, access_mode):
    """Chunked insert of auto links for every (episode, target) pair.
    ignore_conflicts means an existing row — manual or auto — is left exactly
    as it is: a manual link is never clobbered or flag-flipped."""
    rows = [
        EpisodeCrossPublication(episode_id=eid, podcast_id=pid,
                                access_mode=access_mode, auto_created=True)
        for eid in episode_ids for pid in target_ids
    ]
    for start in range(0, len(rows), BULK_CHUNK):
        EpisodeCrossPublication.objects.bulk_create(
            rows[start:start + BULK_CHUNK], ignore_conflicts=True)


def sync_cross_publications(episode, targets, *, added_by=None, modes=None):
    """Make the episode's MANUAL cross-publication set exactly `targets`.

    Blind to auto-created rows: they are never deleted or re-moded here.
    A submitted target colliding with an existing auto link is PROMOTED
    (auto_created=False) so it survives feed-level teardown.

    `targets` is an iterable of Podcast objects or ids. `modes` is an optional
    {podcast_id: access_mode} dict; rows not listed keep (or default to) their
    current mode. Returns (added_ids, removed_ids).
    """
    target_ids = {p if isinstance(p, int) else p.id for p in targets}
    existing = dict(
        episode.cross_publications.filter(auto_created=False)
        .values_list('podcast_id', 'access_mode')
    )

    added_ids = sorted(target_ids - set(existing))
    removed_ids = sorted(set(existing) - target_ids)

    valid_modes = dict(EpisodeCrossPublication.AccessMode.choices)

    if removed_ids:
        episode.cross_publications.filter(
            podcast_id__in=removed_ids, auto_created=False).delete()

    if added_ids:
        EpisodeCrossPublication.objects.bulk_create(
            [
                EpisodeCrossPublication(
                    episode=episode,
                    podcast_id=pid,
                    added_by=added_by,
                    access_mode=(modes or {}).get(pid)
                    if (modes or {}).get(pid) in valid_modes
                    else EpisodeCrossPublication.AccessMode.INHERIT,
                )
                for pid in added_ids
            ],
            ignore_conflicts=True,
        )
        # Inserts that collided with an auto row were ignored — promote those
        # rows to manual instead, and let the modes pass below re-mode them.
        promoted = dict(
            episode.cross_publications.filter(podcast_id__in=added_ids, auto_created=True)
            .values_list('podcast_id', 'access_mode')
        )
        if promoted:
            episode.cross_publications.filter(
                podcast_id__in=promoted, auto_created=True
            ).update(auto_created=False, added_by=added_by)
            existing.update(promoted)

    if modes:
        for pid, mode in modes.items():
            if pid in target_ids and pid in existing and mode in valid_modes and existing[pid] != mode:
                episode.cross_publications.filter(podcast_id=pid).update(access_mode=mode)

    if added_ids or removed_ids:
        logger.info(
            f"[cross_publish] ep {episode.id}: +{added_ids} -{removed_ids} (by {getattr(added_by, 'username', None)})"
        )
    return added_ids, removed_ids


def add_cross_publications(episode, targets, *, added_by=None):
    """Add-only entry point for the bulk Cross-Publish tab: unions `targets`
    with the episode's current manual set so sync never removes anything.
    Returns the added target ids (promotions of colliding auto links count
    as adds)."""
    union = set(current_target_ids(episode)) | {
        p.id for p in targets if p.id != episode.podcast_id
    }
    added, _ = sync_cross_publications(episode, union, added_by=added_by)
    return added


def apply_auto_cross_publish(episode, *, stdout=None):
    """Ensure an auto link exists from this episode into each of its parent
    feed's auto_crosspublish_targets. Idempotent — fires on every ingest and
    publish save. Returns the target ids actually linked this call."""
    parent = episode.podcast
    targets = dict(
        parent.auto_crosspublish_targets.exclude(id=parent.id)
        .values_list('id', 'title')
    )
    if not targets:
        return []
    have = set(
        episode.cross_publications.filter(podcast_id__in=targets)
        .values_list('podcast_id', flat=True)
    )
    missing = [pid for pid in targets if pid not in have]
    if not missing:
        return []
    _create_auto_links([episode.id], missing, parent.auto_crosspublish_access_mode)
    if stdout:
        for pid in missing:
            stdout.write(f"  [AUTO-CP] '{episode.title}' -> {targets[pid]}")
    return missing


def sync_feed_auto_targets(source_feed, *, added_ids=None, removed_ids=None):
    """Feed-level engine: backfill auto links into newly added destinations,
    tear down auto links for removed ones. Manual links always survive.
    Returns the destination ids whose feed shells need busting."""
    added_ids = [pid for pid in (added_ids or []) if pid != source_feed.id]
    removed_ids = list(removed_ids or [])
    touched = set()

    if removed_ids:
        EpisodeCrossPublication.objects.filter(
            episode__podcast=source_feed, podcast_id__in=removed_ids,
            auto_created=True).delete()
        touched.update(removed_ids)

    if added_ids:
        episode_ids = list(source_feed.episodes.values_list('id', flat=True))
        _create_auto_links(episode_ids, added_ids, source_feed.auto_crosspublish_access_mode)
        touched.update(added_ids)

    if touched:
        logger.info(
            f"[cross_publish] feed {source_feed.id} '{source_feed.title}' auto targets: "
            f"+{sorted(added_ids)} -{sorted(removed_ids)}"
        )
    return touched


def resync_feed_auto_access_mode(source_feed):
    """Re-apply the feed's auto_crosspublish_access_mode to its existing auto
    links (D8 mode change). Manual links untouched. Returns the destination
    ids whose shells need busting."""
    stale = EpisodeCrossPublication.objects.filter(
        episode__podcast=source_feed, auto_created=True,
    ).exclude(access_mode=source_feed.auto_crosspublish_access_mode)
    touched = set(stale.values_list('podcast_id', flat=True))
    if touched:
        stale.update(access_mode=source_feed.auto_crosspublish_access_mode)
        logger.info(
            f"[cross_publish] feed {source_feed.id} auto links re-moded to "
            f"{source_feed.auto_crosspublish_access_mode} across {sorted(touched)}"
        )
    return touched


def reeval_auto_cross_publish(episode_ids, new_parent):
    """Parent change (ingest auto-migration or manual move): tear down ALL
    auto links on the moved episodes, then apply the new parent's auto
    targets. Manual links are never touched, so they carry across the move.
    Returns destination ids (old union new) whose shells need busting."""
    episode_ids = [int(eid) for eid in episode_ids]
    stale = EpisodeCrossPublication.objects.filter(
        episode_id__in=episode_ids, auto_created=True)
    touched = set(stale.values_list('podcast_id', flat=True))
    stale.delete()

    target_ids = list(
        new_parent.auto_crosspublish_targets.exclude(id=new_parent.id)
        .values_list('id', flat=True)
    )
    if target_ids:
        _create_auto_links(episode_ids, target_ids, new_parent.auto_crosspublish_access_mode)
        touched.update(target_ids)
    return touched
