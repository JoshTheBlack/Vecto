"""
Cross-publication helpers: the single source of truth for which podcasts an
episode may be cross-published into, and for syncing the link set.

The same-network rule lives ONLY in validate_cross_targets — relaxing it
later (e.g. cross-network when the publisher owns both networks) is a change
to that one function.
"""
import logging

from ..models import EpisodeCrossPublication, Podcast

logger = logging.getLogger(__name__)


def validate_cross_targets(episode, raw_ids, network):
    """Coerce raw form/JSON ids into valid target podcasts: same network,
    never the episode's own parent. Invalid or foreign ids are dropped
    silently."""
    ids = set()
    for raw in raw_ids or []:
        try:
            ids.add(int(raw))
        except (TypeError, ValueError):
            continue
    if not ids:
        return Podcast.objects.none()
    return Podcast.objects.filter(network=network, id__in=ids).exclude(id=episode.podcast_id)


def current_target_ids(episode):
    return sorted(episode.cross_publications.values_list('podcast_id', flat=True))


def sync_cross_publications(episode, targets, *, added_by=None, modes=None):
    """Make the episode's cross-publication set exactly `targets`.

    `modes` is an optional {podcast_id: access_mode} dict; rows not listed
    keep (or default to) their current mode. Returns (added_ids, removed_ids).
    """
    target_ids = {p.id for p in targets}
    existing = dict(episode.cross_publications.values_list('podcast_id', 'access_mode'))

    added_ids = sorted(target_ids - set(existing))
    removed_ids = sorted(set(existing) - target_ids)

    if removed_ids:
        episode.cross_publications.filter(podcast_id__in=removed_ids).delete()

    if added_ids:
        valid_modes = dict(EpisodeCrossPublication.AccessMode.choices)
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

    if modes:
        valid_modes = dict(EpisodeCrossPublication.AccessMode.choices)
        for pid, mode in modes.items():
            if pid in target_ids and pid in existing and mode in valid_modes and existing[pid] != mode:
                episode.cross_publications.filter(podcast_id=pid).update(access_mode=mode)

    if added_ids or removed_ids:
        logger.info(
            f"[cross_publish] ep {episode.id}: +{added_ids} -{removed_ids} (by {getattr(added_by, 'username', None)})"
        )
    return added_ids, removed_ids
