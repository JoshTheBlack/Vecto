"""
Episode move: the single source of truth for what "moving an episode to a
different podcast" means — parent reassignment, pin stamping, cross-publish
cleanup, R2 rekey dispatch, and fragment rebuilds.

Callers own network scoping (validate episode_ids belong to their network
before calling, like sync_cross_publications) and their own user messages.
"""
import logging

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

from ..models import Episode, EpisodeCrossPublication

logger = logging.getLogger(__name__)


def move_episodes(episode_ids, target_podcast, *, base_url, moved_by=None,
                  pin=True, rebuild_fragments=True):
    """Move episodes to target_podcast. Pure domain function — no request/
    response objects. Returns {'count': int, 'target': Podcast}.

    pin=False (ingest auto-migration) skips the podcast_pinned_* stamp — the
    pin means "a human moved this on purpose; auto-migration hands off".
    rebuild_fragments=False lets call sites that already rebuild fragments
    themselves skip the redundant dispatch.
    """
    # Call sites disagree on trailing slashes — the service owns the
    # normalization invariant so no caller can get it wrong.
    base_url = base_url.rstrip('/')

    pin_fields = {}
    if pin:
        pin_fields = {'podcast_pinned_at': timezone.now(),
                      'podcast_pinned_by': moved_by}
    count = Episode.objects.filter(id__in=episode_ids).update(
        podcast=target_podcast, **pin_fields)

    # An episode moved into a podcast it was cross-published to would now
    # self-reference — drop the redundant links.
    EpisodeCrossPublication.objects.filter(
        episode_id__in=episode_ids, podcast=target_podcast).delete()

    # Parent changed: tear down auto links created for the old parents and
    # apply the new parent's auto_crosspublish_targets (manual links carry
    # across untouched). Destination membership is composed live at serve
    # time, so busting the touched shells is the whole refresh.
    from .cross_publish import reeval_auto_cross_publish
    for dest_id in reeval_auto_cross_publish(episode_ids, target_podcast):
        cache.delete(f"feed_shell_public_{dest_id}")
        cache.delete(f"feed_shell_private_{dest_id}")

    # Re-key any mirrored episodes so their R2 object lands under the new
    # parent's network_id/podcast_id (backup accuracy — section J). Async;
    # idempotent.
    if getattr(settings, 'R2_MIRROR_ENABLED', True):
        from ..tasks import task_rekey_episode_audio
        moved_mirrored = (Episode.objects.filter(id__in=episode_ids)
                          .exclude(r2_url__isnull=True).exclude(r2_url='')
                          .values_list('id', flat=True))
        for ep_id in moved_mirrored:
            task_rekey_episode_audio.delay(ep_id)

    if rebuild_fragments:
        from ..tasks import task_rebuild_episode_fragments
        for ep_id in episode_ids:
            task_rebuild_episode_fragments.delay(int(ep_id), base_url)

    actor = moved_by.username if moved_by else "ingest"
    logger.info(f"[move] Moved {count} episode(s) to '{target_podcast.title}' "
                f"(id={target_podcast.id}) by {actor}")
    return {'count': count, 'target': target_podcast}
