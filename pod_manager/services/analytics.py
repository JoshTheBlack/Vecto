"""
Analytics & cache-invalidation services. Called from views (live stats),
feed handlers (RSS activity), and tasks (cache version bumps).
"""
import logging
from collections import defaultdict
from datetime import timedelta

import redis

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone

from ..models import Episode, Podcast

logger = logging.getLogger(__name__)


# Active-user presence recorder. Called from every RSS feed view on a
# feed_token-authenticated request. Writes two key shapes per (network, user, day):
#   analytics:rss:{net_id}:{user_id}:{date}    — RSS poll counter (INCR)
#   billing:active:{net_id}:{user_id}:{date}   — presence flag (SET 1)
# Both are drained by sweep_analytics_buffer into NetworkMembership.last_active_date.
# BillingPresenceMiddleware writes only the billing:active shape for authenticated
# web sessions; keeping both here ensures RSS-only listeners count the same way.
def _record_active_user(network_ids, user_id):
    if not user_id:
        return
    today = timezone.now().strftime('%Y-%m-%d')
    for net_id in network_ids:
        if net_id is None:
            continue
        rss_key = f"analytics:rss:{net_id}:{user_id}:{today}"
        try:
            cache.incr(rss_key)
        except ValueError:
            cache.set(rss_key, 1, timeout=172800)
        cache.set(f"billing:active:{net_id}:{user_id}:{today}", 1, timeout=172800)


def get_live_user_stats(tenant_profile):
    from ..tasks import parse_duration_to_hours

    live_play_hits = tenant_profile.total_playback_hits or 0
    live_hours = tenant_profile.total_hours_accessed or 0.0
    live_streak_days = tenant_profile.streak_days or 0
    live_streak_weeks = tenant_profile.streak_weeks or 0
    live_obsession_title = tenant_profile.current_obsession.title if tenant_profile.current_obsession else "Wandering Adventurer"

    # "Random Encounters" — the total is withheld ("?") until the hunt is down
    # to its last entry, so the finish line only appears once it's in reach.
    notfound_seen = tenant_profile.seen_notfound_entries.count()
    notfound_total = tenant_profile.network.notfound_entries.count()
    notfound_reveal_total = notfound_total == 0 or notfound_seen >= notfound_total - 1

    cache_backend = settings.CACHES['default'].get('BACKEND', '').lower()
    if 'locmem' in cache_backend or 'dummy' in cache_backend:
        return {
            'playback_hits': live_play_hits, 'hours_accessed': round(live_hours, 2),
            'streak_days': live_streak_days, 'streak_weeks': live_streak_weeks,
            'obsession_title': live_obsession_title,
            'notfound_seen': notfound_seen, 'notfound_total': notfound_total,
            'notfound_reveal_total': notfound_reveal_total,
        }

    try:
        redis_url = settings.CACHES['default']['LOCATION']
        redis_client = redis.from_url(redis_url)

        global_user_id = tenant_profile.user.patron_profile.id
        play_keys = redis_client.keys(f"*analytics:play:{global_user_id}:*")
        pending_episode_ids = set()
        podcast_hits = defaultdict(int)

        for key_bytes in play_keys:
            hits = redis_client.get(key_bytes)
            if hits:
                key_str = key_bytes.decode('utf-8')
                clean_key = key_str.split('analytics:play:')[-1]
                parts = clean_key.split(':')
                if len(parts) == 3:
                    e_id, pod_id = int(parts[1]), int(parts[2])
                    if Podcast.objects.filter(id=pod_id, network=tenant_profile.network).exists():
                        live_play_hits += int(hits)
                        pending_episode_ids.add(e_id)
                        podcast_hits[pod_id] += int(hits)

        if pending_episode_ids:
            episodes = Episode.objects.filter(id__in=pending_episode_ids)
            for ep in episodes:
                live_hours += parse_duration_to_hours(ep.duration)

        if podcast_hits:
            top_pod_id = max(podcast_hits, key=podcast_hits.get)
            obsession_pod = Podcast.objects.filter(id=top_pod_id).first()
            if obsession_pod:
                live_obsession_title = obsession_pod.title

    except Exception as e:
        logger.error(f"Failed to fetch live stats from Redis: {e}")

    return {
        'playback_hits': live_play_hits,
        'hours_accessed': round(live_hours, 2),
        'streak_days': live_streak_days,
        'streak_weeks': live_streak_weeks,
        'obsession_title': live_obsession_title,
        'notfound_seen': notfound_seen,
        'notfound_total': notfound_total,
        'notfound_reveal_total': notfound_reveal_total,
    }
