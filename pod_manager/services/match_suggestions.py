"""
EpisodeMatchSuggestion lifecycle helpers — recording, dismissing, and resolving
the ambiguous auto-migrate pairs surfaced by commit_episode's guids_diverge
branch. Mirrors services/edits.py: unit-testable without HTTP or ingest.

See planned_migration_match_suggestions.txt §3.1/§3.2 for the mechanics. The
recording path is DEFENSIVE by contract: any persistence error is caught and
logged, never propagated — ingest resilience outranks this feature, and the
ingester's early return (the 1b bug fix) happens regardless of whether recording
succeeded.
"""
import logging

from django.db import IntegrityError, transaction
from django.utils import timezone

logger = logging.getLogger(__name__)


def record_match_suggestion(public_episode, private_episode, *, source, target, reason):
    """Persist (or refresh) a PENDING EpisodeMatchSuggestion for an ambiguous
    auto-migrate pair, and return the row (or None on any handled failure).

    Roles are fixed as detected: ``public_episode`` matched the incoming public
    GUID, ``private_episode`` the incoming private GUID. ``source`` is the
    low-priority feed the public row sits in; ``target`` the normal feed that was
    ingesting. GUID snapshots are read off the episode rows at detection time.

    Mechanics:
      - STICKY DISMISS: if a DISMISSED row already matches the (network, pub_guid,
        priv_guid) triple, bail without inserting — dismissal is GUID-keyed so it
        survives one of the episode rows being deleted and re-created.
      - INSERT, catching IntegrityError from the partial unique constraint: on a
        concurrent-poll race the existing PENDING row wins and its last_seen_at is
        bumped instead.

    Never raises: any unexpected error is logged and swallowed so ingest keeps
    running.
    """
    from pod_manager.models import EpisodeMatchSuggestion

    try:
        network = target.network
        pub_guid = public_episode.guid_public or ''
        priv_guid = private_episode.guid_private or ''

        # Sticky dismissal — GUID triple, not the FK pair (survives row churn).
        if EpisodeMatchSuggestion.objects.filter(
            network=network,
            pub_guid=pub_guid,
            priv_guid=priv_guid,
            status=EpisodeMatchSuggestion.Status.DISMISSED,
        ).exists():
            logger.info(
                "[match] Suggestion suppressed by sticky dismissal "
                "(network=%s pub_guid=%s priv_guid=%s)",
                network.id, pub_guid, priv_guid,
            )
            return None

        try:
            # Wrap the INSERT so the IntegrityError doesn't poison an enclosing
            # transaction (ingest may run inside one).
            with transaction.atomic():
                return EpisodeMatchSuggestion.objects.create(
                    network=network,
                    public_episode=public_episode,
                    private_episode=private_episode,
                    pub_guid=pub_guid,
                    priv_guid=priv_guid,
                    source_podcast=source,
                    target_podcast=target,
                    detected_reason=reason,
                    status=EpisodeMatchSuggestion.Status.PENDING,
                    last_seen_at=timezone.now(),
                )
        except IntegrityError:
            # A PENDING row for this pair already exists (partial unique
            # constraint). Re-detection: bump last_seen_at AND refresh the GUID
            # snapshots — the episodes' GUIDs may have changed since first
            # detection (e.g. a manually-added guid_public), and the Suggested
            # Pairs card renders the snapshots. Refreshing a PENDING row is
            # safe: the sticky-dismiss triple is only consulted pre-insert, and
            # a later dismissal captures the refreshed (current) triple.
            existing = EpisodeMatchSuggestion.objects.filter(
                public_episode=public_episode,
                private_episode=private_episode,
                status=EpisodeMatchSuggestion.Status.PENDING,
            ).first()
            if existing:
                existing.pub_guid = pub_guid
                existing.priv_guid = priv_guid
                existing.last_seen_at = timezone.now()
                existing.save(update_fields=['pub_guid', 'priv_guid', 'last_seen_at'])
            return existing

    except Exception:
        # Ingest resilience > feature: never break a poll over a suggestion.
        logger.exception("[match] Failed to record match suggestion")
        return None


def dismiss_match_suggestion(suggestion, *, user=None):
    """Mark a suggestion DISMISSED (sticky per the GUID-triple check in
    record_match_suggestion) with resolved_at/by audit."""
    from pod_manager.models import EpisodeMatchSuggestion

    suggestion.status = EpisodeMatchSuggestion.Status.DISMISSED
    suggestion.resolved_at = timezone.now()
    suggestion.resolved_by = user
    suggestion.save(update_fields=['status', 'resolved_at', 'resolved_by'])
    return suggestion


def resolve_match_suggestion(suggestion, *, user=None):
    """Mark a suggestion RESOLVED with resolved_at/by audit. Called by the merge
    editor's commit path before deleting the loser row (CASCADE would remove the
    suggestion anyway — this keeps the resolved_by audit)."""
    from pod_manager.models import EpisodeMatchSuggestion

    suggestion.status = EpisodeMatchSuggestion.Status.RESOLVED
    suggestion.resolved_at = timezone.now()
    suggestion.resolved_by = user
    suggestion.save(update_fields=['status', 'resolved_at', 'resolved_by'])
    return suggestion
