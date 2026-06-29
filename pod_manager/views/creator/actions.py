"""
POST action handlers for the creator settings dashboard.

Each handler accepts (request, current_network) and returns either:
  - None  →  caller redirects to creator_settings (default behaviour)
  - HttpResponse  →  caller returns this response directly (early exit)
"""
import json
import logging

from django.contrib import messages
from django.contrib.auth.models import User
from django.core.cache import cache
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.utils import timezone

from ...models import (
    NetworkMembership, Podcast, Episode, EpisodeCrossPublication, PatreonTier, NetworkMix,
    EpisodeEditSuggestion,
)
from ...services.cross_publish import sync_cross_publications, validate_cross_targets
from ...services.edits import chapter_items, score_contribution
from ...services.patreon import sync_network_patrons
from ...tasks import task_rebuild_episode_fragments, task_rebuild_podcast_fragments
from ...utils import sanitize_user_html

logger = logging.getLogger(__name__)


def _check_scalar_approval(request, approve_key, value_key, snapshot_val, ep_field, suggested_key, ep, edit, sanitize=False):
    """Apply an approved scalar field onto the episode + suggested_data. Returns
    1 if it changed (so the caller can record it in `changes`), else 0. Scoring is
    owned by score_contribution — this only mutates."""
    if request.POST.get(approve_key) != 'on':
        return 0
    raw_val = request.POST.get(value_key, '').strip()
    new_val = sanitize_user_html(raw_val) if sanitize else raw_val
    if not new_val or new_val == snapshot_val:
        return 0
    setattr(ep, ep_field, new_val)
    edit.suggested_data[suggested_key] = new_val
    return 1


def _restore_sequence_fields(ep, original_data):
    """Restore season/episode/type onto the episode from a rollback snapshot
    (transcript_rollback.md §8a). Key-presence guarded so pre-feature edits — whose
    original_data predates the sequence snapshot — don't wipe the current values."""
    original_data = original_data or {}
    if 'season_number' in original_data:
        ep.season_number = original_data.get('season_number')
    if 'episode_number' in original_data:
        ep.episode_number = original_data.get('episode_number')
    if 'episode_type' in original_data:
        ep.episode_type = original_data.get('episode_type')


def _restore_metadata_values(edit, ep, current_network):
    """Restore episode fields from a metadata edit's pre-approval snapshot (shared
    by single + bulk rollback). Saves the episode."""
    od = edit.original_data or {}
    ep.title = od.get('title', ep.title)
    ep.clean_description = od.get('description', ep.clean_description)
    ep.tags = od.get('tags', ep.tags)
    # Approve mirrors new chapters onto BOTH columns, so restore both — otherwise
    # the edited chapters stay baked into chapters_private after a rollback.
    restored_chapters = od.get('chapters', ep.chapters_public)
    ep.chapters_public = restored_chapters
    ep.chapters_private = restored_chapters
    # Key-presence guarded so pre-feature edits don't wipe current values (§8a).
    _restore_sequence_fields(ep, od)
    ep.save()
    if 'cross_publish_podcast_ids' in od:
        restore_ids = od.get('cross_publish_podcast_ids') or []
        sync_cross_publications(ep, validate_cross_targets(ep, restore_ids, current_network))


def _maybe_unlock_metadata(ep):
    """Clear the metadata lock once an episode has no APPROVED edits left — a
    rollback that removes the last approved edit returns the episode to editable."""
    if ep.is_metadata_locked and not EpisodeEditSuggestion.objects.filter(
        episode=ep, status=EpisodeEditSuggestion.Status.APPROVED).exists():
        ep.is_metadata_locked = False
        ep.save(update_fields=['is_metadata_locked'])


def _reverse_award(edit, membership):
    """Reverse the EXACT trust + counters this edit banked at approval — trust from
    edit.points, every counter (incl first_responder_count) from edit.counter_deltas.
    Mutates the membership in place (caller saves). Legacy rows have empty deltas /
    points 0, so they reverse nothing by design. Used by single AND bulk rollback,
    so both reverse exactly what was awarded."""
    membership.trust_score = max(0, membership.trust_score - (edit.points or 0))
    for attr, amt in (edit.counter_deltas or {}).items():
        setattr(membership, attr, max(0, getattr(membership, attr) - (amt or 0)))


def _handle_approve_edit(request, current_network):
    edit_id = request.POST.get('edit_id')
    edit = get_object_or_404(EpisodeEditSuggestion, id=edit_id, episode__podcast__network=current_network)
    membership, _ = NetworkMembership.objects.get_or_create(user=edit.user, network=current_network)

    ep = edit.episode
    # `changes` records the APPLIED fields (key presence) + quantities; score_contribution
    # turns it into the exact (points, counter_deltas) banked on the edit so rollback
    # is an exact wash. cross_publish is applied but never scored.
    changes = {}
    cross_published = False

    # Snapshot the live episode state RIGHT NOW, before any field is touched.
    # This becomes the new edit.original_data after approval, so single-edit
    # rollback restores the actual pre-approval state.
    pre_approval_snapshot = {
        'title': ep.title,
        'description': ep.clean_description or '',
        'tags': list(ep.tags or []),
        # Effective chapters (what the editor saw + what approve overwrites onto
        # both columns), so rollback restores the meaningful pre-edit chapters.
        'chapters': ep.chapters_private or ep.chapters_public or [],
        # Sequence fields captured so rollback restores the pre-approval values,
        # not just the edits_sequence counter (transcript_rollback.md §8a).
        'season_number': ep.season_number,
        'episode_number': ep.episode_number,
        'episode_type': ep.episode_type,
        'cross_publish_podcast_ids': sorted(ep.cross_publications.values_list('podcast_id', flat=True)),
    }
    # User's submission-time snapshot, used to compute deltas.
    user_snapshot = edit.original_data or {}

    # 0. PROCESS TITLE
    if _check_scalar_approval(
        request, 'approve_title', 'edited_title', pre_approval_snapshot['title'],
        'title', 'title', ep, edit,
    ):
        changes['title'] = True

    # 1. PROCESS DESCRIPTION
    if _check_scalar_approval(
        request, 'approve_description', 'edited_description', pre_approval_snapshot['description'],
        'clean_description', 'description', ep, edit, sanitize=True,
    ):
        changes['description'] = True

    # 2. PROCESS TAGS — set-delta merge.
    if request.POST.get('approve_tags') == 'on':
        raw_tags = request.POST.get('edited_tags', '[]')
        try:
            user_intended_tags = json.loads(raw_tags)
            user_baseline_tags = user_snapshot.get('tags') or []

            if isinstance(user_intended_tags, list):
                added = [t for t in user_intended_tags if t not in user_baseline_tags]
                removed = set(user_baseline_tags) - set(user_intended_tags)

                current_list = list(ep.tags or [])
                merged = [t for t in current_list if t not in removed]
                for t in added:
                    if t not in merged:
                        merged.append(t)

                if merged != current_list:
                    ep.tags = merged
                    edit.suggested_data['tags'] = merged
                    changes['tags'] = len(added)   # +1 pt (key present), +len(added) ctr
        except Exception as e:
            logger.warning(f"Failed to parse tags from inbox for edit #{edit_id}: {e}")

    # 3. PROCESS CHAPTERS — full replacement (order-sensitive, can't merge cleanly).
    if request.POST.get('approve_chapters') == 'on':
        raw_chapters = request.POST.get('edited_chapters', '')
        if raw_chapters:
            try:
                new_chapters = json.loads(raw_chapters)
                if chapter_items(new_chapters) != chapter_items(pre_approval_snapshot['chapters']):
                    # Mirror to both columns; see submit_episode_edit
                    ep.chapters_public = new_chapters
                    ep.chapters_private = new_chapters
                    edit.suggested_data['chapters'] = new_chapters
                    changes['chapters'] = len(chapter_items(new_chapters))   # +N pt, +N ctr
            except Exception as e:
                logger.warning(f"Failed to parse chapters from inbox for edit #{edit_id}: {e}")

    # 4-6. PROCESS SEQUENCE FIELDS (season / episode # / type)
    if request.POST.get('approve_season_number') == 'on':
        raw_val = request.POST.get('edited_season_number', '').strip()
        new_val = int(raw_val) if raw_val.isdigit() else None
        if new_val != pre_approval_snapshot.get('season_number'):
            ep.season_number = new_val
            edit.suggested_data['season_number'] = new_val
            changes['season_number'] = True

    if request.POST.get('approve_episode_number') == 'on':
        raw_val = request.POST.get('edited_episode_number', '').strip()
        new_val = int(raw_val) if raw_val.isdigit() else None
        if new_val != pre_approval_snapshot.get('episode_number'):
            ep.episode_number = new_val
            edit.suggested_data['episode_number'] = new_val
            changes['episode_number'] = True

    if request.POST.get('approve_episode_type') == 'on':
        new_val = request.POST.get('edited_episode_type', '').strip()[:50]
        if new_val != (pre_approval_snapshot.get('episode_type') or ''):
            ep.episode_type = new_val
            edit.suggested_data['episode_type'] = new_val
            changes['episode_type'] = True

    # 7. PROCESS CROSS-PUBLISH TARGETS — applied but NEVER scored (no pts/counter).
    if request.POST.get('approve_cross_publish') == 'on':
        raw_ids = request.POST.get('edited_cross_publish_ids', '')
        try:
            new_ids = json.loads(raw_ids) if raw_ids else []
            targets = list(validate_cross_targets(ep, new_ids, current_network))
            new_id_list = sorted(t.id for t in targets)
            if new_id_list != pre_approval_snapshot['cross_publish_podcast_ids']:
                sync_cross_publications(ep, targets, added_by=edit.user)
                edit.suggested_data['cross_publish_podcast_ids'] = new_id_list
                cross_published = True
        except Exception as e:
            logger.warning(f"Failed to parse cross-publish ids from inbox for edit #{edit_id}: {e}")

    # --- SPEAKER LABEL APPROVAL ---
    # Speaker label edits carry suggested_data = {"speaker_mappings": {...}} and
    # bypass the normal field-approval flow. Replay recomputes the whole transcript
    # from the speaker_id base + the approved chain, so it must run AFTER this edit
    # is flipped to APPROVED below — here we only score it and defer the apply.
    speaker_mappings = edit.suggested_data.get('speaker_mappings')
    apply_speaker = bool(
        speaker_mappings and isinstance(speaker_mappings, dict)
        and request.POST.get('approve_speaker_labels') == 'on'
    )
    if apply_speaker:
        from pod_manager.services.transcription import fold_speaker_mappings, speaker_edit_points
        prior_mapping = fold_speaker_mappings(ep.id)
        speaker_points, _newly_named = speaker_edit_points(speaker_mappings, prior_mapping)
        changes['speaker'] = speaker_points   # key present even if 0 (no-op rename is still an action)

    # --- THE ZERO-APPROVAL TRAP ---
    # Nothing applied at all (no scored field, no cross-publish, no speaker) →
    # convert to a penalising rejection. A speaker/cross-publish action counts even
    # when it scores 0, so it must not be trapped.
    if not changes and not cross_published:
        edit.status = EpisodeEditSuggestion.Status.REJECTED
        edit.resolved_at = timezone.now()
        edit.save()
        membership.trust_score = max(0, membership.trust_score - 2)
        membership.save()
        messages.warning(request, "No sections selected for approval. Edit converted to rejection. User penalized -2 Trust.")
        return

    # Score once — same scorer the auto-approve path uses (auto == manual).
    total_points, counter_deltas = score_contribution(changes, is_first=edit.is_first_responder)

    # Speaker-only edits don't touch Episode model fields — skip lock + save
    is_speaker_only = set(edit.suggested_data.keys()) == {'speaker_mappings'}
    if not is_speaker_only:
        ep.is_metadata_locked = True
        ep.save()

    # Rewrite original_data to the pre-approval snapshot so single-edit rollback
    # restores the state that existed right before this approval. For speaker-only
    # edits the original_data is already the prior mappings.
    if not is_speaker_only:
        edit.original_data = pre_approval_snapshot
    # Bank the trust delta + the per-counter deltas so rollback is an exact wash (§3.4).
    edit.points = total_points
    edit.counter_deltas = counter_deltas
    edit.status = EpisodeEditSuggestion.Status.APPROVED
    edit.resolved_at = timezone.now()
    edit.save()

    # Now that the row is APPROVED, replay the chain so this edit's mapping folds
    # into the materialised transcript (the final fragment rebuild below picks it up).
    if apply_speaker:
        try:
            from pod_manager.services.transcription import apply_speaker_labels
            apply_speaker_labels(ep.id)
        except Exception as e:
            logger.error("Speaker label approval failed for episode %d: %s", ep.id, e)

    membership.trust_score += total_points
    for attr, amt in counter_deltas.items():
        setattr(membership, attr, getattr(membership, attr) + amt)
    membership.save()

    logger.info(f"Edit #{edit.id} approved for episode '{ep.title}' — user {edit.user.username} awarded +{total_points} trust")
    messages.success(request, f"Edit approved! User awarded +{total_points} Trust Score.")

    base_url = request.build_absolute_uri('/')[:-1]
    task_rebuild_episode_fragments.delay(ep.id, base_url)


def _handle_reject_edit(request, current_network):
    edit_id = request.POST.get('edit_id')
    edit = get_object_or_404(EpisodeEditSuggestion, id=edit_id, episode__podcast__network=current_network)
    membership, _ = NetworkMembership.objects.get_or_create(user=edit.user, network=current_network)
    edit.status = EpisodeEditSuggestion.Status.REJECTED
    edit.resolved_at = timezone.now()
    edit.save()
    membership.trust_score = max(0, membership.trust_score - 2)
    membership.save()
    logger.info(f"Edit #{edit.id} rejected for episode '{edit.episode.title}' — user {edit.user.username} penalized -2 trust")
    messages.warning(request, "Edit rejected. User penalized -2 Trust.")


def _handle_rollback_single_edit(request, current_network):
    edit = get_object_or_404(EpisodeEditSuggestion, id=request.POST.get('edit_id'), episode__podcast__network=current_network, status=EpisodeEditSuggestion.Status.APPROVED)
    membership, _ = NetworkMembership.objects.get_or_create(user=edit.user, network=current_network)

    is_speaker = 'speaker_mappings' in (edit.suggested_data or {})

    # Block when newer approved edits exist on the same episode — but ONLY for
    # snapshot-based metadata edits. Speaker edits replay from the immutable base
    # over the remaining approved chain, so removing one is order-correct
    # regardless of which edit it is (transcript_rollback.md §3.4); no blocker.
    if not is_speaker:
        newer_approved = EpisodeEditSuggestion.objects.filter(
            episode=edit.episode,
            status=EpisodeEditSuggestion.Status.APPROVED,
            resolved_at__gt=edit.resolved_at,
        ).select_related('user').order_by('resolved_at')

        if newer_approved.exists():
            blockers = ", ".join(
                f"#{e.id} by {e.user.username}" for e in newer_approved[:5]
            )
            extra = "" if newer_approved.count() <= 5 else f" (and {newer_approved.count() - 5} more)"
            messages.error(
                request,
                f"Cannot roll back edit #{edit.id}: later approved edits exist on this episode "
                f"({blockers}{extra}). Roll those back first, or leave this edit in place."
            )
            return

    ep = edit.episode
    if is_speaker:
        # Flip to ROLLED_BACK first, then replay — the fold now excludes this edit
        # and rebuilds current state from the base + remaining approved chain.
        edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        edit.resolved_at = timezone.now()
        edit.save()
        from pod_manager.services.transcription import apply_speaker_labels
        apply_speaker_labels(ep.id)
    else:
        _restore_metadata_values(edit, ep, current_network)
        edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        edit.resolved_at = timezone.now()
        edit.save()

    base_url = request.build_absolute_uri('/')[:-1]
    task_rebuild_episode_fragments.delay(ep.id, base_url)

    # If this removed the last approved edit, the episode is editable again.
    _maybe_unlock_metadata(ep)

    # Exact wash: reverse the trust + every counter this edit banked at approval.
    _reverse_award(edit, membership)
    membership.save()
    logger.info(
        f"{'Speaker edit' if is_speaker else 'Edit'} #{edit.id} rolled back on episode "
        f"'{ep.title}' — user {edit.user.username} penalized -{edit.points or 0} trust"
    )
    messages.success(request, "Edit rolled back and user penalized.")


def _handle_bulk_rollback(request, current_network):
    spammer_id = request.POST.get('spammer_id')
    spammer = get_object_or_404(User, id=spammer_id)
    membership, _ = NetworkMembership.objects.get_or_create(user=spammer, network=current_network)

    approved_edits = EpisodeEditSuggestion.objects.filter(
        user=spammer,
        episode__podcast__network=current_network,
        status=EpisodeEditSuggestion.Status.APPROVED,
    )

    base_url = request.build_absolute_uri('/')[:-1]

    count = 0
    speaker_episode_ids = set()
    affected_eps = {}  # id -> Episode, captured before statuses flip
    for edit in approved_edits:
        ep = edit.episode
        affected_eps[ep.id] = ep
        if 'speaker_mappings' in (edit.suggested_data or {}):
            # Speaker edits: just flip the status — the actual recompute is a single
            # replay per affected episode after the loop (deltas don't restore
            # field-by-field; replay rebuilds from the base + remaining chain).
            edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
            edit.resolved_at = timezone.now()
            edit.save()
            speaker_episode_ids.add(ep.id)
        else:
            _restore_metadata_values(edit, ep, current_network)
            task_rebuild_episode_fragments.delay(ep.id, base_url)
            edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
            edit.resolved_at = timezone.now()
            edit.save()
        # Exact reversal per edit — summed across all the user's edits this equals
        # everything they earned from them (no blanket zeroing needed).
        _reverse_award(edit, membership)
        count += 1

    # Replay once per affected episode now that all of this user's speaker edits
    # are ROLLED_BACK (deduped — a griefer may have many edits on one episode).
    if speaker_episode_ids:
        from pod_manager.services.transcription import apply_speaker_labels
        for ep_id in speaker_episode_ids:
            apply_speaker_labels(ep_id)
            task_rebuild_episode_fragments.delay(ep_id, base_url)

    # Unlock any affected episode left with no approved edits.
    for ep in affected_eps.values():
        _maybe_unlock_metadata(ep)

    membership.save()

    logger.warning(f"Bulk rollback: reverted {count} edits by {spammer.username} on network '{current_network.name}'")
    messages.success(request, f"Bulk rollback complete. Reverted {count} edits and reversed their exact trust/counter awards.")


def handle_run_manual_sync(request, current_network):
    count, error = sync_network_patrons(current_network)
    if error:
        logger.error(f"Manual patron sync failed for network '{current_network.name}': {error}")
        messages.error(request, f"Sync Failed: {error}")
    else:
        logger.info(f"Manual patron sync complete for '{current_network.name}': {count} patrons updated")
        messages.success(request, f"Synced {count} patrons.")


def handle_update_network(request, current_network):
    theme_config_str = request.POST.get('theme_config', '{}')
    try:
        current_network.theme_config = json.loads(theme_config_str)
    except json.JSONDecodeError:
        messages.error(request, f"Invalid JSON format for {current_network.name}. Settings not saved.")
        return redirect(f"{reverse('creator_settings')}?network={current_network.slug}")

    current_network.patreon_campaign_id = request.POST.get('patreon_campaign_id', '')
    current_network.website_url = request.POST.get('website_url', '')
    current_network.default_image_url = request.POST.get('default_image_url', '')
    current_network.ignored_title_tags = request.POST.get('ignored_title_tags', '')
    current_network.description_cut_triggers = request.POST.get('description_cut_triggers', '')
    current_network.global_footer_public = request.POST.get('footer_public', '')
    current_network.global_footer_private = request.POST.get('footer_private', '')

    # Transcription defaults
    current_network.whisper_initial_prompt = request.POST.get('whisper_initial_prompt', '')
    # Blank = use the system default (WHISPER_DEFAULT_MODEL / WHISPER_MODEL) rather
    # than forcing a network-level model.
    current_network.whisper_model    = request.POST.get('whisper_model', '').strip()
    current_network.whisper_language = request.POST.get('whisper_language', 'en').strip() or 'en'
    for field in ('whisper_min_speakers', 'whisper_num_speakers', 'whisper_max_speakers'):
        raw = request.POST.get(field)
        if raw is not None:
            try:
                setattr(current_network, field, int(raw))
            except (TypeError, ValueError):
                pass

    current_network.save()
    messages.success(request, f"{current_network.name} settings saved successfully!")

    base_url = request.build_absolute_uri('/')[:-1]
    for pod in current_network.podcasts.all():
        task_rebuild_podcast_fragments.delay(pod.id, base_url)


def handle_update_show(request, current_network):
    show_id = request.POST.get('show_id')
    show = get_object_or_404(Podcast, id=show_id, network=current_network)
    show.public_feed_url = request.POST.get('public_feed_url', show.public_feed_url)
    show.subscriber_feed_url = request.POST.get('subscriber_feed_url', show.subscriber_feed_url)

    tier_id = request.POST.get('tier_id')
    show.required_tier = get_object_or_404(PatreonTier, id=tier_id, network=current_network) if tier_id else None
    show.show_footer_public = request.POST.get('show_footer_public', '')
    show.show_footer_private = request.POST.get('show_footer_private', '')

    # Transcription overrides — blank/missing = null (inherit from network)
    prompt_raw = request.POST.get('whisper_initial_prompt', None)
    show.whisper_initial_prompt = prompt_raw if prompt_raw is not None else None
    model_raw = request.POST.get('whisper_model', '').strip()
    show.whisper_model    = model_raw or None
    lang_raw = request.POST.get('whisper_language', '').strip()
    show.whisper_language = lang_raw or None
    for field in ('whisper_min_speakers', 'whisper_num_speakers', 'whisper_max_speakers'):
        raw = request.POST.get(field, '').strip()
        try:
            setattr(show, field, int(raw) if raw else None)
        except (TypeError, ValueError):
            setattr(show, field, None)

    # Per-feed R2 serving override (checkbox — absent in POST means unchecked).
    show.force_r2_serve = bool(request.POST.get('force_r2_serve'))

    show.save()
    messages.success(request, f"{show.title} updated successfully!")

    base_url = request.build_absolute_uri('/')[:-1]
    task_rebuild_podcast_fragments.delay(show.id, base_url)


def handle_add_show(request, current_network):
    title = request.POST.get('title')
    slug = request.POST.get('slug')
    tier_id = request.POST.get('tier_id')
    req_tier = get_object_or_404(PatreonTier, id=tier_id, network=current_network) if tier_id else None
    new_show = Podcast.objects.create(
        network=current_network, title=title, slug=slug,
        public_feed_url=request.POST.get('public_feed_url'),
        subscriber_feed_url=request.POST.get('subscriber_feed_url'),
        required_tier=req_tier,
    )
    logger.info(f"Show '{title}' (id={new_show.id}) added to network '{current_network.name}' by {request.user.username}")
    messages.success(request, f"Show '{title}' added! Starting live ingestion...")
    return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&auto_import={new_show.id}")


def handle_merge_episodes(request, current_network):
    pub_id = request.POST.get('public_episode_id')
    priv_id = request.POST.get('private_episode_id')
    if pub_id and priv_id:
        pub_ep = Episode.objects.get(id=pub_id, podcast__network=current_network)
        priv_ep = Episode.objects.get(id=priv_id, podcast__network=current_network)
        pub_ep.guid_private = priv_ep.guid_private or priv_ep.guid_public
        pub_ep.audio_url_subscriber = priv_ep.audio_url_subscriber
        if priv_ep.chapters_private: pub_ep.chapters_private = priv_ep.chapters_private
        if priv_ep.tags and not pub_ep.tags: pub_ep.tags = priv_ep.tags
        pub_ep.match_reason = "Manual Merge (Merge Desk)"
        pub_ep.save()
        priv_ep.delete()
        base_url = request.build_absolute_uri('/')[:-1]
        task_rebuild_episode_fragments.delay(pub_ep.id, base_url)
        logger.info(f"Episodes merged: '{priv_ep.title}' (id={priv_id}) into '{pub_ep.title}' (id={pub_id}) by {request.user.username}")
        messages.success(request, f"Successfully merged '{priv_ep.title}' into '{pub_ep.title}'.")


def handle_split_episode(request, current_network):
    ep = Episode.objects.get(id=request.POST.get('episode_id'), podcast__network=current_network)
    new_ep = Episode.objects.create(
        podcast=ep.podcast, title=ep.title, pub_date=ep.pub_date,
        raw_description=ep.raw_description, clean_description=ep.clean_description,
        duration=ep.duration, link=ep.link, tags=ep.tags,
        guid_private=ep.guid_private, audio_url_subscriber=ep.audio_url_subscriber,
        chapters_private=ep.chapters_private, match_reason="Manually Unpaired",
    )
    ep.guid_private = None
    ep.audio_url_subscriber = ""
    ep.chapters_private = None
    ep.match_reason = "Manually Unpaired"
    ep.save()
    base_url = request.build_absolute_uri('/')[:-1]
    task_rebuild_episode_fragments.delay(ep.id, base_url)
    task_rebuild_episode_fragments.delay(new_ep.id, base_url)
    logger.info(f"Episode split: '{ep.title}' (id={ep.id}) → new episode id={new_ep.id} by {request.user.username}")
    messages.success(request, f"Successfully split '{ep.title}'.")


def handle_move_episodes(request, current_network):
    episode_ids = request.POST.getlist('episode_ids')
    target_podcast_id = request.POST.get('target_podcast_id')
    new_podcast_title = request.POST.get('new_podcast_title', '').strip()
    new_podcast_slug = request.POST.get('new_podcast_slug', '').strip()
    new_podcast_tier_id = request.POST.get('new_podcast_tier_id')

    if not episode_ids or not (target_podcast_id or new_podcast_title):
        return

    source_ep = Episode.objects.filter(id=episode_ids[0], podcast__network=current_network).select_related('podcast').first()
    inherited_image_url = source_ep.podcast.image_url if source_ep else ""

    if new_podcast_title:
        if not new_podcast_slug:
            messages.error(request, "A URL Slug is required to create a new podcast.")
            return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&tab=move")

        if Podcast.objects.filter(network=current_network, slug=new_podcast_slug).exists():
            messages.error(request, f"A podcast with the slug '{new_podcast_slug}' already exists. Please choose another.")
            return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&tab=move")

        req_tier = None
        if new_podcast_tier_id:
            req_tier = get_object_or_404(PatreonTier, id=new_podcast_tier_id, network=current_network)

        target_pod = Podcast.objects.create(
            network=current_network,
            title=new_podcast_title,
            slug=new_podcast_slug,
            required_tier=req_tier,
            image_url=inherited_image_url,
        )
    else:
        target_pod = get_object_or_404(Podcast, id=target_podcast_id, network=current_network)

    eps = Episode.objects.filter(id__in=episode_ids, podcast__network=current_network)
    count = eps.update(podcast=target_pod)
    # An episode moved into a podcast it was cross-published to would now
    # self-reference — drop the redundant links.
    EpisodeCrossPublication.objects.filter(episode_id__in=episode_ids, podcast=target_pod).delete()

    # Re-key any mirrored episodes so their R2 object lands under the new parent's
    # network_id/podcast_id (backup accuracy — section J). Async; idempotent.
    from django.conf import settings as _settings
    if getattr(_settings, 'R2_MIRROR_ENABLED', True):
        from ...tasks import task_rekey_episode_audio
        moved_mirrored = (Episode.objects.filter(id__in=episode_ids)
                          .exclude(r2_url__isnull=True).exclude(r2_url='')
                          .values_list('id', flat=True))
        for ep_id in moved_mirrored:
            task_rekey_episode_audio.delay(ep_id)
    logger.info(f"Moved {count} episodes to '{target_pod.title}' (id={target_pod.id}) by {request.user.username}")
    messages.success(request, f"Successfully moved and locked {count} episodes to '{target_pod.title}'.")

    base_url = request.build_absolute_uri('/')[:-1]
    for ep_id in episode_ids:
        task_rebuild_episode_fragments.delay(int(ep_id), base_url)


def handle_cross_publish_episodes(request, current_network):
    """Bulk-add existing episodes into other podcasts' feeds without changing
    their parent. Accepts multiple targets (the Cross-Publish tab) and falls
    back to the single target_podcast_id field for older callers."""
    episode_ids = request.POST.getlist('episode_ids')
    target_ids = request.POST.getlist('target_podcast_ids') or request.POST.getlist('target_podcast_id')
    if not episode_ids or not target_ids:
        messages.error(request, "Select episodes and at least one target podcast to cross-publish.")
        return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&tab=crosspub")

    targets = list(Podcast.objects.filter(id__in=target_ids, network=current_network))
    if not targets:
        messages.error(request, "No valid target podcasts selected.")
        return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&tab=crosspub")

    eps = list(Episode.objects.filter(id__in=episode_ids, podcast__network=current_network))
    already_linked = set(
        EpisodeCrossPublication.objects.filter(
            podcast__in=targets, episode_id__in=[ep.id for ep in eps]
        ).values_list('episode_id', 'podcast_id')
    )
    new_links = [
        EpisodeCrossPublication(episode=ep, podcast=target, added_by=request.user)
        for ep in eps for target in targets
        if ep.podcast_id != target.id and (ep.id, target.id) not in already_linked
    ]
    EpisodeCrossPublication.objects.bulk_create(new_links, ignore_conflicts=True)
    skipped = len(eps) * len(targets) - len(new_links)
    target_names = ", ".join(t.title for t in targets)
    logger.info(
        f"Cross-published {len(eps)} episodes into [{target_names}] "
        f"({len(new_links)} new links) by {request.user.username}"
    )
    messages.success(
        request,
        f"Added {len(new_links)} feed placement(s) across {len(targets)} podcast(s)."
        + (f" Skipped {skipped} (already placed or parented there)." if skipped else "")
    )


def handle_add_network_mix(request, current_network):
    try:
        tier_id = request.POST.get('tier_id') or None
        req_tier = get_object_or_404(PatreonTier, id=tier_id, network=current_network) if tier_id else None
        mix = NetworkMix.objects.create(
            network=current_network,
            name=request.POST.get('name', '').strip(),
            slug=request.POST.get('slug', '').strip(),
            image_url=request.POST.get('mix_image', '').strip(),
            required_tier=req_tier,
        )
        if 'mix_image_upload' in request.FILES and request.FILES['mix_image_upload']:
            mix.image_upload = request.FILES['mix_image_upload']
        mix.selected_podcasts.set(request.POST.getlist('podcasts'))
        mix.save()
        messages.success(request, f"Network mix '{mix.name}' created.")
    except Exception as e:
        logger.error(f"Failed to create network mix: {e}", exc_info=True)
        messages.error(request, f"Failed to create network mix: {e}")


def handle_edit_network_mix(request, current_network):
    mix = get_object_or_404(NetworkMix, id=request.POST.get('mix_id'), network=current_network)
    mix.name = request.POST.get('name', '').strip() or mix.name
    new_slug = request.POST.get('slug', '').strip()
    if new_slug:
        mix.slug = new_slug

    tier_id = request.POST.get('tier_id') or None
    mix.required_tier = get_object_or_404(PatreonTier, id=tier_id, network=current_network) if tier_id else None

    if 'mix_image_upload' in request.FILES and request.FILES['mix_image_upload']:
        if mix.image_upload:
            mix.image_upload.delete(save=False)
        mix.image_upload = request.FILES['mix_image_upload']
        mix.image_url = ""
    else:
        posted_url = request.POST.get('mix_image', '').strip()
        if posted_url and posted_url != mix.image_url:
            mix.image_url = posted_url
            if mix.image_upload:
                mix.image_upload.delete(save=False)

    mix.selected_podcasts.set(request.POST.getlist('podcasts'))
    cache.delete(f"shell_net_mix_{mix.id}")
    mix.save()
    messages.success(request, f"Network mix '{mix.name}' updated.")


def handle_delete_network_mix(request, current_network):
    mix = get_object_or_404(NetworkMix, id=request.POST.get('mix_id'), network=current_network)
    cache.delete(f"shell_net_mix_{mix.id}")
    name = mix.name
    mix.delete()
    messages.warning(request, f"Network mix '{name}' deleted.")


def handle_generate_s3_report(request, current_network):
    logger.info("=======================================================")
    logger.info("[DIAGNOSTIC] ACTION CAUGHT: generate_s3_report")
    try:
        from ...tasks import task_generate_s3_reports
        result = task_generate_s3_reports.delay()
        logger.info(f"[DIAGNOSTIC] Task dispatched. ID: {result.id}")
        logger.info("=======================================================")
        messages.success(request, "Report generation started! Please wait a few moments and refresh this page.")
    except Exception as e:
        logger.error(f"[DIAGNOSTIC] Failed to dispatch task: {e}")
        messages.error(request, f"Task dispatch error: {e}")

    target_tab = request.GET.get('tab', 'sync')
    return redirect(f"{reverse('creator_settings')}?network={current_network.slug}&tab={target_tab}")


ACTION_HANDLERS = {
    'approve_edit':         _handle_approve_edit,
    'reject_edit':          _handle_reject_edit,
    'rollback_single_edit': _handle_rollback_single_edit,
    'bulk_rollback':        _handle_bulk_rollback,
    'run_manual_sync':      handle_run_manual_sync,
    'update_network':       handle_update_network,
    'update_show':          handle_update_show,
    'add_show':             handle_add_show,
    'merge_episodes':       handle_merge_episodes,
    'split_episode':        handle_split_episode,
    'move_episodes':        handle_move_episodes,
    'cross_publish_episodes': handle_cross_publish_episodes,
    'add_network_mix':      handle_add_network_mix,
    'edit_network_mix':     handle_edit_network_mix,
    'delete_network_mix':   handle_delete_network_mix,
    'generate_s3_report':   handle_generate_s3_report,
}
