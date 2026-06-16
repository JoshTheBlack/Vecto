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
from ...services.edits import chapter_items
from ...services.patreon import sync_network_patrons
from ...tasks import task_rebuild_episode_fragments, task_rebuild_podcast_fragments
from ...utils import sanitize_user_html

logger = logging.getLogger(__name__)


def _check_scalar_approval(request, approve_key, value_key, snapshot_val, ep_field, suggested_key, ep, edit, membership, counter_attr, sanitize=False):
    if request.POST.get(approve_key) != 'on':
        return 0
    raw_val = request.POST.get(value_key, '').strip()
    new_val = sanitize_user_html(raw_val) if sanitize else raw_val
    if not new_val or new_val == snapshot_val:
        return 0
    setattr(ep, ep_field, new_val)
    edit.suggested_data[suggested_key] = new_val
    setattr(membership, counter_attr, getattr(membership, counter_attr) + 1)
    return 1


def _handle_approve_edit(request, current_network):
    edit_id = request.POST.get('edit_id')
    edit = get_object_or_404(EpisodeEditSuggestion, id=edit_id, episode__podcast__network=current_network)
    membership, _ = NetworkMembership.objects.get_or_create(user=edit.user, network=current_network)

    ep = edit.episode
    points = 0

    # Snapshot the live episode state RIGHT NOW, before any field is touched.
    # This becomes the new edit.original_data after approval, so single-edit
    # rollback restores the actual pre-approval state.
    pre_approval_snapshot = {
        'title': ep.title,
        'description': ep.clean_description or '',
        'tags': list(ep.tags or []),
        'chapters': ep.chapters_public if ep.chapters_public is not None else [],
        'cross_publish_podcast_ids': sorted(ep.cross_publications.values_list('podcast_id', flat=True)),
    }
    # User's submission-time snapshot, used to compute deltas.
    user_snapshot = edit.original_data or {}

    # 0. PROCESS TITLE
    points += _check_scalar_approval(
        request, 'approve_title', 'edited_title', pre_approval_snapshot['title'],
        'title', 'title', ep, edit, membership, 'edits_title',
    )

    # 1. PROCESS DESCRIPTION
    points += _check_scalar_approval(
        request, 'approve_description', 'edited_description', pre_approval_snapshot['description'],
        'clean_description', 'description', ep, edit, membership, 'edits_descriptions', sanitize=True,
    )

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
                    points += 1

                    if added:
                        membership.edits_tags += len(added)
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
                    points += 1

                    if isinstance(new_chapters, dict):
                        membership.edits_chapters += len(new_chapters.get('chapters', []))
                    elif isinstance(new_chapters, list):
                        membership.edits_chapters += len(new_chapters)
            except Exception as e:
                logger.warning(f"Failed to parse chapters from inbox for edit #{edit_id}: {e}")

    # 4. PROCESS SEASON NUMBER
    if request.POST.get('approve_season_number') == 'on':
        raw_val = request.POST.get('edited_season_number', '').strip()
        new_val = int(raw_val) if raw_val.isdigit() else None
        if new_val != pre_approval_snapshot.get('season_number'):
            ep.season_number = new_val
            edit.suggested_data['season_number'] = new_val
            points += 1

    # 5. PROCESS EPISODE NUMBER
    if request.POST.get('approve_episode_number') == 'on':
        raw_val = request.POST.get('edited_episode_number', '').strip()
        new_val = int(raw_val) if raw_val.isdigit() else None
        if new_val != pre_approval_snapshot.get('episode_number'):
            ep.episode_number = new_val
            edit.suggested_data['episode_number'] = new_val
            points += 1

    # 6. PROCESS EPISODE TYPE
    if request.POST.get('approve_episode_type') == 'on':
        new_val = request.POST.get('edited_episode_type', '').strip()[:50]
        if new_val != (pre_approval_snapshot.get('episode_type') or ''):
            ep.episode_type = new_val
            edit.suggested_data['episode_type'] = new_val
            points += 1

    # 7. PROCESS CROSS-PUBLISH TARGETS — full replacement of the link set.
    if request.POST.get('approve_cross_publish') == 'on':
        raw_ids = request.POST.get('edited_cross_publish_ids', '')
        try:
            new_ids = json.loads(raw_ids) if raw_ids else []
            targets = list(validate_cross_targets(ep, new_ids, current_network))
            new_id_list = sorted(t.id for t in targets)
            if new_id_list != pre_approval_snapshot['cross_publish_podcast_ids']:
                sync_cross_publications(ep, targets, added_by=edit.user)
                edit.suggested_data['cross_publish_podcast_ids'] = new_id_list
                points += 1
        except Exception as e:
            logger.warning(f"Failed to parse cross-publish ids from inbox for edit #{edit_id}: {e}")

    # --- SPEAKER LABEL APPROVAL ---
    # Speaker label edits carry suggested_data = {"speaker_mappings": {...}}
    # and bypass the normal field-approval flow — the whole mapping is applied atomically.
    speaker_mappings = edit.suggested_data.get('speaker_mappings')
    if speaker_mappings and isinstance(speaker_mappings, dict) and request.POST.get('approve_speaker_labels') == 'on':
        try:
            from pod_manager.services.transcription import apply_speaker_labels
            apply_speaker_labels(ep.id, speaker_mappings)
            # Bust the RSS fragment cache so the updated transcript is picked up
            network = ep.podcast.network
            if network.custom_domain:
                _base = f"https://{network.custom_domain}".rstrip('/')
            else:
                _base = getattr(settings, 'SITE_URL', 'http://localhost:8000').rstrip('/')
            task_rebuild_episode_fragments.delay(ep.id, _base)
            points += 1
        except Exception as e:
            logger.error("Speaker label approval failed for episode %d: %s", ep.id, e)

    # --- THE ZERO-APPROVAL TRAP ---
    if points == 0:
        edit.status = EpisodeEditSuggestion.Status.REJECTED
        edit.resolved_at = timezone.now()
        edit.save()
        membership.trust_score = max(0, membership.trust_score - 2)
        membership.save()
        messages.warning(request, "No sections selected for approval. Edit converted to rejection. User penalized -2 Trust.")
        return

    # --- PERFECT SWEEP BONUS ---
    if points == 3:
        points += 2

    # Speaker-only edits don't touch Episode model fields — skip lock + save
    is_speaker_only = set(edit.suggested_data.keys()) == {'speaker_mappings'}
    if not is_speaker_only:
        ep.is_metadata_locked = True
        ep.save()

    # Rewrite original_data to the pre-approval snapshot so single-edit
    # rollback restores the state that existed right before this approval.
    # For speaker-only edits the original_data is already the prior mappings.
    if not is_speaker_only:
        edit.original_data = pre_approval_snapshot
    edit.status = EpisodeEditSuggestion.Status.APPROVED
    edit.resolved_at = timezone.now()
    edit.save()

    membership.trust_score += points
    if edit.is_first_responder:
        membership.first_responder_count += 1
    membership.save()

    logger.info(f"Edit #{edit.id} approved for episode '{ep.title}' — user {edit.user.username} awarded +{points} trust")
    messages.success(request, f"Partial edit approved! User awarded +{points} Trust Score.")

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

    # Block when newer approved edits exist on the same episode.
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
    ep.title = edit.original_data.get('title', ep.title)
    ep.clean_description = edit.original_data.get('description', ep.clean_description)
    ep.tags = edit.original_data.get('tags', ep.tags)
    ep.chapters_public = edit.original_data.get('chapters', ep.chapters_public)
    ep.save()

    # Key-presence guard: pre-feature edits have no cross-publish snapshot and
    # must not wipe the episode's current links.
    if 'cross_publish_podcast_ids' in (edit.original_data or {}):
        restore_ids = edit.original_data.get('cross_publish_podcast_ids') or []
        sync_cross_publications(ep, validate_cross_targets(ep, restore_ids, current_network))

    base_url = request.build_absolute_uri('/')[:-1]
    task_rebuild_episode_fragments.delay(ep.id, base_url)

    edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
    edit.resolved_at = timezone.now()
    edit.save()

    membership.trust_score = max(0, membership.trust_score - 5)
    if edit.suggested_data.get('title') != edit.original_data.get('title'): membership.edits_title = max(0, membership.edits_title - 1)
    if edit.suggested_data.get('chapters') != edit.original_data.get('chapters'): membership.edits_chapters = max(0, membership.edits_chapters - len(edit.suggested_data.get('chapters', [])))
    if edit.suggested_data.get('tags') != edit.original_data.get('tags'): membership.edits_tags = max(0, membership.edits_tags - 1)
    if edit.suggested_data.get('description') != edit.original_data.get('description'): membership.edits_descriptions = max(0, membership.edits_descriptions - 1)
    if edit.is_first_responder: membership.first_responder_count = max(0, membership.first_responder_count - 1)
    membership.save()
    logger.info(f"Edit #{edit.id} rolled back on episode '{ep.title}' — user {edit.user.username} penalized -5 trust")
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
    for edit in approved_edits:
        ep = edit.episode
        ep.title = edit.original_data.get('title', ep.title)
        ep.clean_description = edit.original_data.get('description', ep.clean_description)
        ep.tags = edit.original_data.get('tags', ep.tags)
        ep.chapters_public = edit.original_data.get('chapters', ep.chapters_public)
        ep.save()

        if 'cross_publish_podcast_ids' in (edit.original_data or {}):
            restore_ids = edit.original_data.get('cross_publish_podcast_ids') or []
            sync_cross_publications(ep, validate_cross_targets(ep, restore_ids, current_network))

        task_rebuild_episode_fragments.delay(ep.id, base_url)

        edit.status = EpisodeEditSuggestion.Status.ROLLED_BACK
        edit.resolved_at = timezone.now()
        edit.save()
        count += 1

    # Nuke their stats for this network
    membership.trust_score = 0
    membership.edits_chapters = 0
    membership.edits_tags = 0
    membership.edits_descriptions = 0
    membership.first_responder_count = 0
    membership.save()

    logger.warning(f"Bulk rollback: reverted {count} edits by {spammer.username} on network '{current_network.name}' — trust zeroed")
    messages.success(request, f"Bulk rollback complete. Reverted {count} edits and dropped trust score to 0.")


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
    current_network.whisper_model    = request.POST.get('whisper_model', 'medium.en').strip() or 'medium.en'
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
