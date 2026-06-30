"""
Thin orchestrators: creator_settings (dispatches POST actions, assembles GET
context from gather_* functions) and submit_episode_edit.
"""
import json
import logging

from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import HttpResponseForbidden
from django.views.decorators.http import require_POST
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from ...models import Episode, Network, NetworkMembership, EpisodeEditSuggestion
from ...services.cross_publish import validate_cross_targets
from ...services.edits import (
    apply_approved_edit, chapter_items, parse_chapter_payload, snapshot_episode,
    update_contribution_stats,
)
from ...tasks import task_rebuild_episode_fragments
from ...utils import diagnostic_page, sanitize_user_html
from .actions import ACTION_HANDLERS
from .data import (
    gather_audit_log,
    gather_cross_publish_context,
    gather_inbox,
    gather_manage_podcasts,
    gather_merge_desk,
    gather_move_context,
    gather_reports_data,
)

logger = logging.getLogger(__name__)


@login_required(login_url='/login/')
def creator_settings(request):
    allowed_networks = Network.objects.all() if request.user.is_superuser else Network.objects.filter(owners=request.user)
    if not allowed_networks.exists():
        return HttpResponseForbidden("No creator access.")

    current_network = allowed_networks.filter(slug=request.GET.get('network')).first() or allowed_networks.first()

    if request.method == 'POST':
        action = request.POST.get('action')
        handler = ACTION_HANDLERS.get(action)
        if handler:
            response = handler(request, current_network)
            if response is not None:
                return response

        target_tab = request.GET.get('tab', '')
        redirect_url = f"{reverse('creator_settings')}?network={current_network.slug}"
        if target_tab:
            redirect_url += f"&tab={target_tab}"
        return redirect(redirect_url)

    # GET
    context = {
        'networks': allowed_networks,
        'current_network': current_network,
        'theme_config_json': json.dumps(current_network.theme_config, indent=2),
        **gather_manage_podcasts(current_network),
        **gather_inbox(current_network),
        **gather_merge_desk(request, current_network),
        **gather_audit_log(request, current_network),
        **gather_move_context(request, current_network),
        **gather_cross_publish_context(request, current_network),
        **gather_reports_data(),
    }

    return render(request, 'pod_manager/creator_settings.html', context)


@require_POST
@login_required(login_url='/login/')
def submit_episode_edit(request, episode_id):
    ep = get_object_or_404(Episode, id=episode_id)
    payload_str = request.POST.get('payload')
    if not payload_str:
        return redirect('episode_detail', episode_id=ep.id)

    try:
        suggested_data = json.loads(payload_str)
        suggested_data['chapters'] = parse_chapter_payload(suggested_data.get('chapters', []))
        if 'description' in suggested_data:
            suggested_data['description'] = sanitize_user_html(suggested_data.get('description') or '')
        for int_field in ('season_number', 'episode_number'):
            if int_field in suggested_data:
                try:
                    val = suggested_data[int_field]
                    suggested_data[int_field] = int(val) if val not in (None, '', 0) else None
                except (ValueError, TypeError):
                    suggested_data.pop(int_field)
        if 'episode_type' in suggested_data:
            suggested_data['episode_type'] = str(suggested_data['episode_type'])[:50]
        if 'cross_publish_podcast_ids' in suggested_data:
            # Cross-publish is owner/admin only (user_edit_rollback.md §8a). The
            # community suggestion form no longer offers it; drop the key server-side
            # (authoritative) so a crafted POST can't bypass the hidden UI. Owners use
            # the dedicated manage_episode → update_cross_publish action instead.
            from .publish import _require_owner
            if not _require_owner(request.user, ep.podcast.network):
                suggested_data.pop('cross_publish_podcast_ids')
            else:
                # Strip foreign-network ids and the parent podcast before storing.
                targets = validate_cross_targets(
                    ep, suggested_data['cross_publish_podcast_ids'], ep.podcast.network
                )
                suggested_data['cross_publish_podcast_ids'] = sorted(t.id for t in targets)

        network = ep.podcast.network
        membership, _ = NetworkMembership.objects.get_or_create(user=request.user, network=network)
        original_data = snapshot_episode(ep)

        # Drop fields the user didn't actually change. The edit form always
        # posts every field, but untouched ones would render in the inbox with
        # Approve pre-toggled and pollute the audit log with phantom edits.
        comparators = {
            'tags': lambda s, o: set(s or []) != set(o or []),
            'chapters': lambda s, o: chapter_items(s) != chapter_items(o),
            'cross_publish_podcast_ids': lambda s, o: sorted(s or []) != sorted(o or []),
        }
        for key in list(suggested_data.keys()):
            if key not in original_data:
                continue
            differs = comparators.get(key, lambda s, o: s != o)
            if not differs(suggested_data[key], original_data[key]):
                suggested_data.pop(key)

        if not suggested_data:
            logger.info(f"Edit submission for Episode {ep.id} by {request.user.username} contained no changes — skipped.")
            messages.info(request, "No changes detected — nothing was submitted.")
            return redirect('episode_detail', episode_id=ep.id)

        is_first = not EpisodeEditSuggestion.objects.filter(episode=ep, status=EpisodeEditSuggestion.Status.APPROVED).exists()
        is_trusted = membership.trust_score >= network.auto_approve_trust_threshold

        edit = EpisodeEditSuggestion.objects.create(
            episode=ep, user=request.user, suggested_data=suggested_data,
            original_data=original_data, status=EpisodeEditSuggestion.Status.APPROVED if is_trusted else EpisodeEditSuggestion.Status.PENDING,
            is_first_responder=is_first,
            resolved_at=timezone.now() if is_trusted else None,
        )

        if is_trusted:
            apply_approved_edit(ep, suggested_data, user=request.user)
            # Auto-approval scores identically to a manual inbox approval.
            points, deltas = update_contribution_stats(membership, suggested_data, original_data, is_first=is_first)
            # Bank trust + per-counter deltas so a later rollback is an exact wash.
            edit.points = points
            edit.counter_deltas = deltas
            edit.save(update_fields=['points', 'counter_deltas'])
            task_rebuild_episode_fragments.delay(ep.id, request.build_absolute_uri('/')[:-1])
            messages.success(request, f"Edit approved instantly. +{points} Trust.")
        else:
            messages.success(request, "Edit submitted for review.")

    except Exception as e:
        logger.error(f"Edit submission failed for Episode {ep.id}: {e}", exc_info=True)
        messages.error(request, "Failed to submit edit.")

    return redirect('episode_detail', episode_id=ep.id)


@require_POST
@login_required(login_url='/login/')
def submit_speaker_labels(request, episode_id):
    """Dedicated endpoint for speaker label edits.

    Creates an EpisodeEditSuggestion whose suggested_data contains ONLY
    {'speaker_mappings': {...}} so the inbox diff shows a targeted speaker
    section rather than a full episode field diff.
    """
    import json as _json
    from django.http import JsonResponse
    ep = get_object_or_404(Episode, id=episode_id)

    try:
        body = _json.loads(request.body or '{}')
    except (ValueError, TypeError):
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    mappings = body.get('speaker_mappings', {})
    if not mappings or not isinstance(mappings, dict):
        return JsonResponse({'error': 'No speaker mappings provided'}, status=400)

    safe_mappings = {str(k)[:20]: str(v)[:80] for k, v in mappings.items() if str(k).strip()}
    if not safe_mappings:
        return JsonResponse({'error': 'No valid mappings'}, status=400)

    network = ep.podcast.network
    membership, _ = NetworkMembership.objects.get_or_create(user=request.user, network=network)
    is_trusted = membership.trust_score >= network.auto_approve_trust_threshold

    # Capture existing mappings as original_data so rollback is meaningful, and the
    # known speaker_id set so we can reject keys that aren't real diarization labels
    # for this episode (§5.1 grief resistance). speaker_id is the immutable base;
    # pre-backfill .words fall back to seg.speaker (mirrors apply_speaker_labels).
    existing_mappings = {}
    known_ids = set()
    try:
        from pod_manager.services.transcription import read_transcript_bytes
        if hasattr(ep, 'transcript') and ep.transcript.words_json_file:
            doc = _json.loads(read_transcript_bytes(ep.id, 'words', ep.transcript.version).decode('utf-8'))
            existing_mappings = doc.get('speaker_mappings', {})
            for seg in doc.get('segments', []):
                sid = seg.get('speaker_id') or seg.get('speaker')
                if sid:
                    known_ids.add(sid)
                for w in seg.get('words', []):
                    wid = w.get('speaker_id') or w.get('speaker')
                    if wid:
                        known_ids.add(wid)
    except Exception:
        pass

    # Reject unknown keys before any scoring/banking so trust reflects only valid
    # speaker_ids. Only enforce when we actually have a known set — an unreadable
    # .words can't validate, and degrading shouldn't lock out the feature.
    if known_ids:
        unknown = [k for k in safe_mappings if k not in known_ids]
        if unknown:
            return JsonResponse(
                {'error': f'Unknown speaker id(s): {", ".join(sorted(unknown))}'},
                status=400,
            )

    # Per-speaker award (user_edit_rollback.md §3.4) replaces the old flat +5.
    # existing_mappings is the .words header cache = the fold BEFORE this edit, so
    # it's the prior mapping the helper needs to tell naming from correction.
    speaker_points = 0
    if is_trusted:
        from pod_manager.services.transcription import speaker_edit_points
        speaker_points, _newly_named = speaker_edit_points(safe_mappings, existing_mappings)

    edit = EpisodeEditSuggestion.objects.create(
        episode=ep,
        user=request.user,
        suggested_data={'speaker_mappings': safe_mappings},
        original_data={'speaker_mappings': existing_mappings},
        status=EpisodeEditSuggestion.Status.APPROVED if is_trusted else EpisodeEditSuggestion.Status.PENDING,
        resolved_at=timezone.now() if is_trusted else None,
        points=speaker_points if is_trusted else 0,
        # edits_speakers is reversed generically from counter_deltas on rollback.
        counter_deltas={'edits_speakers': speaker_points} if (is_trusted and speaker_points) else {},
    )

    if is_trusted:
        from pod_manager.services.edits import apply_approved_edit
        apply_approved_edit(ep, {'speaker_mappings': safe_mappings})
        membership.trust_score += speaker_points
        if speaker_points:
            membership.edits_speakers += speaker_points
        membership.save()
        from pod_manager.tasks import task_rebuild_episode_fragments
        task_rebuild_episode_fragments.delay(ep.id, request.build_absolute_uri('/')[:-1])
        # apply_approved_edit() bumped Transcript.version; hand it back so the page
        # can refetch the rewritten .words at the canonical ?v=N (clean cache key).
        new_version = None
        try:
            ep.transcript.refresh_from_db(fields=['version'])
            new_version = ep.transcript.version
        except Exception:
            pass
        return JsonResponse({'status': 'approved', 'edit_id': edit.id, 'version': new_version})

    return JsonResponse({'status': 'pending', 'edit_id': edit.id})