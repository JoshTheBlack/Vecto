"""
Thin orchestrators: creator_settings (dispatches POST actions, assembles GET
context from gather_* functions) and submit_episode_edit.
"""
import json
import logging
import time

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
from ...utils import sanitize_user_html
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
    t_total = time.time()
    logger.info(f"--- [DIAGNOSTIC] Loading Creator Settings for {request.user} ---")

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

    logger.info(f"[DIAGNOSTIC] PAGE READY. Total: {time.time() - t_total:.2f}s")
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

        EpisodeEditSuggestion.objects.create(
            episode=ep, user=request.user, suggested_data=suggested_data,
            original_data=original_data, status=EpisodeEditSuggestion.Status.APPROVED if is_trusted else EpisodeEditSuggestion.Status.PENDING,
            is_first_responder=is_first,
            resolved_at=timezone.now() if is_trusted else None,
        )

        if is_trusted:
            apply_approved_edit(ep, suggested_data, user=request.user)
            update_contribution_stats(membership, suggested_data, original_data, is_first=is_first)
            task_rebuild_episode_fragments.delay(ep.id, request.build_absolute_uri('/')[:-1])
            messages.success(request, "Edit approved instantly. +5 Trust.")
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

    # Capture existing mappings as original_data so rollback is meaningful
    existing_mappings = {}
    try:
        from pod_manager.services.transcription import transcript_path
        if hasattr(ep, 'transcript') and ep.transcript.words_json_file:
            words_path = transcript_path(ep.id, 'words')
            doc = _json.loads(words_path.read_text(encoding='utf-8'))
            existing_mappings = doc.get('speaker_mappings', {})
    except Exception:
        pass

    edit = EpisodeEditSuggestion.objects.create(
        episode=ep,
        user=request.user,
        suggested_data={'speaker_mappings': safe_mappings},
        original_data={'speaker_mappings': existing_mappings},
        status=EpisodeEditSuggestion.Status.APPROVED if is_trusted else EpisodeEditSuggestion.Status.PENDING,
        resolved_at=timezone.now() if is_trusted else None,
    )

    if is_trusted:
        from pod_manager.services.edits import apply_approved_edit
        apply_approved_edit(ep, {'speaker_mappings': safe_mappings})
        membership.trust_score += 5
        membership.save()
        from pod_manager.tasks import task_rebuild_episode_fragments
        task_rebuild_episode_fragments.delay(ep.id, request.build_absolute_uri('/')[:-1])
        return JsonResponse({'status': 'approved', 'edit_id': edit.id})

    return JsonResponse({'status': 'pending', 'edit_id': edit.id})