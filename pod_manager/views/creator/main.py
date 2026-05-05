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
from ...services.edits import (
    apply_approved_edit, parse_chapter_payload, snapshot_episode,
    update_contribution_stats,
)
from ...tasks import task_rebuild_episode_fragments
from ...utils import sanitize_user_html
from .actions import ACTION_HANDLERS
from .data import (
    gather_audit_log,
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

        network = ep.podcast.network
        membership, _ = NetworkMembership.objects.get_or_create(user=request.user, network=network)
        original_data = snapshot_episode(ep)
        is_first = not EpisodeEditSuggestion.objects.filter(episode=ep, status='approved').exists()
        is_trusted = membership.trust_score >= network.auto_approve_trust_threshold

        EpisodeEditSuggestion.objects.create(
            episode=ep, user=request.user, suggested_data=suggested_data,
            original_data=original_data, status='approved' if is_trusted else 'pending',
            is_first_responder=is_first,
            resolved_at=timezone.now() if is_trusted else None,
        )

        if is_trusted:
            apply_approved_edit(ep, suggested_data)
            update_contribution_stats(membership, suggested_data, original_data, is_first=is_first)
            task_rebuild_episode_fragments.delay(ep.id, request.build_absolute_uri('/')[:-1])
            messages.success(request, "Edit approved instantly. +5 Trust.")
        else:
            messages.success(request, "Edit submitted for review.")

    except Exception as e:
        logger.error(f"Edit submission failed for Episode {ep.id}: {e}", exc_info=True)
        messages.error(request, "Failed to submit edit.")

    return redirect('episode_detail', episode_id=ep.id)
