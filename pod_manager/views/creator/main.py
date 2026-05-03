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
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from ...models import Episode, Network, NetworkMembership, EpisodeEditSuggestion
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


@login_required(login_url='/login/')
def submit_episode_edit(request, episode_id):
    if request.method != 'POST':
        return HttpResponseForbidden("Only POST allowed")

    ep = get_object_or_404(Episode, id=episode_id)
    payload_str = request.POST.get('payload')
    if not payload_str:
        return redirect('episode_detail', episode_id=ep.id)

    try:
        suggested_data = json.loads(payload_str)

        # --- STRICT CHAPTER & LOCATION SANITIZATION ---
        raw_chapters_input = suggested_data.get('chapters', [])

        is_dict_format = isinstance(raw_chapters_input, dict)
        raw_chap_list = raw_chapters_input.get('chapters', []) if is_dict_format else raw_chapters_input
        waypoints_enabled = raw_chapters_input.get('waypoints', False) if is_dict_format else False

        clean_chapters = []
        for chap in raw_chap_list:
            if 'startTime' in chap and 'title' in chap:
                try:
                    start_val = float(chap['startTime'])
                    start_time = int(start_val) if start_val.is_integer() else start_val

                    c = {
                        "startTime": start_time,
                        "title": str(chap['title']).strip()
                    }
                    if 'endTime' in chap and chap['endTime'] not in [None, ""]:
                        end_val = float(chap['endTime'])
                        c['endTime'] = int(end_val) if end_val.is_integer() else end_val
                    if 'url' in chap and str(chap['url']).startswith('http'):
                        c['url'] = str(chap['url']).strip()
                    if 'img' in chap and str(chap['img']).startswith('http'):
                        c['img'] = str(chap['img']).strip()
                    if 'toc' in chap and chap['toc'] is False:
                        c['toc'] = False

                    if 'location' in chap and isinstance(chap['location'], dict):
                        loc = chap['location']
                        if 'name' in loc and 'geo' in loc:
                            c_loc = {
                                "name": str(loc['name']).strip(),
                                "geo": str(loc['geo']).strip()
                            }
                            if 'osm' in loc and loc['osm']:
                                c_loc['osm'] = str(loc['osm']).strip()
                            c['location'] = c_loc

                    clean_chapters.append(c)
                except ValueError:
                    pass

        suggested_data['chapters'] = {"version": "1.2.0", "chapters": clean_chapters}
        if waypoints_enabled:
            suggested_data['chapters']["waypoints"] = True

        if 'description' in suggested_data:
            suggested_data['description'] = sanitize_user_html(suggested_data.get('description') or '')

        network = ep.podcast.network
        membership, _ = NetworkMembership.objects.get_or_create(user=request.user, network=network)

        original_data = {"title": ep.title, "description": ep.clean_description, "tags": ep.tags or [], "chapters": ep.chapters_public or []}
        is_first = not EpisodeEditSuggestion.objects.filter(episode=ep, status='approved').exists()

        is_trusted = membership.trust_score >= network.auto_approve_trust_threshold
        final_status = 'approved' if is_trusted else 'pending'

        EpisodeEditSuggestion.objects.create(
            episode=ep, user=request.user, suggested_data=suggested_data,
            original_data=original_data, status=final_status, is_first_responder=is_first,
            resolved_at=timezone.now() if is_trusted else None,
        )

        if is_trusted:
            ep.title = suggested_data.get('title', ep.title)
            ep.clean_description = suggested_data.get('description', ep.clean_description)
            ep.tags = suggested_data.get('tags', ep.tags)
            new_chapters = suggested_data.get('chapters', ep.chapters_public)
            ep.chapters_public = new_chapters
            ep.chapters_private = new_chapters
            ep.is_metadata_locked = True
            ep.save()

            base_url = request.build_absolute_uri('/')[:-1]
            task_rebuild_episode_fragments.delay(ep.id, base_url)

            membership.trust_score += 5
            if suggested_data.get('title') != original_data.get('title'): membership.edits_title += 1
            if suggested_data.get('chapters') != original_data.get('chapters'):
                chap_data = suggested_data.get('chapters', [])
                if isinstance(chap_data, dict):
                    membership.edits_chapters += len(chap_data.get('chapters', []))
                else:
                    membership.edits_chapters += len(chap_data)
            if suggested_data.get('tags') != original_data.get('tags'): membership.edits_tags += 1
            if suggested_data.get('description') != original_data.get('description'): membership.edits_descriptions += 1
            if is_first: membership.first_responder_count += 1
            membership.save()
            messages.success(request, "Edit approved instantly. +5 Trust.")
        else:
            messages.success(request, "Edit submitted for review.")

    except Exception as e:
        logger.error(f"Edit submission failed for Episode {ep.id}: {e}", exc_info=True)
        messages.error(request, "Failed to submit edit.")

    return redirect('episode_detail', episode_id=ep.id)
