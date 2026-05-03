"""
Misc API & utility endpoints: Traefik dynamic config, audio status check,
avatar preferences, custom avatar uploads, SSE feed-import streamer, and
profile preference toggles.
"""
import asyncio
import logging
import os

import requests

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.core.files.base import ContentFile
from django.http import (
    JsonResponse, HttpResponseForbidden, StreamingHttpResponse,
)
from django.shortcuts import redirect
from django.views.decorators.http import require_POST

from ..models import NetworkMembership, Network, PatronProfile
from ..tasks import task_ingest_feed
from ..utils import validate_public_url

logger = logging.getLogger(__name__)


def process_mix_image_url(image_url, mix_instance):
    if not image_url: return None
    ok, reason = validate_public_url(image_url)
    if not ok:
        return reason
    try:
        res = requests.get(image_url, timeout=5)
        if res.status_code == 200:
            temp_name = os.path.basename(image_url).split('?')[0] or "cover.jpg"
            mix_instance.image_upload.save(temp_name, ContentFile(res.content), save=False)
            mix_instance.image_url = ""
            return None
        return f"Server returned status {res.status_code}."
    except requests.exceptions.RequestException:
        return "URL invalid or unreachable."


@login_required(login_url='/login/')
@require_POST
def update_avatar_preference(request):
    source = request.POST.get('source')
    if source in ['patreon', 'discord', 'custom']:
        NetworkMembership.objects.filter(user=request.user, network=request.network).update(preferred_avatar_source=source)
        return JsonResponse({'status': 'success'})
    return JsonResponse({'status': 'error', 'message': 'Invalid source'}, status=400)


@login_required(login_url='/login/')
@require_POST
def upload_custom_avatar(request):
    membership = NetworkMembership.objects.filter(user=request.user, network=request.network).first()
    if membership:
        if 'custom_image_upload' in request.FILES and request.FILES['custom_image_upload']:
            if membership.custom_image_upload:
                membership.custom_image_upload.delete(save=False)

            membership.custom_image_upload = request.FILES['custom_image_upload']
            membership.custom_image_url = ""

        elif request.POST.get('custom_image_url'):
            candidate_url = request.POST.get('custom_image_url').strip()
            ok, reason = validate_public_url(candidate_url)
            if not ok:
                messages.error(request, f"Avatar URL rejected: {reason}")
                return redirect('user_profile')

            membership.custom_image_url = candidate_url

            if membership.custom_image_upload:
                membership.custom_image_upload.delete(save=False)

        membership.preferred_avatar_source = 'custom'
        membership.save()
        messages.success(request, "Custom avatar updated!")

    return redirect('user_profile')


def traefik_config_api(request):
    expected_token = getattr(settings, 'TRAEFIK_API_TOKEN', None)
    if request.GET.get('token') != expected_token: return HttpResponseForbidden("Unauthorized access.")

    routers = {}
    networks = Network.objects.exclude(custom_domain__isnull=True).exclude(custom_domain__exact='')

    for network in networks:
        routers[f"custom-domain-{network.id}"] = {
            "rule": f"Host(`{network.custom_domain}`)",
            "entryPoints": ["https"],
            "service": "vecto-service@file",
            "tls": {"certResolver": "http_resolver"}
        }

    return JsonResponse({"http": {"routers": routers}})


@login_required(login_url='/login/')
def stream_feed_import(request, show_id):
    task_id = f"import_logs_{show_id}"

    if not cache.get(task_id):
        cache.set(task_id, "data: [QUEUED] Waiting for Celery worker...\n\n", timeout=3600)
        task_ingest_feed.delay(show_id)

    async def event_stream():
        last_length = 0
        while True:
            logs = await cache.aget(task_id, "")
            if len(logs) > last_length:
                new_logs = logs[last_length:]
                yield new_logs
                last_length = len(logs)
                if "[DONE]" in new_logs:
                    await cache.adelete(task_id)
                    break
            await asyncio.sleep(0.5)

    return StreamingHttpResponse(event_stream(), content_type='text/event-stream')


def check_audio_status(request):
    """Async endpoint for the frontend to verify MP3 reachability without blocking page loads."""
    url = request.GET.get('url')
    if not url:
        return JsonResponse({'reachable': False})
    # Endpoint is publicly callable, so the URL is fully attacker-controlled —
    # block redirects and any URL that resolves to a non-public address.
    ok, _ = validate_public_url(url)
    if not ok:
        return JsonResponse({'reachable': False})
    try:
        # allow_redirects=False so a redirect chain can't bounce us into a
        # private network the initial host wouldn't have allowed.
        res = requests.head(url, timeout=3, allow_redirects=False)
        return JsonResponse({'reachable': res.status_code < 400})
    except requests.RequestException:
        return JsonResponse({'reachable': False})


@login_required(login_url='/login/')
@require_POST
def toggle_totp_mode(request):
    mode = request.POST.get('mode', '')
    if mode not in (PatronProfile.TOTP_REPLACE, PatronProfile.TOTP_MFA):
        return JsonResponse({'error': 'Invalid mode'}, status=400)

    if mode == PatronProfile.TOTP_MFA and not request.user.totpdevice_set.filter(confirmed=True).exists():
        return JsonResponse({'error': 'No authenticator configured'}, status=400)

    profile, _ = PatronProfile.objects.get_or_create(user=request.user)
    profile.totp_mode = mode
    profile.save(update_fields=['totp_mode'])
    return JsonResponse({'ok': True, 'totp_mode': mode})
