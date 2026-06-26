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

from django.shortcuts import get_object_or_404

from ..models import NetworkMembership, Network, PatronProfile, Podcast, Episode, Transcript
from ..tasks import task_ingest_feed
from ..utils import get_membership, validate_public_url

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
        logger.warning(f"Mix image fetch returned {res.status_code} for URL: {image_url}")
        return f"Server returned status {res.status_code}."
    except requests.exceptions.RequestException as e:
        logger.warning(f"Mix image fetch failed for URL {image_url}: {e}")
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
    membership = get_membership(request)
    if membership:
        if 'custom_image_upload' in request.FILES and request.FILES['custom_image_upload']:
            # No pre-delete: the stable key is deterministic and storage
            # overwrites in place, so save() PUTs over any existing object —
            # an explicit delete would just add a round-trip and a momentary
            # 404 gap before the PUT lands.
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
    get_object_or_404(Podcast, id=show_id, network=request.network)
    task_id = f"import_logs_{show_id}"

    if not cache.get(task_id):
        cache.set(task_id, "data: [QUEUED] Waiting for Celery worker...\n\n", timeout=3600)
        task_ingest_feed.delay(show_id)

    async def event_stream():
        last_length = 0
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 120
        while True:
            if loop.time() >= deadline:
                yield "data: [ERROR] Timed out waiting for Celery worker. Is the worker running?\n\n"
                yield "data: [DONE]\n\n"
                await cache.adelete(task_id)
                break
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
    logger.info(f"TOTP mode set to '{mode}' for user {request.user.username}")
    return JsonResponse({'ok': True, 'totp_mode': mode})


@require_POST
def backfill_transcripts_api(request):
    """
    Staff-only endpoint to queue transcript backfill for a podcast or all podcasts.

    POST body (JSON):
        podcast_slug   str | null   Filter to a single podcast. Null/omit = all.
        stagger        int          Celery countdown seconds between dispatches (default 30).
        model          str | null   Whisper model override (e.g. 'large'). Null = use defaults.
        language       str | null   Language code override (e.g. 'es'). Null = use defaults.
        initial_prompt str | null   Vocabulary hint override. Null = use defaults.
        min_speakers   int | null   Min speaker count override. Null = use defaults.
        num_speakers   int | null   Expected speaker count override. Null = use defaults.
        max_speakers   int | null   Max speaker count override. Null = use defaults.

    Episodes are only queued if they have subscriber audio and no completed/pending/processing
    transcript. Failed transcripts are re-queued.

    Returns: {"queued": N, "podcast": slug|"all"}
    """
    if not request.user.is_staff:
        return JsonResponse({'error': 'Forbidden'}, status=403)

    import json as _json
    try:
        body = _json.loads(request.body or '{}')
    except ValueError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    podcast_slug = body.get('podcast_slug') or None

    # Collect optional transcription overrides — only pass non-null values
    transcription_kwargs = {}
    for key in ('model', 'language', 'initial_prompt'):
        val = body.get(key)
        if val is not None and str(val).strip():
            transcription_kwargs[key] = str(val).strip()
    for key in ('min_speakers', 'num_speakers', 'max_speakers'):
        val = body.get(key)
        if val is not None:
            try:
                transcription_kwargs[key] = int(val)
            except (TypeError, ValueError):
                pass

    if podcast_slug:
        try:
            podcast = Podcast.objects.get(slug=podcast_slug)
        except Podcast.DoesNotExist:
            return JsonResponse({'error': f'Podcast not found: {podcast_slug}'}, status=404)

    from django.db.models import Q
    episodes = (
        Episode.objects
        .select_related('podcast__network')
        .filter(audio_url_subscriber__isnull=False)
        .exclude(audio_url_subscriber='')
        .filter(
            Q(transcript__isnull=True) | Q(transcript__status=Transcript.Status.FAILED)
        )
        .order_by('pub_date')
    )
    if podcast_slug:
        episodes = episodes.filter(podcast__slug=podcast_slug)

    from pod_manager.tasks import transcribe_episode
    from pod_manager.services.transcription import run_transcription, route_transcription
    from django.utils import timezone

    episode_list = list(episodes)
    queued = 0
    for ep in episode_list:
        Transcript.objects.update_or_create(
            episode=ep,
            defaults={
                'status': Transcript.Status.PENDING,
                'requested_at': timezone.now(),
                'error_message': None,
            },
        )
        if settings.IS_IDE:
            run_transcription(ep.pk, **transcription_kwargs)
        else:
            queue, priority = route_transcription(ep, model=transcription_kwargs.get('model'))
            transcribe_episode.apply_async(
                args=[ep.pk],
                kwargs=transcription_kwargs,
                queue=queue,
                priority=priority,
            )
        queued += 1

    # When source audio retention is on, also verify completed episodes have their files.
    audio_checked = 0
    if settings.WHISPER_KEEP_SOURCE_AUDIO:
        from pod_manager.tasks import task_ensure_source_audio
        completed = (
            Episode.objects
            .filter(audio_url_subscriber__isnull=False)
            .exclude(audio_url_subscriber='')
            .filter(transcript__status=Transcript.Status.COMPLETED)
        )
        if podcast_slug:
            completed = completed.filter(podcast__slug=podcast_slug)
        for ep in completed:
            task_ensure_source_audio.apply_async(args=[ep.pk])
            audio_checked += 1

    logger.info(
        "backfill_transcripts_api: queued=%d audio_checked=%d podcast=%s by %s",
        queued, audio_checked, podcast_slug or 'all', request.user.username,
    )
    return JsonResponse({'queued': queued, 'audio_checked': audio_checked, 'podcast': podcast_slug or 'all'})


@require_POST
@login_required
def retranscribe_episode_api(request, episode_id):
    """Queue a forced re-transcription for a single episode.

    Network-owner only. Accepts optional transcription overrides in POST body.
    Resets transcript to pending and re-queues regardless of current status.

    POST body (JSON):
        model, language, initial_prompt, min_speakers, num_speakers, max_speakers
        priority   "high" | "default" | "low"   queue priority (default if omitted)
    """
    import json as _json
    ep = get_object_or_404(Episode, id=episode_id)
    if not ep.podcast.network.owners.filter(id=request.user.id).exists():
        return JsonResponse({'error': 'Forbidden'}, status=403)

    if not ep.audio_url_subscriber:
        return JsonResponse({'error': 'Episode has no subscriber audio'}, status=400)

    try:
        body = _json.loads(request.body or '{}')
    except ValueError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    transcription_kwargs = {}
    for key in ('model', 'language', 'initial_prompt'):
        val = body.get(key)
        if val is not None and str(val).strip():
            transcription_kwargs[key] = str(val).strip()
    for key in ('min_speakers', 'num_speakers', 'max_speakers'):
        val = body.get(key)
        if val is not None:
            try:
                transcription_kwargs[key] = int(val)
            except (TypeError, ValueError):
                pass

    # Optional explicit audio source (R2 / private / public). Default null =
    # current behavior (cache-or-subscriber). A chosen source forces a fresh
    # download from it and clears the cached file. Must be one of THIS episode's
    # own audio URLs — validate here for a clean 400 (run_transcription re-checks).
    audio_source_url = body.get('audio_source_url')
    if audio_source_url is not None and str(audio_source_url).strip():
        audio_source_url = str(audio_source_url).strip()
        allowed = {u for u in (ep.r2_url, ep.audio_url_subscriber, ep.audio_url_public) if u}
        if audio_source_url not in allowed:
            return JsonResponse({'error': 'Invalid audio source for this episode'}, status=400)
        transcription_kwargs['audio_source_url'] = audio_source_url

    # Priority level (high|default|low). route_transcription maps it to the
    # right queue + Redis priority band for the episode's effective model.
    level = str(body.get('priority', '')).lower()
    if level not in ('high', 'default', 'low'):
        level = 'default'

    from pod_manager.models import Transcript
    transcript, _ = Transcript.objects.get_or_create(episode=ep)
    transcript.status = Transcript.Status.PENDING
    transcript.error_message = None
    transcript.save(update_fields=['status', 'error_message'])

    from pod_manager.tasks import transcribe_episode
    from pod_manager.services.transcription import run_transcription, route_transcription
    if settings.IS_IDE:
        run_transcription(ep.pk, **transcription_kwargs)
        queue = priority = None
    else:
        queue, priority = route_transcription(ep, level=level, model=transcription_kwargs.get('model'))
        transcribe_episode.apply_async(args=[ep.pk], kwargs=transcription_kwargs, queue=queue, priority=priority)

    logger.info(
        "retranscribe_episode_api: episode %d re-queued by %s (level=%s, queue=%s, priority=%s, kwargs=%s)",
        ep.pk, request.user.username, level, queue, priority, transcription_kwargs,
    )
    return JsonResponse({'status': 'queued', 'episode_id': ep.pk})
