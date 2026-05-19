"""
Views for the GDrive Audio Recovery tab in Creator Settings.

GET  /creator/gdrive-recovery/files/          — JSON list of input CSVs + their run history
POST /creator/gdrive-recovery/run/            — Start one recovery run per podcast selection
GET  /creator/gdrive-recovery/stream/<id>/   — SSE stream for a running task
POST /creator/gdrive-recovery/rewind/         — Rewind a completed live run
"""
import csv as csv_module
import json
import os
import time
import uuid

from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, StreamingHttpResponse
from django.core.cache import cache
from django.views.decorators.http import require_POST


def _csv_entry_count(csv_path):
    """Count unique filenames in a CSV."""
    try:
        seen = set()
        with open(csv_path, newline='', encoding='utf-8') as f:
            for row in csv_module.DictReader(f):
                fname = row.get('Filename', '').strip()
                if fname:
                    seen.add(fname.lower())
        return len(seen)
    except Exception:
        return 0


def _recovery_dir():
    return os.path.join(settings.MEDIA_ROOT, 'Recovery')


def _runs_dir():
    return os.path.join(settings.MEDIA_ROOT, 'Recovery', 'runs')


def _load_runs_for_csv(csv_filename):
    runs_dir = _runs_dir()
    if not os.path.isdir(runs_dir):
        return []
    runs = []
    for fname in sorted(os.listdir(runs_dir), reverse=True):
        if not fname.endswith('.json'):
            continue
        try:
            with open(os.path.join(runs_dir, fname), encoding='utf-8') as f:
                meta = json.load(f)
            if meta.get('csv_filename') == csv_filename:
                runs.append(meta)
        except Exception:
            pass
    return runs


@login_required(login_url='/login/')
def gdrive_recovery_files(request):
    recovery_dir = _recovery_dir()
    os.makedirs(recovery_dir, exist_ok=True)

    csv_files = []
    for fname in sorted(os.listdir(recovery_dir)):
        full_path = os.path.join(recovery_dir, fname)
        if fname.endswith('.csv') and os.path.isfile(full_path):
            csv_files.append({
                'filename': fname,
                'runs': _load_runs_for_csv(fname),
                'total_entries': _csv_entry_count(full_path),
            })

    return JsonResponse({'files': csv_files})


@require_POST
@login_required(login_url='/login/')
def gdrive_recovery_run(request):
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    csv_filename = data.get('csv_filename', '').strip()
    # podcast_titles: [] means run against all podcasts as one task
    podcast_titles = data.get('podcast_titles', [])
    dry_run = bool(data.get('dry_run', False))
    min_confidence = data.get('min_confidence', 'HIGH').strip().upper()
    if min_confidence not in ('HIGH', 'MEDIUM', 'LOW'):
        min_confidence = 'HIGH'

    if not csv_filename:
        return JsonResponse({'error': 'csv_filename required'}, status=400)

    csv_path = os.path.join(_recovery_dir(), csv_filename)
    if not os.path.exists(csv_path):
        return JsonResponse({'error': 'CSV not found'}, status=404)

    try:
        from ...tasks import task_run_gdrive_recovery
    except ImportError:
        return JsonResponse({'error': 'Celery/tasks unavailable in this environment. Deploy to Docker to run recovery.'}, status=503)

    # One task per podcast (or a single "all" task when none selected)
    targets = podcast_titles if podcast_titles else [None]
    runs = []
    for podcast_title in targets:
        run_id = str(uuid.uuid4())
        task_run_gdrive_recovery.delay(run_id, csv_path, podcast_title, dry_run, min_confidence)
        runs.append({'run_id': run_id, 'podcast_title': podcast_title or 'all'})

    return JsonResponse({'runs': runs})


@login_required(login_url='/login/')
def gdrive_recovery_stream(request, run_id):
    # Sanitise run_id to a valid UUID before using it as a cache key
    try:
        uuid.UUID(run_id)
    except ValueError:
        return JsonResponse({'error': 'Invalid run ID'}, status=400)

    task_id = f"gdrive_recovery_{run_id}"

    def event_stream():
        last_length = 0
        deadline = time.time() + 300  # 5-minute hard timeout
        while True:
            if time.time() >= deadline:
                yield "data: [ERROR] Stream timed out waiting for worker.\n\n"
                yield "data: [DONE]\n\n"
                break
            logs = cache.get(task_id, "") or ""
            if len(logs) > last_length:
                new_chunk = logs[last_length:]
                yield new_chunk
                last_length = len(logs)
                if "[DONE]" in new_chunk:
                    cache.delete(task_id)
                    break
            time.sleep(0.5)

    return StreamingHttpResponse(
        event_stream(),
        content_type='text/event-stream',
        headers={'X-Accel-Buffering': 'no', 'Cache-Control': 'no-cache'},
    )


@require_POST
@login_required(login_url='/login/')
def gdrive_recovery_rewind(request):
    try:
        data = json.loads(request.body)
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    recovery_csv_url = data.get('recovery_csv_url', '').strip()
    if not recovery_csv_url:
        return JsonResponse({'error': 'recovery_csv_url required'}, status=400)

    media_url = settings.MEDIA_URL
    if not recovery_csv_url.startswith(media_url):
        return JsonResponse({'error': 'Invalid CSV URL'}, status=400)

    rel_path = recovery_csv_url[len(media_url):]
    csv_path = os.path.join(settings.MEDIA_ROOT, rel_path)

    if not os.path.exists(csv_path):
        return JsonResponse({'error': 'Recovery CSV not found on disk'}, status=404)

    try:
        from ...tasks import task_run_gdrive_rewind
    except ImportError:
        return JsonResponse({'error': 'Celery/tasks unavailable in this environment. Deploy to Docker to run recovery.'}, status=503)

    run_id = str(uuid.uuid4())
    task_run_gdrive_rewind.delay(run_id, csv_path)

    return JsonResponse({'run_id': run_id})
