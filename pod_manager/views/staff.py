import json
import os
import time
from functools import wraps

from django.conf import settings
from django.contrib.auth.views import redirect_to_login
from django.core.cache import cache
from django.db import connection
from django.http import HttpResponseForbidden, JsonResponse, StreamingHttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from pod_manager.log_handler import CACHE_KEY, DEBUG_MODE_TTL
from pod_manager.models import LogEntry

_LEVEL_MAP = {
    'DEBUG': 10,
    'INFO': 20,
    'WARNING': 30,
    'ERROR': 40,
    'CRITICAL': 50,
}


def staff_required(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect_to_login(request.get_full_path(), 'patreon_login')
        if not request.user.is_staff:
            return HttpResponseForbidden("Staff access required.")
        return view_func(request, *args, **kwargs)
    return wrapper


def superuser_required(view_func):
    """Gate a view behind ``is_superuser`` (Admin Command Console, §3).

    Mirrors :func:`staff_required` but checks ``is_superuser`` — the console is
    superuser-only, and every console endpoint carries this so the hidden nav is
    never the only line of defense."""
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect_to_login(request.get_full_path(), 'patreon_login')
        if not request.user.is_superuser:
            return HttpResponseForbidden("Superuser access required.")
        return view_func(request, *args, **kwargs)
    return wrapper


@staff_required
def log_viewer(request):
    override = cache.get(CACHE_KEY)
    effective_level = override if override else getattr(settings, 'LOG_LEVEL', 'INFO')
    debug_active = (effective_level == 'DEBUG')
    return render(request, 'pod_manager/log_viewer.html', {
        'debug_active': debug_active,
        'effective_level': effective_level,
        'configured_level': getattr(settings, 'LOG_LEVEL', 'INFO'),
    })


@staff_required
@require_POST
def log_level_toggle(request):
    action = request.POST.get('action')
    if action == 'enable_debug':
        cache.set(CACHE_KEY, 'DEBUG', timeout=DEBUG_MODE_TTL)
        return JsonResponse({'active': True, 'level': 'DEBUG', 'ttl_hours': DEBUG_MODE_TTL // 3600})
    elif action == 'disable_debug':
        cache.delete(CACHE_KEY)
        configured = getattr(settings, 'LOG_LEVEL', 'INFO')
        return JsonResponse({'active': False, 'level': configured})
    return JsonResponse({'error': 'Invalid action'}, status=400)


@staff_required
@require_GET
def log_stream(request):
    level = request.GET.get('level', 'INFO').upper()
    last_id = int(request.GET.get('last_id', 0))
    username = request.GET.get('user', '').strip()
    min_level = _LEVEL_MAP.get(level, 20)

    def _base_qs():
        qs = LogEntry.objects.filter(level_no__gte=min_level)
        if username:
            qs = qs.filter(user__username__iexact=username)
        return qs.select_related('user')

    def _to_payload(entry):
        return {
            'id': entry.id,
            'level': entry.level,
            'logger': entry.logger_name,
            'module': entry.module,
            'func': entry.func_name,
            'line': entry.lineno,
            'message': entry.message,
            'ts': entry.created_at.strftime('%H:%M:%S'),
            'user': entry.user.username if entry.user else None,
        }

    def event_stream():
        nonlocal last_id
        # Flush headers immediately so the browser fires onopen without waiting
        # for the first real log entry to arrive.
        yield ": connected\n\n"

        # Bootstrap: on a fresh connect (last_id=0) send the last 20 entries so
        # the viewer isn't blank while waiting for the next live entry.
        if last_id == 0:
            recent = list(_base_qs().order_by('-id')[:20])
            for entry in reversed(recent):
                last_id = entry.id
                yield f"id: {entry.id}\ndata: {json.dumps(_to_payload(entry))}\n\n"

        heartbeat_ticks = 0
        while True:
            for entry in _base_qs().filter(id__gt=last_id).order_by('id')[:100]:
                last_id = entry.id
                yield f"id: {entry.id}\ndata: {json.dumps(_to_payload(entry))}\n\n"

            heartbeat_ticks += 1
            if heartbeat_ticks >= 5:
                yield ": heartbeat\n\n"
                heartbeat_ticks = 0

            time.sleep(1)

    response = StreamingHttpResponse(event_stream(), content_type='text/event-stream')
    response['Cache-Control'] = 'no-cache'
    response['X-Accel-Buffering'] = 'no'
    return response


@staff_required
@require_GET
def log_poll(request):
    """Regular JSON endpoint polled every 2 s by the log viewer.
    More reliable than SSE with gunicorn sync workers behind Traefik."""
    level = request.GET.get('level', 'INFO').upper()
    last_id = int(request.GET.get('last_id', 0))
    username = request.GET.get('user', '').strip()
    min_level = _LEVEL_MAP.get(level, 20)

    qs = LogEntry.objects.filter(level_no__gte=min_level)
    if username:
        qs = qs.filter(user__username__iexact=username)
    qs = qs.select_related('user')

    if last_id == 0:
        entries = list(qs.order_by('-id')[:50])
        entries.reverse()
    else:
        entries = list(qs.filter(id__gt=last_id).order_by('id')[:100])

    return JsonResponse({
        'entries': [
            {
                'id': e.id,
                'level': e.level,
                'logger': e.logger_name,
                'module': e.module,
                'func': e.func_name,
                'line': e.lineno,
                'message': e.message,
                'ts': e.created_at.strftime('%H:%M:%S'),
                'user': e.user.username if e.user else None,
            }
            for e in entries
        ],
        'last_id': entries[-1].id if entries else last_id,
    })


@staff_required
@require_GET
def log_resources(request):
    data = {}

    try:
        import psutil
        data['cpu_percent'] = psutil.cpu_percent(interval=None)
        vm = psutil.virtual_memory()
        data['ram'] = {
            'total': vm.total,
            'used': vm.used,
            'available': vm.available,
            'percent': vm.percent,
        }
        disk = psutil.disk_usage('/')
        data['disk'] = {
            'total': disk.total,
            'used': disk.used,
            'free': disk.free,
            'percent': disk.percent,
        }
    except ImportError:
        data['psutil_missing'] = True

    redis_url = getattr(settings, 'REDIS_URL', None)
    if redis_url:
        try:
            import redis as redis_lib
            r = redis_lib.from_url(redis_url, socket_timeout=1)
            info = r.info()
            data['redis'] = {
                'used_memory': info['used_memory'],
                'used_memory_peak': info['used_memory_peak'],
                'connected_clients': info['connected_clients'],
                'keyspace_hits': info.get('keyspace_hits', 0),
                'keyspace_misses': info.get('keyspace_misses', 0),
                'uptime_seconds': info.get('uptime_in_seconds', 0),
            }
        except Exception as exc:
            data['redis'] = {'error': str(exc)}
    else:
        data['redis'] = None

    try:
        engine = connection.settings_dict.get('ENGINE', '')
        if 'postgresql' in engine:
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_database_size(current_database())")
                db_size = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE state = 'active'"
                )
                active_conns = cursor.fetchone()[0]
            data['db'] = {'size': db_size, 'active_connections': active_conns, 'engine': 'postgresql'}
        else:
            db_path = connection.settings_dict.get('NAME', '')
            size = os.path.getsize(db_path) if db_path and os.path.exists(str(db_path)) else None
            data['db'] = {'size': size, 'engine': 'sqlite'}
    except Exception as exc:
        data['db'] = {'error': str(exc)}

    return JsonResponse(data)
