"""Admin Command Console backend (design §9).

Superuser-only JSON endpoints the Step 3 frontend consumes. No templates yet —
every route returns JSON:

    GET  /admin-console/                      console        — command list (grouped)
    GET  /admin-console/command/<name>/       command_detail — schema + docs + recent runs
    POST /admin-console/command/<name>/run/   run            — validate + dispatch, returns run_id
    GET  /admin-console/run/<run_id>/poll/    run_poll       — {chunk, offset, status}
    GET  /admin-console/runs/                  history        — global run history (filterable)
    GET  /admin-console/run/<run_id>/         run_detail     — one CommandRun (log + summary)
    GET  /admin-console/lookup/episodes/      episode_search — episode typeahead (§5a)

Every endpoint independently enforces ``superuser_required`` — never trust the
hidden nav (§3).
"""

import json
import logging
import uuid

from django.core.cache import cache
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from pod_manager.admin_console.registry import REGISTRY, get_spec
from pod_manager.admin_console.schema import (
    InvalidInvocation,
    build_schema,
    discover_commands,
    reconstruct_invocation,
    unregistered_commands,
)
from pod_manager.models import CommandRun
from .staff import superuser_required

logger = logging.getLogger(__name__)

# How many recent runs to surface per command / globally without an explicit page.
RECENT_RUNS_LIMIT = 25


def _run_to_dict(run, include_log=False):
    data = {
        "run_id": str(run.run_id),
        "command": run.command,
        "status": run.status,
        "user": run.user.username if run.user else None,
        "command_line": run.command_line,
        "args": run.args,
        "options": run.options,
        "created_at": run.created_at.isoformat() if run.created_at else None,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "duration_seconds": run.duration_seconds,
        "error": run.error,
        "result_summary": run.result_summary,
    }
    if include_log:
        data["log"] = run.log
    return data


def _console_payload():
    """Registered commands grouped by category + the discovered-but-unregistered
    notice (§4c). Cheap — does not introspect any command module."""
    categories = {}
    for name, spec in sorted(REGISTRY.items()):
        categories.setdefault(spec.category, []).append({
            "name": name,
            "danger": spec.danger,
            "danger_fields": sorted(spec.danger_fields),
            "runnable": spec.runnable,
            "deep_link": bool(spec.deep_link),
        })
    grouped = [
        {"category": cat, "commands": cmds}
        for cat, cmds in sorted(categories.items())
    ]
    return {
        "categories": grouped,
        "unregistered": unregistered_commands(),
        "discovered_count": len(discover_commands()),
    }


@superuser_required
@require_GET
def console(request):
    """The console page shell (§10). Renders the sidebar from the cheap registry
    payload; the detail pane, log pane, and history are populated client-side from
    the JSON endpoints below."""
    return render(request, "pod_manager/admin_console.html", _console_payload())


@superuser_required
@require_GET
def command_detail(request, name):
    """Introspected form schema + merged in-code docs + this command's recent runs."""
    if name not in REGISTRY:
        return JsonResponse({"error": f"Command {name!r} is not registered."}, status=404)
    schema = build_schema(name)
    recent = CommandRun.objects.filter(command=name)[:RECENT_RUNS_LIMIT]
    schema["recent_runs"] = [_run_to_dict(r) for r in recent]
    return JsonResponse(schema)


@superuser_required
@require_POST
def build(request, name):
    """Serialize the posted form state → the paste-ready command line, WITHOUT running
    (§5b: copy box and Execute share one serializer).

    Backs the live copy box for *every* registered command — including the deep-link /
    docs-only ones (`recover_gdrive_audio`, `rewind_gdrive_audio`, `crawl_by_id`,
    `run_discord_bot`) whose invocation is meant for a terminal even though the console
    won't execute them. Returns ``{valid, command_line, error}``; an incomplete form is
    simply ``valid=False`` (nothing to copy yet) rather than an HTTP error. The command
    line is shell-quoted and secret-redacted exactly as the dispatched run records it."""
    if name not in REGISTRY:
        return JsonResponse({"error": f"Command {name!r} is not registered."}, status=404)
    try:
        body = json.loads(request.body or b"{}")
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON body."}, status=400)

    try:
        invocation = reconstruct_invocation(name, body.get("fields", {}))
    except InvalidInvocation as exc:
        return JsonResponse({"valid": False, "command_line": None, "error": str(exc)})
    return JsonResponse({"valid": True, "command_line": invocation["command_line"], "error": None})


@superuser_required
@require_POST
def run(request, name):
    """Validate the posted form, create the ``CommandRun`` row, dispatch the worker.

    Enforces: registered + runnable, the danger typed-confirm gate, and a full
    re-validation of the reassembled args through the command's real parser (§15.5).
    Returns the ``run_id`` the client then polls (§8)."""
    spec = get_spec(name)
    if spec is None:
        return JsonResponse({"error": f"Command {name!r} is not registered."}, status=404)
    if not spec.runnable:
        # Defensive: docs-only / deep-link commands have no Execute button, but the
        # backend rejects a forged request anyway (§7, §11).
        return JsonResponse({"error": f"Command {name!r} is not runnable from the console."}, status=403)

    try:
        body = json.loads(request.body or b"{}")
    except (json.JSONDecodeError, ValueError):
        return JsonResponse({"error": "Invalid JSON body."}, status=400)

    payload = body.get("fields", {})

    # Danger gate: destructive runs require typing the command name to confirm (§11).
    # A run is destructive when the command is always-dangerous (`danger`) OR this run
    # activates a destructive sub-mode (`danger_fields`, e.g. --prune) — but ONLY when
    # the run actually executes (`--apply`). A preview/dry-run mutates nothing, so it
    # never needs confirmation; the gate engages exactly when Apply is set on a
    # destructive command.
    destructive = spec.danger or any(payload.get(d) for d in spec.danger_fields)
    is_danger = destructive and bool(payload.get("apply"))
    if is_danger and body.get("confirm", "").strip() != name:
        return JsonResponse(
            {"error": f"This run is destructive — confirm by sending the command name {name!r}."},
            status=400,
        )

    try:
        invocation = reconstruct_invocation(name, payload)
    except InvalidInvocation as exc:
        return JsonResponse({"error": str(exc)}, status=400)

    # Concurrency: soft-block a second *identical* run while one is still queued or
    # running (§14 — resolved: soft-block duplicates). Identity is the command plus its
    # redacted command line, so two genuinely different invocations of the same command
    # (e.g. two podcasts) never collide — only an accidental repeat does.
    if CommandRun.objects.filter(
        command=name,
        command_line=invocation["command_line"],
        status__in=(CommandRun.Status.QUEUED, CommandRun.Status.RUNNING),
    ).exists():
        return JsonResponse(
            {"error": f"An identical {name!r} run is already in progress — wait for it to "
                      "finish or check Recent Runs."},
            status=409,
        )

    # Degrade gracefully when the worker/broker is unavailable (§7), and only create
    # the history row once we know we can dispatch.
    try:
        from pod_manager.tasks import task_run_management_command
    except ImportError:
        return JsonResponse(
            {"error": "Celery/tasks unavailable in this environment. Deploy to Docker to run commands."},
            status=503,
        )

    run_id = uuid.uuid4()
    cmd_run = CommandRun.objects.create(
        run_id=run_id,
        command=name,
        args=invocation["redacted_args"],
        options=invocation["redacted_options"],
        command_line=invocation["command_line"],
        user=request.user if request.user.is_authenticated else None,
        status=CommandRun.Status.QUEUED,
    )
    logger.info(
        "Admin console: %s dispatched %s (run %s) — %s",
        request.user, name, run_id, invocation["command_line"],
    )
    # Enqueue. Importing the task only guards a missing-Celery environment; if the
    # broker itself is down, `.delay()` raises (e.g. kombu OperationalError) — catch it
    # so the operator gets a clean 503 instead of a 500, and drop the row we just
    # created so no orphan `queued` record lingers in history (§7).
    try:
        task_run_management_command.delay(
            str(run_id), name, invocation["args"], invocation["options"],
        )
    except Exception as exc:  # noqa: BLE001 — broker/connection failure → graceful 503
        cmd_run.delete()
        logger.warning(
            "Admin console: could not enqueue %s (run %s): %s", name, run_id, exc, exc_info=True,
        )
        return JsonResponse(
            {"error": "Celery broker unavailable — the command could not be queued. "
                      "Is the worker / Redis running?"},
            status=503,
        )
    return JsonResponse({
        "run_id": str(run_id),
        "status": cmd_run.status,
        "command_line": cmd_run.command_line,
    })


@superuser_required
@require_GET
def run_poll(request, run_id):
    """Delta of the live log buffer since ``offset`` + current status (§8).

    Shape ``{chunk, offset, status}``. The client appends ``chunk``, advances its
    ``offset``, and stops when status is terminal or it sees ``[DONE]``."""
    task_id = f"admin_cmd_{run_id}"
    try:
        offset = int(request.GET.get("offset", 0))
    except (TypeError, ValueError):
        offset = 0

    buf = cache.get(task_id, "") or ""
    chunk = buf[offset:] if offset <= len(buf) else buf
    new_offset = len(buf)

    status = (
        CommandRun.objects.filter(run_id=run_id)
        .values_list("status", flat=True)
        .first()
    )
    return JsonResponse({"chunk": chunk, "offset": new_offset, "status": status})


@superuser_required
@require_GET
def run_detail(request, run_id):
    """One ``CommandRun`` with its full log + result_summary (replay / expand)."""
    try:
        cmd_run = CommandRun.objects.get(run_id=run_id)
    except CommandRun.DoesNotExist:
        return JsonResponse({"error": "Run not found."}, status=404)
    return JsonResponse(_run_to_dict(cmd_run, include_log=True))


@superuser_required
@require_GET
def history(request):
    """Global run history, filterable by command / user / status (§8a)."""
    qs = CommandRun.objects.select_related("user")
    command = request.GET.get("command", "").strip()
    username = request.GET.get("user", "").strip()
    status = request.GET.get("status", "").strip()
    if command:
        qs = qs.filter(command=command)
    if username:
        qs = qs.filter(user__username__iexact=username)
    if status:
        qs = qs.filter(status=status)

    try:
        limit = min(int(request.GET.get("limit", RECENT_RUNS_LIMIT)), 200)
    except (TypeError, ValueError):
        limit = RECENT_RUNS_LIMIT

    runs = qs[:limit]
    return JsonResponse({"runs": [_run_to_dict(r) for r in runs]})


@superuser_required
@require_GET
def episode_search(request):
    """Episode typeahead backing the episode picker (§5a/§9). Episodes number in the
    tens of thousands, so they can't ride along inline like Networks/Podcasts."""
    from pod_manager.models import Episode

    q = request.GET.get("q", "").strip()
    qs = Episode.objects.select_related("podcast")
    if q:
        if q.isdigit():
            qs = qs.filter(id=int(q))
        else:
            qs = qs.filter(title__icontains=q)
    else:
        qs = qs.none()

    results = [
        {"id": e.id, "title": e.title, "podcast": e.podcast.title if e.podcast else None}
        for e in qs[:20]
    ]
    return JsonResponse({"results": results})
