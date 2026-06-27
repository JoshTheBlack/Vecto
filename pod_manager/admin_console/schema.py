"""Argument introspection → form schema, and the reverse (form → invocation).

Two halves of the same contract (design §5 / §5a / §15.5):

* :func:`build_schema` reconstructs a command's ``argparse`` parser, reads its
  actions back, and resolves each to a **semantic widget** plus its inline option
  list — the JSON the Step 3 frontend renders a form from, with zero per-command
  code. Building a schema imports the command's module, so it is lazy (only on the
  detail view) and fault-tolerant: an un-importable command (e.g. ``run_discord_bot``
  pulling discord libs) surfaces as ``import_error`` rather than breaking the console
  (§15.8).
* :func:`reconstruct_invocation` does the inverse: it turns a posted ``{dest: value}``
  payload back into ``call_command`` ``*args`` / ``**options`` *and* re-validates it
  through the command's real parser before dispatch — never trust the posted form
  (§15.5). It also produces the redacted command-line string and the redacted
  args/options persisted to ``CommandRun`` (§15.6).
"""

import inspect
import logging
import shlex
import sys

from django.core.management import get_commands, load_command_class
from django.core.management.base import CommandError
from django.urls import NoReverseMatch, reverse

from .registry import get_spec

logger = logging.getLogger(__name__)

APP_LABEL = "pod_manager"

REDACTED = "***"

# argparse / Django globals we never surface in the generated form. Hidden by dest.
_HIDDEN_DESTS = frozenset({
    "help", "version", "settings", "pythonpath", "traceback",
    "verbosity", "no_color", "force_color", "skip_checks",
})

# Conventional dest names that auto-resolve to DB-backed pickers (§5a).
_NETWORK_DESTS = frozenset({"network"})
_NETWORK_MULTI_DESTS = frozenset({"networks"})
_PODCAST_DESTS = frozenset({"podcast", "podcast_id"})
_PODCAST_MULTI_DESTS = frozenset({"podcasts"})
_EPISODE_DESTS = frozenset({"episode", "episode_id"})


class InvalidInvocation(Exception):
    """A posted payload failed reconstruction / re-validation (→ HTTP 400)."""


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

def discover_commands():
    """Every management command owned by our app (design §4a). Pure metadata —
    does not import any command module."""
    return sorted(name for name, app in get_commands().items() if app == APP_LABEL)


def unregistered_commands():
    """Discovered ``pod_manager`` commands missing a registry entry (§4c notice)."""
    from .registry import REGISTRY
    return [name for name in discover_commands() if name not in REGISTRY]


# ---------------------------------------------------------------------------
# Option-list data sources (§5a)
# ---------------------------------------------------------------------------

def audio_origin_choices():
    """Fetchable ``Episode.audio_origin()`` classes for ``--origins`` (enum_multi).
    The unfetchable classes (s3_dead/megaphone/none) are never mirror targets."""
    return [
        {"value": "gdrive", "label": "Google Drive"},
        {"value": "libsyn", "label": "Libsyn"},
        {"value": "other", "label": "Other"},
    ]


def whisper_model_choices():
    """Common Whisper model names for the `backfill_transcripts --model` picker.

    The underlying arg is free-form (any model a worker has pinned is valid); this is a
    curated convenience list of the standard sizes + `.en` variants and the large
    revisions. Leaving the field blank passes nothing, so the network/podcast setting
    (or ``WHISPER_DEFAULT_MODEL``) applies. For an exotic pin, use the copy-box CLI."""
    models = []
    for s in ("tiny", "base", "small", "medium"):
        models.append({"value": s, "label": s})
        models.append({"value": f"{s}.en", "label": f"{s}.en (English-only)"})
    models += [
        {"value": "large", "label": "large"},
        {"value": "large-v2", "label": "large-v2"},
        {"value": "large-v3", "label": "large-v3"},
    ]
    return models


def whisper_language_choices():
    """Curated common language codes for the `backfill_transcripts --language` picker.

    Whisper supports ~99 languages; this is the realistic podcast subset. Blank leaves
    the field unset (network/podcast default, or Whisper auto-detect). For a code not
    listed, use the copy-box CLI."""
    return [
        {"value": "en", "label": "English (en)"},
        {"value": "es", "label": "Spanish (es)"},
        {"value": "fr", "label": "French (fr)"},
        {"value": "de", "label": "German (de)"},
        {"value": "it", "label": "Italian (it)"},
        {"value": "pt", "label": "Portuguese (pt)"},
        {"value": "nl", "label": "Dutch (nl)"},
        {"value": "ru", "label": "Russian (ru)"},
        {"value": "ja", "label": "Japanese (ja)"},
        {"value": "zh", "label": "Chinese (zh)"},
        {"value": "ko", "label": "Korean (ko)"},
        {"value": "ar", "label": "Arabic (ar)"},
        {"value": "hi", "label": "Hindi (hi)"},
    ]


# Named server-side lists referenced by `enum:<name>` (single) / `enum_multi:<name>`
# (multi) registry overrides.
_NAMED_ENUMS = {
    "audio_origins": audio_origin_choices,
    "whisper_models": whisper_model_choices,
    "whisper_languages": whisper_language_choices,
}


def list_recovery_csvs():
    """CSV files in the GDrive Recovery dir, for the `csv_path` picker (reuses the
    Creator GDrive Recovery store)."""
    import os
    from django.conf import settings
    recovery_dir = os.path.join(settings.MEDIA_ROOT, "Recovery")
    try:
        names = sorted(
            f for f in os.listdir(recovery_dir)
            if f.endswith(".csv") and os.path.isfile(os.path.join(recovery_dir, f))
        )
    except (FileNotFoundError, OSError):
        return []
    return [{"value": f, "label": f} for f in names]


def _network_options(by_id=False):
    from pod_manager.models import Network
    return [
        {"value": n.id if by_id else n.slug, "label": n.name}
        for n in Network.objects.all().order_by("name")
    ]


def _podcast_options(by_id=False):
    from pod_manager.models import Podcast
    return [
        {"value": p.id if by_id else p.slug, "label": p.title}
        for p in Podcast.objects.all().order_by("title")
    ]


# ---------------------------------------------------------------------------
# Widget resolution (§5a)
# ---------------------------------------------------------------------------

def _is_flag(action):
    return action.nargs == 0


def _resolve_widget(action, spec):
    """Assign a semantic widget id to one argparse action (§5a). Registry override
    wins, then argparse `choices=`, then conventional dest names, then base shape."""
    dest = action.dest
    override = spec.field_widgets.get(dest) if spec else None
    if override:
        return override
    if action.choices:
        return "choice"
    if _is_flag(action):
        return "flag"
    if dest in _NETWORK_MULTI_DESTS:
        return "network_multi"
    if dest in _NETWORK_DESTS:
        return "network"
    if dest in _PODCAST_MULTI_DESTS:
        return "podcast_multi"
    if dest in _PODCAST_DESTS:
        return "podcast"
    if dest in _EPISODE_DESTS:
        return "episode"
    if action.type in (int, float):
        return "number"
    return "text"


def _is_multi(action, widget):
    if widget.endswith("_multi") or widget.startswith("enum_multi"):
        return True
    if action.nargs in ("+", "*"):
        return True
    # action='append'
    return action.__class__.__name__ == "_AppendAction"


def _widget_options(widget, action):
    """Inline option list shipped with the schema for dropdown/multiselect widgets.
    ``None`` means the widget needs no inline list (text/number/flag/episode)."""
    # A field typed `int` (e.g. ingest_feed's `podcast_id`) consumes the DB id;
    # a slug-typed field (e.g. mirror's `--podcast watchmen`) consumes the slug. The
    # option *value* must match what the command actually accepts, or reconstruction
    # int()-coerces a slug and crashes.
    by_id = action is not None and action.type is int
    try:
        if widget in ("network", "network_multi"):
            return _network_options(by_id=by_id)
        if widget in ("podcast", "podcast_multi"):
            return _podcast_options(by_id=by_id)
        if widget == "choice":
            return [{"value": c, "label": str(c)} for c in (action.choices or [])]
        if widget == "csv_path":
            return list_recovery_csvs()
        if widget.startswith("enum_multi:") or widget.startswith("enum:"):
            name = widget.split(":", 1)[1]
            resolver = _NAMED_ENUMS.get(name)
            return resolver() if resolver else []
    except Exception:  # pragma: no cover - option lists are best-effort
        logger.warning("admin console: failed to build options for widget %s", widget, exc_info=True)
        return []
    return None


def _humanize(dest):
    return dest.replace("_", " ").strip().capitalize()


# ---------------------------------------------------------------------------
# Schema (form + docs)
# ---------------------------------------------------------------------------

def _public_actions(parser):
    """Parser actions minus argparse/Django globals (§5 denylist)."""
    return [a for a in parser._actions if a.dest not in _HIDDEN_DESTS]


def _deep_link_payload(spec):
    if not spec or not spec.deep_link:
        return None
    # (url_name, label) or (url_name, label, query) — the optional query string
    # (e.g. "tab=gdrive") selects the right tab in the target UI.
    url_name, label = spec.deep_link[0], spec.deep_link[1]
    query = spec.deep_link[2] if len(spec.deep_link) > 2 else None
    try:
        url = reverse(url_name)
        if query:
            url = f"{url}?{query}"
    except NoReverseMatch:
        url = None
    return {"url": url, "label": label}


def build_schema(name):
    """Introspected form schema + merged in-code docs for one command (§5/§6).

    Fault-tolerant (§15.8): if the command module can't be imported, returns a
    schema with ``import_error`` set and no fields so the UI shows a disabled card
    instead of the whole console failing.
    """
    spec = get_spec(name)
    base = {
        "name": name,
        "registered": spec is not None,
        "category": spec.category if spec else "General",
        "danger": bool(spec and spec.danger),
        "danger_fields": sorted(spec.danger_fields) if spec else [],
        "runnable": bool(spec.runnable) if spec else False,
        "deep_link": _deep_link_payload(spec),
        "sensitive": sorted(spec.sensitive) if spec else [],
        "summary": "",
        "long_doc": "",
        "examples": list(spec.examples) if spec else [],
        "fields": [],
        "import_error": None,
    }

    try:
        command = load_command_class(APP_LABEL, name)
        parser = command.create_parser("manage.py", name)
    except Exception as exc:  # noqa: BLE001 - one bad command must not break the console
        logger.warning("admin console: could not introspect %s: %s", name, exc, exc_info=True)
        base["import_error"] = str(exc)
        return base

    # Docs: registry override (escape hatch) else in-code sources (§6).
    base["summary"] = (spec.summary if spec and spec.summary else (command.help or "")).strip()
    module_doc = inspect.getdoc(sys.modules.get(command.__class__.__module__)) or ""
    base["long_doc"] = (spec.long_doc if spec and spec.long_doc else module_doc).strip()

    for action in _public_actions(parser):
        widget = _resolve_widget(action, spec)
        multi = _is_multi(action, widget)
        positional = not action.option_strings
        if positional:
            required = action.nargs not in ("?", "*")
        else:
            required = bool(action.required)
        label = (spec.field_labels.get(action.dest) if spec else None) or _humanize(action.dest)
        base["fields"].append({
            "dest": action.dest,
            "flags": list(action.option_strings),
            "positional": positional,
            "required": required,
            "help": (action.help or "").strip(),
            "default": action.default if not _is_flag(action) else bool(action.default),
            "widget": widget,
            "multi": multi,
            "choices": list(action.choices) if action.choices else None,
            "options": _widget_options(widget, action),
            "sensitive": bool(spec and action.dest in spec.sensitive),
            "label": label,
        })
    return base


# ---------------------------------------------------------------------------
# Reconstruction (form → invocation), §15.5 / §15.6
# ---------------------------------------------------------------------------

def _coerce(action, value):
    if action.type in (int, float):
        try:
            return action.type(value)
        except (TypeError, ValueError):
            raise InvalidInvocation(
                f"{action.dest!r} expects a{'n integer' if action.type is int else ' number'}, "
                f"got {value!r}."
            )
    return value if isinstance(value, str) else str(value)


def _long_option(action):
    longs = [o for o in action.option_strings if o.startswith("--")]
    return longs[0] if longs else action.option_strings[0]


def reconstruct_invocation(name, payload):
    """Turn a posted ``{dest: value}`` payload into a validated invocation.

    Returns a dict with the *real* ``args``/``options`` for ``call_command`` plus
    the *redacted* ``args``/``options``/``command_line`` to persist + display
    (§15.6). Raises :class:`InvalidInvocation` if the command can't be introspected
    or the reassembled argv fails the command's own parser (§15.5).
    """
    spec = get_spec(name)
    sensitive = spec.sensitive if spec else frozenset()
    try:
        command = load_command_class(APP_LABEL, name)
        parser = command.create_parser("manage.py", name)
    except Exception as exc:  # noqa: BLE001
        raise InvalidInvocation(f"Cannot load command {name!r}: {exc}") from exc

    if not isinstance(payload, dict):
        raise InvalidInvocation("Payload must be an object of {dest: value}.")

    args = []                 # real positionals, in order
    options = {}              # real options {dest: value}
    redacted_args = []
    redacted_options = {}
    argv = []                 # for re-validation through the real parser
    display = []              # redacted, for the command_line string

    def _emit_value(action, raw):
        """Append one coerced value to args + argv + display (redacting secrets)."""
        coerced = _coerce(action, raw)
        red = REDACTED if action.dest in sensitive else coerced
        return coerced, red

    for action in _public_actions(parser):
        dest = action.dest
        positional = not action.option_strings
        widget_multi = action.nargs in ("+", "*") or action.__class__.__name__ == "_AppendAction"
        present = dest in payload
        value = payload.get(dest)

        if positional:
            if widget_multi:  # nargs '+' / '*'
                values = value if isinstance(value, (list, tuple)) else ([value] if present and value not in (None, "") else [])
                if action.nargs == "+" and not values:
                    raise InvalidInvocation(f"{dest!r} requires at least one value.")
                for raw in values:
                    coerced, red = _emit_value(action, raw)
                    args.append(coerced)
                    redacted_args.append(red)
                    argv.append(str(coerced))
                    display.append(str(red))
            else:
                required = action.nargs not in ("?", "*")
                if value in (None, ""):
                    if required:
                        raise InvalidInvocation(f"Positional {dest!r} is required.")
                    continue
                coerced, red = _emit_value(action, value)
                args.append(coerced)
                redacted_args.append(red)
                argv.append(str(coerced))
                display.append(shlex.quote(str(red)))
            continue

        # --- option ---
        if _is_flag(action):
            if value:
                options[dest] = True
                redacted_options[dest] = True
                argv.append(_long_option(action))
                display.append(_long_option(action))
            continue

        if action.__class__.__name__ == "_AppendAction":
            values = value if isinstance(value, (list, tuple)) else ([value] if value not in (None, "") else [])
            for raw in values:
                coerced, red = _emit_value(action, raw)
                options.setdefault(dest, []).append(coerced)
                redacted_options.setdefault(dest, []).append(red)
                argv += [_long_option(action), str(coerced)]
                display += [_long_option(action), shlex.quote(str(red))]
            continue

        # plain valued option. A multi-select widget over a single-valued arg
        # (e.g. mirror's `--origins` comma list) arrives as a list → comma-join.
        if isinstance(value, (list, tuple)):
            value = ",".join(str(v) for v in value if v not in (None, ""))
        if value in (None, ""):
            continue  # let the command's own default apply
        coerced, red = _emit_value(action, value)
        options[dest] = coerced
        redacted_options[dest] = red
        argv += [_long_option(action), str(coerced)]
        display += [f"{_long_option(action)}={shlex.quote(str(red))}"]

    # Re-validate the reassembled argv through the command's real parser (§15.5).
    try:
        parser.parse_args(argv)
    except CommandError as exc:
        raise InvalidInvocation(str(exc)) from exc
    except SystemExit as exc:  # pragma: no cover - CommandParser raises CommandError, but be safe
        raise InvalidInvocation(f"Invalid arguments for {name}.") from exc

    command_line = " ".join(["python", "manage.py", name, *display])
    return {
        "args": args,
        "options": options,
        "redacted_args": redacted_args,
        "redacted_options": redacted_options,
        "command_line": command_line,
    }
