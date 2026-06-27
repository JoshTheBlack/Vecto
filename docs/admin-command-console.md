# Admin Command Console — Design Document

**Status:** Draft for review — **Steps 0–4 implemented 2026-06-26.**
**Author:** Josh + Claude (design session 2026-06-26)
**Implementation:** Step 0 (command normalization, §16), Step 1 (log-buffer refactor, §8 "As-built"), Step 2 (backend skeleton — registry, introspection, `CommandRun`, runner task, superuser routes; §13 "As-built"), and Steps 3–4 (frontend: nav tab, console page, data-driven form renderer, command-line builder, Execute/Open-in, polling log pane, run history + re-run, and the in-code docs wired into the detail pane; §13 "As-built (Steps 3 & 4)") landed in the working tree; Steps 5–6 (safety polish + doc conversion) remain.

---

## 0. About this document

This file is **both** the design spec **and** the eventual living documentation for
the feature. As the Admin Command Console is implemented, we convert this document
in place — section by section — from *"here's what we'll build"* into *"here's how
it works."* Concretely, during/after implementation:

- Flip the header **Status** from `Draft for review` → `Implemented` and update the
  Implementation line with the landing PR/commit.
- Rewrite forward-looking phrasing ("we will", "proposed", "should") into present
  tense describing the shipped behavior.
- Replace the §13 build order with an "as-built" overview, and fold any resolved
  §14 questions into the relevant sections.
- Keep the file/line references and the §12 enumeration current as commands change.

So the implementing session should treat doc updates as part of "done," not a
follow-up — this is where the post-implementation documentation lives.

---

## 1. Goal

Give superusers a browser-based console that exposes the project's Django
management commands, so they can be inspected and launched without shell access.
For each command the console shows its documentation and usage, lets the operator
fill in options/arguments through a generated form, executes it on the Celery
worker, and streams the live log output back into the page.

The console must be **dynamic**: adding a new management command should require, at
most, a small registry entry — never a template rewrite. (See §4 for exactly what
"registering" a command looks like.)

### Non-goals

- Not a replacement for `manage.py` in the shell; it's a curated, safer subset.
- Not a general task scheduler (we already have Celery beat for that).
- Not exposed to staff or creators — **superusers only** (§3).

---

## 2. What already exists (reused, not rebuilt)

The hard parts are already solved in the codebase; this feature mostly generalizes
patterns we use today.

| Capability | Where it lives now | How we reuse it |
|---|---|---|
| Run a command off the request thread | `task_ingest_feed`, `task_run_gdrive_recovery` in [tasks.py](../pod_manager/tasks.py) call `call_command(..., stdout=stream)` | Generalize into one `task_run_management_command` |
| Buffer command output for streaming | **Done (Step 1):** one shared `CommandLogStream` ([admin_console/log_stream.py](../pod_manager/admin_console/log_stream.py)) — was the duplicated `CacheLogStream` / `_RecoveryStream` | Single shared util, used by all callers (§8) |
| Deliver live command output to the browser | SSE: `stream_feed_import`, `gdrive_recovery_stream`; **polling**: `log_poll` ([staff.py:125](../pod_manager/views/staff.py#L125)) | Console uses the **polling** path (more reliable behind gunicorn/Traefik — §8), keyed by run id |
| Superuser/staff gating | `staff_required` ([staff.py:26](../pod_manager/views/staff.py#L26)); nav already conditional on `is_staff` / `is_superuser` ([base.html:281-296](../pod_manager/templates/pod_manager/base.html#L281)) | Add a `superuser_required` decorator + nav block |
| Live log viewer UI precedent | `/staff/logs/` log viewer + poll/stream endpoints | Visual + interaction reference for the console's log pane |

The streaming contract is already standardized: the worker appends
`data: <line>\n\n` chunks to a cache key and writes a terminal `[DONE]` sentinel;
the SSE view tails the key and closes on `[DONE]`. We keep that contract verbatim.

---

## 3. Access control

**Superusers only.** The tab is not rendered for anyone else, and every backend
endpoint independently enforces it (never trust the hidden nav alone).

- New decorator `superuser_required` (mirror `staff_required`, swap
  `is_staff` → `is_superuser`). Lives alongside the existing one.
- Nav: a new `{% if request.user.is_superuser %}` block in
  [base.html](../pod_manager/templates/pod_manager/base.html) renders the **Admin**
  link next to the existing **Logs** link.
- All console views (`console`, `run`, `stream`) carry `@superuser_required`.

---

## 4. Command discovery + the registry (the part you asked about)

Decision: **auto-discover + opt-in registry.** Here is exactly what that flow is.

### 4a. Discovery (automatic)

Django already knows every command. `django.core.management.get_commands()` returns
a dict of `{command_name: app_label}`. We filter to commands owned by our app:

```python
from django.core.management import get_commands
ours = [name for name, app in get_commands().items() if app == 'pod_manager']
```

This gives us the live list with zero maintenance. But discovery alone is not what
gets shown — the registry gate decides visibility and behavior.

### 4b. The registry (opt-in)

A single module, e.g. `pod_manager/admin_console/registry.py`, holds one entry per
command we choose to surface. The registry's **primary, load-bearing job is
structured metadata** — visibility, danger/runnable flags, and the **semantic
field-widget hints** that argparse can't self-describe (§5a). Documentation prose
is *not* its job: all command text comes from in-code sources (§6). In practice no
command needs registry prose — `long_doc`/`examples` exist only as an escape hatch.

```python
# pod_manager/admin_console/registry.py
from dataclasses import dataclass, field

@dataclass
class CommandSpec:
    name: str                       # must match the management command name
    category: str = "General"       # groups commands in the sidebar
    danger: bool = False            # destructive → red styling + typed confirm
    runnable: bool = True           # False = no in-console Execute button
    # If set, the detail pane shows an "Open in <label>" deep-link instead of an
    # Execute button — execution lives in an existing dedicated UI. The command
    # builder/copy box still renders (so it's runnable from a terminal).
    deep_link: tuple | None = None  # (url_name, button_label), e.g.
                                    # ("creator_settings", "Open GDrive Recovery")
    # Semantic widget overrides for args argparse can't self-describe.
    # arg dest -> widget id (see §5a). Most fields auto-infer; this is the override.
    field_widgets: dict = field(default_factory=dict)
    field_labels: dict = field(default_factory=dict)   # arg dest -> friendly label
    # --- optional doc escape hatch (rarely needed; prefer in-code help) ---
    summary: str = ""               # overrides Command.help if set
    long_doc: str = ""              # overrides module docstring if set
    examples: list[str] = field(default_factory=list)

REGISTRY: dict[str, CommandSpec] = {
    "ingest_feed": CommandSpec(
        name="ingest_feed",
        category="Feeds",
        summary="Re-ingest a single podcast feed.",
        examples=["ingest_feed 42"],
    ),
    "mirror_audio_to_r2": CommandSpec(
        name="mirror_audio_to_r2",
        category="R2 / Storage",
        # --network / --podcast / --episode auto-infer; --origins can't:
        field_widgets={"origins": "enum_multi:audio_origins"},
    ),
    "purge_r2_dev": CommandSpec(
        name="purge_r2_dev",
        category="R2 / Storage",
        danger=True,                # forces a typed-confirmation gate
    ),
    "crawl_by_id": CommandSpec(
        name="crawl_by_id",
        category="Archive / Vestigial",
        runnable=False,             # ~110k Celery tasks; docs-only by design
    ),
    "recover_gdrive_audio": CommandSpec(
        name="recover_gdrive_audio",
        category="GDrive",
        runnable=False,             # execute via the existing Creator UI…
        deep_link=("creator_settings", "Open GDrive Recovery"),
        field_widgets={"csv_path": "csv_path"},  # …but still build the CLI here
    ),
    "run_discord_bot": CommandSpec(
        name="run_discord_bot",
        category="Daemons",
        runnable=False,             # launched by docker-compose; docs-only here
    ),
    # ... one entry per command we expose
}
```

### 4c. How the two combine

At render time the console computes three sets:

1. **Discovered** — every `pod_manager` command (from `get_commands()`).
2. **Registered** — keys of `REGISTRY`.
3. **Shown** — the registered ones, grouped by `category`.

Because only registered commands are shown, **a newly added command is invisible
until someone adds a one-line registry entry** — safe by default, no surprise
exposure of a half-finished or dangerous command. To make this self-policing, the
console renders a small **"Discovered but unregistered"** notice listing any
`pod_manager` command missing from the registry, so we never silently forget one.
(A unit test can assert the same thing in CI if we want it enforced.)

> **Registering a command, end to end:** write the command as usual → add a
> `CommandSpec(name="…")` to `REGISTRY` → (optionally) flesh out `Command.help` and
> per-argument `help=` in the command file → it appears in the console, fully
> formed, with its form auto-generated. No template or view changes.

---

## 5. Argument introspection → auto-generated form

This is what makes the form dynamic. Django commands build an `argparse` parser via
`add_arguments`. We reconstruct it and read it back:

```python
from django.core.management import load_command_class

cmd = load_command_class('pod_manager', name)
parser = cmd.create_parser('manage.py', name)
for action in parser._actions:
    # action.option_strings, action.dest, action.help, action.default,
    # action.choices, action.type, action.nargs, action.required, action.const
```

Every command in the repo already declares clean arguments, so the base mapping is
mechanical:

| argparse shape | UI control | Notes |
|---|---|---|
| `action='store_true'` | checkbox | sends the flag only when checked |
| `choices=[...]` | `<select>` dropdown | e.g. `--min-confidence HIGH/MEDIUM/LOW` |
| `type=int` / `type=float` | number input | e.g. `--limit`, `--stagger`, `--age-days` |
| `type=str` / default | text input | |
| positional, required | required text input | e.g. `csv_path` |
| positional `nargs='?'` | optional text input | e.g. `podcast_title` |
| positional `nargs='+'` | repeatable input list | e.g. `csv_paths` (rewind) |
| `required=True` option | required field + marker | e.g. `--cookie_name` |

We hide argparse's built-ins (`-h/--help`, `--version`, `--settings`,
`--pythonpath`, `--traceback`, `--no-color`, etc.) behind a denylist so the form
shows only the command's own options. `--verbosity` can be surfaced in an
"Advanced" disclosure if we want.

The form posts a structured payload (arg dest → value); the backend reassembles it
into `*args` / `**options` for `call_command` (§7), applying the same rules in
reverse and validating against the parser before dispatch.

### 5a. Semantic field widgets (the rich pickers)

Argparse describes a field's *shape*, never its *meaning*: `--network` is just
`type=str`, so the base mapping would render a bare text box. To get DB-backed
pickers — "this takes one network, give me a dropdown; this takes many podcasts,
give me a multi-select" — we add a thin **semantic layer** on top of the base
mapping. A resolver assigns each field a **widget id**, mostly by convention, with
the registry's `field_widgets` overriding the exceptions.

| Widget id | Auto-inferred when… | Data source / behavior |
|---|---|---|
| `network` | dest `network`, single-valued | `Network.objects` → slug; dropdown |
| `network_multi` | dest `network(s)` + `action='append'` | `Network.objects`; multi-select |
| `podcast` | dest `podcast` / `podcast_id`, single | `Podcast.objects` → slug/id; dropdown |
| `podcast_multi` | dest `podcasts` + `action='append'` | `Podcast.objects`; multi-select |
| `episode` | dest `episode` / `episode_id` | typeahead via `episode_search` endpoint (§9) — not an inline list |
| `choice` | argparse `choices=[…]` present | the choices (free, no registry needed) |
| `enum_multi:<name>` | **registry override only** | a named server-side list (e.g. `audio_origins` from `Episode.audio_origin()`) |
| `csv_path` | **registry override only** | lists CSVs in the Recovery dir (reuse gdrive UI) |
| `flag` / `number` / `text` | fallback from base mapping | — |

The schema endpoint (§9) resolves each field to `{widget, options?, default,
required, multi}` and ships the option lists inline, so the frontend renders the
right control with zero per-command code. Convention covers `--network`,
`--podcast`, `--episode`, and every `choices=` field automatically; the registry
only names the handful argparse can't describe (`--origins`, the CSV positionals,
optional Whisper `--model`/`--language` enums).

### 5b. Live command-line builder + Execute

The detail pane keeps a **read-only, copy-able command line** that reflects the
current form state in real time — e.g. selecting network `baldmove`, origins
`gdrive`, and ticking dry-run renders:

```
python manage.py mirror_audio_to_r2 --network=baldmove --origins=gdrive --dry-run
```

This is paste-ready for the web/celery container shell. The **Execute** button runs
the *same* assembled invocation via the Celery path (§7) and streams its output
(§8). Both the copy box and Execute derive from one source of truth (the form
state → serialized args), so they can never drift.

The primary action below the copy box depends on the spec:

- **`runnable=True`** → **Execute** button (Celery + live stream).
- **`runnable=False` + `deep_link`** → an **"Open in …"** button linking to the
  existing dedicated UI. Used by `recover_gdrive_audio` / `rewind_gdrive_audio`:
  we already built the GDrive Recovery flow in Creator Settings, so the console
  documents them and builds the CLI (for terminal use) but hands execution off to
  that UI rather than duplicating it.
- **`runnable=False`, no deep-link** (`run_discord_bot`, `crawl_by_id`) → copy box
  only, no action button.

---

## 6. Documentation strategy — three in-code sources, no registry prose

All command *text* comes from in-code sources, merged at display time. The registry
is for structure (§4b), not documentation.

1. **`Command.help`** → summary / usage line.
2. **Per-argument `help=`** → field help next to each control.
3. **Module docstring** (`inspect.getdoc(sys.modules[cmd.__module__])`) → the rich
   detail-pane body. This is the key addition: our best material (the multi-example
   usage blocks and conceptual context in `backfill_media_to_r2`,
   `mirror_audio_to_r2`, the R2 maintenance commands, etc.) already lives in module
   docstrings, which `Command.help` introspection would otherwise miss.

`summary`/`long_doc`/`examples` on the registry exist only as an override escape
hatch and, in practice, go unused — every command's text is covered in-code.

**Small in-code doc backfill is part of this project.** A review of all 23 commands
(§12) found in-code help is already strong almost everywhere. The only real gap is
**`crawl_by_id`**, whose four arguments have no `help=` text at all; we add those
strings to the command file. A couple of trivially-simple commands could gain a
one-line example in `Command.help`. That's the whole backfill — there is no
separate doc registry to maintain.

---

## 7. Execution model — Celery worker

Mirror the existing pattern exactly. One generic task replaces the per-command
tasks:

```python
@shared_task
def task_run_management_command(run_id, name, args, options):
    task_id = f"admin_cmd_{run_id}"
    stream = CommandLogStream(task_id)          # shared util, §8
    run = CommandRun.objects.get(run_id=run_id) # row pre-created by the view (§8a)
    run.mark_running()
    try:
        stream.write(f"[SYSTEM] Running: {name} {args} {options}\n")
        call_command(name, *args, stdout=stream, stderr=stream,
                     no_color=True, **options)
        run.mark_finished('completed')
    except Exception as e:
        stream.write(f"\n[ERROR] {e}\n")
        run.mark_finished('failed', error=str(e))
        raise
    finally:
        run.log = stream.captured()             # persist full output (§8a)
        run.save(update_fields=['log', 'status', 'finished_at', 'error'])
        stream.write("[DONE]")
```

- **Dispatch:** the `run` view validates the command is registered + `runnable`,
  re-validates args against the command's own parser, enforces the danger
  confirmation, mints a `run_id` (UUID), **creates the `CommandRun` history row
  (§8a) in `queued` state**, kicks `task_run_management_command.delay()`, and
  returns the `run_id`.
- **Why Celery (already decided):** matches `ingest_feed` / `gdrive_recovery`,
  survives the request lifecycle, doesn't tie up a gunicorn worker, and behaves
  behind Traefik. The console must degrade gracefully when the worker is down —
  reuse the existing `ImportError`/timeout messaging ("Celery/tasks unavailable…").
- **Daemons / non-runnable:** `run_discord_bot` and anything with
  `runnable=False` render docs + usage but **no Run button**; the `run` view
  rejects them defensively even if the request is forged.

---

## 8. Log streaming — buffer util + polling delivery

### Buffer (shared util) — **implemented (Step 1, 2026-06-26)**

The near-duplicate `CacheLogStream` and `_RecoveryStream` are now one canonical class,
`CommandLogStream`, living in
[pod_manager/admin_console/log_stream.py](../pod_manager/admin_console/log_stream.py).
All call sites use it (see "As-built" below):

```python
class CommandLogStream(io.StringIO):
    """Tees command stdout/stderr to a cache key (the live buffer) while keeping a
    raw capture for post-run parsing / persistence. Terminal marker is '[DONE]'."""
    def __init__(self, task_id): ...
    def write(self, s): ...        # append new output to the cache key (SSE-framed)
    def captured(self): ...        # raw text, for CommandRun.log + summary parsing
```

It keeps the `captured()` capability `_RecoveryStream` relied on, so the GDrive
callers migrated with no behavior loss. Buffer key for the console = `admin_cmd_{run_id}`
(the existing callers keep their own keys: `import_logs_{show_id}`,
`gdrive_recovery_{run_id}`).

> **As-built (Step 1 — 2026-06-26).** Extracted `CommandLogStream` into the new
> `pod_manager/admin_console/` package
> ([log_stream.py](../pod_manager/admin_console/log_stream.py)) and repointed all
> three callers in [tasks.py](../pod_manager/tasks.py):
> `task_ingest_feed` ([tasks.py:64](../pod_manager/tasks.py#L64)),
> `task_run_gdrive_recovery` ([tasks.py:690](../pod_manager/tasks.py#L690)), and
> `task_run_gdrive_rewind` ([tasks.py:739](../pod_manager/tasks.py#L739)). The two
> old classes and the now-unused `import io` were removed from `tasks.py`.
>
> **Unification details (no behavior change):** the merged `write()` (a) calls
> `super().write()` so `captured()` is just `getvalue()` — no separate list/string
> accumulator; (b) keeps `_RecoveryStream`'s empty-write guard and "only `cache.set`
> when there's a framed chunk" (a strict superset of the old `CacheLogStream`, which
> set the key unconditionally — harmless either way); (c) returns the char count from
> `write()` (both old classes returned `None`; proven-safe, now strictly more correct).
> The `[DONE]` sentinel is still written by each caller in its `finally` block, not by
> the stream — `write('[DONE]')` frames to `data: [DONE]\n\n` exactly as before, so the
> SSE views' `"[DONE]" in chunk` close condition is untouched. The cache timeout (3600s)
> is preserved as a module constant `CACHE_TIMEOUT`.
>
> **Logging.** Added an INFO line per task naming the cache key it streams to
> (pre-seeds the §11 audit trail / aids ops debugging). The util module has its own
> `logger` for future use.
>
> **Tests.** New `CommandLogStreamTests` (5 tests, [tests.py](../pod_manager/tests.py))
> assert the contract: SSE framing in cache, raw `captured()`, `[DONE]` framing,
> empty-write no-op, and blank-line-captured-but-not-framed. Verified: full
> `pod_manager` suite (311 tests) green; `manage.py check` clean.

### Delivery — polling, not SSE

**The console polls; it does not hold an SSE connection.** This is a deliberate
reversal of the original draft, driven by evidence in our own code: `staff.py`'s
`log_poll` exists precisely because polling is *"More reliable than SSE with
gunicorn sync workers behind Traefik"* ([staff.py:125](../pod_manager/views/staff.py#L125)).
We follow that precedent rather than fight it.

- The **poll endpoint** (`run_poll`, §9) reads the cache buffer for
  `admin_cmd_{run_id}` and returns the delta since the client's `offset`, plus the
  current `CommandRun.status`. Shape: `{chunk, offset, status}`.
- The **client** polls every ~1–2 s, appending `chunk` to the log pane, and stops
  when `status` is terminal (`completed`/`failed`) or it sees `[DONE]`.
- **No wall-clock cutoff on the view** — long backfills are fine because nothing is
  held open. Completion is driven by `CommandRun.status`, not a timer. The real
  backstop is the Celery task's soft/hard time limit; `CommandRun` records the
  outcome either way (§8a).
- **Stall indicator:** if `status == running` but the buffer hasn't grown for N
  minutes, the UI shows "still running (no recent output)" rather than an error.
- The existing SSE callers (`stream_feed_import`, `gdrive_recovery_stream`) are left
  as-is for now; only the new console uses polling. If we later unify, the shared
  util makes either delivery trivial.

This buffer refactor is low-risk and independently valuable (removes duplication) —
it can land first.

---

## 8a. Run history & results (persisted for every command)

Every console-initiated run is recorded, so we keep a durable, queryable audit of
what ran, by whom, with what arguments, and how it ended — including the full log.

### Model

New `CommandRun` model in `pod_manager/models.py`:

| Field | Purpose |
|---|---|
| `run_id` (UUID, unique) | matches the cache stream key `admin_cmd_{run_id}` |
| `command` (str) | management command name |
| `args` / `options` (JSON) | exactly what was dispatched (the serialized form state) |
| `command_line` (str) | the assembled `python manage.py …` string (§5b) for display/replay |
| `user` (FK → User) | who launched it (the superuser) |
| `status` (choices) | `queued` → `running` → `completed` / `failed` |
| `created_at` / `started_at` / `finished_at` | lifecycle timestamps (America/New_York app TZ) |
| `error` (text) | exception string when `failed` |
| `log` (text) | full captured stdout/stderr from `CommandLogStream.captured()` |
| `result_summary` (JSON, optional) | structured outcome where a command exposes one (see below) |

Helper methods `mark_running()` / `mark_finished(status, error=None)` keep the task
body small (§7). Rows are written by the worker, so history survives even if the
operator closes the browser mid-run.

### Results / summaries

`log` is always captured. For richer outcomes we reuse the pattern
`task_run_gdrive_recovery` already uses — regex/parse the captured output into a
small `result_summary` dict (e.g. counts migrated/skipped/failed, bytes moved,
"would update N"). v1 can parse a couple of high-value commands and leave
`result_summary` null for the rest; the full `log` is the universal fallback.

### Relationship to the existing GDrive `runs/*.json`

The GDrive Recovery flow already persists rich per-run JSON under
`media/Recovery/runs/`. Those runs execute in the Creator UI (not the console), so
they keep their existing richer store as-is. `CommandRun` is the canonical history
for **console-initiated** runs; we can optionally surface the GDrive runs read-only
later, but we don't migrate or duplicate them now.

### UI

- **Per-command history:** the detail pane lists recent `CommandRun`s for the
  selected command (status badge, who, when, duration), each expandable to its
  stored `log` + `result_summary`.
- **Global history:** a "Recent runs" view across all commands (filter by command,
  user, status) — handy as an at-a-glance audit.
- **Re-run:** because `command_line`/`args`/`options` are stored, a past run can
  pre-fill the form for a one-click repeat (respecting the danger gate).
- **Retention:** keep a generous window; a `prune_*` style cleanup (or a `--days`
  option mirroring `prune_logs`) can trim old rows if the `log` text grows large.

---

## 9. URLs, views, files

New module: `pod_manager/admin_console/` (registry, log_stream util, helpers) plus
views. Proposed routes (under `/admin-console/` to avoid colliding with Django's
`/admin/`):

```
GET  /admin-console/                      console  — page shell + command list
GET  /admin-console/command/<name>/       command_detail — JSON: docs + form schema + recent runs
POST /admin-console/command/<name>/build/ build    — JSON: {valid, command_line} serializer for the copy box (§5b), any command
POST /admin-console/command/<name>/run/   run      — validate + create CommandRun + dispatch, returns run_id
GET  /admin-console/run/<run_id>/poll/    run_poll — JSON: {chunk, offset, status} (polled, §8)
GET  /admin-console/runs/                  history  — JSON: global run history (filterable)
GET  /admin-console/run/<run_id>/         run_detail — JSON: one CommandRun (log + summary), for replay/expand
GET  /admin-console/lookup/episodes/      episode_search — JSON typeahead for the episode picker (§5a)
```

The **`build`** endpoint is the server-side realization of §5b's "one source of truth":
it runs the same `reconstruct_invocation` serializer as `run` but **without
dispatching**, returning the shell-quoted, secret-redacted command line. It works for
*every* registered command — crucially the deep-link / docs-only ones
(`recover_gdrive_audio`, `crawl_by_id`, …), whose CLI is meant for a terminal even
though the console won't execute them. An incomplete form returns `valid=False` (nothing
to copy yet) rather than an error.

All `@superuser_required`. `command_detail` returns the introspected form schema +
merged docs + the command's recent `CommandRun`s as JSON so the frontend renders
the form and history dynamically (no per-command template). The page shell can be a
single template; the detail pane is populated client-side from the JSON.

The **`episode_search`** endpoint backs the episode picker: it takes `?q=` and
returns a small page of `{id, title, podcast}` matches (id and/or title search). It
exists because Episodes number in the tens of thousands — unlike Networks/Podcasts,
they can't be shipped as an inline option list (§5a). Networks and Podcasts are
small enough that their option lists ride along in the `command_detail` schema; only
Episodes need a live lookup.

Views can live in a new `pod_manager/views/admin_console.py` (sibling of
`staff.py`), wired into `config/urls.py` next to the existing `staff/...` routes.

---

## 10. Frontend

Single template `admin_console.html` (extends `base.html`), three regions:

1. **Sidebar** — commands grouped by `category`; danger commands flagged; a
   "Discovered but unregistered" footnote (§4c).
2. **Detail pane** — selected command's docs (from the three in-code sources, §6),
   the **auto-generated form** of semantic widgets (§5a), the **live command-line
   builder** copy box (§5b), the primary action (**Execute** / **Open in …** /
   none, per §5b), and a **recent-runs list** for this command (§8a) with
   expand-to-log and one-click re-run. Danger commands require typing the command
   name to enable Execute.
3. **Log pane** — reuses the **polling** log-viewer interaction from `/staff/logs/`
   (`log_poll`, not SSE — §8): polls `run_poll`, appends new chunks, shows
   running/stalled/done/error state, stop-following toggle, copy-output button. On
   completion the run is persisted to history (§8a).
4. **History view** — a global "Recent runs" table across all commands (§8a),
   filterable by command / user / status.

No template change is needed per command — the form, docs, command line, and
history are all data-driven from JSON. Styling uses the project's theme
variables and squared/themed design language, including the `--vecto-radius-*`
tokens — no hardcoded colors or radii (see project conventions).

---

## 11. Safety considerations

- **Superuser-only**, enforced on every endpoint, not just the nav.
- **Registry gate** — only explicitly registered commands are runnable.
- **Danger flag** — destructive commands (`purge_r2_dev`, `purge_r2_media_dev`,
  `clear_transcription_queue`, and anything with a `--yes` guard) get red styling
  and a typed confirmation before the Run button enables. The underlying command's
  own `--yes`/`--dry-run` guards remain the real safety net.
- **Non-runnable** — `run_discord_bot` (daemon) and `crawl_by_id` (vestigial; one
  run queues ~110k Celery tasks) are docs-only (`runnable=False`); the `run` view
  rejects them even if the request is forged.
- **Argument re-validation** — the `run` view re-parses the submitted args through
  the command's real parser before dispatch; never trust the posted form alone.
- **Audit trail** — log every dispatch (who/what/args) at INFO so it lands in the
  existing `LogEntry` viewer. Consider rejecting/queueing if an identical command is
  already running (optional, v2).
- **Long-running / blocking** — some runnable commands (e.g. `backfill_*`,
  `mirror_audio_to_r2 --all`) dispatch many downstream Celery jobs; the console
  streams the *launching* command's output, not the downstream jobs. Note this in
  each such command's doc so operators aren't surprised.

---

## 12. Full command + argument enumeration

Every command in `pod_manager/management/commands/`, every argument, and the widget
each maps to (`*` = positional; **bold widget** = needs a registry `field_widgets`
override; everything else auto-infers). "Run" / "Danger" drive `runnable` and the
typed-confirm gate.

| Command | Args → widget | Run | Danger |
|---|---|---|---|
| `ingest_feed` | `podcast_id`* → podcast picker | ✅ | |
| `transcribe_episode` | `episode_id`* → episode picker | ✅ | |
| `backfill_baldmove_tags` | `--network` → network dropdown (default `baldmove`); `--force` → checkbox; `--apply` → checkbox | ✅ | |
| `crawl_by_id` | `--start`/`--end` → number; `--cookie_name`/`--cookie_value` → text (required; `--cookie_value` **sensitive**) | ❌ docs-only | heavy |
| `backfill_transcripts` | `--podcast` → podcast **multi-select**; `--stagger` → number (30); `--apply` → checkbox; `--model`/`--language`/`--initial-prompt` → text (`model` could be a **registry enum**); `--min/num/max-speakers` → number | ✅ | |
| `backfill_transcripts_to_r2` | `--all`,`--force`,`--only-missing`,`--apply`,`--yes`,`--verify`,`--prune` → checkbox; `--network` → network dropdown; `--podcast` → podcast dropdown; `--limit` → number | ✅ | prune (needs `--yes`) |
| `backfill_media_to_r2` | `--avatars`,`--covers`,`--all`,`--force`,`--only-missing`,`--apply`,`--yes`,`--verify`,`--prune` → checkbox; `--limit` → number | ✅ | prune (needs `--yes`) |
| `mirror_audio_to_r2` | `--episode` → episode picker; `--local-path` → text; `--all`,`--force`,`--only-missing`,`--apply`,`--sync` → checkbox; `--network` → network dropdown; `--podcast` → podcast dropdown; `--origins` → **enum multi** (`audio_origins`); `--stagger` → number; `--limit` → number | ✅ | |
| `rename_source_audio_to_r2` | `--network` → network dropdown; `--podcast` → podcast dropdown; `--limit` → number; `--apply` → danger gate (preview default; moves local files) | ✅ | apply |
| `r2_gc` | `--apply` → checkbox; `--age-days` → number (7) | ✅ | apply |
| `r2_cleanup_orphans` | `--apply --yes` → danger gate | ✅ | **yes** |
| `r2_smoke_test` | `--keep` → checkbox | ✅ | |
| `purge_r2_dev` | `--apply --yes` → danger gate | ✅ | **yes** |
| `purge_r2_media_dev` | `--apply --yes` → danger gate | ✅ | **yes** |
| `clear_transcription_queue` | `--apply --yes` → danger gate | ✅ | **yes** |
| `prune_logs` | `--days` → number; `--apply --yes` → danger gate | ✅ | **yes** |
| `clean_mix_images` | `--apply --yes` → danger gate | ✅ | **yes** |
| `generate_s3_report` | *(no args)* | ✅ | |
| `list_recurly_plans` | *(no args)* | ✅ | |
| `gdrive_discord_report` | `csv_path`* → **CSV picker**; `--output` → text | ✅ | |
| `recover_gdrive_audio` | `csv_path`* → **CSV picker**; `podcast_title`* → podcast dropdown (by title, optional); `--apply` → checkbox; `--min-confidence` → choice dropdown (auto); `--output`/`--prefix-map` → text | 🔗 deep-link | |
| `rewind_gdrive_audio` | `csv_paths`* → **CSV multi-picker** (`nargs='+'`); `--apply` → checkbox | 🔗 deep-link | caution |
| `run_discord_bot` | *(daemon — no console args)* | ❌ docs-only | n/a |

Run column: ✅ = in-console Execute; 🔗 = no Execute, deep-links to the existing
Creator UI (CLI builder still shown); ❌ = docs-only.

The argument lists above are **as-built after Step 0** — every mutating command now
follows the uniform `--apply` / `--apply --yes` convention (§16 "As-built").
`--yes`/`--apply` flags are bound to the danger gate: the operator confirms once in
the UI and the console passes the flag for them. Auto-inference covers all
`--network`/`--podcast`/`--episode`/`choices=` fields; the only true registry
`field_widgets` overrides are `mirror_audio_to_r2 --origins`, the three CSV-path
positionals (`gdrive_discord_report`, `recover_gdrive_audio`, `rewind_gdrive_audio`),
and optionally `backfill_transcripts --model`/`--language`.

---

## 13. Suggested build order

0. ✅ **Command normalization pass — DONE (2026-06-26).** Full spec **and as-built
   record** in §16. Two parts, both complete: (a) **Safety-idiom unification** —
   every mutating command now follows uniform `--apply` / `--apply --yes`, with all
   programmatic call sites and adjacent feature docs updated; (b) **Docs** — module
   docstrings added to the 11 commands missing them (plus `rewind_gdrive_audio`),
   and `crawl_by_id`'s four bare arg `help=` strings filled in. Logging and a
   `CommandSafetyIdiomTests` regression suite were added. Verified non-issue: stdout
   capture (only `run_discord_bot` logs instead of writing to stdout, and it's
   non-runnable).
1. ✅ **Log-buffer refactor — DONE (2026-06-26).** `CacheLogStream`/`_RecoveryStream`
   → one shared `CommandLogStream` util in
   [pod_manager/admin_console/log_stream.py](../pod_manager/admin_console/log_stream.py);
   all three callers in [tasks.py](../pod_manager/tasks.py) repointed (no behavior
   change). Logging + `CommandLogStreamTests` added. As-built note in §8.
2. ✅ **Backend skeleton — DONE (2026-06-26).** `superuser_required`, the registry
   module, introspection + semantic-widget resolver → schema helper, the `CommandRun`
   model + migration, `task_run_management_command`, and the eight superuser JSON
   routes (incl. the §5b `build` copy-box serializer). As-built record below.
3. ✅ **Frontend — DONE (2026-06-26).** Nav tab, console page, dynamic form renderer
   with the semantic widgets + DB-backed pickers, live command-line builder +
   Execute/Open-in actions, polling log pane, per-command + global run history with
   expand-to-log and one-click re-run. As-built record below.
4. ✅ **In-code doc backfill + wiring — DONE.** The four `crawl_by_id` `help=` strings
   landed in Step 0 (§16 as-built); Step 3's `build_schema` already merges the three
   in-code doc sources (§6) and the detail pane now renders them (summary, module
   docstring, per-arg help). No registry prose was needed. See the as-built below.
5. **Safety polish:** danger confirmations, audit logging (via `CommandRun`),
   "discovered but unregistered" notice + optional CI test, graceful Celery-down
   messaging.
6. **Convert this doc to living documentation** (see §0): as each piece lands,
   update the matching section to describe what *is*, not what's *planned*.

---

### As-built (Step 2 — implemented 2026-06-26)

The backend skeleton landed; the frontend (Step 3) will consume the JSON these
routes return. Ground truth for the next session:

**New files**

| File | Contents |
|---|---|
| [admin_console/registry.py](../pod_manager/admin_console/registry.py) | `CommandSpec` dataclass (`category`/`danger`/`runnable`/`deep_link`/`field_widgets`/`field_labels`/`sensitive` + the doc escape-hatch fields) and `REGISTRY` — **one entry per command, all 23 from §12 registered**. `get_spec(name)`. |
| [admin_console/schema.py](../pod_manager/admin_console/schema.py) | `discover_commands` / `unregistered_commands` (§4a/§4c); `build_schema(name)` (introspection + widget resolver + in-code docs, fault-tolerant §15.8); `reconstruct_invocation(name, payload)` (form → validated `*args`/`**options` + redacted, **shell-quoted** copies, §15.5/§15.6); option-list sources (`_network_options`/`_podcast_options`/`audio_origin_choices`/`list_recovery_csvs`); `InvalidInvocation`. |
| [views/admin_console.py](../pod_manager/views/admin_console.py) | The eight `@superuser_required` JSON views (§9): `console`, `command_detail`, `build`, `run`, `run_poll`, `run_detail`, `history`, `episode_search`. |
| [migrations/0086_commandrun.py](../pod_manager/migrations/0086_commandrun.py) | `CommandRun` table. |

**Touched files**
- [views/staff.py](../pod_manager/views/staff.py#L37) — added `superuser_required` (mirrors `staff_required`, `is_staff`→`is_superuser`).
- [models.py](../pod_manager/models.py) — `CommandRun` model (§8a) with `Status` choices, `mark_running()`/`mark_finished()`, `duration_seconds`.
- [tasks.py](../pod_manager/tasks.py) — `task_run_management_command(run_id, name, args, options)` (§7), reusing the Step 1 `CommandLogStream`.
- [views/__init__.py](../pod_manager/views/__init__.py) + [config/urls.py](../config/urls.py) — exports + the `/admin-console/…` routes. (The `NetworkResolver` middleware already whitelists `/admin*`, so the console path needs no middleware change.)

**Decisions made during the build (deviations / resolutions worth keeping):**

1. **`danger` is two-tier: always-danger vs. danger-only-when-pruning.** Static
   `danger=True` is set for the **six commands whose *primary* action irreversibly
   deletes** (the "**yes**" rows in §12: `purge_r2_dev`, `purge_r2_media_dev`,
   `r2_cleanup_orphans`, `clear_transcription_queue`, `prune_logs`, `clean_mix_images`)
   — these always require the typed-confirm gate. The two `backfill_*_to_r2` commands
   are benign backfills whose *only* destructive path is `--prune`, so rather than a
   blanket `danger` (which would force a confirm on an ordinary backfill), they declare
   `danger_fields=frozenset({"prune"})`: the run view engages the gate when
   `spec.danger` **or** any `danger_fields` dest is truthy in the payload. Net effect —
   a plain `--apply` backfill runs without a confirm; the same command with `--prune`
   checked requires typing the command name. (`danger_fields` is surfaced in the schema
   so the Step 3 UI can light the field up red dynamically.) This honors §12's
   distinction between "prune (needs --yes)" and "**yes**" while keeping §11's intent
   that anything actually deleting gets confirmed.
2. **Secret redaction is now real (closes the §16/§15.6 stub).** `reconstruct_invocation`
   returns both the *real* `args`/`options` (passed to `call_command`) and *redacted*
   copies (`***` for any dest in `spec.sensitive`); `CommandRun.args`/`options`/
   `command_line` persist the redacted form, and the worker's `[SYSTEM]` echo uses the
   redacted `command_line`, so secrets never reach the DB or the live log. `crawl_by_id`'s
   `--cookie_value` is wired as `sensitive` (it's non-runnable, so this is purely the
   forward-looking guard §15.6 asked for). **Caveat:** the *real* values still travel in
   the Celery message body (necessary to execute); §15.6 concerns DB persistence + the
   copy box, both covered. No runnable command currently has a sensitive arg.
3. **`enum_multi` over a single-valued arg comma-joins.** `mirror_audio_to_r2 --origins`
   is one `type=str` comma-list, not `append`; the multi-select widget posts a list and
   reconstruction joins it (`--origins=gdrive,libsyn`). The append-backed multis
   (`backfill_transcripts --podcast`) repeat the flag instead. Both are handled.
4. **Globals hidden, not surfaced.** `--verbosity` and the argparse/Django built-ins are
   denylisted out of the form (`_HIDDEN_DESTS`). The §14 "Advanced disclosure for
   verbosity" idea is **not** implemented — deferred; revisit in Step 3 if wanted.
5. **`recover_gdrive_audio`'s `podcast_title` positional** resolves to a plain `text`
   widget (not the slug picker) because its dest isn't a conventional name and it matches
   by *partial title*, not slug. It's a deep-link/non-runnable command, so low priority;
   left as text rather than inventing a `podcast_by_title` widget.
6. **`run` view ordering:** validates registered + runnable → danger typed-confirm →
   `reconstruct_invocation` (re-parse) → Celery import guard (503 if unavailable) →
   *then* creates the `CommandRun` row and dispatches. The row is only written once
   dispatch is certain, so there are no orphan `queued` rows from a rejected request.
7. **Copy box served by `build`, not built blind in JS.** The paste-ready command line
   comes from the backend `build` endpoint (same serializer as `run`), so it's
   shell-quoted (`shlex.quote` — Recovery CSV names contain spaces) and secret-redacted
   identically to what a dispatched run records. The Step 3 frontend posts form state
   and renders the returned string; it never reassembles the CLI itself, so the copy box
   and Execute can't drift. Works for deep-link/docs-only commands too (their whole
   point is a terminal-ready CLI).

**Open §14 questions still open after Step 2:** concurrency (duplicate-run blocking) is
**not** implemented (a second identical run is allowed); `result_summary` is left `null`
for every command (the full `log` is the universal fallback) — both are v1-acceptable per
§8a/§14 and deferred.

**Logging.** `task_run_management_command` logs the cache key it streams to (INFO); the
`run` view logs every dispatch at INFO with user + redacted command line (§11 audit
trail, lands in the `LogEntry` viewer).

**Tests.** Three new classes in [tests.py](../pod_manager/tests.py), **38 tests**:
`AdminConsoleSchemaTests` (widget inference, registry overrides, sensitive flagging,
shell-quoted command line, import-error fault tolerance, reconstruction coercion/
validation/redaction), `AdminConsoleViewTests` (superuser gating, console grouping,
detail, `build` copy-box serializer, run dispatch + non-runnable/static-danger/
dynamic-prune-danger/invalid rejection, poll/detail/history, episode search), and
`TaskRunManagementCommandTests` (runner lifecycle on success + failure). Verified: full
`pod_manager` suite (**349 tests**) green; `manage.py check` clean.

---

### As-built (Steps 3 & 4 — implemented 2026-06-26)

The frontend landed and wires the in-code docs into the UI; the backend JSON
contract from Step 2 is consumed unchanged. Ground truth for future sessions:

**New files**

| File | Contents |
|---|---|
| [templates/pod_manager/admin_console.html](../pod_manager/templates/pod_manager/admin_console.html) | The single page shell (extends `base.html`, §10). Renders the **sidebar** server-side from the cheap registry payload (categories + the §4c "discovered but unregistered" notice); the detail pane, log pane, and global-history modal are populated client-side. Embeds `csrf_token` via `json_script` + a small `window.ADMIN_CONSOLE` config (base URL), then loads the CSS/JS below. |
| [static/pod_manager/css/admin_console.css](../pod_manager/static/pod_manager/css/admin_console.css) | All console styling. Squared/themed; rides the `--vecto-*` tokens + the `--vecto-radius-*` scale (no hardcoded colors/radii). One console-local token `--ac-danger: var(--bs-danger)` for the destructive accent. |
| [static/pod_manager/js/admin_console.js](../pod_manager/static/pod_manager/js/admin_console.js) | The whole data-driven frontend (one IIFE, vendored — no CDN): sidebar select/filter, `command_detail` fetch → detail render, the **per-widget form renderer** (§5a), the live **command-line builder** (debounced `build` POST), the primary action (Execute / Open-in / none, §5b), the **polling log pane** (`run_poll`, parses the `data: …\n\n` SSE frames + `[DONE]`, stall indicator §8), per-command + global **run history** (expand-to-log via `run_detail`, one-click **re-run** that prefills the form from stored args/options), and the history modal. |

**Touched files**
- [templates/pod_manager/base.html](../pod_manager/templates/pod_manager/base.html) — new `{% if request.user.is_superuser %}` **Admin** nav link next to **Logs** (§3).
- [views/admin_console.py](../pod_manager/views/admin_console.py) — `console` now **renders the template** (page shell) instead of returning JSON; grouping extracted to `_console_payload()` (now also ships `danger_fields` per command so the sidebar/UI can flag dynamic-danger commands). All other routes unchanged.
- [tests.py](../pod_manager/tests.py) — `test_console_groups_commands_and_reports_no_unregistered` updated to assert the template + `resp.context` (the view no longer returns JSON).

**Frontend ↔ schema mapping (the widget renderers):** `flag`→checkbox; `choice`/
`network`/`podcast`/`csv_path` (single)→`<select>`; `network_multi`/`podcast_multi`/
`enum_multi:*`/any multi `csv_path` (rewind's `nargs='+'`)→`<select multiple>`;
`episode`→typeahead backed by `episode_search`; `number`→number input; everything
else→text. The form never reassembles the CLI itself — the copy box and Execute both
come from the backend `build`/`run` serializer (§5b / Step 2 as-built item 7), so they
can't drift.

**Decisions made during the build:**

1. **`console` became an HTML view (deviation from the Step 2 JSON stub).** §9 always
   described `/admin-console/` as the *page shell*; Step 2 had it return JSON as a
   placeholder. Step 3 makes it render `admin_console.html`, with the sidebar grouped
   server-side (no extra round-trip for the static command list) and the dynamic panes
   fetched from the JSON endpoints. The grouping logic is preserved in
   `_console_payload()` and still covered by a test (now via `resp.context`).
2. **Danger flags are shown as real checkboxes, not auto-supplied.** For destructive
   commands the form renders the actual `--apply`/`--yes`/`--prune` checkboxes; the
   operator ticks the flags and (for an apply) types the name. This is more explicit
   than silently injecting the flags (the command line reflects exactly what runs) and
   matches the run view's contract that `apply`/`yes` arrive as normal `fields`.
   **The typed-confirm gate engages only on a real apply.** A preview/dry-run mutates
   nothing, so it never asks for confirmation — both the frontend (`dangerActiveNow`)
   and the `run` view require the destructive condition *and* `--apply` (falling back
   to always-confirm only for a destructive command that has no `--apply` flag). So
   ticking only Apply+Yes on `purge_r2_dev` shows the gate; running it with no flags
   (a preview) dispatches straight away.
3. **Log frame parsing on the client.** The poll buffer is SSE-framed (`data: <line>\n\n`
   from `CommandLogStream`); the poller keeps a remainder buffer and only renders
   complete frames, so a delta that splits mid-frame never corrupts a line. `[SYSTEM]`/
   `[ERROR]` prefixes get accent coloring; `[DONE]` ends the poll.
4. **Re-run prefill.** Stored `args` (positionals, in declared order) + `options`
   (`{dest: value}`) are mapped back onto the schema's fields to repopulate the form
   for a one-click repeat. Cross-command re-run (from the global history modal) loads
   the target command's schema first, then prefills. Redacted secrets would prefill as
   `***`, but no runnable command has a sensitive arg today.
5. **Stall indicator (§8).** If status is `running` but the buffer hasn't grown for
   >2 min, the badge shows `stalled` rather than an error; polling continues.

**Conventions honored:** all JS/CSS is self-hosted under `static/` (no CDN —
`VendoredAssetTests` only flags CDN `<script>`/`<link>`, which the page has none of);
styling uses the `--vecto-*` tokens and the `--vecto-radius-*` scale exclusively.

**Verification.** `manage.py check` clean; `AdminConsoleViewTests` (22, incl. the
template-render assertion) + `AdminConsoleSchemaTests`/`TaskRunManagementCommandTests`
(19) + `VendoredAssetTests` green via the project venv; JS passes `node --check`. One
runtime bug was caught and fixed during Josh's first page load: `csrf_token` is a
`SimpleLazyObject`, so `json_script` couldn't encode it — the config block now emits the
token as a plain string (`"{{ csrf_token }}"`). Live UI behavior is Josh's to verify
(he browser-tests himself, per project convention). **Step 5** (safety polish —
confirmations + the unregistered notice are in; remaining: optional CI test for
unregistered, richer Celery-down messaging) and **Step 6** (final doc conversion) are
the only build-order items left.

---

## 14. Decisions locked & remaining open questions

**Locked:**
- **Run history:** persist every console run with results via `CommandRun` (§8a).
- **GDrive commands:** deep-link to the existing Creator UI for execution; still
  show docs + CLI builder for terminal use (§5b).
- **Safety-idiom uniformity:** unify all mutating commands on the `--apply` / `--yes`
  convention as Step 0, including call-site updates. Full spec in **§16**.

**Still open for the next session:**
- Surface `--verbosity` (and other safe global options) in an "Advanced" section,
  or hide globals entirely?
- Concurrency: block a second identical run while one is in flight, or allow it?
- `result_summary` parsing: which commands get structured summaries in v1 vs.
  log-only (§8a)?

---

## 15. Implementation risks & gotchas

Captured during design review so the build session decides them deliberately rather
than mid-build. None block starting.

1. **Polling, not SSE (resolved → §8).** Our own `log_poll` exists because SSE is
   unreliable with gunicorn sync workers behind Traefik. The console polls
   `run_poll`. Don't reintroduce a held SSE connection for the console path.

2. **Long backfills (resolved → §8).** Polling removes the held-connection timeout,
   so there's no wall-clock cap. Completion is driven by `CommandRun.status`; the
   Celery soft/hard time limit is the backstop; a stall indicator covers "running
   but quiet." Make sure the chosen Celery time limit is generous enough for the
   largest backfill, or set it per-task.

3. **Queue topology (resolved, non-issue).** Transcription runs on its own two
   queues; the default queue handles everything else. Console runs share the default
   queue exactly as today — no new starvation risk. Documented so it isn't re-raised.

4. **Episode picker scale (resolved → §5a/§9).** Episodes are too many for an inline
   list; the `episode_search` typeahead endpoint backs that one widget. Networks and
   Podcasts stay inline.

5. **`call_command` reconstruction.** Translate form values → typed kwargs by
   argparse **dest** and `action.type`: `--min-confidence` → `min_confidence="HIGH"`,
   `store_true` → real `bool`, `nargs='+'` → `list`, positionals → ordered `*args`.
   Re-parse through the command's real parser to validate before dispatch; don't
   trust the posted form. (Note: passing kwargs to `call_command` can bypass argparse
   `required`/default handling, which is exactly why we re-parse.)

6. **Secret redaction (resolved → Step 2 as-built §13, item 2).** `CommandSpec.sensitive`
   (a frozenset of arg dests) marks secrets; `reconstruct_invocation` redacts them to
   `***` in the persisted `CommandRun.args/options/command_line` and the worker's
   `[SYSTEM]` echo, while the real values pass to `call_command` only. `crawl_by_id`'s
   `--cookie_value` is wired as the first `sensitive` field.

7. **Command style uniformity (Step 0).** Findings: stdout capture is already
   uniform (✅). Module docstrings are missing on ~11 commands — add them.
   `crawl_by_id` has bare arg help — fix it. The act/preview **safety idiom is
   inconsistent** across three patterns (`--apply` previews-by-default; `--dry-run`
   acts-by-default; `--yes` confirms). **Decision (locked): unify it now** — full
   detailed spec, scope, and call-site sweep in **§16**.

8. **Lazy, fault-tolerant introspection.** Building a command's schema imports its
   module (`run_discord_bot` pulls discord libs, transcription pulls whisper
   services). Introspect per-command (only on detail view), wrap in try/except, and
   surface an import failure as a disabled card — never let one un-importable command
   break the whole console.

9. **CSRF + standard hardening.** The `run` POST needs the CSRF token (the existing
   AJAX endpoints show the pattern); keep `@superuser_required` on every endpoint
   including the lookups and poll.

---

## 16. Step 0 — Safety-idiom normalization (detailed spec)

Today three different idioms mean "do it for real": `--apply` (preview by default),
`--dry-run` (act by default), and `--yes` (destructive confirm). We unify them
**before** building the console so the console — and the CLI — behave predictably.

> The resulting convention is captured as a standalone, forward-looking reference
> for writing *new* commands: [management-command-conventions.md](management-command-conventions.md).
> This section is the one-time normalization plan for the *existing* commands.

### The convention

1. **Preview by default.** Any command that mutates state defaults to a **dry run**:
   it reports what it *would* do and changes nothing.
2. **`--apply` executes.** One flag, same name everywhere, performs the changes.
3. **`--yes` confirms destruction.** Commands that *irreversibly delete* objects/
   files/rows require `--yes` **in addition to** `--apply`; with `--apply` but no
   `--yes` they abort with a clear message. (The console danger gate supplies both
   after the typed confirmation.)
4. **Exempt commands act directly.** Read-only commands and single-target
   operational actions (ingest one feed, transcribe one episode, generate a report,
   smoke test) take neither flag — running them *is* the intent.
5. **Remove the legacy flags.** Delete `--dry-run` (its behavior is now the default)
   and the standalone "`--yes` means go" semantics (now `--apply` = go, `--yes` =
   confirm-destruction). A destructive sub-mode like `--prune` requires `--yes` when
   combined with `--apply`.

Net rule of thumb: **reversible mutation → `--apply`; irreversible deletion →
`--apply --yes`; read-only / single action → neither.**

### Per-command target state

**Exempt — act directly, no flag change:**
`ingest_feed`, `transcribe_episode`, `generate_s3_report`, `list_recurly_plans`,
`gdrive_discord_report`, `r2_smoke_test`. (`crawl_by_id`, `run_discord_bot` are
non-runnable; leave as-is.)

**Reversible mutation → `--apply` (preview by default):**

| Command | Today | Change |
|---|---|---|
| `rename_source_audio_to_r2` | `--apply` | none (reference impl ✓) |
| `r2_gc` | `--apply` | none ✓ |
| `mirror_audio_to_r2` | `--dry-run` (acts by default) | drop `--dry-run`; add `--apply`; flip default to preview |
| `backfill_media_to_r2` | `--dry-run` | drop `--dry-run`; add `--apply`; `--prune` now implies `--yes` |
| `backfill_transcripts_to_r2` | `--dry-run` | drop `--dry-run`; add `--apply`; `--prune` implies `--yes` |
| `backfill_transcripts` | `--dry-run` | drop `--dry-run`; add `--apply` |
| `backfill_baldmove_tags` | always acts | add `--apply`; flip default to preview |
| `recover_gdrive_audio` | `--dry-run` | drop `--dry-run`; add `--apply` (see call sites) |

**Irreversible deletion → `--apply --yes`:**

| Command | Today | Change |
|---|---|---|
| `r2_cleanup_orphans` | `--apply` | add required `--yes` gate |
| `purge_r2_dev` | `--yes` (= go) | default → dry run; require `--apply --yes` to delete |
| `purge_r2_media_dev` | `--yes` + `--dry-run` | collapse to `--apply --yes`; drop `--dry-run` |
| `clear_transcription_queue` | `--yes` (= go) | default → dry run; require `--apply --yes` |
| `clean_mix_images` | always acts | default → dry run; require `--apply --yes` |
| `prune_logs` | always acts (`--days`) | default → dry run; require `--apply --yes`; keep `--days` |

**Undo command:** `rewind_gdrive_audio` (deep-link only). **Decided (2026-06-26):
added `--apply`** (preview by default) for full uniformity. The Creator UI rewind
button has no preview toggle, so its call site (`task_run_gdrive_rewind`,
[tasks.py:784](../pod_manager/tasks.py#L784)) now passes `apply=True` unconditionally.

### Call sites to update (don't miss these)

Flipping defaults is a behavior change, so every programmatic invocation must be
updated to pass `--apply` (and `--yes`) where it currently relies on act-by-default:

| Call site | Command | Update |
|---|---|---|
| [tasks.py:340](../pod_manager/tasks.py#L340) `task_clean_mix_images` (nightly) | `clean_mix_images` | pass `apply=True, yes=True` so the sweep still deletes |
| [tasks.py:743](../pod_manager/tasks.py#L743) `task_run_gdrive_recovery` | `recover_gdrive_audio` | change `dry_run=dry_run` → `apply=not dry_run` |
| [tasks.py:784](../pod_manager/tasks.py#L784) `task_run_gdrive_rewind` | `rewind_gdrive_audio` | if `--apply` added, pass `apply=True` |
| [gdrive_recovery.py](../pod_manager/views/creator/gdrive_recovery.py) `gdrive_recovery_run` | (via task) | UI keeps its "dry run" toggle; map it to `apply` at the task boundary |
| [tests.py:3755-3761](../pod_manager/tests.py#L3755) | `mirror_audio_to_r2` | update test invocations to pass `apply=True` where they assert action |
| `config/celery.py` beat schedule | wrappers | confirm any scheduled wrapper that should *act* passes `apply`/`yes` (note: `task_prune_logs` deletes via the ORM directly, **not** the command, so it's unaffected) |

Also sweep for out-of-repo invocations during implementation: `docker-compose.yml`,
container entrypoints, and any host cron that calls these commands with `--dry-run`/
`--yes`. (The `task_prune_logs` ORM duplication is worth noting but out of scope.)

### Docs portion of Step 0

- Add module docstrings to the commands missing them: `ingest_feed`,
  `transcribe_episode`, `backfill_baldmove_tags`, `backfill_transcripts`,
  `gdrive_discord_report`, `generate_s3_report`, `list_recurly_plans`,
  `clean_mix_images`, `prune_logs`, `clear_transcription_queue`, `crawl_by_id`.
- Fix `crawl_by_id`'s four bare arg `help=` strings.
- Re-document each changed command's `--apply`/`--yes` behavior in its `Command.help`
  and docstring examples (the new default is preview, so examples should show
  `--apply`).

### Tests

**Existing tests that break (must update):**

- **`R2BackfillCommandTests`** ([tests.py:3723+](../pod_manager/tests.py#L3723)) —
  the only command-level suite among the changing commands. Because
  `mirror_audio_to_r2` flips to `--apply`:
  - Add `--apply` to every `_run(...)` that asserts dispatch:
    `test_all_selects_premium_fetchable_missing`, `test_origins_filter`,
    `test_origins_alone_is_a_valid_scope`, `test_limit_caps_dispatch_count`, and the
    `--force` test.
  - Rewrite `test_dry_run_dispatches_nothing` ([tests.py:3791](../pod_manager/tests.py#L3791)):
    `--dry-run` is removed, so call `_run('--all')` (no flag) and assert nothing
    dispatched + "would mirror" output — it becomes the "preview is the default" test.
  - `test_requires_a_scope` is unaffected.

**Existing tests that are safe (no change):**

- The R2 maintenance suite ([tests.py:3846-3942](../pod_manager/tests.py#L3846))
  calls the **service functions** (`reconcile_orphans`/`cleanup_orphans`/
  `purge_dev_prefix`) directly with `apply=`, bypassing the command layer. The new
  `--yes` gate lives in the command `handle()`, not the service, so these pass
  unchanged. `r2_gc` / `rename_source_audio_to_r2` already use `--apply` — no change.

**New tests to add** (commands with no current coverage — `recover_gdrive_audio`,
`backfill_*`, `clean_mix_images`, `prune_logs`, `clear_transcription_queue`,
`purge_r2_media_dev`, …): assert the uniform contract — no-flag = dry run (no
writes), `--apply` = writes, destructive without `--yes` = aborts. These double as
the regression guard for the call-site changes above.

---

### As-built (Step 0 — implemented 2026-06-26)

This subsection records what actually shipped, including deviations from the plan
above. Future sessions can trust this as ground truth for the command layer.

**Final per-command state** (matches the convention in
[management-command-conventions.md](management-command-conventions.md)):

| Command | Final flags | Notes |
|---|---|---|
| `mirror_audio_to_r2` | `--apply` (bulk); `--sync` requires `--apply` | `--episode` single-target still acts inline with no flag (exempt). `--dry-run` removed. |
| `backfill_media_to_r2` | `--apply`, `--yes` | `--prune --apply` requires `--yes`; `--verify` is read-only. `--dry-run` removed. |
| `backfill_transcripts_to_r2` | `--apply`, `--yes` | Same as above. R2-enabled guard refined to `needs_r2 = apply or verify or prune` (a plain preview needs no R2). |
| `backfill_transcripts` | `--apply` | `--dry-run` removed. Docstring added. |
| `backfill_baldmove_tags` | `--apply` | Preview still scrapes (network calls) but skips the DB write. Docstring added. |
| `recover_gdrive_audio` | `--apply` | `--dry-run` removed; internal `dry_run = not options['apply']`. Still writes the dry-run analysis CSV in preview. |
| `rewind_gdrive_audio` | `--apply` | New (was always-acts). Docstring added. |
| `r2_cleanup_orphans` | `--apply --yes` | `--yes` gate added in `handle()` (service unchanged). |
| `purge_r2_dev` | `--apply --yes` | Service `purge_dev_prefix()` gained a `dry_run` param (mirrors `purge_media_dev_prefix`); now returns `{deleted, keys, dry_run}`. |
| `purge_r2_media_dev` | `--apply --yes` | `--dry-run` removed; preview is the default. |
| `clear_transcription_queue` | `--apply --yes` | **The interactive `input()` prompt was removed** (console-incompatible). Preview counts only; `--apply --yes` purges. Docstring added. The Django-admin "Clear transcription queue" view ([admin.py](../pod_manager/admin.py) `clear_queue_view`) calls the **service** `purge_transcription_queue()` directly with its own confirm page, **not** this command, so it is unaffected. |
| `clean_mix_images` | `--apply --yes` | Was always-acts. Docstring added. |
| `prune_logs` | `--apply --yes` (keeps `--days`) | Was always-acts. Docstring added. |
| `r2_gc`, `rename_source_audio_to_r2` | `--apply` | Reference impls — unchanged. |
| Exempt (no flags) | — | `ingest_feed`, `transcribe_episode`, `generate_s3_report`, `list_recurly_plans`, `gdrive_discord_report`, `r2_smoke_test`. Docstrings added where missing. |
| Non-runnable | — | `crawl_by_id` (4 bare arg `help=` strings filled; docstring added; `--cookie_value` flagged as a credential in its help → mark `sensitive` in the registry at Step 2), `run_discord_bot` (untouched). |

**Logging.** A module `logger` was added to each destructive command; it emits one
INFO line on the apply path (`r2_cleanup_orphans`, `purge_r2_dev`,
`purge_r2_media_dev`, `clear_transcription_queue`, `clean_mix_images`, `prune_logs`,
`rewind_gdrive_audio`). These land in the existing `LogEntry` viewer and pre-seed
the §11 audit trail before the console exists. (Reminder per §6/conventions §2:
operator-facing progress still goes to `self.stdout`, not `logging`.)

**Call sites updated** (all in [tasks.py](../pod_manager/tasks.py)):
- `task_clean_mix_images` → `call_command('clean_mix_images', apply=True, yes=True)`.
- `task_run_gdrive_recovery` → `apply=not dry_run` (UI keeps its `dry_run` toggle; mapped at the task boundary, so the view in `gdrive_recovery.py` is unchanged).
- `task_run_gdrive_rewind` → `apply=True`.
- **Beat schedule unaffected:** `task_prune_logs` deletes via the ORM directly (not the command); `task_clean_mix_images` is defined but **not** actually in `CELERY_BEAT_SCHEDULE`. No out-of-repo (`docker-compose`/cron/shell) invocations of these commands exist.

**Adjacent feature docs corrected** (removed now-invalid `--dry-run`/`--yes` usages):
[audio-mirroring.md](audio-mirroring.md), [transcription.md](transcription.md),
[images.md](images.md).

**Tests.** `R2BackfillCommandTests._run()` now injects `--apply`;
`test_dry_run_dispatches_nothing` became `test_preview_is_the_default`. A new
`CommandSafetyIdiomTests` class (in `pod_manager/tests.py`, **18 tests**) asserts the
contract for `prune_logs`, `clean_mix_images`, the three destructive R2 gates, the
`clear_transcription_queue` gate, `backfill_baldmove_tags`, the `--prune --apply`
gates on both `backfill_*_to_r2`, and `rewind_gdrive_audio` preview-vs-apply (the last
guards the `task_run_gdrive_rewind` call-site flip). Verified: those plus
`R2BackfillCommandTests` + `R2MaintenanceTests` pass; `manage.py check` clean;
`--help` builds for every changed command.

**Test coverage gaps (acceptable, by command):** `recover_gdrive_audio` has **no**
dedicated test — its matching logic needs a heavy CSV+episode fixture and the
call-site flip is exercised manually via the Creator UI. `backfill_transcripts`
preview/apply isn't unit-tested (its dispatch path depends on `settings.IS_IDE` +
transcription routing; the `mirror_audio_to_r2` suite covers the equivalent
preview/dispatch pattern). The destructive R2 commands' *apply* paths delegate to
services already covered by `R2MaintenanceTests`; only their new `--yes` **gates** are
tested at the command layer.

**Not done in Step 0 (intentionally deferred):** secret redaction is only stubbed
(crawl_by_id's `--cookie_value` is *labelled* a credential but nothing redacts it
yet — that's the registry `sensitive` flag in §15.6 / Step 2).
