# Admin Command Console

A superuser-only, browser-based console for the project's Django **management
commands**. It shows each command's documentation, generates a form for its
arguments, builds a paste-ready command line, runs the command on the Celery worker,
and streams the live log back into the page — no shell access required.

- **Where:** the **Admin** link in the top nav → `/admin-console/`.
- **Who:** superusers only. Every endpoint enforces this independently — the nav link
  is not the gate.
- **Source map:** backend in [`pod_manager/admin_console/`](../pod_manager/admin_console/)
  + [`pod_manager/views/admin_console.py`](../pod_manager/views/admin_console.py);
  frontend in [`admin_console.html`](../pod_manager/templates/pod_manager/admin_console.html)
  / [`admin_console.js`](../pod_manager/static/pod_manager/js/admin_console.js)
  / [`admin_console.css`](../pod_manager/static/pod_manager/css/admin_console.css).
- **Full design + build history:** [admin-command-console-design.md](admin-command-console-design.md) (archived).

---

## Using the console (operators)

**Sidebar.** Commands grouped by category. Icons flag destructive commands (shield),
commands with a destructive sub-mode, deep-link-only, and docs-only commands. A filter
box narrows the list. A "discovered but unregistered" banner appears if any
`pod_manager` command has no registry entry (it stays hidden until registered).

**Detail pane** (select a command):
- **Docs** — pulled from the command's own `Command.help`, module docstring, and
  per-argument `help=` text.
- **Form** — auto-generated from the command's arguments, with the right control per
  field (dropdowns for networks/podcasts/choices, a typeahead for episodes, checkboxes
  for flags, etc.).
- **Command line** — a live, copy-able `python manage.py …` string reflecting the form.
  Paste it into a terminal if you'd rather run it there. Secrets are redacted to `***`.
- **Primary action** — one of:
  - **Execute** — runs it on the worker and streams the log (most commands).
  - **Open in …** — a deep link to an existing UI that owns execution (the GDrive
    recovery commands open Creator Settings). The command line is still shown for
    terminal use.
  - *none* — docs-only commands (daemons, vestigial) show the command line only.
- **Recent runs** — the last runs of this command (status, who, when, duration),
  expandable to the full log + summary, each with one-click **Re-run** (pre-fills the
  form from the stored arguments).

**Destructive commands.** A command whose action irreversibly deletes (or a run that
enables a destructive sub-mode like `--prune`) requires typing the command name to
confirm before Execute enables. A **preview / dry run mutates nothing, so it never asks
to confirm** — the gate only engages on a real `--apply`.

**Running a command.** Execute streams the worker's log into the log pane (it polls;
it does not hold a connection). **Follow** auto-scrolls to the newest output; **Dismiss
(×)** hides the pane (the run keeps going and stays in history). A run that goes quiet
for a while shows a "stalled" badge rather than an error. When it finishes the row lands
in history with its status and, if the command emitted one, a result summary.

**Recent Runs** (top-right button) is a global, filterable history across all commands
(by command, user, status).

---

## Adding a command to the console (developers)

Surfacing a command takes **one registry entry** — no template, view, or per-command
frontend code.

1. **Write the command** following the act/preview safety conventions
   ([management-command-conventions.md](management-command-conventions.md)): mutations
   preview by default and act on `--apply`; irreversible deletes also require `--yes`.

2. **Register it** — add a `CommandSpec` to `REGISTRY` in
   [`registry.py`](../pod_manager/admin_console/registry.py):

   ```python
   "my_command": CommandSpec(
       name="my_command",          # must match the management command name
       category="Maintenance",     # sidebar grouping
   ),
   ```

   Until it has an entry the command stays hidden (safe by default), and the console
   shows it in the "discovered but unregistered" banner. A CI test
   (`test_every_discovered_command_is_registered`) **fails** if any command is left
   unregistered, so don't forget it.

   `CommandSpec` fields you may set:

   | Field | Purpose |
   |---|---|
   | `category` | Sidebar group. |
   | `danger=True` | Always destructive → red styling + typed-confirm gate. |
   | `danger_fields={"prune"}` | Destructive only when these dests are set (e.g. a `--prune` sub-mode); the gate engages just for those runs. |
   | `runnable=False` | No Execute button (daemons / vestigial / deep-link-only). The `run` endpoint also rejects it defensively. |
   | `deep_link=(url_name, label[, query])` | Show an "Open in …" button to a UI that owns execution instead of Execute. |
   | `field_widgets={dest: widget}` | Override a field's control where argparse can't describe it (see widgets below). |
   | `field_labels={dest: "Friendly"}` | Friendly field label. |
   | `sensitive={"cookie_value"}` | Mark secret args — redacted to `***` in history and the copy box. |
   | `summary` / `long_doc` / `examples` | Rarely-used doc escape hatch; prefer in-code docs. |

3. **Document it in code** (not the registry): `Command.help` (the summary line),
   per-argument `help=` (field help), and the **module docstring** (the rich detail-pane
   body — keep it command-relevant; it renders verbatim). No registry prose needed.

4. **The form auto-generates** from the command's `argparse` arguments. Most controls
   are inferred; you only set `field_widgets` for the exceptions:

   | Widget | Inferred when… | Control |
   |---|---|---|
   | `flag` | `action='store_true'` | checkbox |
   | `number` | `type=int` / `type=float` | number input |
   | `text` | everything else | text input |
   | `choice` | argparse `choices=[…]` | dropdown of the choices |
   | `network` / `network_multi` | dest `network` / `networks` | DB dropdown / multi-select |
   | `podcast` / `podcast_multi` | dest `podcast`/`podcast_id` / `podcasts` | DB dropdown / multi-select |
   | `episode` | dest `episode` / `episode_id` | typeahead (episodes are too many to inline) |
   | `enum:<name>` | **`field_widgets` override** | single-select from a named server-side list |
   | `enum_multi:<name>` | **`field_widgets` override** | multi-select from a named server-side list |
   | `csv_path` | **`field_widgets` override** | picks a CSV from the Recovery dir |

   Picker **values** match the arg's `type` — an int field (e.g. `podcast_id`) ships DB
   ids; a slug field ships slugs. Named `enum`/`enum_multi` lists live in
   [`schema.py`](../pod_manager/admin_console/schema.py) (`_NAMED_ENUMS`) — e.g.
   `whisper_models`, `whisper_languages`, `audio_origins`. Add a function there and
   reference it as `enum:<name>` / `enum_multi:<name>`.

5. **(Optional) emit a result summary.** Rather than have the console parse your output,
   emit one structured line near the end of the command:

   ```python
   from pod_manager.admin_console.summary import emit_summary
   emit_summary(self.stdout, {"mode": "celery", "dispatched": dispatched})
   ```

   The runner slices that `[SUMMARY] {…}` line into `CommandRun.result_summary` (the
   **last** one wins) and the console renders it as a key/value block + an at-a-glance
   chip. Missing/malformed is fine — the full log is always the fallback.
   [`mirror_audio_to_r2`](../pod_manager/management/commands/mirror_audio_to_r2.py) is
   the reference.

---

## How it works

**Request flow.** The page (`console`) renders the sidebar from the registry. Selecting
a command fetches `command_detail` (introspected form schema + merged docs + recent
runs). The form posts to `build` for the live copy box and to `run` to execute. `run`
re-validates the arguments through the command's real parser, enforces the danger gate
and the duplicate-run block, creates a `CommandRun` row, and dispatches the generic
`task_run_management_command` Celery task. The browser then polls `run_poll` for the log
delta until the run reaches a terminal status.

**Routes** (all `@superuser_required`, under `/admin-console/`):

| Route | View | Returns |
|---|---|---|
| `/` | `console` | the page shell (sidebar + unregistered notice) |
| `command/<name>/` | `command_detail` | form schema + docs + recent runs (JSON) |
| `command/<name>/build/` | `build` | the copy-box command line, without running |
| `command/<name>/run/` | `run` | validate + dispatch, returns `run_id` |
| `run/<run_id>/poll/` | `run_poll` | `{chunk, offset, status}` log delta |
| `run/<run_id>/` | `run_detail` | one run's full log + summary |
| `runs/` | `history` | global run history (filterable) |
| `lookup/episodes/` | `episode_search` | episode typeahead |

**Key files.** [`registry.py`](../pod_manager/admin_console/registry.py) (what's exposed)
· [`schema.py`](../pod_manager/admin_console/schema.py) (argparse → form schema, and
form → validated invocation with redaction) ·
[`summary.py`](../pod_manager/admin_console/summary.py) (command-emitted summaries) ·
[`log_stream.py`](../pod_manager/admin_console/log_stream.py) (`CommandLogStream`, the
shared log buffer) · `CommandRun` model in [`models.py`](../pod_manager/models.py) ·
`task_run_management_command` in [`tasks.py`](../pod_manager/tasks.py).

---

## Non-obvious choices

- **Polling, not SSE.** The log pane polls a cache buffer rather than holding a
  Server-Sent-Events connection, because SSE is unreliable with gunicorn sync workers
  behind Traefik. The two legacy live-log features (feed import, GDrive recovery) were
  **migrated to the same polling model** for the same reason — they share the
  `CommandLogStream` buffer (SSE-framed `data: <line>\n\n`, terminated by `[DONE]`),
  which the clients parse frame-by-frame.

- **Danger gate fires only on a real `--apply`.** A preview mutates nothing, so it
  dispatches without confirmation. Both the frontend and the `run` view enforce this.
  The command's own `--apply`/`--yes` guards remain the real safety net.

- **Secrets never persist.** Args marked `sensitive` are redacted to `***` in
  `CommandRun.args/options/command_line` and the copy box; the real values reach
  `call_command` only (they do travel in the Celery message, which is necessary to run).

- **Registry gate, safe by default.** Only registered commands are runnable; a new
  command is invisible until someone registers it, and CI fails if one is left out.

- **Identical in-flight runs are soft-blocked** (HTTP 409) to stop accidental
  double-runs. Genuinely different invocations of the same command don't collide.

- **Command-emitted summaries**, not console-side parsing — each command owns its own
  numbers via `emit_summary`, so a wording change never breaks the console.

- **Graceful when Celery is down.** A missing-Celery import or an unreachable broker
  returns a clean 503 (and drops the just-created `CommandRun` row) instead of a 500.

- **In the IDE, tasks run inline**, so Execute blocks until the command finishes and the
  log fills in one shot; in production dispatch returns immediately and the log streams.

---

## Maintenance notes

- **Keep the registry current** — every new `pod_manager` command needs a `CommandSpec`
  (CI enforces it).
- **`result_summary`** is populated for commands that opt in via `emit_summary` — the
  bulk/maintenance/report commands all do (migrate/prune/delete/scan counts, etc.).
  Single-target actions (`ingest_feed`, `transcribe_episode`) and the non-runnable
  commands stay log-only. Adding a summary to another command is one `emit_summary` call.
- **`CommandRun` retention** — rows (including full log text) are not currently pruned.
  Volume is low (manual superuser runs only), so this is fine; if it ever grows, add a
  periodic cleanup mirroring `prune_logs` (which only trims `LogEntry`, not `CommandRun`).
- **Adding a `field_widgets` option list** — add the source function to `_NAMED_ENUMS`
  in `schema.py` and reference it as `enum:<name>` / `enum_multi:<name>`.
