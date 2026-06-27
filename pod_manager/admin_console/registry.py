"""Admin Command Console — the opt-in command registry (design §4b).

Django auto-discovers every ``pod_manager`` management command, but the console
only *shows* the ones registered here. The registry's load-bearing job is
**structured metadata** argparse can't self-describe — visibility, danger /
runnable flags, deep-links, the semantic field-widget overrides (§5a), and which
arguments hold secrets. Documentation prose is NOT its job: all command text
comes from in-code sources (``Command.help``, per-argument ``help=``, the module
docstring — §6). The ``summary``/``long_doc``/``examples`` fields exist only as a
rarely-used escape hatch.

To surface a new command: write it as usual, then add one ``CommandSpec`` entry
below. Until it has an entry it stays invisible (safe by default); the console
lists any discovered-but-unregistered command as a notice (§4c).
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class CommandSpec:
    name: str                               # must match the management command name
    category: str = "General"               # groups commands in the sidebar
    danger: bool = False                     # destructive → red styling + typed confirm
    # Dests whose presence makes an *otherwise-safe* command dangerous for this run
    # only (e.g. the backfills' `--prune` delete sub-mode). The typed-confirm gate
    # engages when `danger` is True OR any `danger_fields` dest is truthy in the
    # payload — so a plain benign backfill runs without a confirm, but `--prune` doesn't.
    danger_fields: frozenset = frozenset()
    runnable: bool = True                    # False = no in-console Execute button
    # If set, the detail pane shows an "Open in <label>" deep-link instead of an
    # Execute button — execution lives in an existing dedicated UI. The command
    # builder / copy box still renders (so it's runnable from a terminal).
    # Shape: (url_name, button_label) or (url_name, button_label, query_string),
    # e.g. ("creator_settings", "Open GDrive Recovery", "tab=gdrive") — the optional
    # query string selects the right tab in the target UI.
    deep_link: Optional[tuple] = None
    # Semantic widget overrides for args argparse can't self-describe (§5a).
    # arg dest -> widget id. Most fields auto-infer; this names the exceptions.
    field_widgets: dict = field(default_factory=dict)
    field_labels: dict = field(default_factory=dict)   # arg dest -> friendly label
    # Arg dests holding a secret (token/cookie/password). Redacted to *** in run
    # history and the command-line preview (§15.6).
    sensitive: frozenset = frozenset()
    # --- optional doc escape hatch (rarely needed; prefer in-code help, §6) ---
    summary: str = ""                        # overrides Command.help if set
    long_doc: str = ""                       # overrides module docstring if set
    examples: tuple = ()                     # extra copy-pasteable invocations


# One entry per command we expose. Source of truth: design doc §12. `danger=True`
# is set for the six commands whose *primary* action irreversibly deletes (the
# "--yes" rows in §12) — they always require the typed-confirm gate. The two
# `backfill_*_to_r2` commands are normally benign backfills, so instead of a blanket
# `danger`, their destructive `--prune` sub-mode is wired via `danger_fields`: the
# gate engages only on a run that actually checks `--prune`. See the §13 as-built note.
REGISTRY = {
    # --- Feeds -------------------------------------------------------------
    "ingest_feed": CommandSpec(
        name="ingest_feed",
        category="Feeds",
    ),
    "backfill_baldmove_tags": CommandSpec(
        name="backfill_baldmove_tags",
        category="Feeds",
    ),
    # --- Transcription -----------------------------------------------------
    "transcribe_episode": CommandSpec(
        name="transcribe_episode",
        category="Transcription",
    ),
    "backfill_transcripts": CommandSpec(
        name="backfill_transcripts",
        category="Transcription",
    ),
    "clear_transcription_queue": CommandSpec(
        name="clear_transcription_queue",
        category="Transcription",
        danger=True,
    ),
    # --- R2 / Storage ------------------------------------------------------
    "mirror_audio_to_r2": CommandSpec(
        name="mirror_audio_to_r2",
        category="R2 / Storage",
        # --episode / --network / --podcast auto-infer; --origins can't:
        field_widgets={"origins": "enum_multi:audio_origins"},
    ),
    "backfill_transcripts_to_r2": CommandSpec(
        name="backfill_transcripts_to_r2",
        category="R2 / Storage",
        danger_fields=frozenset({"prune"}),  # benign backfill, but --prune deletes
    ),
    "backfill_media_to_r2": CommandSpec(
        name="backfill_media_to_r2",
        category="R2 / Storage",
        danger_fields=frozenset({"prune"}),
    ),
    "rename_source_audio_to_r2": CommandSpec(
        name="rename_source_audio_to_r2",
        category="R2 / Storage",
    ),
    "r2_gc": CommandSpec(
        name="r2_gc",
        category="R2 / Storage",
    ),
    "r2_cleanup_orphans": CommandSpec(
        name="r2_cleanup_orphans",
        category="R2 / Storage",
        danger=True,
    ),
    "r2_smoke_test": CommandSpec(
        name="r2_smoke_test",
        category="R2 / Storage",
    ),
    "purge_r2_dev": CommandSpec(
        name="purge_r2_dev",
        category="R2 / Storage",
        danger=True,
    ),
    "purge_r2_media_dev": CommandSpec(
        name="purge_r2_media_dev",
        category="R2 / Storage",
        danger=True,
    ),
    # --- GDrive ------------------------------------------------------------
    "gdrive_discord_report": CommandSpec(
        name="gdrive_discord_report",
        category="GDrive",
        field_widgets={"csv_path": "csv_path"},
    ),
    "recover_gdrive_audio": CommandSpec(
        name="recover_gdrive_audio",
        category="GDrive",
        runnable=False,             # execute via the existing Creator UI…
        deep_link=("creator_settings", "Open GDrive Recovery", "tab=gdrive"),
        field_widgets={"csv_path": "csv_path"},  # …but still build the CLI here
    ),
    "rewind_gdrive_audio": CommandSpec(
        name="rewind_gdrive_audio",
        category="GDrive",
        runnable=False,
        deep_link=("creator_settings", "Open GDrive Recovery", "tab=gdrive"),
        field_widgets={"csv_paths": "csv_path"},  # nargs='+' → multi CSV picker
    ),
    # --- Maintenance -------------------------------------------------------
    "clean_mix_images": CommandSpec(
        name="clean_mix_images",
        category="Maintenance",
        danger=True,
    ),
    "prune_logs": CommandSpec(
        name="prune_logs",
        category="Maintenance",
        danger=True,
    ),
    # --- Reports -----------------------------------------------------------
    "generate_s3_report": CommandSpec(
        name="generate_s3_report",
        category="Reports",
    ),
    "list_recurly_plans": CommandSpec(
        name="list_recurly_plans",
        category="Reports",
    ),
    # --- Archive / Vestigial ----------------------------------------------
    "crawl_by_id": CommandSpec(
        name="crawl_by_id",
        category="Archive / Vestigial",
        runnable=False,             # ~110k Celery tasks; docs-only by design
        sensitive=frozenset({"cookie_value"}),  # auth cookie — redact (§15.6)
    ),
    # --- Daemons -----------------------------------------------------------
    "run_discord_bot": CommandSpec(
        name="run_discord_bot",
        category="Daemons",
        runnable=False,             # launched by docker-compose; docs-only here
    ),
}


def get_spec(name):
    """Return the :class:`CommandSpec` for ``name`` or ``None`` if unregistered."""
    return REGISTRY.get(name)
