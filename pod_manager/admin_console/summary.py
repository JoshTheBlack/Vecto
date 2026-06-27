"""Command-emitted run summaries — the non-fragile `result_summary` (design §8a).

Rather than teach the console to parse each command's free-text output (brittle, breaks
when wording changes), a command opts in by emitting **one structured marker line** near
the end of its run:

    [SUMMARY] {"dispatched": 12, "skipped": 3}

``emit_summary(stdout, mapping)`` writes that line; the runner task
(``task_run_management_command``) calls ``extract_summary(captured)`` to pull the last
such line into ``CommandRun.result_summary``. The command stays the source of truth for
its own numbers; the console slices a JSON line it never has to understand. Emitting is
harmless on the CLI (just one extra line) and optional — a command with no summary simply
leaves ``result_summary`` null and the full log remains the universal record.
"""

import json
import logging

logger = logging.getLogger(__name__)

# Prefix that marks the structured summary line in a command's stdout.
SUMMARY_MARKER = "[SUMMARY] "


def emit_summary(stdout, mapping):
    """Write the structured summary line a console run will capture.

    ``stdout`` is the command's ``self.stdout`` (or any writer). The mapping is
    JSON-serialized; non-serializable values fall back to ``str``. A serialization
    failure is logged and skipped rather than breaking the command.
    """
    try:
        payload = json.dumps(mapping, default=str)
    except (TypeError, ValueError):  # pragma: no cover - default=str makes this rare
        logger.warning("emit_summary: could not serialize %r", mapping, exc_info=True)
        return
    stdout.write(f"{SUMMARY_MARKER}{payload}")


def extract_summary(captured):
    """Return the dict from the LAST ``[SUMMARY]`` line in ``captured``, or ``None``.

    Fault-tolerant: missing or malformed markers yield ``None`` (the full log is the
    fallback). The *last* marker wins, so a command may emit progress summaries and a
    final one.
    """
    if not captured:
        return None
    found = None
    for line in captured.splitlines():
        stripped = line.strip()
        if not stripped.startswith(SUMMARY_MARKER):
            continue
        payload = stripped[len(SUMMARY_MARKER):].strip()
        try:
            parsed = json.loads(payload)
        except (json.JSONDecodeError, ValueError):
            logger.warning("extract_summary: malformed summary line: %r", payload)
            continue
        if isinstance(parsed, dict):
            found = parsed
    return found
