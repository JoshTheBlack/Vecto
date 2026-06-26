"""Shared command log-buffer stream.

``CommandLogStream`` is the single, canonical replacement for the two near-duplicate
stream classes that used to live in ``pod_manager/tasks.py`` (``CacheLogStream`` and
``_RecoveryStream``). It tees a command's stdout/stderr to a cache key — the live
buffer the SSE views and (eventually) the Admin Command Console poller tail — while
keeping a raw, unformatted capture for post-run parsing and persistence.

The streaming contract is unchanged and shared across every caller:

* Each write appends ``data: <line>\\n\\n`` chunks (one per non-empty line) to
  ``cache[task_id]``.
* The terminal ``[DONE]`` sentinel is written by the *caller* once the command
  finishes (in a ``finally`` block), not by this class — the SSE views close on it.
* :meth:`captured` returns the raw text (no SSE framing) for summary parsing and,
  later, ``CommandRun.log`` persistence (§8a).
"""

import io
import logging

from django.core.cache import cache

logger = logging.getLogger(__name__)

# How long the live buffer lingers in the cache after the last write. Matches the
# value both legacy stream classes used.
CACHE_TIMEOUT = 3600


class CommandLogStream(io.StringIO):
    """Tee a command's output to a cache buffer while capturing raw text.

    Pass an instance as ``stdout``/``stderr`` to ``call_command``. The live buffer
    lives at ``cache[task_id]`` (SSE-framed); :meth:`captured` returns the raw text.
    Callers own the ``[DONE]`` sentinel.
    """

    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id

    def write(self, s):
        # Empty writes carry no output; skip them so we never churn the cache key.
        if not s:
            return 0
        # Keep the raw text in the underlying StringIO for captured().
        count = super().write(s)
        formatted = "".join(f"data: {line}\n\n" for line in s.splitlines() if line)
        if formatted:
            current = cache.get(self.task_id, "")
            cache.set(self.task_id, current + formatted, timeout=CACHE_TIMEOUT)
        return count

    def flush(self):
        # No-op: there is nothing to flush beyond the cache writes, which happen
        # eagerly in write(). Django's OutputWrapper calls flush(), so keep it cheap.
        pass

    def captured(self):
        """Return the raw, unformatted text written so far (no SSE framing)."""
        return self.getvalue()
