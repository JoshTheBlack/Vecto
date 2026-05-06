import logging
import threading

# ---------------------------------------------------------------------------
# Thread-local request-user tracking
# ---------------------------------------------------------------------------
_request_local = threading.local()


def set_current_user(user_id):
    _request_local.user_id = user_id


def get_current_user_id():
    return getattr(_request_local, 'user_id', None)


def clear_current_user():
    _request_local.user_id = None


# ---------------------------------------------------------------------------

_LEVEL_MAP = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL,
}

CACHE_KEY = 'log_db_level_override'
DEBUG_MODE_TTL = 7200  # 2 hours


class MinLevelFilter(logging.Filter):
    """Static minimum-level gate used on the console handler."""
    def __init__(self, min_level='INFO'):
        super().__init__()
        self.min_levelno = _LEVEL_MAP.get(min_level.upper(), logging.INFO)

    def filter(self, record):
        return record.levelno >= self.min_levelno


class DatabaseLogHandler(logging.Handler):
    """
    Writes log records to the LogEntry model.

    Effective DB level is read from the Django cache on each write so that
    staff can toggle debug mode live without restarting the stack.  Falls back
    to settings.LOG_LEVEL when no override is active.

    The DB write runs in a daemon thread so it never adds latency to the
    HTTP request that triggered the log call.  A thread-local re-entrancy
    guard stops the ORM from recursing back into this handler.
    """
    _local = threading.local()

    def _effective_levelno(self):
        try:
            from django.core.cache import cache
            from django.conf import settings
            override = cache.get(CACHE_KEY)
            level_name = override if override else getattr(settings, 'LOG_LEVEL', 'INFO')
            return _LEVEL_MAP.get(level_name.upper(), logging.INFO)
        except Exception:
            return logging.INFO

    def emit(self, record):
        if getattr(self._local, 'active', False):
            return
        if record.levelno < self._effective_levelno():
            return
        message = self.format(record)
        # Capture user_id here on the request thread; the daemon thread has no
        # access to the thread-local after the handoff.
        user_id = get_current_user_id()
        args = (record.levelname, record.levelno, record.name,
                record.module, record.funcName, record.lineno, message, user_id)
        threading.Thread(target=self._write, args=args, daemon=True).start()

    def _write(self, levelname, levelno, logger_name, module, func_name, lineno, message, user_id):
        if getattr(self._local, 'active', False):
            return
        self._local.active = True
        try:
            from pod_manager.models import LogEntry
            LogEntry.objects.create(
                level=levelname,
                level_no=levelno,
                logger_name=logger_name,
                module=module,
                func_name=func_name,
                lineno=lineno,
                message=message,
                user_id=user_id,
            )
        except Exception:
            pass
        finally:
            self._local.active = False
