"""
Structured JSON logging for Conductor.

Provides ``JsonFormatter``, ``setup_logging``, and utility helpers for
enriching log records with structured context (task_id, worker_id,
duration, etc.).

Typical usage::

    from conductor.observability.logging import setup_logging

    setup_logging(level="INFO", fmt="json")
"""

from __future__ import annotations

import datetime
import json
import logging
import logging.config
import os
import socket
import traceback
from typing import Any, Optional

# ---------------------------------------------------------------------------
# JSON Formatter
# ---------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """Output log records as JSON lines.

    Every record includes: ``timestamp``, ``level``, ``logger``, ``module``,
    ``function``, ``line``, ``message``, plus any keyword arguments passed
    via the ``extra`` parameter to the log call.

    Example output::

        {"timestamp": "2026-07-30T10:30:45.123456Z", "level": "INFO",
         "logger": "conductor.core.worker", "message": "Task submitted",
         "task_id": "abc-123", "task_type": "email"}
    """

    def __init__(self, fmt: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(fmt, **kwargs)

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as a JSON string."""
        data: dict[str, Any] = {
            "timestamp": self._format_time(record),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "message": record.getMessage(),
        }

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": "".join(traceback.format_exception(*record.exc_info)).rstrip(),
            }

        # Merge any extra fields passed via the ``extra`` parameter.
        # We skip standard LogRecord attributes to avoid duplication.
        skip_keys = {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
        }
        for key, value in record.__dict__.items():
            if key not in skip_keys:
                data[key] = _safe_serialize(value)

        return json.dumps(data, default=_json_default, ensure_ascii=False)

    @staticmethod
    def _format_time(record: logging.LogRecord) -> str:
        """Return an ISO-8601 timestamp with microseconds and Z suffix."""
        dt = datetime.datetime.fromtimestamp(
            record.created,
            tz=datetime.timezone.utc,
        )
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------


def setup_logging(
    level: str = "INFO",
    fmt: str = "json",
) -> None:
    """Configure the ``conductor`` logger hierarchy.

    Args:
        level: One of ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``.
        fmt: ``"json"`` for structured JSON output, ``"text"`` for standard
             human-readable format.
    """
    root = logging.getLogger("conductor")
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove any existing handlers to avoid duplicates on re-configuration
    root.handlers.clear()

    handler = logging.StreamHandler()
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S%z",
            )
        )

    root.addHandler(handler)
    root.addFilter(HostnameFilter())


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


class HostnameFilter(logging.Filter):
    """Inject ``hostname`` and ``pid`` into every log record."""

    def __init__(self) -> None:
        super().__init__()
        self._hostname = socket.gethostname()
        self._pid = os.getpid()

    def filter(self, record: logging.LogRecord) -> bool:
        record.hostname = self._hostname
        record.pid = self._pid
        return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_serialize(value: Any) -> Any:
    """Convert common non-JSON types to their JSON-safe equivalents.

    - ``datetime`` / ``date`` → ISO-8601 string
    - ``Exception`` → ``str(exc)``
    - ``set`` → ``list``
    - bytes → ``str`` (decoded)
    """
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, Exception):
        return str(value)
    if isinstance(value, set):
        return list(value)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _json_default(obj: Any) -> Any:
    """Fallback JSON encoder for types not handled by ``_safe_serialize``."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, Exception):
        return str(obj)
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="replace")
    return str(obj)
