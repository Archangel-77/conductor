"""
Unit tests for structured JSON logging.

These tests do **not** require a database — they test log formatting only.
"""

from __future__ import annotations

import datetime
import io
import json
import logging

from conductor.observability.logging import (
    JsonFormatter,
    setup_logging,
    _safe_serialize,
)

# ===================================================================
# JsonFormatter tests
# ===================================================================


class TestJsonFormatter:
    """Verify the JSON output structure and field types."""

    def _format_record(self, msg: str, **extra: object) -> dict[str, object]:
        """Helper: create a log record, format it, and return the parsed JSON."""
        logger = logging.getLogger("conductor.test.logging")
        logger.setLevel(logging.DEBUG)
        handler = logging.StreamHandler(io.StringIO())
        handler.setFormatter(JsonFormatter())
        logger.handlers.clear()
        logger.addHandler(handler)

        logger.info(msg, extra=extra)
        output = handler.stream.getvalue()
        result: dict[str, object] = json.loads(output.strip())
        return result

    def test_basic_fields(self) -> None:
        """Verify JSON output contains timestamp, level, logger, message."""
        data = self._format_record("hello world")
        assert data["message"] == "hello world"
        assert data["level"] == "INFO"
        assert data["logger"] == "conductor.test.logging"
        assert "timestamp" in data
        assert "module" in data
        assert "function" in data
        assert "line" in data

    def test_timestamp_format(self) -> None:
        """Verify timestamp is ISO-8601 with Z suffix."""
        data = self._format_record("test")
        ts = str(data["timestamp"])
        # Should look like: 2026-07-30T10:30:45.123456Z
        assert ts.endswith("Z"), f"Timestamp '{ts}' should end with Z"
        assert "T" in ts, f"Timestamp '{ts}' should contain T separator"
        # Verify parseable
        parsed = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        assert isinstance(parsed, datetime.datetime)

    def test_extra_fields_appear_in_json(self) -> None:
        """Verify extra kwargs are merged into the JSON output."""
        data = self._format_record(
            "Task done",
            task_id="abc-123",
            task_type="email",
            duration_ms=42.0,
        )
        assert data["task_id"] == "abc-123"
        assert data["task_type"] == "email"
        assert data["duration_ms"] == 42.0

    def test_datetime_in_extra_is_serialized(self) -> None:
        """Verify datetime objects in extra are converted to ISO strings."""
        now = datetime.datetime.now(datetime.timezone.utc)
        data = self._format_record("scheduled", scheduled_at=now)
        iso = str(data["scheduled_at"])
        # Should be an ISO-8601 string, not a datetime repr
        assert "T" in iso
        assert iso.endswith("+00:00") or iso.endswith("Z")

    def test_exception_in_extra_is_serialized(self) -> None:
        """Verify exception objects in extra are converted to strings."""
        exc = ValueError("something broke")
        data = self._format_record("error", error=exc)
        assert data["error"] == "something broke"

    def test_set_in_extra_is_serialized(self) -> None:
        """Verify set objects in extra are converted to lists."""
        data = self._format_record("tags", tags={"a", "b", "c"})
        assert isinstance(data["tags"], list)
        assert set(data["tags"]) == {"a", "b", "c"}

    def test_bytes_in_extra_is_serialized(self) -> None:
        """Verify bytes objects in extra are decoded to strings."""
        data = self._format_record("raw", raw_data=b"hello")
        assert data["raw_data"] == "hello"


# ===================================================================
# setup_logging tests
# ===================================================================


class TestSetupLogging:
    """Verify the setup_logging function configures the logger correctly."""

    def test_setup_logging_json(self) -> None:
        """Verify setup_logging with fmt='json' creates a JsonFormatter."""
        logger = logging.getLogger("conductor.setup.test")
        # Clear any existing handlers
        logger.handlers.clear()
        logger.propagate = False

        setup_logging(level="DEBUG", fmt="json")
        # setup_logging configures the root "conductor" logger, so our
        # child logger inherits the handler via propagation.
        # We check the root conductor logger instead.
        root = logging.getLogger("conductor")
        json_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        assert len(json_handlers) >= 1
        assert isinstance(json_handlers[0].formatter, JsonFormatter)

    def test_setup_logging_text(self) -> None:
        """Verify setup_logging with fmt='text' creates a standard formatter."""
        logger = logging.getLogger("conductor.setup.text.test")
        logger.handlers.clear()
        logger.propagate = False

        setup_logging(level="INFO", fmt="text")
        root = logging.getLogger("conductor")
        text_handlers = [h for h in root.handlers if isinstance(h, logging.StreamHandler)]
        assert len(text_handlers) >= 1
        assert not isinstance(text_handlers[0].formatter, JsonFormatter)
        # Should be a standard logging.Formatter
        assert isinstance(text_handlers[0].formatter, logging.Formatter)


# ===================================================================
# _safe_serialize tests
# ===================================================================


class TestSafeSerialize:
    """Verify the JSON-safe serialization helper."""

    def test_datetime_to_iso(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        result = _safe_serialize(now)
        assert isinstance(result, str)
        assert "T" in result

    def test_date_to_iso(self) -> None:
        d = datetime.date(2026, 7, 30)
        result = _safe_serialize(d)
        assert result == "2026-07-30"

    def test_exception_to_string(self) -> None:
        result = _safe_serialize(RuntimeError("fail"))
        assert result == "fail"

    def test_set_to_list(self) -> None:
        result = _safe_serialize({1, 2, 3})
        assert isinstance(result, list)
        assert sorted(result) == [1, 2, 3]

    def test_bytes_to_string(self) -> None:
        result = _safe_serialize(b"hello")
        assert result == "hello"

    def test_unknown_type_returns_itself(self) -> None:
        result = _safe_serialize(42)
        assert result == 42
        result = _safe_serialize("plain")
        assert result == "plain"
        result = _safe_serialize(None)
        assert result is None
