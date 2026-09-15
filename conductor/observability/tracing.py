"""
OpenTelemetry tracing.

Conductor emits spans for the whole task lifecycle:

* **Producer side** (``TaskQueue``) — ``conductor.task.submit``,
  ``conductor.task.submit_many``, ``conductor.task.cancel``,
  ``conductor.dlq.retry``, ``conductor.dlq.discard``,
  ``conductor.recurring.fire``.
* **Consumer side** (``Worker``) — ``conductor.task.execute`` plus span events
  for retry scheduling and DLQ moves, and a child span for blocked-dependent
  propagation.

Trace context crosses the process boundary: the submitter persists a W3C
``traceparent`` on the task row, and the worker extracts it, so a task executed
by a different process appears as a child of the span that submitted it.  The
same column is preserved through the dead-letter queue, and retries are linked
(not nested) because each attempt is a separate execution.

The module has **no import-time dependency on OpenTelemetry**: without the
``otel`` extra installed everything degrades to no-ops, and
:func:`setup_tracing` reports that loudly rather than crashing a worker.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional, Protocol, runtime_checkable

from conductor.exceptions import TracingError

logger = logging.getLogger("conductor.observability.tracing")

EXPORTERS: tuple[str, ...] = ("none", "console", "otlp")
"""Supported exporter names."""

SPAN_SUBMIT = "conductor.task.submit"
"""Span name for submitting a single task."""

SPAN_SUBMIT_MANY = "conductor.task.submit_many"
"""Span name for a batch submission."""

SPAN_EXECUTE = "conductor.task.execute"
"""Span name for executing a task on a worker."""

SPAN_CANCEL = "conductor.task.cancel"
"""Span name for cancelling a task."""

SPAN_DLQ_RETRY = "conductor.dlq.retry"
"""Span name for retrying a dead-letter task."""

SPAN_DLQ_DISCARD = "conductor.dlq.discard"
"""Span name for discarding a dead-letter task."""

SPAN_RECURRING_FIRE = "conductor.recurring.fire"
"""Span name for firing one recurring definition."""

SPAN_BLOCK_DEPENDENTS = "conductor.task.block_dependents"
"""Span name for propagating a terminal failure to dependent tasks."""

ATTR_TASK_ID = "task.id"
"""Span attribute: task identifier."""

ATTR_TASK_TYPE = "task.type"
"""Span attribute: logical task type."""

ATTR_TASK_ROUTE = "task.route"
"""Span attribute: task route."""

ATTR_TASK_PRIORITY = "task.priority"
"""Span attribute: task priority."""

ATTR_TASK_ATTEMPT = "task.attempt"
"""Span attribute: retry attempt number."""

ATTR_TASK_STATUS = "task.status"
"""Span attribute: final task status."""

ATTR_WORKER_ID = "conductor.worker_id"
"""Span attribute: worker identity."""


@dataclass(frozen=True)
class TracingConfig:
    """Configuration for OpenTelemetry tracing."""

    enabled: bool = False
    """Whether tracing is active.  When ``False`` every span is a no-op."""

    service_name: str = "conductor"
    """Value reported as ``service.name`` on the resource."""

    exporter: str = "otlp"
    """Exporter to use: ``none``, ``console`` or ``otlp``."""

    endpoint: Optional[str] = None
    """OTLP endpoint; ``None`` falls back to ``OTEL_EXPORTER_OTLP_ENDPOINT``."""

    sample_ratio: float = 1.0
    """Fraction of traces to sample (``0.0``–``1.0``)."""

    def validate(self) -> None:
        """Validate the configuration.

        Raises:
            TracingError: If a value is out of range or unknown.
        """
        if self.exporter not in EXPORTERS:
            raise TracingError(
                f"Invalid tracing exporter '{self.exporter}'. " f"Must be one of {list(EXPORTERS)}"
            )
        if not self.service_name or not self.service_name.strip():
            raise TracingError("service_name must not be empty")
        if not 0.0 <= self.sample_ratio <= 1.0:
            raise TracingError(
                f"sample_ratio must be between 0.0 and 1.0 (got {self.sample_ratio})"
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary of the configuration."""
        return {
            "enabled": self.enabled,
            "service_name": self.service_name,
            "exporter": self.exporter,
            "endpoint": self.endpoint,
            "sample_ratio": self.sample_ratio,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> TracingConfig:
        """Build a configuration from a dictionary."""
        return cls(
            enabled=bool(data.get("enabled", False)),
            service_name=str(data.get("service_name", "conductor")),
            exporter=str(data.get("exporter", "otlp")),
            endpoint=data.get("endpoint"),
            sample_ratio=float(data.get("sample_ratio", 1.0)),
        )


@runtime_checkable
class SpanLike(Protocol):
    """The span surface Conductor uses (satisfied by the OTel SDK ``Span``)."""

    def set_attribute(self, key: str, value: Any) -> None:
        """Attach an attribute to the span."""
        ...

    def set_status(self, status: Any) -> None:
        """Set the span status."""
        ...

    def record_exception(self, exception: BaseException) -> None:
        """Record an exception on the span."""
        ...

    def add_event(self, name: str, attributes: Optional[Mapping[str, Any]] = None) -> None:
        """Add a timed event to the span."""
        ...

    def is_recording(self) -> bool:
        """Return ``True`` if the span still accepts data."""
        ...


@runtime_checkable
class SpanContextManager(Protocol):
    """A span that can be used as a context manager (sets itself as current)."""

    def __enter__(self) -> SpanLike:
        """Enter the span context."""
        ...

    def __exit__(self, *exc_info: Any) -> None:
        """Exit and end the span."""
        ...


@runtime_checkable
class TracingBackend(Protocol):
    """A tracer implementation (OpenTelemetry SDK or the built-in no-op)."""

    name: str
    """Backend name (``none`` or ``otel``)."""

    def start_span(
        self,
        name: str,
        *,
        attributes: Optional[Mapping[str, Any]] = None,
        parent: Optional[Any] = None,
        links: Optional[Sequence[Any]] = None,
    ) -> SpanContextManager:
        """Start a span, optionally parented/linked to an extracted context."""
        ...

    def current_traceparent(self) -> Optional[str]:
        """Return the W3C ``traceparent`` of the active span, if any."""
        ...

    def add_event_to_current(
        self, name: str, attributes: Optional[Mapping[str, Any]] = None
    ) -> None:
        """Add a timed event to the active span."""
        ...

    def mark_current_success(self) -> None:
        """Mark the active span as successful."""
        ...

    def mark_current_error(self, message: str, exception: Optional[BaseException] = None) -> None:
        """Mark the active span as failed, recording the exception."""
        ...

    def extract_traceparent(self, traceparent: Optional[str]) -> Optional[Any]:
        """Build a parent context from a W3C ``traceparent`` string."""
        ...

    def build_link(self, traceparent: Optional[str]) -> Optional[Any]:
        """Build a span link from a stored ``traceparent`` string."""
        ...

    def current_trace_ids(self) -> tuple[Optional[str], Optional[str]]:
        """Return ``(trace_id, span_id)`` of the active span, if any."""
        ...

    def shutdown(self) -> None:
        """Flush and release exporter resources."""
        ...


class NoOpSpan:
    """A span that discards everything (used when tracing is disabled)."""

    def __enter__(self) -> NoOpSpan:
        """Enter the span context."""
        return self

    def __exit__(self, *exc_info: Any) -> None:
        """Exit the span context."""

    def set_attribute(self, key: str, value: Any) -> None:
        """Discard an attribute."""

    def set_status(self, status: Any) -> None:
        """Discard a status."""

    def record_exception(self, exception: BaseException) -> None:
        """Discard an exception."""

    def add_event(self, name: str, attributes: Optional[Mapping[str, Any]] = None) -> None:
        """Discard an event."""

    def is_recording(self) -> bool:
        """No-op spans never record."""
        return False

    def end(self) -> None:
        """No-op."""


class NoOpBackend:
    """The default backend: every operation is a no-op."""

    name = "none"

    def start_span(
        self,
        name: str,
        *,
        attributes: Optional[Mapping[str, Any]] = None,
        parent: Optional[Any] = None,
        links: Optional[Sequence[Any]] = None,
    ) -> SpanContextManager:
        """Return a span that discards everything."""
        return NoOpSpan()

    def current_traceparent(self) -> Optional[str]:
        """No active trace context."""
        return None

    def add_event_to_current(
        self, name: str, attributes: Optional[Mapping[str, Any]] = None
    ) -> None:
        """Discard the event (there is no active span)."""

    def mark_current_success(self) -> None:
        """No active span to mark."""

    def mark_current_error(self, message: str, exception: Optional[BaseException] = None) -> None:
        """No active span to mark."""

    def extract_traceparent(self, traceparent: Optional[str]) -> Optional[Any]:
        """No parent context."""
        return None

    def build_link(self, traceparent: Optional[str]) -> Optional[Any]:
        """No span link."""
        return None

    def current_trace_ids(self) -> tuple[Optional[str], Optional[str]]:
        """No active trace."""
        return (None, None)

    def shutdown(self) -> None:
        """No-op."""


_backend: TracingBackend = NoOpBackend()
"""The active tracing backend (swapped by :func:`setup_tracing`)."""

_otel_import_error: Optional[str] = None
"""Why the OpenTelemetry extra could not be imported, if it could not."""


def is_available() -> bool:
    """Return ``True`` if the OpenTelemetry extra is installed."""
    global _otel_import_error

    if _otel_import_error is not None:
        return False
    try:
        import conductor.observability._otel  # noqa: F401
    except ImportError as exc:  # pragma: no cover - depends on the environment
        _otel_import_error = str(exc)
        return False
    return True


def is_enabled() -> bool:
    """Return ``True`` if a real tracing backend is active."""
    return _backend.name != "none"


def get_backend_name() -> str:
    """Return the active backend name (``none`` or ``otel``)."""
    return _backend.name


def setup_tracing(config: TracingConfig) -> None:
    """Install the tracing backend described by *config*.

    Passing ``enabled=False`` (or ``exporter="none"``) resets tracing to the
    no-op backend.

    Args:
        config: The tracing configuration.

    Raises:
        TracingError: If the configuration is invalid, or tracing is requested
            while the ``otel`` extra is not installed.  Worker startup catches
            this and logs a warning (like the metrics/gRPC/dashboard servers),
            so a misconfiguration never takes a worker down.
    """
    global _backend

    config.validate()

    if not config.enabled or config.exporter == "none":
        _backend = NoOpBackend()
        logger.debug("Tracing disabled (no-op backend).")
        return

    if not is_available():
        raise TracingError(
            "Tracing is enabled but the OpenTelemetry extra is not installed. "
            "Install it with: pip install 'conductor-task-queue[otel]'"
        )

    from conductor.observability._otel import OtelBackend

    _backend = OtelBackend(config)
    logger.info(
        "Tracing enabled (exporter=%s, service=%s, sample_ratio=%s).",
        config.exporter,
        config.service_name,
        config.sample_ratio,
    )


def shutdown_tracing() -> None:
    """Flush and release the active tracing backend (safe to call twice)."""
    global _backend

    _backend.shutdown()
    _backend = NoOpBackend()


def get_backend() -> TracingBackend:
    """Return the active backend (used by tests and diagnostics)."""
    return _backend


@contextmanager
def span(
    name: str,
    *,
    attributes: Optional[Mapping[str, Any]] = None,
    parent: Optional[Any] = None,
    links: Optional[Sequence[Any]] = None,
) -> Iterator[SpanLike]:
    """Start a span and make it current for the duration of the block.

    Args:
        name: Span name (use the ``SPAN_*`` constants).
        attributes: Optional initial attributes.
        parent: Optional parent context from :func:`extract_traceparent`; when
            omitted the span is a child of whatever is current.
        links: Optional span links (used by retries, so an attempt points at the
            original execution without nesting inside it).

    Yields:
        The span (a no-op span when tracing is disabled).
    """
    with _backend.start_span(name, attributes=attributes, parent=parent, links=links) as active:
        yield active


def current_traceparent() -> Optional[str]:
    """Return the W3C ``traceparent`` of the active span, if there is one.

    This is what gets persisted on the task row so the worker can continue the
    trace in another process.
    """
    return _backend.current_traceparent()


def add_event(name: str, attributes: Optional[Mapping[str, Any]] = None) -> None:
    """Annotate the currently active span with a timed event.

    Used for milestones inside a span (retry scheduled, DLQ move, dependents
    blocked) so a single span tells the whole story of one execution.
    """
    _backend.add_event_to_current(name, attributes)


def mark_success() -> None:
    """Mark the active span as successful (no-op without an active span)."""
    _backend.mark_current_success()


def record_error(message: str, exception: Optional[BaseException] = None) -> None:
    """Mark the active span as failed and record the exception."""
    _backend.mark_current_error(message, exception)


def extract_traceparent(traceparent: Optional[str]) -> Optional[Any]:
    """Turn a stored W3C ``traceparent`` string into a parent context."""
    if not traceparent:
        return None
    return _backend.extract_traceparent(traceparent)


def span_link(traceparent: Optional[str]) -> Optional[Any]:
    """Build a span *link* from a stored ``traceparent`` string.

    Retries are separate executions, so a new attempt links to the execution it
    repeats rather than pretending to be its child.
    """
    if not traceparent:
        return None
    return _backend.build_link(traceparent)


def current_trace_ids() -> tuple[Optional[str], Optional[str]]:
    """Return ``(trace_id, span_id)`` of the active span (``None`` when idle).

    Used by :class:`~conductor.observability.logging.SpanContextFilter` to put
    the ids on every log line.
    """
    return _backend.current_trace_ids()


def set_task_attributes(
    active: SpanLike,
    *,
    task_id: Optional[str] = None,
    task_type: Optional[str] = None,
    route: Optional[str] = None,
    priority: Optional[int] = None,
    attempt: Optional[int] = None,
    status: Optional[str] = None,
    worker_id: Optional[str] = None,
) -> None:
    """Attach the standard task attributes to *active* (skipping ``None``)."""
    values: dict[str, Any] = {
        ATTR_TASK_ID: task_id,
        ATTR_TASK_TYPE: task_type,
        ATTR_TASK_ROUTE: route,
        ATTR_TASK_PRIORITY: priority,
        ATTR_TASK_ATTEMPT: attempt,
        ATTR_TASK_STATUS: status,
        ATTR_WORKER_ID: worker_id,
    }
    for key, value in values.items():
        if value is not None:
            active.set_attribute(key, value)


__all__: list[str] = [
    "ATTR_TASK_ATTEMPT",
    "ATTR_TASK_ID",
    "ATTR_TASK_PRIORITY",
    "ATTR_TASK_ROUTE",
    "ATTR_TASK_STATUS",
    "ATTR_TASK_TYPE",
    "ATTR_WORKER_ID",
    "EXPORTERS",
    "NoOpBackend",
    "NoOpSpan",
    "SPAN_BLOCK_DEPENDENTS",
    "SPAN_CANCEL",
    "SPAN_DLQ_DISCARD",
    "SPAN_DLQ_RETRY",
    "SPAN_EXECUTE",
    "SPAN_RECURRING_FIRE",
    "SPAN_SUBMIT",
    "SPAN_SUBMIT_MANY",
    "SpanLike",
    "TracingBackend",
    "TracingConfig",
    "add_event",
    "current_trace_ids",
    "current_traceparent",
    "extract_traceparent",
    "get_backend",
    "get_backend_name",
    "is_available",
    "is_enabled",
    "mark_success",
    "record_error",
    "set_task_attributes",
    "setup_tracing",
    "shutdown_tracing",
    "span",
    "span_link",
]
