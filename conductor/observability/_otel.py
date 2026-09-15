"""
OpenTelemetry SDK glue.

This module is imported **only** when the ``otel`` extra is installed (see
``conductor.observability.tracing.is_available``), so importing it is the
canary for "is OpenTelemetry usable here?".

The tracer provider is kept private to Conductor rather than installed as the
global provider: a process can only set the global provider once, and overriding
a host application's provider would be rude.  Conductor's own spans are exported
through the provider configured here; applications that already run their own
OpenTelemetry SDK can keep doing so.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from typing import Any, Optional, cast

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SimpleSpanProcessor,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Link, Status, StatusCode, Tracer, get_current_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from conductor.observability.tracing import SpanContextManager, TracingConfig
from conductor.exceptions import TracingError

logger = logging.getLogger("conductor.observability.tracing")

_INSTRUMENTATION_SCOPE = "conductor"
"""Instrumentation scope name reported on every span."""


class OtelBackend:
    """Tracing backend backed by the OpenTelemetry SDK."""

    name = "otel"

    def __init__(self, config: TracingConfig) -> None:
        self._config = config
        self._provider = self._build_provider(config)
        self._tracer: Tracer = self._provider.get_tracer(_INSTRUMENTATION_SCOPE)
        self._propagator = TraceContextTextMapPropagator()

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_provider(config: TracingConfig) -> TracerProvider:
        """Create the tracer provider, resource, sampler and exporter."""
        resource = Resource.create({"service.name": config.service_name})
        sampler = ParentBased(root=TraceIdRatioBased(config.sample_ratio))
        provider = TracerProvider(resource=resource, sampler=sampler)

        exporter = _build_exporter(config)
        if exporter is not None:
            # Console output is for humans watching a terminal, so spans are
            # printed as they end; OTLP batches to keep the task path cheap.
            if config.exporter == "console":
                provider.add_span_processor(SimpleSpanProcessor(exporter))
            else:
                provider.add_span_processor(BatchSpanProcessor(exporter))

        return provider

    # ------------------------------------------------------------------
    # TracingBackend
    # ------------------------------------------------------------------

    def start_span(
        self,
        name: str,
        *,
        attributes: Optional[Mapping[str, Any]] = None,
        parent: Optional[Any] = None,
        links: Optional[Sequence[Any]] = None,
    ) -> SpanContextManager:
        """Start a span, optionally parented to an extracted context."""
        # ``start_as_current_span`` returns a context manager that also ends the
        # span on exit; cast it to the protocol Conductor consumes.
        return cast(
            SpanContextManager,
            self._tracer.start_as_current_span(
                name,
                context=parent,
                attributes=dict(attributes) if attributes else None,
                links=list(links) if links else None,
            ),
        )

    def current_traceparent(self) -> Optional[str]:
        """Return the ``traceparent`` of the active span, if it is recording."""
        carrier: dict[str, str] = {}
        self._propagator.inject(carrier)
        traceparent = carrier.get("traceparent")
        if traceparent is None:
            return None
        return str(traceparent)

    def add_event_to_current(
        self, name: str, attributes: Optional[Mapping[str, Any]] = None
    ) -> None:
        """Add an event to the active span (no-op when nothing is recording)."""
        get_current_span().add_event(name, attributes=dict(attributes) if attributes else None)

    def mark_current_success(self) -> None:
        """Set ``OK`` on the active span."""
        get_current_span().set_status(Status(StatusCode.OK))

    def mark_current_error(self, message: str, exception: Optional[BaseException] = None) -> None:
        """Set ``ERROR`` on the active span and record the exception."""
        active = get_current_span()
        active.set_status(Status(StatusCode.ERROR, message))
        if exception is not None:
            active.record_exception(exception)

    def extract_traceparent(self, traceparent: Optional[str]) -> Optional[Any]:
        """Build a parent context from a ``traceparent`` string."""
        if not traceparent:
            return None
        context = self._propagator.extract({"traceparent": traceparent})
        if not get_current_span(context).get_span_context().is_valid:
            logger.debug("Ignoring invalid traceparent '%s'.", traceparent)
            return None
        return context

    def build_link(self, traceparent: Optional[str]) -> Optional[Any]:
        """Build a span link from a stored ``traceparent`` string."""
        return build_span_link(traceparent)

    def current_trace_ids(self) -> tuple[Optional[str], Optional[str]]:
        """Return the hex trace/span ids of the active span, if it is valid."""
        span_context = get_current_span().get_span_context()
        if not span_context.is_valid:
            return (None, None)
        return (format(span_context.trace_id, "032x"), format(span_context.span_id, "016x"))

    def shutdown(self) -> None:
        """Flush pending spans and release exporter resources."""
        try:
            self._provider.shutdown()
        except Exception as exc:  # noqa: BLE001 - shutdown must never raise
            logger.warning("Error while shutting down tracing: %s", exc)


def _build_exporter(config: TracingConfig) -> Optional[SpanExporter]:
    """Create the exporter described by *config*.

    Raises:
        TracingError: If the OTLP exporter cannot be constructed (for example
            when the extra was installed without an endpoint and without the
            ``OTEL_EXPORTER_OTLP_ENDPOINT`` environment variable).
    """
    if config.exporter == "console":
        return ConsoleSpanExporter()

    if config.exporter != "otlp":
        return None

    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

    try:
        if config.endpoint:
            return OTLPSpanExporter(endpoint=config.endpoint)
        return OTLPSpanExporter()
    except Exception as exc:  # noqa: BLE001 - surface a clear configuration error
        raise TracingError(f"Could not configure the OTLP exporter: {exc}") from exc


def build_span_link(traceparent: Optional[str]) -> Optional[Any]:
    """Build a span *link* from a stored ``traceparent``.

    Retries are separate executions, so a new attempt links to the execution it
    is repeating instead of pretending to be its child.
    """
    if not traceparent:
        return None
    context = TraceContextTextMapPropagator().extract({"traceparent": traceparent})
    span_context = get_current_span(context).get_span_context()
    if not span_context.is_valid:
        return None
    return Link(span_context)
