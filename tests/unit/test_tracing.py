"""
Unit tests for OpenTelemetry tracing.

Covers the configuration surface, the no-op fallback (when the ``otel`` extra is
absent or tracing is disabled), W3C ``traceparent`` propagation and the span
attribute/event helpers.  No database is required.

The OpenTelemetry SDK is exercised with an in-memory exporter, so the tests
never need a collector.
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator
from typing import Any

import pytest

from conductor.exceptions import TracingError
from conductor.observability import tracing
from conductor.observability.tracing import TracingConfig

pytestmark = pytest.mark.unit

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


@pytest.fixture(autouse=True)
def _reset_tracing() -> Iterator[None]:
    """Leave the global tracing backend in its default state after each test."""
    yield
    tracing.shutdown_tracing()


@pytest.fixture
def memory_exporter() -> Any:
    """Install a tracing backend whose spans land in an in-memory exporter."""
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    tracing.setup_tracing(
        tracing.TracingConfig(enabled=True, exporter="console", service_name="test-service")
    )
    backend = tracing.get_backend()
    # Swap the console exporter for the in-memory one: same provider, no output.
    provider = backend._provider  # noqa: SLF001 - test hook into the SDK provider
    provider._active_span_processor._span_processors = ()  # noqa: SLF001
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor

    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


# ===================================================================
# Configuration
# ===================================================================


class TestTracingConfig:

    def test_defaults_are_disabled(self) -> None:
        config = TracingConfig()
        config.validate()
        assert config.enabled is False
        assert config.exporter == "otlp"
        assert config.service_name == "conductor"
        assert config.sample_ratio == 1.0

    @pytest.mark.parametrize("exporter", ["none", "console", "otlp"])
    def test_accepts_known_exporters(self, exporter: str) -> None:
        TracingConfig(exporter=exporter).validate()

    def test_rejects_unknown_exporter(self) -> None:
        with pytest.raises(TracingError, match="Invalid tracing exporter"):
            TracingConfig(exporter="jaeger").validate()

    def test_rejects_empty_service_name(self) -> None:
        with pytest.raises(TracingError, match="service_name"):
            TracingConfig(service_name="  ").validate()

    @pytest.mark.parametrize("ratio", [-0.1, 1.1])
    def test_rejects_out_of_range_sample_ratio(self, ratio: float) -> None:
        with pytest.raises(TracingError, match="sample_ratio"):
            TracingConfig(sample_ratio=ratio).validate()

    def test_dict_round_trip(self) -> None:
        config = TracingConfig(
            enabled=True,
            service_name="api",
            exporter="console",
            endpoint="http://localhost:4318/v1/traces",
            sample_ratio=0.25,
        )
        assert TracingConfig.from_dict(config.to_dict()) == config


# ===================================================================
# No-op fallback
# ===================================================================


class TestNoOpBackend:

    def test_disabled_by_default(self) -> None:
        assert tracing.is_enabled() is False
        assert tracing.get_backend_name() == "none"
        assert tracing.current_traceparent() is None
        assert tracing.current_trace_ids() == (None, None)
        assert tracing.extract_traceparent(TRACEPARENT) is None
        assert tracing.span_link(None) is None

    def test_spans_are_no_ops(self) -> None:
        with tracing.span("anything", attributes={"a": 1}) as active:
            assert active.is_recording() is False
            tracing.set_task_attributes(active, task_id="t1", task_type="echo")
            tracing.add_event("event", {"k": "v"})
            tracing.mark_success()
            tracing.record_error("nope", RuntimeError("nope"))
            active.set_attribute("b", 2)
            active.end()

    def test_setup_with_tracing_disabled_is_a_noop(self) -> None:
        tracing.setup_tracing(tracing.TracingConfig(enabled=True, exporter="none"))
        assert tracing.is_enabled() is False

    def test_shutdown_is_idempotent(self) -> None:
        tracing.shutdown_tracing()
        tracing.shutdown_tracing()
        assert tracing.get_backend_name() == "none"

    def test_missing_extra_raises_a_clear_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Enabling tracing without the extra must name the install command."""
        real_import = builtins.__import__

        def blocked_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "conductor.observability._otel" or name.startswith("opentelemetry"):
                raise ImportError("No module named 'opentelemetry'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", blocked_import)
        monkeypatch.setattr(tracing, "_otel_import_error", None, raising=False)

        assert tracing.is_available() is False
        with pytest.raises(TracingError, match=r"\[otel\]"):
            tracing.setup_tracing(tracing.TracingConfig(enabled=True, exporter="console"))


# ===================================================================
# Real backend (in-memory exporter)
# ===================================================================


class TestOtelBackend:

    def test_extra_is_available(self) -> None:
        assert tracing.is_available() is True

    def test_span_is_recorded_with_attributes(self, memory_exporter: Any) -> None:
        assert tracing.is_enabled() is True
        with tracing.span(
            tracing.SPAN_SUBMIT, attributes={tracing.ATTR_TASK_TYPE: "echo"}
        ) as active:
            tracing.set_task_attributes(active, task_id="t1", route="default", priority=5)
        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 1
        span = spans[0]
        assert span.name == tracing.SPAN_SUBMIT
        assert span.attributes[tracing.ATTR_TASK_TYPE] == "echo"
        assert span.attributes[tracing.ATTR_TASK_ID] == "t1"
        assert span.attributes[tracing.ATTR_TASK_PRIORITY] == 5
        assert span.resource.attributes["service.name"] == "test-service"

    def test_traceparent_is_well_formed(self, memory_exporter: Any) -> None:
        with tracing.span(tracing.SPAN_SUBMIT):
            traceparent = tracing.current_traceparent()
        assert traceparent is not None
        version, trace_id, span_id, flags = traceparent.split("-")
        assert version == "00"
        assert len(trace_id) == 32 and len(span_id) == 16 and len(flags) == 2

    def test_child_span_continues_the_trace(self, memory_exporter: Any) -> None:
        with tracing.span(tracing.SPAN_SUBMIT):
            traceparent = tracing.current_traceparent()

        parent = tracing.extract_traceparent(traceparent)
        with tracing.span(tracing.SPAN_EXECUTE, parent=parent):
            child_traceparent = tracing.current_traceparent()

        spans = memory_exporter.get_finished_spans()
        assert len(spans) == 2
        submit, execute = spans
        assert execute.context.trace_id == submit.context.trace_id
        assert execute.parent is not None
        assert execute.parent.span_id == submit.context.span_id
        assert child_traceparent is not None
        assert child_traceparent.split("-")[1] == traceparent.split("-")[1]

    def test_retry_links_instead_of_nesting(self, memory_exporter: Any) -> None:
        with tracing.span(tracing.SPAN_SUBMIT):
            first_attempt_traceparent = tracing.current_traceparent()

        # An attempt has no parent context (it happens later, in another
        # process), but links back to the execution it repeats.
        link = tracing.span_link(first_attempt_traceparent)
        assert link is not None
        with tracing.span(tracing.SPAN_EXECUTE, links=[link]):
            pass

        spans = memory_exporter.get_finished_spans()
        submit, retry = spans
        assert retry.parent is None
        assert len(retry.links) == 1
        assert retry.links[0].context.trace_id == submit.context.trace_id

    def test_status_and_events(self, memory_exporter: Any) -> None:
        from opentelemetry.trace import StatusCode

        with tracing.span(tracing.SPAN_EXECUTE):
            tracing.add_event("retry.scheduled", {"retry.delay_seconds": 2.0})
            tracing.record_error("handler exploded", RuntimeError("handler exploded"))

        span = memory_exporter.get_finished_spans()[0]
        assert span.status.status_code is StatusCode.ERROR
        assert span.status.description == "handler exploded"
        assert span.events[0].name == "retry.scheduled"
        assert span.events[1].name == "exception"

    def test_mark_success_sets_ok(self, memory_exporter: Any) -> None:
        from opentelemetry.trace import StatusCode

        with tracing.span(tracing.SPAN_EXECUTE):
            tracing.mark_success()
        assert memory_exporter.get_finished_spans()[0].status.status_code is StatusCode.OK

    def test_trace_ids_available_inside_a_span(self, memory_exporter: Any) -> None:
        with tracing.span(tracing.SPAN_EXECUTE):
            trace_id, span_id = tracing.current_trace_ids()
        assert trace_id is not None and len(trace_id) == 32
        assert span_id is not None and len(span_id) == 16

    def test_invalid_traceparent_is_ignored(self, memory_exporter: Any) -> None:
        assert tracing.extract_traceparent("not-a-traceparent") is None
        assert tracing.span_link("not-a-traceparent") is None

    def test_shutdown_releases_the_backend(self, memory_exporter: Any) -> None:
        tracing.shutdown_tracing()
        assert tracing.is_enabled() is False
        assert tracing.get_backend_name() == "none"


# ===================================================================
# Log correlation
# ===================================================================


class TestSpanContextFilter:

    def test_records_carry_trace_ids(self, memory_exporter: Any) -> None:
        import logging

        from conductor.observability.logging import SpanContextFilter

        record = logging.LogRecord("conductor.test", logging.INFO, __file__, 1, "hi", (), None)
        with tracing.span(tracing.SPAN_EXECUTE):
            assert SpanContextFilter().filter(record) is True
            assert len(record.trace_id) == 32  # type: ignore[attr-defined]
            assert len(record.span_id) == 16  # type: ignore[attr-defined]

    def test_records_unchanged_when_tracing_is_off(self) -> None:
        import logging

        from conductor.observability.logging import SpanContextFilter

        record = logging.LogRecord("conductor.test", logging.INFO, __file__, 1, "hi", (), None)
        assert SpanContextFilter().filter(record) is True
        assert not hasattr(record, "trace_id")
