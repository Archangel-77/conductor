"""
Observability package.

Structured logging, Prometheus metrics, health checks, and OpenTelemetry
tracing.

Tracing is an optional extra (``pip install "conductor-task-queue[otel]"``);
without it every tracing call degrades to a no-op.
"""

from __future__ import annotations

from conductor.exceptions import TracingError
from conductor.observability.logging import (
    JsonFormatter,
    SpanContextFilter,
    setup_logging,
)
from conductor.observability.metrics import MetricsExporter
from conductor.observability.health import HealthChecker, HealthResult, HealthStatus
from conductor.observability.tracing import (
    TracingConfig,
    is_available,
    is_enabled,
    setup_tracing,
    shutdown_tracing,
    span,
)

__all__: list[str] = [
    "HealthChecker",
    "HealthResult",
    "HealthStatus",
    "JsonFormatter",
    "MetricsExporter",
    "SpanContextFilter",
    "TracingConfig",
    "TracingError",
    "is_available",
    "is_enabled",
    "setup_logging",
    "setup_tracing",
    "shutdown_tracing",
    "span",
]
