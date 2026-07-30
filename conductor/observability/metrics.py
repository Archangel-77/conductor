"""
Prometheus metrics exporter for Conductor.

Provides ``MetricsExporter`` — an aiohttp-based HTTP server that exposes
Prometheus metrics at ``/metrics`` and health check results at ``/health``.

Metric hook functions are module-level so they can be imported and called
directly from Worker and TaskQueue without requiring a reference to the
exporter instance.

Typical usage::

    from conductor.observability.metrics import MetricsExporter

    exporter = MetricsExporter(pool, health_checker, port=8000)
    await exporter.start()
    # ... later ...
    await exporter.stop()
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from aiohttp import web

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)

from conductor.observability.health import HealthChecker

logger = logging.getLogger("conductor.observability.metrics")

# ==================================================================
# Prometheus metric definitions
# ==================================================================

# -- Counters --
tasks_submitted = Counter(
    "conductor_tasks_submitted_total",
    "Total number of tasks submitted to the queue.",
    labelnames=["task_type"],
)

tasks_completed = Counter(
    "conductor_tasks_completed_total",
    "Total number of tasks completed successfully.",
    labelnames=["task_type"],
)

tasks_failed = Counter(
    "conductor_tasks_failed_total",
    "Total number of tasks that failed execution.",
    labelnames=["task_type"],
)

tasks_retried = Counter(
    "conductor_tasks_retried_total",
    "Total number of tasks scheduled for retry.",
    labelnames=["task_type"],
)

# -- Histogram --
task_duration = Histogram(
    "conductor_task_duration_seconds",
    "Execution duration of tasks in seconds.",
    labelnames=["task_type"],
    buckets=(
        0.005,
        0.01,
        0.025,
        0.05,
        0.075,
        0.1,
        0.25,
        0.5,
        0.75,
        1.0,
        2.5,
        5.0,
        7.5,
        10.0,
        30.0,
        60.0,
    ),
)

# -- Gauges --
workers_active = Gauge(
    "conductor_workers_active",
    "Number of workers with recent heartbeats.",
)

dlq_size = Gauge(
    "conductor_dlq_size",
    "Number of tasks currently in the dead-letter queue.",
)

pending_tasks = Gauge(
    "conductor_pending_tasks",
    "Number of tasks with status 'pending'.",
)


# ==================================================================
# Metric hook functions
# ==================================================================


def inc_tasks_submitted(task_type: str) -> None:
    """Increment the submitted-task counter for *task_type*."""
    tasks_submitted.labels(task_type=task_type).inc()


def inc_tasks_completed(task_type: str) -> None:
    """Increment the completed-task counter for *task_type*."""
    tasks_completed.labels(task_type=task_type).inc()


def inc_tasks_failed(task_type: str) -> None:
    """Increment the failed-task counter for *task_type*."""
    tasks_failed.labels(task_type=task_type).inc()


def inc_tasks_retried(task_type: str) -> None:
    """Increment the retried-task counter for *task_type*."""
    tasks_retried.labels(task_type=task_type).inc()


def observe_task_duration(task_type: str, duration_seconds: float) -> None:
    """Record an observation of task execution duration.

    Args:
        task_type: The type of task that was executed.
        duration_seconds: Wall-clock duration in seconds.
    """
    task_duration.labels(task_type=task_type).observe(duration_seconds)


def set_workers_active(count: int) -> None:
    """Set the active-workers gauge to *count*."""
    workers_active.set(count)


def set_dlq_size(count: int) -> None:
    """Set the DLQ-size gauge to *count*."""
    dlq_size.set(count)


def set_pending_tasks(count: int) -> None:
    """Set the pending-tasks gauge to *count*."""
    pending_tasks.set(count)


# ==================================================================
# Metrics exporter
# ==================================================================


class MetricsExporter:
    """HTTP server that exposes Prometheus metrics and health check data.

    Manages an ``aiohttp.web.Application`` that listens on a configurable
    port and serves two endpoints:

    - ``GET /metrics`` — Prometheus text format metrics
    - ``GET /health`` — JSON health check response

    The server runs as a background asyncio task alongside the worker.

    Args:
        pool: Database pool used for periodic gauge updates.
        health_checker: ``HealthChecker`` instance for the ``/health`` endpoint.
        port: TCP port to listen on (default: ``8000``).
    """

    def __init__(
        self,
        pool: Any,
        health_checker: HealthChecker,
        port: int = 8000,
    ) -> None:
        self._pool = pool
        self._health_checker = health_checker
        self._port = port

        self._app: web.Application = web.Application()
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._server_task: Optional[asyncio.Task[None]] = None
        self._gauge_task: Optional[asyncio.Task[None]] = None
        self._started = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the HTTP server and the background gauge updater."""
        if self._started:
            return

        self._setup_routes()

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, host="0.0.0.0", port=self._port)

        try:
            await self._site.start()
        except OSError as exc:
            logger.warning(
                "Metrics exporter could not bind to port %d: %s. "
                "Skipping metrics/health server.",
                self._port,
                exc,
            )
            await self._runner.cleanup()
            self._runner = None
            self._site = None
            return

        # Start background gauge updater
        self._gauge_task = asyncio.create_task(
            self._update_gauges_background(),
            name="metrics-gauge-updater",
        )

        self._started = True
        logger.info(
            "Metrics exporter started on 0.0.0.0:%d.",
            self._port,
        )

    async def stop(self) -> None:
        """Stop the HTTP server and the background gauge updater."""
        if not self._started:
            return

        # Stop gauge updater
        if self._gauge_task is not None and not self._gauge_task.done():
            self._gauge_task.cancel()
            try:
                await self._gauge_task
            except asyncio.CancelledError:
                pass

        # Stop HTTP server
        if self._runner is not None:
            await self._runner.cleanup()

        self._started = False
        logger.info("Metrics exporter stopped.")

    # ------------------------------------------------------------------
    # Route setup
    # ------------------------------------------------------------------

    def _setup_routes(self) -> None:
        """Register the ``/metrics`` and ``/health`` routes."""
        self._app.router.add_get("/metrics", self._handle_metrics)
        self._app.router.add_get("/health", self._handle_health)

    async def _handle_metrics(self, _request: web.Request) -> web.Response:
        """Return Prometheus metrics in text format."""
        return web.Response(
            body=generate_latest(),
            content_type="text/plain; version=0.0.4",
        )

    async def _handle_health(self, _request: web.Request) -> web.Response:
        """Return health check result as JSON."""
        result = await self._health_checker.check()
        return web.json_response(result.to_dict())

    # ------------------------------------------------------------------
    # Gauge updating
    # ------------------------------------------------------------------

    async def _update_gauges_background(self) -> None:
        """Periodically refresh gauge metrics from the database.

        Runs every 15 seconds until cancelled.
        """
        try:
            while True:
                await self._update_gauges()
                await asyncio.sleep(15.0)
        except asyncio.CancelledError:
            pass

    async def _update_gauges(self) -> None:
        """Query the database and update gauge values."""
        from conductor.db.queries import QueryBuilder

        queries = QueryBuilder(self._pool)
        try:
            pending, dlq, workers = await asyncio.gather(
                queries.count_tasks_by_status("pending"),
                queries.count_dlq_tasks(include_discarded=False),
                queries.select_active_workers(heartbeat_timeout=30.0),
                return_exceptions=True,
            )

            if not isinstance(pending, Exception):
                set_pending_tasks(pending)
            if not isinstance(dlq, Exception):
                set_dlq_size(dlq)
            if not isinstance(workers, Exception):
                set_workers_active(len(workers))

        except (OSError, RuntimeError) as exc:
            logger.warning("Failed to update metric gauges: %s", exc)
