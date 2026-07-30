"""
Integration tests for the Metrics Exporter.

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio

from conductor.exceptions import ConductorConnectionError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="exporter")
async def _metrics_exporter_factory() -> Any:
    """Create a MetricsExporter + HealthChecker connected to test DB.

    Skips if the database is not running.
    """
    from tests.conftest import TEST_DATABASE_URL, db_available
    from conductor.db.connection import DatabasePool
    from conductor.db.schema import SchemaManager
    from conductor.observability.health import HealthChecker
    from conductor.observability.metrics import MetricsExporter

    if not db_available():
        pytest.skip("Test database not available")

    pool = DatabasePool(
        dsn=TEST_DATABASE_URL,
        min_size=1,
        max_size=2,
        timeout=5.0,
    )
    try:
        await pool.connect()
        await SchemaManager(pool).ensure_schema()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    health_checker = HealthChecker(pool)
    exporter = MetricsExporter(
        pool=pool,
        health_checker=health_checker,
        port=8765,  # Use a non-default port to avoid conflicts
    )
    await exporter.start()

    yield exporter

    await exporter.stop()
    await pool.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def auto_cleanup(exporter: Any) -> Any:
    """Clean all rows from conductor tables after each test."""
    yield

    # Access the pool from the exporter's health checker
    pool = exporter._pool
    if pool.is_connected:
        await pool.execute("DELETE FROM conductor_retries")
        await pool.execute("DELETE FROM conductor_dead_letter")
        await pool.execute("DELETE FROM conductor_tasks")
        await pool.execute("DELETE FROM conductor_workers")


# ===================================================================
# Tests
# ===================================================================


class TestMetricsEndpoint:
    """Verify the /metrics endpoint."""

    async def test_metrics_endpoint_returns_200(self, exporter: Any) -> None:
        """GET /metrics should return HTTP 200 with text content type."""
        assert exporter is not None
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8765/metrics") as resp:
                assert resp.status == 200
                content_type = resp.headers.get("Content-Type", "")
                assert "text/plain" in content_type

    async def test_metrics_contains_expected_metric_names(
        self,
        exporter: Any,
    ) -> None:
        """The Prometheus output should contain our metric names."""
        assert exporter is not None
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8765/metrics") as resp:
                body = await resp.text()

        # Check for expected metric names
        assert "conductor_tasks_submitted_total" in body
        assert "conductor_tasks_completed_total" in body
        assert "conductor_tasks_failed_total" in body
        assert "conductor_tasks_retried_total" in body
        assert "conductor_task_duration_seconds" in body
        assert "conductor_workers_active" in body
        assert "conductor_dlq_size" in body
        assert "conductor_pending_tasks" in body

    async def test_counter_increments_appear_in_metrics(
        self,
        exporter: Any,
    ) -> None:
        """Calling inc_tasks_submitted should be reflected in /metrics."""
        assert exporter is not None
        from conductor.observability.metrics import inc_tasks_submitted

        # Reset state by calling once
        inc_tasks_submitted("test_metrics_counter")

        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8765/metrics") as resp:
                body = await resp.text()

        # Find the line for our counter
        lines = body.split("\n")
        counter_lines = [
            line
            for line in lines
            if 'conductor_tasks_submitted_total{task_type="test_metrics_counter"}' in line
        ]
        assert len(counter_lines) >= 1
        # The last occurrence has the current value
        last_line = counter_lines[-1]
        # Format is: conductor_tasks_submitted_total{...} VALUE
        value = float(last_line.split()[-1])
        assert value >= 1.0

    async def test_histogram_observation_appears_in_metrics(
        self,
        exporter: Any,
    ) -> None:
        """Calling observe_task_duration should be reflected in /metrics."""
        assert exporter is not None
        from conductor.observability.metrics import observe_task_duration

        observe_task_duration("test_histogram", 0.5)

        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8765/metrics") as resp:
                body = await resp.text()

        # Check that the histogram buckets exist for our task_type
        assert "conductor_task_duration_seconds_bucket" in body
        assert 'task_type="test_histogram"' in body


class TestHealthEndpoint:
    """Verify the /health endpoint served by MetricsExporter."""

    async def test_health_endpoint_returns_200(self, exporter: Any) -> None:
        """GET /health should return HTTP 200 with JSON content type."""
        assert exporter is not None
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8765/health") as resp:
                assert resp.status == 200
                content_type = resp.headers.get("Content-Type", "")
                assert "application/json" in content_type

    async def test_health_endpoint_returns_valid_json(
        self,
        exporter: Any,
    ) -> None:
        """GET /health should return valid JSON with expected fields."""
        assert exporter is not None
        import aiohttp

        async with aiohttp.ClientSession() as session:
            async with session.get("http://localhost:8765/health") as resp:
                body = await resp.json()

        assert "status" in body
        assert "database" in body
        assert "pending_tasks" in body
        assert "dead_letter_queue" in body
        assert "workers_active" in body
        assert "uptime_seconds" in body
        assert "last_check" in body
        assert body["database"] == "connected"
