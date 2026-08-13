"""
Integration tests for the web dashboard API (``DashboardServer``).

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,missing-function-docstring
# pylint: disable=import-outside-toplevel,protected-access,redefined-outer-name,unused-argument

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest
import pytest_asyncio

from conductor.exceptions import ConductorConnectionError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]

DASHBOARD_URL = "http://127.0.0.1:8767"


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="server")
async def _dashboard_factory() -> Any:
    """Start a DashboardServer against the test DB on a non-default port."""
    from tests.conftest import TEST_DATABASE_URL, db_available
    from conductor.api.server import DashboardServer
    from conductor.db.connection import DatabasePool
    from conductor.db.schema import SchemaManager

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

    server = DashboardServer(pool, host="127.0.0.1", port=8767)
    await server.start()

    # Wait until uvicorn accepts connections.
    deadline = time.monotonic() + 10
    async with httpx.AsyncClient(base_url=DASHBOARD_URL) as http:
        while time.monotonic() < deadline:
            try:
                resp = await http.get("/api/health")
                if resp.status_code == 200:
                    break
            except httpx.ConnectError:
                pass
            await asyncio_sleep(0.1)

    yield server

    await server.stop()
    await pool.disconnect()


async def asyncio_sleep(seconds: float) -> None:
    """Sleep without importing asyncio at module scope."""
    import asyncio

    await asyncio.sleep(seconds)


@pytest_asyncio.fixture
async def client(server: Any) -> Any:
    """Async HTTP client against the running dashboard server."""
    async with httpx.AsyncClient(base_url=DASHBOARD_URL) as c:
        yield c


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def _cleanup(server: Any) -> Any:
    """Clean all conductor tables before and after each test."""
    pool = server._pool

    async def truncate() -> None:
        if pool.is_connected:
            await pool.execute("DELETE FROM conductor_retries")
            await pool.execute("DELETE FROM conductor_dead_letter")
            await pool.execute("DELETE FROM conductor_tasks")
            await pool.execute("DELETE FROM conductor_workers")
            await pool.execute("DELETE FROM conductor_recurring_tasks")

    await truncate()
    yield
    await truncate()


def _task_dict(task_id: str, **overrides: Any) -> dict[str, Any]:
    """Build a minimal task dict for direct insertion via the server's queries."""
    from conductor.core.models import utc_now

    base = {
        "task_id": task_id,
        "task_type": "api.task",
        "payload": {"k": "v"},
        "status": "pending",
        "priority": 0,
        "route": "default",
        "attempt": 0,
        "max_retries": 3,
        "retry_policy": {
            "max_retries": 3,
            "backoff_strategy": "exponential",
            "initial_delay": 1.0,
            "max_delay": 3600.0,
        },
        "scheduled_for": None,
        "worker_id": None,
        "result": None,
        "error_message": None,
        "created_at": utc_now(),
        "started_at": None,
        "completed_at": None,
    }
    base.update(overrides)
    return base


# ===================================================================
# Tasks
# ===================================================================


class TestTasksEndpoint:

    async def test_list_tasks_empty(self, client: Any) -> None:
        resp = await client.get("/api/tasks")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 0
        assert body["items"] == []

    async def test_list_tasks_with_filters_and_search(self, client: Any, server: Any) -> None:
        queries = server._queries
        await queries.insert_task(_task_dict("api-1", route="emails"))
        await queries.insert_task(_task_dict("api-2", route="sms"))

        resp = await client.get("/api/tasks")
        assert resp.status_code == 200
        assert resp.json()["total"] == 2

        resp = await client.get("/api/tasks", params={"route": "emails"})
        assert resp.json()["total"] == 1
        assert resp.json()["items"][0]["task_id"] == "api-1"

        resp = await client.get("/api/tasks", params={"search": "api-2"})
        assert resp.json()["total"] == 1
        assert resp.json()["items"][0]["task_id"] == "api-2"

        resp = await client.get("/api/tasks", params={"task_type": "api.task"})
        assert resp.json()["total"] == 2

    async def test_task_detail_includes_retries(self, client: Any, server: Any) -> None:
        from conductor.core.models import utc_now

        queries = server._queries
        await queries.insert_task(_task_dict("api-detail", status="failed"))
        await queries.insert_retry_record(
            {
                "id": "api-retry-1",
                "task_id": "api-detail",
                "attempt": 1,
                "error_message": "boom",
                "scheduled_at": utc_now(),
            }
        )

        resp = await client.get("/api/tasks/api-detail")
        assert resp.status_code == 200
        body = resp.json()
        assert body["task_id"] == "api-detail"
        assert body["status"] == "failed"
        assert len(body["retries"]) == 1

    async def test_task_detail_not_found(self, client: Any) -> None:
        resp = await client.get("/api/tasks/does-not-exist")
        assert resp.status_code == 404

    async def test_cancel_flow(self, client: Any, server: Any) -> None:
        queries = server._queries
        await queries.insert_task(_task_dict("api-cancel"))

        resp = await client.post("/api/tasks/api-cancel/cancel")
        assert resp.status_code == 200
        assert resp.json() == {"status": "cancelled"}

        # Verify persisted in the DB
        row = await queries.select_task("api-cancel")
        assert row is not None
        assert row["status"] == "cancelled"

        # Second cancel → conflict
        resp = await client.post("/api/tasks/api-cancel/cancel")
        assert resp.status_code == 409


# ===================================================================
# Workers
# ===================================================================


class TestWorkersEndpoint:

    async def test_list_workers(self, client: Any, server: Any) -> None:
        from conductor.core.models import utc_now

        queries = server._queries
        await queries.upsert_worker(
            {
                "worker_id": "api-worker-1",
                "status": "idle",
                "current_task_id": None,
                "hostname": "host-a",
                "pid": 1,
                "uptime_seconds": 10.0,
                "tasks_processed_total": 5,
                "tasks_failed_total": 0,
                "last_heartbeat": utc_now(),
                "started_at": utc_now(),
            }
        )

        resp = await client.get("/api/workers")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["items"]) == 1
        assert body["items"][0]["worker_id"] == "api-worker-1"


# ===================================================================
# Metrics & health
# ===================================================================


class TestMetricsAndHealthEndpoint:

    async def test_metrics_json(self, client: Any) -> None:
        resp = await client.get("/api/metrics")
        assert resp.status_code == 200
        metrics = resp.json()["metrics"]
        assert isinstance(metrics, list)
        names = {m["name"] for m in metrics}
        assert "conductor_tasks_submitted" in names

    async def test_health(self, client: Any) -> None:
        resp = await client.get("/api/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] in {"healthy", "degraded", "unhealthy"}
        assert "database" in body
        assert "pending_tasks" in body


# ===================================================================
# DLQ
# ===================================================================


class TestDlqEndpoint:

    async def test_dlq_retry_discard_flow(self, client: Any, server: Any) -> None:
        queries = server._queries
        # Insert a task and move it to the DLQ
        await queries.insert_task(_task_dict("api-dlq", status="failed"))
        await queries.update_task_status("api-dlq", "failed")
        await queries.insert_dlq_task(
            {
                "task_id": "api-dlq",
                "task_type": "api.task",
                "payload": {},
                "error_message": "boom",
                "attempts": 3,
                "retry_policy": {},
                "route": "default",
                "priority": 0,
            }
        )

        resp = await client.get("/api/dlq")
        assert resp.status_code == 200
        assert resp.json()["items"][0]["task_id"] == "api-dlq"

        # Retry: DLQ entry disappears and the task goes back to pending
        resp = await client.post("/api/dlq/api-dlq/retry")
        assert resp.status_code == 200
        assert resp.json() == {"task_id": "api-dlq"}

        resp = await client.get("/api/dlq")
        assert resp.json()["items"] == []

        row = await queries.select_task("api-dlq")
        assert row is not None
        assert row["status"] == "pending"

    async def test_dlq_discard(self, client: Any, server: Any) -> None:
        queries = server._queries
        await queries.insert_task(_task_dict("api-discard", status="failed"))
        await queries.update_task_status("api-discard", "failed")
        await queries.insert_dlq_task(
            {
                "task_id": "api-discard",
                "task_type": "api.task",
                "payload": {},
                "error_message": "boom",
                "attempts": 3,
                "retry_policy": {},
                "route": "default",
                "priority": 0,
            }
        )

        resp = await client.post("/api/dlq/api-discard/discard")
        assert resp.status_code == 200
        assert resp.json() == {"status": "discarded", "task_id": "api-discard"}

        # Hidden by default, visible with include_discarded
        resp = await client.get("/api/dlq")
        assert resp.json()["items"] == []
        resp = await client.get("/api/dlq", params={"include_discarded": "true"})
        assert len(resp.json()["items"]) == 1

    async def test_dlq_retry_not_found(self, client: Any) -> None:
        resp = await client.post("/api/dlq/missing/retry")
        assert resp.status_code == 404
