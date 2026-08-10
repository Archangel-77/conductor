"""
Unit tests for the dashboard FastAPI app (``conductor.api.app.create_app``).

Uses ``fastapi.testclient.TestClient`` with faked queries, health checker,
and DLQ — no database required.
"""

# pylint: disable=missing-class-docstring,missing-function-docstring

from __future__ import annotations

from typing import Any, Optional

import pytest
from fastapi.testclient import TestClient

from conductor.api.app import create_app
from conductor.exceptions import TaskError
from conductor.observability.health import HealthResult, HealthStatus

VALID_STATUSES = {
    "pending",
    "processing",
    "completed",
    "failed",
    "retrying",
    "cancelled",
}


# ===================================================================
# Fakes
# ===================================================================


def task_dict(task_id: str, status: str = "pending") -> dict[str, Any]:
    """Build a minimal task dict shaped like a DB row."""
    return {
        "task_id": task_id,
        "task_type": "test",
        "payload": {"k": "v"},
        "status": status,
        "priority": 0,
        "route": "default",
        "attempt": 0,
        "max_retries": 3,
        "retry_policy": {},
        "scheduled_for": None,
        "worker_id": None,
        "result": None,
        "error_message": None,
        "created_at": "2026-01-01T00:00:00+00:00",
        "started_at": None,
        "completed_at": None,
    }


class FakeQueries:
    """In-memory stand-in for ``QueryBuilder``."""

    def __init__(self) -> None:
        self.tasks: list[dict[str, Any]] = []
        self.workers: list[dict[str, Any]] = []
        self.dlq: list[dict[str, Any]] = []
        self.retries: list[dict[str, Any]] = []
        self.select_tasks_calls: list[tuple[Any, ...]] = []

    async def select_tasks(
        self,
        limit: int,
        offset: int,
        *,
        status: Optional[str] = None,
        route: Optional[str] = None,
        task_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        self.select_tasks_calls.append((limit, offset, status, route, task_type, search))
        if status is not None and status not in VALID_STATUSES:
            raise ValueError(f"Invalid task status '{status}'.")
        return self.tasks

    async def count_tasks(
        self,
        *,
        status: Optional[str] = None,
        route: Optional[str] = None,
        task_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> int:
        return len(self.tasks)

    async def select_task(self, task_id: str) -> Optional[dict[str, Any]]:
        for task in self.tasks:
            if task["task_id"] == task_id:
                return task
        return None

    async def select_retries_for_task(self, task_id: str) -> list[dict[str, Any]]:
        return self.retries

    async def cancel_task(self, task_id: str) -> bool:
        for task in self.tasks:
            if task["task_id"] == task_id and task["status"] in ("pending", "retrying"):
                task["status"] = "cancelled"
                return True
        return False

    async def select_all_workers(self, limit: int, offset: int) -> list[dict[str, Any]]:
        return self.workers

    async def select_dlq_tasks(
        self,
        limit: int,
        offset: int,
        include_discarded: bool = False,
    ) -> list[dict[str, Any]]:
        return self.dlq


class FakeHealthChecker:
    """Stand-in for ``HealthChecker``."""

    def __init__(self) -> None:
        self.result = HealthResult(
            status=HealthStatus.HEALTHY,
            database="connected",
            pending_tasks=1,
            dead_letter_queue=2,
            workers_active=3,
            uptime_seconds=4.0,
        )

    async def check(self) -> HealthResult:
        return self.result


class FakeDLQ:
    """Stand-in for ``DeadLetterQueue``."""

    def __init__(self) -> None:
        self.known: set[str] = set()
        self.retried: list[str] = []
        self.discarded: list[str] = []

    async def retry_task(self, task_id: str) -> str:
        if task_id not in self.known:
            raise TaskError(f"Task '{task_id}' not found in the dead-letter queue.")
        self.retried.append(task_id)
        return task_id

    async def discard_task(self, task_id: str, reason: Optional[str] = None) -> None:
        if task_id not in self.known:
            raise TaskError(f"Task '{task_id}' not found in the dead-letter queue.")
        self.discarded.append(task_id)


# ===================================================================
# Fixtures
# ===================================================================


@pytest.fixture
def queries() -> FakeQueries:
    return FakeQueries()


@pytest.fixture
def health_checker() -> FakeHealthChecker:
    return FakeHealthChecker()


@pytest.fixture
def dlq() -> FakeDLQ:
    return FakeDLQ()


@pytest.fixture
def client(queries: FakeQueries, health_checker: FakeHealthChecker, dlq: FakeDLQ) -> TestClient:
    """Client against an open (no API key) dashboard app."""
    app = create_app(
        queries=queries,  # type: ignore[arg-type]
        health_checker=health_checker,  # type: ignore[arg-type]
        dlq=dlq,  # type: ignore[arg-type]
        api_key=None,
    )
    return TestClient(app)


@pytest.fixture
def auth_client(
    queries: FakeQueries,
    health_checker: FakeHealthChecker,
    dlq: FakeDLQ,
) -> TestClient:
    """Client against a dashboard app protected by an API key."""
    app = create_app(
        queries=queries,  # type: ignore[arg-type]
        health_checker=health_checker,  # type: ignore[arg-type]
        dlq=dlq,  # type: ignore[arg-type]
        api_key="secret",
    )
    return TestClient(app)


# ===================================================================
# Tasks endpoint
# ===================================================================


class TestTasksEndpoint:

    def test_list_tasks(self, client: TestClient, queries: FakeQueries) -> None:
        queries.tasks = [task_dict("t1", "pending"), task_dict("t2", "completed")]
        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 2
        assert len(body["items"]) == 2
        assert body["limit"] == 10
        assert body["offset"] == 0

    def test_list_tasks_passes_filters(self, client: TestClient, queries: FakeQueries) -> None:
        client.get(
            "/api/tasks",
            params={
                "status": "pending",
                "route": "default",
                "task_type": "email",
                "search": "abc",
                "limit": 5,
                "offset": 10,
            },
        )
        assert queries.select_tasks_calls[-1] == (
            5,
            10,
            "pending",
            "default",
            "email",
            "abc",
        )

    def test_list_tasks_invalid_status(self, client: TestClient) -> None:
        resp = client.get("/api/tasks", params={"status": "bogus"})
        assert resp.status_code == 400

    def test_list_tasks_invalid_limit(self, client: TestClient) -> None:
        resp = client.get("/api/tasks", params={"limit": 0})
        assert resp.status_code == 422

    def test_task_detail(self, client: TestClient, queries: FakeQueries) -> None:
        queries.tasks = [task_dict("t1", "failed")]
        queries.retries = [{"id": "r1", "attempt": 1, "error_message": "boom"}]
        resp = client.get("/api/tasks/t1")
        assert resp.status_code == 200
        body = resp.json()
        assert body["task_id"] == "t1"
        assert body["retries"] == [{"id": "r1", "attempt": 1, "error_message": "boom"}]

    def test_task_detail_not_found(self, client: TestClient) -> None:
        resp = client.get("/api/tasks/missing")
        assert resp.status_code == 404

    def test_cancel_pending(self, client: TestClient, queries: FakeQueries) -> None:
        queries.tasks = [task_dict("t1", "pending")]
        resp = client.post("/api/tasks/t1/cancel")
        assert resp.status_code == 200
        assert resp.json() == {"status": "cancelled"}
        assert queries.tasks[0]["status"] == "cancelled"

    def test_cancel_not_found(self, client: TestClient) -> None:
        resp = client.post("/api/tasks/missing/cancel")
        assert resp.status_code == 404

    def test_cancel_not_cancellable(self, client: TestClient, queries: FakeQueries) -> None:
        queries.tasks = [task_dict("t1", "completed")]
        resp = client.post("/api/tasks/t1/cancel")
        assert resp.status_code == 409


# ===================================================================
# Workers endpoint
# ===================================================================


class TestWorkersEndpoint:

    def test_list_workers(self, client: TestClient, queries: FakeQueries) -> None:
        queries.workers = [
            {"worker_id": "w1", "status": "idle"},
            {"worker_id": "w2", "status": "processing"},
        ]
        resp = client.get("/api/workers")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["items"]) == 2
        assert body["items"][0]["worker_id"] == "w1"


# ===================================================================
# Metrics endpoint
# ===================================================================


class TestMetricsEndpoint:

    def test_metrics(self, client: TestClient) -> None:
        resp = client.get("/api/metrics")
        assert resp.status_code == 200
        body = resp.json()
        assert isinstance(body["metrics"], list)
        assert all("name" in m and "samples" in m for m in body["metrics"])


# ===================================================================
# DLQ endpoint
# ===================================================================


class TestDlqEndpoint:

    def test_list_dlq(self, client: TestClient, queries: FakeQueries) -> None:
        queries.dlq = [{"task_id": "d1", "task_type": "test"}]
        resp = client.get("/api/dlq")
        assert resp.status_code == 200
        assert resp.json()["items"] == [{"task_id": "d1", "task_type": "test"}]

    def test_dlq_retry(self, client: TestClient, dlq: FakeDLQ) -> None:
        dlq.known = {"d1"}
        resp = client.post("/api/dlq/d1/retry")
        assert resp.status_code == 200
        assert resp.json() == {"task_id": "d1"}
        assert "d1" in dlq.retried

    def test_dlq_retry_not_found(self, client: TestClient) -> None:
        resp = client.post("/api/dlq/missing/retry")
        assert resp.status_code == 404

    def test_dlq_discard(self, client: TestClient, dlq: FakeDLQ) -> None:
        dlq.known = {"d1"}
        resp = client.post("/api/dlq/d1/discard")
        assert resp.status_code == 200
        assert resp.json() == {"status": "discarded", "task_id": "d1"}
        assert "d1" in dlq.discarded

    def test_dlq_discard_not_found(self, client: TestClient) -> None:
        resp = client.post("/api/dlq/missing/discard")
        assert resp.status_code == 404


# ===================================================================
# Health endpoint
# ===================================================================


class TestHealthEndpoint:

    def test_health(self, client: TestClient) -> None:
        resp = client.get("/api/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert body["database"] == "connected"
        assert body["pending_tasks"] == 1


# ===================================================================
# API-key auth
# ===================================================================


class TestAuth:

    def test_open_when_no_key(self, client: TestClient) -> None:
        assert client.get("/api/health").status_code == 200

    def test_requires_key_when_configured(self, auth_client: TestClient) -> None:
        resp = auth_client.get("/api/health")
        assert resp.status_code == 403

    def test_accepts_valid_key(self, auth_client: TestClient) -> None:
        resp = auth_client.get("/api/health", headers={"X-API-Key": "secret"})
        assert resp.status_code == 200

    def test_rejects_wrong_key(self, auth_client: TestClient) -> None:
        resp = auth_client.get("/api/health", headers={"X-API-Key": "wrong"})
        assert resp.status_code == 403
