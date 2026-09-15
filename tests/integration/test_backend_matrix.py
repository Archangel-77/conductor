"""
Backend parity matrix.

Runs the same core flows against **every configured backend** so behaviour is
proven identical rather than merely importable.  Backends come from
``CONDUCTOR_TEST_DATABASE_URLS`` (comma-separated); when that is unset the
matrix runs against the primary PostgreSQL test URL plus a temporary SQLite
database — so the SQLite leg executes everywhere, including CI jobs with no
service container at all.

Every test is parametrized by the ``backend`` fixture and skips independently
when its database is unreachable, so a missing PostgreSQL does not stop the
SQLite leg (and vice versa).  The MySQL/MariaDB leg is normally exercised by
the dedicated CI job, which passes a ``mysql://`` DSN in
``CONDUCTOR_TEST_DATABASE_URLS``.
"""

from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import RetryPolicy
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.exceptions import ConductorConnectionError, TaskError
from tests.conftest import TEST_DATABASE_URL, truncate_all

pytestmark = pytest.mark.integration

SUPPORTED_BACKENDS: tuple[str, ...] = ("postgresql", "sqlite", "mysql")
"""Backends the matrix knows how to run; each is skipped when unavailable."""

_SQLITE_PATH = Path(tempfile.gettempdir()) / f"conductor_matrix_{os.getpid()}.db"
"""Temporary SQLite database used when no explicit DSN list is configured."""

_Handler = Callable[[dict[str, Any]], Any]


@dataclass(frozen=True)
class Backend:
    """One backend under test."""

    name: str
    """Backend label (``postgresql``, ``sqlite``, ``mysql``)."""

    dsn: str
    """Connection string."""

    pool: DatabasePool
    """A connected, migrated pool for this backend."""


def _matrix_dsns() -> dict[str, str]:
    """Resolve the backends to test, keyed by backend label."""
    from conductor.db.backends.registry import detect_backend

    configured = [
        dsn.strip()
        for dsn in os.environ.get("CONDUCTOR_TEST_DATABASE_URLS", "").split(",")
        if dsn.strip()
    ]
    if not configured:
        configured = [TEST_DATABASE_URL, f"sqlite:///{_SQLITE_PATH}"]

    resolved: dict[str, str] = {}
    for dsn in configured:
        try:
            resolved[detect_backend(dsn)] = dsn
        except ConductorConnectionError:  # pragma: no cover - bad configuration
            continue
    return resolved


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def backend_pools() -> AsyncIterator[dict[str, Backend]]:
    """Connect and migrate every configured backend (unreachable ones omitted)."""
    backends: dict[str, Backend] = {}

    for name, dsn in _matrix_dsns().items():
        pool = DatabasePool(
            dsn=dsn,
            min_size=1,
            max_size=2,
            timeout=5.0,
            max_retries=1,
            busy_timeout=5.0,
        )
        try:
            await pool.connect()
        except ConductorConnectionError:
            continue
        try:
            await SchemaManager(pool).ensure_schema()
        except Exception:  # noqa: BLE001 - one bad backend must not abort the others
            await pool.disconnect()
            continue
        backends[name] = Backend(name=name, dsn=dsn, pool=pool)

    if not backends:
        pytest.skip("No test backend available")

    yield backends

    for backend in backends.values():
        await backend.pool.disconnect()
    for suffix in ("", "-wal", "-shm"):
        leftover = Path(f"{_SQLITE_PATH}{suffix}")
        if leftover.exists():
            leftover.unlink()


@pytest_asyncio.fixture(params=SUPPORTED_BACKENDS, loop_scope="session")
async def backend(  # noqa: N802  # pylint: disable=redefined-outer-name
    request: pytest.FixtureRequest,
    backend_pools: dict[str, Backend],
) -> AsyncIterator[Backend]:
    """Yield the backend named by the parameter, with clean tables."""
    name = str(request.param)
    if name not in backend_pools:
        pytest.skip(f"{name} backend not available")

    selected = backend_pools[name]
    await truncate_all(selected.pool)
    yield selected
    await truncate_all(selected.pool)


@pytest_asyncio.fixture(loop_scope="session")
async def queue(backend: Backend) -> AsyncIterator[TaskQueue]:  # noqa: N802
    """A connected :class:`TaskQueue` for the backend under test."""
    task_queue = TaskQueue(backend.dsn)
    await task_queue.connect()
    yield task_queue
    await task_queue.disconnect()


@asynccontextmanager
async def running_worker(
    backend: Backend,
    handlers: dict[str, _Handler],
    *,
    worker_id: str = "matrix-worker",
    routes: list[str] | None = None,
) -> AsyncIterator[Worker]:
    """Run a connected worker with *handlers* registered.

    Args:
        backend: The backend under test.
        handlers: Mapping of task type to handler coroutine function.
        worker_id: Worker identity to register.
        routes: Routes to poll; ``None`` polls every route (the programmatic
            default).  The CLI defaults to ``["default"]`` instead.

    Yields:
        The connected worker (disconnected on exit, even on failure).
    """
    worker = Worker(backend.dsn, worker_id=worker_id, poll_interval=0.01, routes=routes)
    await worker.connect()
    try:
        for task_type, handler in handlers.items():
            worker.task(task_type)(handler)
        yield worker
    finally:
        await worker.disconnect()


async def run_polls(
    backend: Backend,
    handlers: dict[str, _Handler],
    *,
    worker_id: str = "matrix-worker",
    polls: int = 1,
    routes: list[str] | None = None,
) -> None:
    """Register *handlers* and run *polls* poll+execute iterations, then stop."""
    async with running_worker(backend, handlers, worker_id=worker_id, routes=routes) as worker:
        for _ in range(polls):
            await worker.run_once()


# ===================================================================
# Type round-trips (the dialect-sensitive core)
# ===================================================================


class TestTypeRoundTrip:

    async def test_payload_round_trip(self, queue: TaskQueue) -> None:
        payload = {
            "nested": {"a": [1, 2, 3], "b": {"c": None}},
            "flag": True,
            "count": 42,
            "ratio": 1.5,
            "text": 'quotes " and\nnewlines\tand unicode ü✓ 中文',
        }
        task_id = await queue.submit("roundtrip", payload)
        task = await queue.get_task(task_id)
        assert task is not None
        assert task.payload == payload

    async def test_result_round_trip(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {"echo": payload, "items": [1, 2], "ok": True}

        task_id = await queue.submit("echo", {"x": 1})
        await run_polls(backend, {"echo": handler})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.result == {"echo": {"x": 1}, "items": [1, 2], "ok": True}

    async def test_timestamps_are_aware_utc_and_ordered(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {}

        before = datetime.now(timezone.utc)
        task_id = await queue.submit("stamped", {})
        await run_polls(backend, {"stamped": handler})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.created_at.tzinfo is not None
        assert task.created_at.utcoffset() == timedelta(0)
        assert before - timedelta(seconds=5) <= task.created_at <= datetime.now(timezone.utc)
        assert task.started_at is not None and task.completed_at is not None
        assert task.created_at <= task.started_at <= task.completed_at

    async def test_depends_on_array_round_trip(self, queue: TaskQueue) -> None:
        first = await queue.submit("first", {})
        second = await queue.submit("second", {}, depends_on=[first])

        task = await queue.get_task(second)
        assert task is not None
        assert task.depends_on == [first]

        empty = await queue.submit("third", {})
        empty_task = await queue.get_task(empty)
        assert empty_task is not None
        assert empty_task.depends_on == []

    async def test_scheduled_for_round_trip(self, queue: TaskQueue) -> None:
        due = datetime.now(timezone.utc) + timedelta(hours=2)
        task_id = await queue.submit("later", {}, scheduled_for=due)

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.scheduled_for is not None
        assert abs((task.scheduled_for - due).total_seconds()) < 1

    async def test_long_error_message_preserved(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("boom 'quoted'\nsecond line " + "x" * 500)

        task_id = await queue.submit("explode", {}, retry_policy=RetryPolicy(max_retries=0))
        await run_polls(backend, {"explode": handler})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.error_message is not None
        assert "boom 'quoted'" in task.error_message
        assert len(task.error_message) > 500


# ===================================================================
# Polling, priority, routing, scheduling
# ===================================================================


class TestPolling:

    async def test_submit_poll_execute_completes(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {"seen": payload["n"]}

        task_id = await queue.submit("work", {"n": 7})
        await run_polls(backend, {"work": handler})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "completed"
        assert task.result == {"seen": 7}
        assert task.worker_id == "matrix-worker"

    async def test_priority_orders_polling(self, queue: TaskQueue) -> None:
        low = await queue.submit("job", {}, priority=-10)
        high = await queue.submit("job", {}, priority=10)
        default = await queue.submit("job", {})

        pending = await queue.list_pending_tasks(limit=10)
        assert [t.task_id for t in pending] == [high, default, low]

    async def test_route_filter_isolates_tasks(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {}

        emails = await queue.submit("job", {}, route="emails")
        reports = await queue.submit("job", {}, route="reports")
        default = await queue.submit("job", {}, route="default")

        # A route-scoped worker only picks up its own route.
        await run_polls(backend, {"job": handler}, worker_id="default-worker", routes=["default"])

        default_task = await queue.get_task(default)
        assert default_task is not None
        assert default_task.status.value == "completed"
        for task_id in (emails, reports):
            task = await queue.get_task(task_id)
            assert task is not None
            assert task.status.value == "pending"

        assert len(await queue.list_pending_tasks(route="emails")) == 1
        assert len(await queue.list_pending_tasks(route="reports")) == 1
        assert await queue.list_pending_tasks(route="default") == []

    async def test_scheduled_task_not_polled_until_due(self, queue: TaskQueue) -> None:
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        await queue.submit("later", {}, scheduled_for=future)
        assert await queue.list_pending_tasks(limit=10) == []

        due = await queue.submit(
            "now", {}, scheduled_for=datetime.now(timezone.utc) - timedelta(seconds=1)
        )
        pending = await queue.list_pending_tasks(limit=10)
        assert [t.task_id for t in pending] == [due]


# ===================================================================
# Exactly-once claiming
# ===================================================================


class TestExactlyOnceClaim:
    """A task must never be handed to two workers.

    ``FOR UPDATE SKIP LOCKED`` alone is not enough — those locks are released
    when the selecting statement ends, so the claim has to move the row to
    ``processing`` in the same statement (or transaction).  With the non-atomic
    implementation every run produced 45-60 executions for 40 tasks on
    PostgreSQL and MySQL with two workers.
    """

    async def test_concurrent_claims_never_overlap(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        queries = QueryBuilder(backend.pool)
        total = 12
        submitted = await queue.submit_many([("work", {"i": index}) for index in range(total)])

        batches = await asyncio.gather(
            *(queries.claim_pending_tasks(limit=10, worker_id=f"w{index}") for index in range(5))
        )

        claimed_ids = [row["task_id"] for batch in batches for row in batch]
        assert len(set(claimed_ids)) == len(claimed_ids), "a task was claimed by two workers"
        for batch in batches:
            for row in batch:
                assert row["status"] == "processing"
                assert str(row["worker_id"]).startswith("w")

        # ``SKIP LOCKED`` lets a claim skip rows another claim is holding (on
        # MySQL a concurrent claim locks the range it scans), so the concurrent
        # calls need not return everything — but every task must be claimable
        # exactly once, and nothing may be claimed twice.
        for index in range(10):
            more = await queries.claim_pending_tasks(limit=10, worker_id=f"drain-{index}")
            if not more:
                break
            claimed_ids.extend(str(row["task_id"]) for row in more)

        assert sorted(claimed_ids) == sorted(submitted)

        # Everything is claimed: a later poll finds nothing.
        assert await queries.claim_pending_tasks(limit=10, worker_id="late") == []
        assert await queue.list_pending_tasks(limit=50) == []

    async def test_released_claim_is_claimable_again(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        queries = QueryBuilder(backend.pool)
        task_id = await queue.submit("work", {})

        first = await queries.claim_pending_tasks(limit=1, worker_id="w1")
        assert [row["task_id"] for row in first] == [task_id]

        assert await queries.release_claim(task_id) is True
        assert await queries.release_claim(task_id) is False  # no claim left to release

        second = await queries.claim_pending_tasks(limit=1, worker_id="w2")
        assert [row["task_id"] for row in second] == [task_id]
        assert second[0]["worker_id"] == "w2"

    async def test_claim_respects_route_and_priority(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        queries = QueryBuilder(backend.pool)
        low = await queue.submit("work", {}, route="emails", priority=-5)
        high = await queue.submit("work", {}, route="emails", priority=5)
        other = await queue.submit("work", {}, route="reports")

        claimed = await queries.claim_pending_tasks(limit=10, worker_id="w1", route="emails")
        assert [row["task_id"] for row in claimed] == [high, low]

        remaining = await queue.list_pending_tasks(limit=10)
        assert [task.task_id for task in remaining] == [other]

    async def test_two_workers_execute_each_task_once(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        """End-to-end regression: concurrent workers, no duplicate execution."""
        total = 12
        seen: list[int] = []

        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            seen.append(int(payload["i"]))
            return {"i": payload["i"]}

        await queue.submit_many([("count", {"i": index}) for index in range(total)])

        async with (
            running_worker(backend, {"count": handler}, worker_id="exactly-once-a") as worker_a,
            running_worker(backend, {"count": handler}, worker_id="exactly-once-b") as worker_b,
        ):
            for _ in range(20):
                await asyncio.gather(worker_a.run_once(), worker_b.run_once())
                if len(seen) >= total:
                    break

        assert sorted(seen) == list(range(total)), f"executions not exactly once: {sorted(seen)}"
        assert await queue.list_pending_tasks(limit=50) == []

    async def test_stale_claim_from_a_dead_worker_is_reclaimed(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        """A worker that dies holding a claim must not strand the task."""
        queries = QueryBuilder(backend.pool)
        await queue.submit("orphan", {})

        claimed = await queries.claim_pending_tasks(limit=1, worker_id="ghost-worker")
        assert len(claimed) == 1
        task_id = str(claimed[0]["task_id"])

        # Too fresh to touch, even though the owner never registered a heartbeat.
        assert await queries.reclaim_stale_tasks(0.05) == 0
        await asyncio.sleep(0.1)
        assert await queries.reclaim_stale_tasks(0.05) == 1

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "pending"
        assert task.worker_id is None

        # And it is claimable again.
        again = await queries.claim_pending_tasks(limit=1, worker_id="w-after-crash")
        assert [row["task_id"] for row in again] == [task_id]

    async def test_live_worker_keeps_its_claim(self, backend: Backend, queue: TaskQueue) -> None:
        """A slow task must not be stolen while its owner keeps heartbeating."""
        queries = QueryBuilder(backend.pool)
        await queue.submit("slow", {})
        await queries.upsert_worker({"worker_id": "slow-worker"})

        claimed = await queries.claim_pending_tasks(limit=1, worker_id="slow-worker")
        assert len(claimed) == 1
        await asyncio.sleep(0.1)
        # The claim is older than the window, but its owner is still alive
        # (heartbeats are what protect a long-running execution).
        await queries.update_worker_heartbeat("slow-worker")
        assert await queries.reclaim_stale_tasks(0.05) == 0

        # Silence the owner past the window and the claim becomes reclaimable.
        await backend.pool.execute(
            "UPDATE conductor_workers SET last_heartbeat = NULL "
            f"WHERE worker_id = {backend.pool.dialect.placeholder(1)}",
            "slow-worker",
        )
        assert await queries.reclaim_stale_tasks(0.05) == 1


# ===================================================================
# Retry + DLQ
# ===================================================================


class TestRetryAndDlq:

    async def test_failure_schedules_retry(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("first attempt fails")

        task_id = await queue.submit(
            "flaky",
            {},
            retry_policy=RetryPolicy(max_retries=2, initial_delay=30.0, max_delay=60.0),
        )
        await run_polls(backend, {"flaky": handler})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "retrying"
        assert task.attempt == 1
        assert task.scheduled_for is not None
        assert task.scheduled_for > datetime.now(timezone.utc)

        retries = await queue._queries.select_retries_for_task(task_id)
        assert len(retries) == 1
        assert retries[0]["attempt"] == 1

    async def test_due_retry_is_reexecuted(self, backend: Backend, queue: TaskQueue) -> None:
        """A scheduled retry must be claimed again once its delay has elapsed."""
        attempts: list[int] = []

        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            attempts.append(1)
            raise RuntimeError("always fails")

        task_id = await queue.submit(
            "flaky",
            {},
            retry_policy=RetryPolicy(max_retries=1, initial_delay=0.01, max_delay=0.02),
        )
        await run_polls(backend, {"flaky": handler})
        assert len(attempts) == 1

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "retrying"
        assert task.attempt == 1

        # Wait out the backoff delay, then poll again: the retry must run.
        await asyncio.sleep(0.05)
        await run_polls(backend, {"flaky": handler})

        assert len(attempts) == 2
        retried = await queue.get_task(task_id)
        assert retried is not None
        # The second execution incremented the attempt counter past max_retries.
        assert retried.attempt == 2
        # Retries exhausted: the task is dead-lettered.
        assert retried.status.value == "failed"
        assert await queue.get_dlq_task(task_id) is not None

    async def test_zero_retries_moves_to_dlq_preserving_metadata(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("terminal")

        parent = await queue.submit("parent", {}, retry_policy=RetryPolicy(max_retries=0))
        # Cancel the dependency: a cancelled dependency counts as satisfied, so
        # the dependent still runs (and fails) instead of being blocked.
        await queue.cancel_task(parent)
        task_id = await queue.submit(
            "doomed",
            {"keep": "me"},
            route="critical",
            priority=42,
            retry_policy=RetryPolicy(max_retries=0),
            depends_on=[parent],
        )
        await run_polls(backend, {"doomed": handler})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "failed"

        dlq_task = await queue.get_dlq_task(task_id)
        assert dlq_task is not None
        assert dlq_task.payload == {"keep": "me"}
        assert dlq_task.route == "critical"
        assert dlq_task.priority == 42
        assert dlq_task.depends_on == [parent]
        assert dlq_task.discarded is False

    async def test_dlq_retry_restores_task(self, backend: Backend, queue: TaskQueue) -> None:
        async def failing(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("nope")

        async def succeeding(payload: dict[str, Any]) -> dict[str, Any]:
            return {"recovered": True}

        task_id = await queue.submit(
            "recoverable", {}, route="retry-route", retry_policy=RetryPolicy(max_retries=0)
        )
        await run_polls(backend, {"recoverable": failing})

        await queue.retry_dlq_task(task_id)

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "pending"
        assert task.attempt == 0
        assert task.worker_id is None
        assert task.route == "retry-route"
        assert await queue.get_dlq_task(task_id) is None

        await run_polls(backend, {"recoverable": succeeding})
        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "completed"

    async def test_dlq_discard_and_count(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("dead")

        task_id = await queue.submit("dead", {}, retry_policy=RetryPolicy(max_retries=0))
        await run_polls(backend, {"dead": handler})

        assert await queue.count_dlq_tasks() == 1
        await queue.discard_dlq_task(task_id, reason="not worth retrying")
        assert await queue.count_dlq_tasks() == 0
        assert await queue.count_dlq_tasks(include_discarded=True) == 1


# ===================================================================
# Dependencies
# ===================================================================


class TestDependencies:

    async def test_met_dependency_releases_task(self, backend: Backend, queue: TaskQueue) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {}

        parent = await queue.submit("parent", {})
        child = await queue.submit("child", {}, depends_on=[parent])

        # The dependency is unmet, so only the parent is pollable.
        pending = await queue.list_pending_tasks(limit=10)
        assert [t.task_id for t in pending] == [parent]

        await run_polls(backend, {"parent": handler, "child": handler}, polls=2)

        child_task = await queue.get_task(child)
        assert child_task is not None
        assert child_task.status.value == "completed"

    async def test_cancelled_dependency_releases_task(self, queue: TaskQueue) -> None:
        parent = await queue.submit("parent", {})
        await queue.submit("child", {}, depends_on=[parent])
        await queue.cancel_task(parent)

        pending = await queue.list_pending_tasks(limit=10)
        assert len(pending) == 1
        assert pending[0].task_type == "child"

    async def test_failed_dependency_blocks_dependent(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("parent failed")

        parent = await queue.submit("parent", {}, retry_policy=RetryPolicy(max_retries=0))
        child = await queue.submit("child", {}, depends_on=[parent])
        grandchild = await queue.submit("grandchild", {}, depends_on=[child])

        await run_polls(backend, {"parent": handler, "child": handler})

        for task_id in (child, grandchild):
            task = await queue.get_task(task_id)
            assert task is not None
            assert task.status.value == "blocked"
            assert task.error_message == f"dependency '{parent}' failed"


# ===================================================================
# Cancellation and maintenance
# ===================================================================


class TestCancellationAndMaintenance:

    async def test_cancel_pending_task(self, queue: TaskQueue) -> None:
        task_id = await queue.submit("cancel-me", {})
        await queue.cancel_task(task_id)

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "cancelled"
        assert task.completed_at is not None

    async def test_cancel_unknown_task_raises(self, queue: TaskQueue) -> None:
        with pytest.raises(TaskError):
            await queue.cancel_task("does-not-exist")

    async def test_delete_completed_tasks_counts_rows(
        self, backend: Backend, queue: TaskQueue
    ) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {}

        for _ in range(3):
            await queue.submit("done", {})
        await run_polls(backend, {"done": handler}, polls=3, worker_id="cleanup-worker")

        deleted = await queue._queries.delete_completed_tasks(
            older_than=datetime.now(timezone.utc) + timedelta(minutes=1)
        )
        assert deleted == 3

    async def test_worker_heartbeat_round_trip(self, queue: TaskQueue) -> None:
        """Worker registration + liveness queries (the "now minus N seconds" path)."""
        queries = queue._queries
        await queries.upsert_worker(
            {
                "worker_id": "heartbeat-worker",
                "status": "idle",
                "hostname": "matrix-host",
                "pid": 1234,
            }
        )
        # A worker whose heartbeat is far in the past must not count as active.
        await queries.upsert_worker(
            {
                "worker_id": "stale-worker",
                "status": "unhealthy",
                "hostname": "matrix-host",
                "pid": 4321,
                "last_heartbeat": datetime.now(timezone.utc) - timedelta(hours=1),
            }
        )

        record = await queries.select_worker("heartbeat-worker")
        assert record is not None
        assert record["hostname"] == "matrix-host"
        assert record["last_heartbeat"] is not None

        active = await queries.select_active_workers(heartbeat_timeout=60.0)
        assert [row["worker_id"] for row in active] == ["heartbeat-worker"]
        assert await queries.count_active_workers(heartbeat_timeout=60.0) == 1
        assert await queries.count_active_workers(heartbeat_timeout=7200.0) == 2

        assert (
            await queries.update_worker_heartbeat(
                "heartbeat-worker", status="processing", uptime_seconds=12.5
            )
            is True
        )
        updated = await queries.select_worker("heartbeat-worker")
        assert updated is not None
        assert updated["status"] == "processing"
        assert updated["uptime_seconds"] == 12.5

        assert await queries.update_worker_heartbeat("no-such-worker") is False


# ===================================================================
# Batch submission
# ===================================================================


class TestBatchSubmission:

    async def test_submit_many_inserts_all(self, queue: TaskQueue) -> None:
        task_ids = await queue.submit_many(
            [("batch", {"i": 0}), ("batch", {"i": 1}), ("batch", {"i": 2})],
            route="bulk",
            priority=3,
        )
        assert len(task_ids) == 3

        for index, task_id in enumerate(task_ids):
            task = await queue.get_task(task_id)
            assert task is not None
            assert task.payload == {"i": index}
            assert task.route == "bulk"
            assert task.priority == 3

    async def test_submit_many_rolls_back_on_failure(
        self, queue: TaskQueue, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failure mid-batch must leave no rows behind (one transaction)."""
        original = type(queue._queries).insert_task
        calls = {"n": 0}

        async def flaky_insert(self: Any, task: dict[str, Any], **kwargs: Any) -> str:
            calls["n"] += 1
            if calls["n"] == 2:
                raise TaskError("simulated failure")
            return await original(self, task, **kwargs)

        monkeypatch.setattr(type(queue._queries), "insert_task", flaky_insert)

        with pytest.raises(TaskError, match="simulated failure"):
            await queue.submit_many([("batch", {}), ("batch", {}), ("batch", {})])

        assert await queue.list_pending_tasks(limit=10) == []


# ===================================================================
# Recurring definitions
# ===================================================================


class TestRecurring:

    async def test_recurring_definition_and_due_claim(self, queue: TaskQueue) -> None:
        recurring_id = await queue.schedule_recurring(
            "cleanup", {"scope": "tmp"}, "*/5 * * * *", route="maintenance", priority=1
        )

        definition = await queue.get_recurring_task(recurring_id)
        assert definition is not None
        assert definition.payload == {"scope": "tmp"}
        assert definition.route == "maintenance"
        assert definition.enabled is True
        assert definition.next_run_at.tzinfo is not None

        # Not due yet: nothing to claim.
        assert await queue._queries.select_due_recurring_tasks(datetime.now(timezone.utc)) == []

        # Claim it as due, then advance the next run.
        now = definition.next_run_at + timedelta(seconds=1)
        claimed = await queue._queries.select_due_recurring_tasks(now)
        assert [row["id"] for row in claimed] == [recurring_id]

        advanced = await queue._queries.update_recurring_run(
            recurring_id,
            last_run_at=now,
            next_run_at=now + timedelta(minutes=5),
        )
        assert advanced is True

        updated = await queue.get_recurring_task(recurring_id)
        assert updated is not None
        assert updated.last_run_at is not None
        assert updated.next_run_at > now

    async def test_pause_and_resume_recurring(self, queue: TaskQueue) -> None:
        recurring_id = await queue.schedule_recurring("cleanup", {}, "0 * * * *")

        await queue.pause_recurring(recurring_id)
        paused = await queue.get_recurring_task(recurring_id)
        assert paused is not None
        assert paused.enabled is False

        await queue.resume_recurring(recurring_id)
        resumed = await queue.get_recurring_task(recurring_id)
        assert resumed is not None
        assert resumed.enabled is True

    async def test_delete_recurring_definition(self, queue: TaskQueue) -> None:
        recurring_id = await queue.schedule_recurring("cleanup", {}, "0 0 * * *")
        await queue.delete_recurring_task(recurring_id)
        assert await queue.get_recurring_task(recurring_id) is None
