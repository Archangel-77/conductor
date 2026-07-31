"""
Comprehensive end-to-end tests for Conductor's full task lifecycle.

This suite consolidates the complete v0.1 workflow scenarios: submit →
poll → execute → complete, retries, dead-letter queue, multiple workers,
graceful shutdown, worker crash recovery, concurrency limiting, and
observability (Prometheus metrics + structured logs).

These tests require a running PostgreSQL instance (see ``docker-compose.yml``)
and are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import RetryPolicy, TaskStatus
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="task_queue")
async def _task_queue_factory() -> Any:
    """Create a TaskQueue connected to the test database."""
    from tests.conftest import TEST_DATABASE_URL, db_available

    if not db_available():
        pytest.skip("Test database not available")

    q = TaskQueue(
        database_url=TEST_DATABASE_URL,
        pool_min_size=1,
        pool_max_size=2,
        pool_timeout=5.0,
        command_timeout=10.0,
    )
    try:
        await q.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    yield q

    await q.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def _cleanup_test_data(task_queue: Any) -> Any:
    """Clean up all data after each test."""
    yield
    if task_queue.is_connected:
        await task_queue.execute_raw("DELETE FROM conductor_retries")
        await task_queue.execute_raw("DELETE FROM conductor_dead_letter")
        await task_queue.execute_raw("DELETE FROM conductor_tasks")
        await task_queue.execute_raw("DELETE FROM conductor_workers")


# ===================================================================
# Helpers
# ===================================================================


def _db_url() -> str:
    """Return the test database URL."""
    from tests.conftest import TEST_DATABASE_URL

    return TEST_DATABASE_URL


async def _wait_for_status(
    task_queue: Any,
    task_id: str,
    status: TaskStatus,
    timeout: float = 10.0,
) -> Any:
    """Poll until a task reaches the given status (or timeout)."""

    async def _poll() -> Any:
        while True:
            task = await task_queue.get_task(task_id)
            if task is not None and task.status == status:
                return task
            await asyncio.sleep(0.05)

    return await asyncio.wait_for(_poll(), timeout=timeout)


async def _wait_for_all_completed(
    task_queue: Any,
    task_ids: list[str],
    timeout: float = 15.0,
) -> None:
    """Wait for every task in *task_ids* to reach ``COMPLETED``."""
    for task_id in task_ids:
        await _wait_for_status(task_queue, task_id, TaskStatus.COMPLETED, timeout=timeout)


def _read_counter_value(
    body: str,
    metric: str,
    label: str,
    label_value: str,
) -> float:
    """Parse the numeric value of a labelled Prometheus counter from text."""
    prefix = f'{metric}{{{label}="{label_value}"}} '
    for line in body.splitlines():
        line = line.strip()
        if line.startswith(prefix):
            try:
                return float(line[len(prefix) :].split()[0])
            except (ValueError, IndexError):
                continue
    return 0.0


# ===================================================================
# Full happy path
# ===================================================================


class TestFullHappyPath:
    """Submit → poll → execute → complete (the core happy path)."""

    async def test_submit_poll_execute_complete(self, task_queue: Any) -> None:
        """Submit a task, run a worker once, and verify completion + result."""
        task_id = await task_queue.submit(
            "e2e_full_greet",
            {"name": "Conductor"},
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-happy-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_greet")
            async def greet(payload: dict[str, Any]) -> dict[str, Any]:
                return {"greeting": f"Hello, {payload['name']}!"}

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"greeting": "Hello, Conductor!"}
        assert task.worker_id == "e2e-full-happy-1"
        assert task.started_at is not None
        assert task.completed_at is not None
        assert task.completed_at >= task.started_at

    async def test_status_transitions_end_to_end(self, task_queue: Any) -> None:
        """Task transitions pending → completed with worker/timestamps set."""
        task_id = await task_queue.submit("e2e_full_transition", {"n": 1})

        pre = await task_queue.get_task(task_id)
        assert pre is not None
        assert pre.status == TaskStatus.PENDING
        assert pre.worker_id is None
        assert pre.started_at is None

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-transition-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_transition")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            await worker.run_once()

        post = await task_queue.get_task(task_id)
        assert post is not None
        assert post.status == TaskStatus.COMPLETED
        assert post.worker_id == "e2e-full-transition-1"
        assert post.started_at is not None
        assert post.completed_at is not None
        assert post.completed_at >= post.started_at


# ===================================================================
# Retry workflow
# ===================================================================


class TestFullRetryWorkflow:
    """Submit → fail → retry → complete, and exhausted retries → DLQ."""

    async def test_fail_then_retry_then_complete(self, task_queue: Any) -> None:
        """Handler fails once, is retried, then succeeds on the next attempt."""
        rp = RetryPolicy(max_retries=2)
        task_id = await task_queue.submit(
            "e2e_full_retry",
            {"fail_until_attempt": 1},
            retry_policy=rp,
        )
        attempt_count = 0

        # Attempt 1: fails and is scheduled for retry.
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-retry-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_retry")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal attempt_count
                attempt_count += 1
                fail_until = payload.get("fail_until_attempt", 0)
                if attempt_count <= fail_until:
                    raise RuntimeError(f"Transient failure #{attempt_count}")
                return {"succeeded_on_attempt": attempt_count}

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RETRYING
        assert task.attempt == 1

        # Advance scheduled_for so the retry is pollable.
        await task_queue.query_builder.update_task_status(
            task_id,
            "pending",
            attempt=1,
            scheduled_for=datetime.now(timezone.utc),
        )

        # Attempt 2: succeeds.
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-retry-2",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker2:

            @worker2.task("e2e_full_retry")
            async def handler2(payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal attempt_count
                attempt_count += 1
                fail_until = payload.get("fail_until_attempt", 0)
                if attempt_count <= fail_until:
                    raise RuntimeError(f"Transient failure #{attempt_count}")
                return {"succeeded_on_attempt": attempt_count}

            await worker2.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"succeeded_on_attempt": 2}

    async def test_exhaust_retries_moved_to_dead_letter(self, task_queue: Any) -> None:
        """Fail all retries → the task is moved to the dead-letter queue."""
        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit(
            "e2e_full_dlq",
            {"should_fail": True},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-dlq-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_dlq")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Always fails")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

        dlq_row = await task_queue.query_builder.select_dlq_task(task_id)
        assert dlq_row is not None
        assert dlq_row["error_message"] == "Always fails"
        assert dlq_row["attempts"] == 1


# ===================================================================
# Multiple workers & concurrency
# ===================================================================


class TestFullMultipleWorkers:
    """Multiple workers sharing a queue, and concurrency limiting."""

    async def test_multiple_workers_share_load(self, task_queue: Any) -> None:
        """Two workers both process tasks from the same shared queue."""
        processed_by: list[str] = []

        async with (
            Worker(
                database_url=_db_url(),
                worker_id="e2e-full-worker-a",
                concurrency=5,
                metrics_enabled=False,
                health_enabled=False,
                pool_min_size=1,
                pool_max_size=2,
                pool_timeout=5.0,
            ) as worker_a,
            Worker(
                database_url=_db_url(),
                worker_id="e2e-full-worker-b",
                concurrency=5,
                metrics_enabled=False,
                health_enabled=False,
                pool_min_size=1,
                pool_max_size=2,
                pool_timeout=5.0,
            ) as worker_b,
        ):

            @worker_a.task("e2e_full_shared")
            async def handler_a(payload: dict[str, Any]) -> dict[str, Any]:
                processed_by.append("a")
                return {"by": "a", "n": payload["n"]}

            @worker_b.task("e2e_full_shared")
            async def handler_b(payload: dict[str, Any]) -> dict[str, Any]:
                processed_by.append("b")
                return {"by": "b", "n": payload["n"]}

            # Alternate which worker polls each task. This is deterministic:
            # ``run_once()`` polls and executes synchronously, and each task is
            # submitted just before its target worker polls, so only that task
            # is pending at that instant.
            task_ids = []
            for i in range(4):
                tid = await task_queue.submit("e2e_full_shared", {"n": i})
                task_ids.append(tid)
                target = worker_a if i % 2 == 0 else worker_b
                await target.run_once()

        assert processed_by == ["a", "b", "a", "b"]
        for task_id in task_ids:
            task = await task_queue.get_task(task_id)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED

    async def test_concurrency_limit_respected(self, task_queue: Any) -> None:
        """A worker never exceeds its configured concurrency limit."""
        in_flight = 0
        max_in_flight = 0

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-concurrency-1",
            concurrency=3,
            poll_interval=0.05,
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_concurrent")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal in_flight, max_in_flight
                in_flight += 1
                max_in_flight = max(max_in_flight, in_flight)
                await asyncio.sleep(0.1)
                in_flight -= 1
                return {"ok": True}

            task_ids = []
            for i in range(6):
                task_ids.append(await task_queue.submit("e2e_full_concurrent", {"n": i}))

            run_task = asyncio.create_task(worker.run())
            try:
                await _wait_for_all_completed(task_queue, task_ids)
            finally:
                await worker.shutdown()
                await run_task

        assert max_in_flight <= 3
        assert max_in_flight >= 2  # proves parallel execution actually occurred


# ===================================================================
# Graceful shutdown
# ===================================================================


class TestFullGracefulShutdown:
    """Shutdown waits for in-flight tasks before exiting."""

    async def test_graceful_shutdown_waits_for_in_flight(self, task_queue: Any) -> None:
        """Shutdown lets already-running handlers finish."""
        completed: list[int] = []
        all_in_flight = asyncio.Event()
        in_flight_count = 0

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-shutdown-1",
            concurrency=3,
            poll_interval=0.05,
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_shutdown")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal in_flight_count
                in_flight_count += 1
                if in_flight_count == 3:
                    all_in_flight.set()
                await asyncio.sleep(0.3)
                completed.append(payload["n"])
                return {"ok": True}

            task_ids = []
            for i in range(3):
                task_ids.append(await task_queue.submit("e2e_full_shutdown", {"n": i}))

            run_task = asyncio.create_task(worker.run())
            try:
                # Wait until all three handlers are running concurrently.
                await asyncio.wait_for(all_in_flight.wait(), timeout=10.0)
                await worker.shutdown()
                await run_task
            finally:
                if not run_task.done():
                    run_task.cancel()
                    await asyncio.gather(run_task, return_exceptions=True)

        assert len(completed) == 3
        for task_id in task_ids:
            task = await task_queue.get_task(task_id)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED


# ===================================================================
# Worker crash recovery
# ===================================================================


class TestFullCrashRecovery:
    """Tasks stuck in PROCESSING after a worker crash, and their recovery."""

    async def test_crashed_worker_leaves_task_processing(self, task_queue: Any) -> None:
        """A worker that dies mid-execution leaves the task PROCESSING.

        v0.1 has no automatic reclaim of stale ``PROCESSING`` tasks, so the
        task stays stuck until an operator intervenes.
        """
        task_id = await task_queue.submit("e2e_full_crash", {"n": 1})

        worker = Worker(
            database_url=_db_url(),
            worker_id="e2e-full-crash-1",
            graceful_shutdown_timeout=0.5,
            poll_interval=0.05,
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )

        @worker.task("e2e_full_crash")
        async def blocking_handler(_payload: dict[str, Any]) -> dict[str, Any]:
            await asyncio.sleep(30)  # never completes → simulates a hung worker
            return {"ok": True}

        run_task = asyncio.create_task(worker.run())
        try:
            # The worker acquires the task and marks it PROCESSING.
            task = await _wait_for_status(task_queue, task_id, TaskStatus.PROCESSING)
            assert task.worker_id == "e2e-full-crash-1"
            assert task.started_at is not None

            # Simulate a crash: cancel the run loop without graceful completion.
            run_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run_task
        finally:
            if not run_task.done():
                run_task.cancel()
                await asyncio.gather(run_task, return_exceptions=True)
            await worker.disconnect()

        # No auto-reclaim: the task remains stuck in PROCESSING.
        stuck = await task_queue.get_task(task_id)
        assert stuck is not None
        assert stuck.status == TaskStatus.PROCESSING
        assert stuck.completed_at is None

    async def test_task_recovered_after_crash_by_new_worker(self, task_queue: Any) -> None:
        """A stuck PROCESSING task can be reset and recovered by a fresh worker."""
        task_id = await task_queue.submit("e2e_full_recover", {"n": 1})

        # Simulate a worker that acquired the task and then crashed.
        await task_queue.query_builder.update_task_status(
            task_id,
            "processing",
            worker_id="crashed-worker-1",
        )

        # A new worker must NOT pick it up while it is PROCESSING.
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-recover-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_full_recover")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"recovered": True}

            await worker.run_once()

        still = await task_queue.get_task(task_id)
        assert still is not None
        assert still.status == TaskStatus.PROCESSING

        # Operator-driven recovery: reset to pending and clear the worker id.
        await task_queue.query_builder.update_task_status(
            task_id,
            "pending",
            attempt=0,
            scheduled_for=datetime.now(timezone.utc),
        )
        await task_queue.query_builder.clear_task_worker_id(task_id)

        # A fresh worker now recovers the task.
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-recover-2",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker2:

            @worker2.task("e2e_full_recover")
            async def handler2(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"recovered": True}

            await worker2.run_once()

        recovered = await task_queue.get_task(task_id)
        assert recovered is not None
        assert recovered.status == TaskStatus.COMPLETED
        assert recovered.result == {"recovered": True}


# ===================================================================
# Observability
# ===================================================================


class TestFullObservability:
    """Prometheus metrics and structured logs for real worker runs."""

    async def test_metrics_endpoint_records_task_events(self, task_queue: Any) -> None:
        """A running worker exports task counters/histograms to /metrics."""
        import aiohttp

        metric = "conductor_tasks_completed_total"
        label_value = "e2e_full_obs"
        base_url = "http://localhost:8766"

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-full-obs-1",
            poll_interval=0.05,
            metrics_port=8766,
            metrics_enabled=True,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task(label_value)
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            run_task = asyncio.create_task(worker.run())

            async def _fetch() -> str:
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{base_url}/metrics") as resp:
                        assert resp.status == 200
                        return await resp.text()

            # Wait for the metrics server to come up before reading the
            # baseline, so we never race the worker's exporter startup.
            deadline = time.monotonic() + 10.0
            while True:
                try:
                    before_body = await _fetch()
                    break
                except (aiohttp.ClientError, OSError):
                    if time.monotonic() >= deadline:
                        raise
                    await asyncio.sleep(0.05)

            try:
                before = _read_counter_value(
                    before_body,
                    metric,
                    "task_type",
                    label_value,
                )

                task_ids = []
                for i in range(3):
                    task_ids.append(await task_queue.submit(label_value, {"n": i}))
                await _wait_for_all_completed(task_queue, task_ids)

                # The counter increment happens just after the DB status update,
                # so poll /metrics until the expected delta is observed.
                poll_deadline = time.monotonic() + 10.0
                after = 0.0
                after_body = ""
                while time.monotonic() < poll_deadline:
                    after_body = await _fetch()
                    after = _read_counter_value(
                        after_body,
                        metric,
                        "task_type",
                        label_value,
                    )
                    if after >= before + 3:
                        break
                    await asyncio.sleep(0.05)

                assert after >= before + 3
                assert "conductor_task_duration_seconds_bucket" in after_body
            finally:
                await worker.shutdown()
                await run_task

    async def test_structured_logs_include_task_context(self, task_queue: Any) -> None:
        """Structured log records carry task_id, task_type, and duration_ms."""
        from conductor.observability.logging import JsonFormatter

        class _ListHandler(logging.Handler):
            """Collect formatted log lines into a list."""

            def __init__(self) -> None:
                super().__init__()
                self.setFormatter(JsonFormatter())
                self.messages: list[str] = []

            def emit(self, record: logging.LogRecord) -> None:
                self.messages.append(self.format(record))

        list_handler = _ListHandler()
        root_logger = logging.getLogger("conductor")
        root_logger.addHandler(list_handler)
        root_logger.setLevel(logging.DEBUG)
        try:
            task_id = await task_queue.submit("e2e_full_log", {"n": 1})

            async with Worker(
                database_url=_db_url(),
                worker_id="e2e-full-log-1",
                pool_min_size=1,
                pool_max_size=2,
                pool_timeout=5.0,
            ) as worker:

                @worker.task("e2e_full_log")
                async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                    return {"logged": True}

                await worker.run_once()
        finally:
            root_logger.removeHandler(list_handler)

        records = [json.loads(m) for m in list_handler.messages]
        assert records, "No structured log records were captured"
        assert any(r.get("task_id") == task_id for r in records)
        assert any(r.get("task_type") == "e2e_full_log" for r in records)
        assert any("duration_ms" in r for r in records)
        assert any("completed" in str(r.get("message", "")) for r in records)
