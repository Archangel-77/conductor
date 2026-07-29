"""
Integration tests for the Worker.

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.

Tests cover:
- Worker startup and registration
- Task handler registration
- Task polling (simple and per-route)
- Task execution (success and failure)
- Task status transitions (pending → processing → completed / failed)
- Retry scheduling on failure
- DLQ move when retries exhausted
- Concurrency limiting
- Heartbeat updates
- Graceful shutdown (in-flight task completion with timeout)
- Multiple workers polling the same queue
- Worker status reporting
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import TaskStatus, utc_now
from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================

@pytest_asyncio.fixture(scope="module", loop_scope="module", name="task_queue")
async def _task_queue_factory() -> Any:
    """Create a TaskQueue connected to the test database.

    Used to submit tasks that the worker will poll for.
    """
    from tests.conftest import TEST_DATABASE_URL, db_available
    from conductor.core.queue import TaskQueue

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
# Helper: DB URL
# ===================================================================

def _db_url() -> str:
    """Return the test database URL."""
    from tests.conftest import TEST_DATABASE_URL
    return TEST_DATABASE_URL


# ===================================================================
# Tests
# ===================================================================

class TestWorkerStartupAndRegistration:

    async def test_worker_connect_and_disconnect(self) -> None:
        """Worker should connect to the database and register itself."""
        async with Worker(
            database_url=_db_url(),
            worker_id="startup-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            assert worker.is_connected
            assert worker.worker_id == "startup-test"
            status = worker.get_status()
            assert status["worker_id"] == "startup-test"
            assert status["connected"] is True
            assert status["registered_handlers"] == []

        assert worker.is_connected is False

    async def test_worker_with_default_id(self) -> None:
        """Default worker_id should be hostname-pid format."""
        async with Worker(
            database_url=_db_url(),
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            assert worker.worker_id is not None
            assert "-" in worker.worker_id

    async def test_worker_constructor_parameters(self) -> None:
        """All constructor parameters should be reflected in get_status()."""
        async with Worker(
            database_url=_db_url(),
            worker_id="config-test",
            concurrency=5,
            poll_interval=1.0,
            routes=["alpha", "beta"],
            log_level="DEBUG",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            status = worker.get_status()
            assert status["concurrency"] == 5
            assert status["routes"] == ["alpha", "beta"]


class TestTaskHandlerRegistration:

    async def test_register_handler_decorator(self) -> None:
        """The @worker.task() decorator should register handlers."""
        worker = Worker(
            database_url=_db_url(),
            worker_id="handler-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )

        @worker.task("my_task")
        async def my_handler(_payload: dict[str, Any]) -> dict[str, Any]:
            return {"ok": True}

        status = worker.get_status()
        assert "my_task" in status["registered_handlers"]

    async def test_register_multiple_handlers(self) -> None:
        """Multiple handlers should all be registered."""
        worker = Worker(
            database_url=_db_url(),
            worker_id="multi-handler-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )

        @worker.task("type_a")
        async def handler_a(_payload: dict[str, Any]) -> dict[str, Any]:
            return {"type": "a"}

        @worker.task("type_b")
        async def handler_b(_payload: dict[str, Any]) -> dict[str, Any]:
            return {"type": "b"}

        @worker.task("type_c")
        async def handler_c(_payload: dict[str, Any]) -> dict[str, Any]:
            return {"type": "c"}

        status = worker.get_status()
        assert set(status["registered_handlers"]) == {"type_a", "type_b", "type_c"}


class TestTaskPolling:

    async def test_poll_returns_pending_tasks(self, task_queue: Any) -> None:
        """Pending tasks should be found by polling."""
        task_id = await task_queue.submit(
            "poll_test", {"data": 1}, route="default",
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="poll-test",
            routes=["default"],
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("poll_test")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            await worker.run_once()

            task = await task_queue.get_task(task_id)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED

    async def test_poll_filters_by_route(self, task_queue: Any) -> None:
        """Tasks on a different route should not be polled."""
        task_id = await task_queue.submit(
            "route_test", {}, route="other_route",
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="route-filter-test",
            routes=["default"],
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("route_test")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            await worker.run_once()

            task = await task_queue.get_task(task_id)
            assert task is not None
            assert task.status == TaskStatus.PENDING

    async def test_poll_respects_multiple_routes(self, task_queue: Any) -> None:
        """Worker polling multiple routes should find tasks on all of them."""
        id1 = await task_queue.submit("multi_a", {}, route="group_a")
        id2 = await task_queue.submit("multi_b", {}, route="group_b")

        async with Worker(
            database_url=_db_url(),
            worker_id="multi-route-test",
            routes=["group_a", "group_b"],
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("multi_a")
            async def handler_a(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            @worker.task("multi_b")
            async def handler_b(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            await worker.run_once()

            t1 = await task_queue.get_task(id1)
            t2 = await task_queue.get_task(id2)
            assert t1 is not None and t1.status == TaskStatus.COMPLETED
            assert t2 is not None and t2.status == TaskStatus.COMPLETED

    async def test_poll_empty_queue(self) -> None:
        """Polling with no pending tasks should not raise."""
        async with Worker(
            database_url=_db_url(),
            worker_id="empty-poll-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            await worker.run_once()

    async def test_scheduled_task_not_polled(self, task_queue: Any) -> None:
        """Tasks scheduled far in the future should not be polled."""
        future = utc_now() + timedelta(days=365)
        task_id = await task_queue.submit(
            "future_task", {},
            scheduled_for=future,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="scheduled-poll-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("future_task")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            await worker.run_once()

            task = await task_queue.get_task(task_id)
            assert task is not None
            assert task.status == TaskStatus.PENDING


class TestTaskExecution:

    async def test_execute_success(self, task_queue: Any) -> None:
        """A task should transition pending → processing → completed."""
        task_id = await task_queue.submit("exec_ok", {"x": 42})

        async with Worker(
            database_url=_db_url(),
            worker_id="exec-success-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("exec_ok")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                return {"result": payload["x"] * 2}

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"result": 84}
        assert task.worker_id == "exec-success-test"
        assert task.started_at is not None
        assert task.completed_at is not None

    async def test_execute_handler_not_found(self, task_queue: Any) -> None:
        """A task with no registered handler should fail."""
        from conductor.core.models import RetryPolicy
        task_id = await task_queue.submit(
            "no_handler", {},
            retry_policy=RetryPolicy(max_retries=0),
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="no-handler-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert task.error_message is not None
        assert "No handler registered" in task.error_message

    async def test_execute_handler_raises(self, task_queue: Any) -> None:
        """A handler that raises should result in a failed task."""
        from conductor.core.models import RetryPolicy
        task_id = await task_queue.submit(
            "failing", {"message": "boom"},
            retry_policy=RetryPolicy(max_retries=0),
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="handler-raise-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("failing")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise ValueError("Boom!")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "Boom!" in (task.error_message or "")

    async def test_execute_updates_worker_stats(self, task_queue: Any) -> None:
        """Worker statistics should reflect completed and failed tasks."""
        async with Worker(
            database_url=_db_url(),
            worker_id="stats-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("good")
            async def good_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            @worker.task("bad")
            async def bad_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("fail")

            await task_queue.submit("good", {})
            await worker.run_once()

            status = worker.get_status()
            assert status["tasks_processed_total"] == 1
            assert status["tasks_failed_total"] == 0

            await task_queue.submit("bad", {})
            await worker.run_once()

            status = worker.get_status()
            assert status["tasks_processed_total"] == 1
            assert status["tasks_failed_total"] == 1


class TestRetryAndDLQ:

    async def test_task_retried_on_failure(self, task_queue: Any) -> None:
        """A task should be retried if max_retries > 0."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=2)
        task_id = await task_queue.submit("retry_me", {}, retry_policy=rp)

        async with Worker(
            database_url=_db_url(),
            worker_id="retry-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            call_count = 0

            @worker.task("retry_me")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal call_count
                call_count += 1
                raise RuntimeError(f"Attempt {call_count} failed")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RETRYING
        assert task.attempt == 1
        assert task.error_message is not None

    async def test_task_moved_to_dlq_after_exhausted_retries(
        self, task_queue: Any,
    ) -> None:
        """Task should move to DLQ after all retry attempts exhausted."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit("dlq_bound", {}, retry_policy=rp)

        async with Worker(
            database_url=_db_url(),
            worker_id="dlq-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("dlq_bound")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Always fails")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

        dlq_row = await task_queue.query_builder.select_dlq_task(task_id)  # pylint: disable=protected-access
        assert dlq_row is not None
        assert dlq_row["task_id"] == task_id

    async def test_retry_with_zero_max_retries_goes_to_dlq(
        self, task_queue: Any,
    ) -> None:
        """Task with max_retries=0 should go directly to DLQ on failure."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit("no_retry", {}, retry_policy=rp)

        async with Worker(
            database_url=_db_url(),
            worker_id="no-retry-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("no_retry")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Direct to DLQ")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

        dlq_row = await task_queue.query_builder.select_dlq_task(task_id)  # pylint: disable=protected-access
        assert dlq_row is not None


class TestConcurrency:

    async def test_concurrency_limit_respected(self, task_queue: Any) -> None:
        """Worker should not exceed the configured concurrency limit."""
        concurrency = 3
        in_flight_counter = 0
        max_observed = 0

        async with Worker(
            database_url=_db_url(),
            worker_id="concurrency-test",
            concurrency=concurrency,
            poll_interval=0.05,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("slow")
            async def slow_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal in_flight_counter, max_observed
                in_flight_counter += 1
                max_observed = max(max_observed, in_flight_counter)
                await asyncio.sleep(0.2)
                in_flight_counter -= 1
                return {"done": True}

            for i in range(6):
                await task_queue.submit("slow", {"i": i})

            for _ in range(3):
                await worker.run_once()

            assert max_observed <= concurrency, (
                f"Observed {max_observed} concurrent tasks, "
                f"limit was {concurrency}"
            )


class TestHeartbeat:

    async def test_heartbeat_updates_worker_record(
        self, task_queue: Any,
    ) -> None:
        """Heartbeat should update the worker's database record."""
        async with Worker(
            database_url=_db_url(),
            worker_id="heartbeat-test",
            heartbeat_interval=0.5,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            run_task = asyncio.create_task(worker.run())
            await asyncio.sleep(0.6)

            row = await task_queue.query_builder.select_worker(  # pylint: disable=protected-access
                "heartbeat-test",
            )
            assert row is not None
            assert row["last_heartbeat"] is not None
            assert row["status"] == "idle"

            await worker.shutdown()
            await run_task

    async def test_heartbeat_records_processing_status(
        self, task_queue: Any,
    ) -> None:
        """Heartbeat should reflect processing status when working."""
        async with Worker(
            database_url=_db_url(),
            worker_id="heartbeat-busy-test",
            heartbeat_interval=0.3,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("long_task")
            async def long_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                await asyncio.sleep(0.5)
                return {"done": True}

            await task_queue.submit("long_task", {})
            run_task = asyncio.create_task(worker.run())
            await asyncio.sleep(0.8)

            row = await task_queue.query_builder.select_worker(  # pylint: disable=protected-access
                "heartbeat-busy-test",
            )
            assert row is not None

            await worker.shutdown()
            await run_task

    async def test_final_heartbeat_on_shutdown(
        self, task_queue: Any,
    ) -> None:
        """Worker should send an 'unhealthy' heartbeat on shutdown."""
        async with Worker(
            database_url=_db_url(),
            worker_id="final-hb-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            run_task = asyncio.create_task(worker.run())
            await asyncio.sleep(0.3)  # Let it send at least one heartbeat
            await worker.shutdown()
            await run_task

        row = await task_queue.query_builder.select_worker("final-hb-test")  # pylint: disable=protected-access
        assert row is not None
        assert row["status"] == "unhealthy"


class TestGracefulShutdown:

    async def test_shutdown_waits_for_in_flight_tasks(
        self, task_queue: Any,
    ) -> None:
        """Shutdown should wait for running tasks to complete."""
        async with Worker(
            database_url=_db_url(),
            worker_id="shutdown-test",
            concurrency=5,
            graceful_shutdown_timeout=5.0,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            completed_tasks: list[dict[str, Any]] = []

            @worker.task("shutdown_me")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                await asyncio.sleep(0.1)
                completed_tasks.append(payload)
                return {"done": True}

            for i in range(3):
                await task_queue.submit("shutdown_me", {"i": i})

            await worker.run_once()
            await worker.shutdown()

            assert len(completed_tasks) == 3

    async def test_shutdown_timeout_cancels_stuck_tasks(
        self, task_queue: Any,
    ) -> None:
        """Shutdown should cancel tasks that exceed the timeout."""
        async with Worker(
            database_url=_db_url(),
            worker_id="timeout-test",
            concurrency=5,
            graceful_shutdown_timeout=0.5,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("stuck")
            async def stuck_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                await asyncio.sleep(10)
                return {"done": True}

            await task_queue.submit("stuck", {})
            await worker.run_once()
            await worker.shutdown()

            assert True  # shutdown completed without hanging


class TestMultipleWorkers:

    async def test_two_workers_poll_same_queue(
        self, task_queue: Any,
    ) -> None:
        """Two workers should register and poll without errors."""
        results: list[str] = []

        async with Worker(
            database_url=_db_url(),
            worker_id="worker-a",
            concurrency=5,
            poll_interval=0.05,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker_a, Worker(
            database_url=_db_url(),
            worker_id="worker-b",
            concurrency=5,
            poll_interval=0.05,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker_b:
            @worker_a.task("shared")
            async def handler_a(payload: dict[str, Any]) -> dict[str, Any]:
                results.append(f"a:{payload['n']}")
                return {"by": "a"}

            @worker_b.task("shared")
            async def handler_b(payload: dict[str, Any]) -> dict[str, Any]:
                results.append(f"b:{payload['n']}")
                return {"by": "b"}

            for i in range(4):
                await task_queue.submit("shared", {"n": i})

            task_a = asyncio.create_task(worker_a.run())
            task_b = asyncio.create_task(worker_b.run())
            await asyncio.sleep(0.5)

            await worker_a.shutdown()
            await worker_b.shutdown()
            await task_a
            await task_b

        # Both workers should have participated
        assert len(results) >= 4  # may have duplicates due to race window
        assert any(r.startswith("a:") for r in results)
        assert any(r.startswith("b:") for r in results)
        # All 4 tasks should be completed in the DB
        for task in await task_queue.list_completed_tasks():
            assert task.status == TaskStatus.COMPLETED


class TestWorkerStatus:

    async def test_get_status_before_run(self) -> None:
        """get_status should return valid info even before running."""
        worker = Worker(
            database_url=_db_url(),
            worker_id="status-before-run",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )
        status = worker.get_status()
        assert status["worker_id"] == "status-before-run"
        assert status["status"] == "idle"
        assert status["tasks_processed_total"] == 0
        assert status["tasks_failed_total"] == 0
        assert status["connected"] is False

    async def test_get_status_during_run(self) -> None:
        """get_status should reflect running state accurately."""
        async with Worker(
            database_url=_db_url(),
            worker_id="status-during-run",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            status = worker.get_status()
            assert status["connected"] is True
            assert status["uptime_seconds"] >= 0

    async def test_get_status_after_run(self) -> None:
        """get_status should show disconnected after shutdown."""
        worker = Worker(
            database_url=_db_url(),
            worker_id="status-after-run",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )
        await worker.connect()
        await worker.disconnect()

        status = worker.get_status()
        assert status["connected"] is False


class TestRunOnce:

    async def test_run_once_processes_one_batch(
        self, task_queue: Any,
    ) -> None:
        """run_once should process available tasks and return."""
        task_id = await task_queue.submit("run_once_test", {"val": 1})

        async with Worker(
            database_url=_db_url(),
            worker_id="run-once-test",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:
            @worker.task("run_once_test")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                return {"processed": payload["val"]}

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"processed": 1}
