"""
End-to-end tests for Conductor's submit → poll → execute → complete workflow.

These tests validate the full task lifecycle across TaskQueue and Worker,
requiring a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.

Workflows tested:
- Submit → Poll → Execute → Complete (happy path)
- Submit → Fail → Retry → Complete (retry workflow)
- Submit → Fail all retries → DLQ (exhausted retries)
- Task result storage and verification
- Task status transitions end-to-end
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import TaskStatus
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
# Helper: DB URL
# ===================================================================


def _db_url() -> str:
    """Return the test database URL."""
    from tests.conftest import TEST_DATABASE_URL

    return TEST_DATABASE_URL


# ===================================================================
# E2E Tests
# ===================================================================


class TestSubmitAndExecute:

    async def test_happy_path_submit_poll_execute_complete(
        self,
        task_queue: Any,
    ) -> None:
        """Full happy path: submit task → worker polls → executes → completes."""
        task_id = await task_queue.submit(
            "e2e_greet",
            {"name": "Conductor"},
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-worker-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_greet")
            async def greet_handler(payload: dict[str, Any]) -> dict[str, Any]:
                return {"greeting": f"Hello, {payload['name']}!"}

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result is not None
        assert task.result["greeting"] == "Hello, Conductor!"
        assert task.worker_id == "e2e-worker-1"
        assert task.started_at is not None
        assert task.completed_at is not None
        assert task.completed_at >= task.started_at

    async def test_task_status_transitions_end_to_end(
        self,
        task_queue: Any,
    ) -> None:
        """Verify each status transition through the full lifecycle."""
        task_id = await task_queue.submit(
            "e2e_transitions",
            {"step": "start"},
        )

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.PENDING
        assert task.worker_id is None
        assert task.started_at is None

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-transitions",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_transitions")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"done": True}

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.worker_id is not None
        assert task.started_at is not None
        assert task.completed_at is not None

    async def test_result_storage_and_retrieval(
        self,
        task_queue: Any,
    ) -> None:
        """Handler result should be stored in the database and retrievable."""
        task_id = await task_queue.submit(
            "e2e_result",
            {"items": [1, 2, 3]},
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-result-store",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_result")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                return {
                    "sum": sum(payload["items"]),
                    "count": len(payload["items"]),
                    "processed": True,
                }

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.result == {
            "sum": 6,
            "count": 3,
            "processed": True,
        }

    async def test_multiple_tasks_sequential(
        self,
        task_queue: Any,
    ) -> None:
        """Multiple tasks submitted sequentially should all complete."""
        task_ids = []
        for i in range(5):
            tid = await task_queue.submit(
                "e2e_sequential",
                {"seq": i},
            )
            task_ids.append(tid)

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-sequential",
            poll_interval=0.1,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_sequential")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                return {"seq": payload["seq"], "status": "done"}

            for _ in range(5):
                await worker.run_once()

        for tid in task_ids:
            task = await task_queue.get_task(tid)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED

    async def test_submit_and_execute_with_custom_retry(
        self,
        task_queue: Any,
    ) -> None:
        """Task with custom retry policy should respect it."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=2)
        task_id = await task_queue.submit(
            "e2e_retry_custom",
            {"will_fail": True},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-retry-custom",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_retry_custom")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Simulated failure")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.RETRYING
        assert task.attempt == 1

    async def test_task_with_result_null(
        self,
        task_queue: Any,
    ) -> None:
        """Handler returning None should store empty result."""
        task_id = await task_queue.submit(
            "e2e_null_result",
            {"data": "test"},
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-null-result",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_null_result")
            async def handler(_payload: dict[str, Any]) -> None:
                return None

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {}


class TestRetryWorkflowE2E:

    async def test_retry_then_succeed(
        self,
        task_queue: Any,
    ) -> None:
        """Task fails first time, retries, and succeeds on second attempt."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=2)
        task_id = await task_queue.submit(
            "e2e_retry_succeed",
            {"fail_until_attempt": 1},
            retry_policy=rp,
        )

        attempt_count = 0

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-retry-succeed",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_retry_succeed")
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

        # Manually move scheduled_for to now so it can be polled again
        await task_queue.query_builder.update_task_status(
            task_id,
            "pending",
            attempt=1,
            scheduled_for=datetime.now(timezone.utc),  # noqa: UP017
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-retry-succeed-2",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker2:

            @worker2.task("e2e_retry_succeed")
            async def handler2(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"succeeded": True}

            await worker2.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED

    async def test_exhausted_retries_move_to_dlq(
        self,
        task_queue: Any,
    ) -> None:
        """Task that exhausts all retries should end up in DLQ."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit(
            "e2e_exhaust",
            {"always_fail": True},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-exhaust",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_exhaust")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Always fails")

            await worker.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

        dlq_task = await task_queue.query_builder.select_dlq_task(task_id)
        assert dlq_task is not None
        assert dlq_task["task_id"] == task_id
        assert dlq_task["error_message"] is not None
        assert "Always fails" in dlq_task["error_message"]

    async def test_dlq_task_can_be_retried(
        self,
        task_queue: Any,
    ) -> None:
        """A task in the DLQ should be retriable (via DLQ API)."""
        from conductor.core.models import RetryPolicy

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit(
            "e2e_dlq_retry",
            {"data": "dlq-test"},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-dlq-retry",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_dlq_retry")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Direct to DLQ")

            await worker.run_once()

        dlq_task = await task_queue.query_builder.select_dlq_task(task_id)
        assert dlq_task is not None

        # Simulate DLQ retry: delete from DLQ, reset task to pending
        await task_queue.query_builder.delete_dlq_task(task_id)
        await task_queue.query_builder.update_task_status(
            task_id,
            "pending",
            attempt=0,
            worker_id=None,
            scheduled_for=datetime.now(timezone.utc),  # noqa: UP017
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-dlq-retry-2",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker2:

            @worker2.task("e2e_dlq_retry")
            async def good_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"recovered": True}

            await worker2.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"recovered": True}


class TestMultipleWorkersE2E:

    async def test_two_workers_process_concurrent_tasks(
        self,
        task_queue: Any,
    ) -> None:
        """Two workers should both process tasks without conflict."""
        results_a: list[int] = []
        results_b: list[int] = []

        task_ids = []

        async with (
            Worker(
                database_url=_db_url(),
                worker_id="e2e-worker-a",
                concurrency=5,
                pool_min_size=1,
                pool_max_size=2,
                pool_timeout=5.0,
            ) as wa,
            Worker(
                database_url=_db_url(),
                worker_id="e2e-worker-b",
                concurrency=5,
                pool_min_size=1,
                pool_max_size=2,
                pool_timeout=5.0,
            ) as wb,
        ):

            @wa.task("e2e_concurrent")
            async def handler_a(payload: dict[str, Any]) -> dict[str, Any]:
                results_a.append(payload["n"])
                return {"processed_by": "a"}

            @wb.task("e2e_concurrent")
            async def handler_b(payload: dict[str, Any]) -> dict[str, Any]:
                results_b.append(payload["n"])
                return {"processed_by": "b"}

            for i in range(6):
                tid = await task_queue.submit("e2e_concurrent", {"n": i})
                task_ids.append(tid)
                # Alternate which worker polls so both deterministically
                # participate (each run_once picks up the single pending task).
                if i % 2 == 0:
                    await wa.run_once()
                else:
                    await wb.run_once()

        # Both workers should have participated
        assert len(results_a) >= 1
        assert len(results_b) >= 1
        # All 6 tasks should be completed in the DB
        for tid in task_ids:
            task = await task_queue.get_task(tid)
            assert task is not None
            assert task.status == TaskStatus.COMPLETED
