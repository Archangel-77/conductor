"""
End-to-end tests for retry and dead-letter queue workflows.

These tests validate the full retry lifecycle across TaskQueue, Worker,
and DeadLetterQueue, requiring a running PostgreSQL instance.

Workflows tested:
- Submit → Fail → Retry → Complete (retry then succeed)
- Submit → Fail → Exhaust retries → DLQ (exhausted retries)
- Submit → Fail → Exhaust → DLQ → Retry via API → Complete
- Submit → Fail → DLQ → Discard via API
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

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


# ===================================================================
# Retry workflow E2E tests
# ===================================================================


class TestRetryWorkflowE2E:
    """End-to-end tests for retry and DLQ workflows."""

    async def test_fail_twice_then_succeed(
        self,
        task_queue: Any,
    ) -> None:
        """Handler fails twice, succeeds on the third attempt.

        Uses ``run_once()`` per attempt to simulate time passing between
        retries, manually advancing ``scheduled_for`` to allow polling.
        """
        rp = RetryPolicy(max_retries=3)
        task_id = await task_queue.submit(
            "e2e_retry_multi",
            {"fail_until_attempt": 2},
            retry_policy=rp,
        )

        attempt_count = 0

        # First run: fails (attempt 1)
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-retry-multi-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_retry_multi")
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

        # Advance scheduled_for so the retry is pollable
        await task_queue.query_builder.update_task_status(
            task_id,
            "pending",
            attempt=1,
            scheduled_for=datetime.now(timezone.utc),
        )

        # Second run: fails again (attempt 2)
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-retry-multi-2",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker2:

            @worker2.task("e2e_retry_multi")
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
        assert task.status == TaskStatus.RETRYING
        assert task.attempt == 2

        # Advance scheduled_for again
        await task_queue.query_builder.update_task_status(
            task_id,
            "pending",
            attempt=2,
            scheduled_for=datetime.now(timezone.utc),
        )

        # Third run: succeeds (attempt 3)
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-retry-multi-3",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker3:

            @worker3.task("e2e_retry_multi")
            async def handler3(payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal attempt_count
                attempt_count += 1
                fail_until = payload.get("fail_until_attempt", 0)
                if attempt_count <= fail_until:
                    raise RuntimeError(f"Transient failure #{attempt_count}")
                return {"succeeded_on_attempt": attempt_count}

            await worker3.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        # The attempt counter reflects the number of failures (2), not
        # total executions. The result confirms it succeeded on the 3rd run.
        assert task.result == {"succeeded_on_attempt": 3}

    async def test_exhausted_retries_moved_to_dlq(
        self,
        task_queue: Any,
    ) -> None:
        """Task with max_retries=0 fails and goes directly to DLQ."""
        from conductor.dlq.dead_letter_queue import DeadLetterQueue

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit(
            "e2e_exhaust_dlq",
            {"data": "exhaust-me"},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-exhaust-dlq",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_exhaust_dlq")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Always fails")

            await worker.run_once()

        # Verify task status is failed
        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED

        # Verify task is in the DLQ via the DeadLetterQueue API
        dlq = DeadLetterQueue(
            database_url=_db_url(),
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )
        try:
            await dlq.connect()
            dlq_task = await dlq.get_task(task_id)
            assert dlq_task is not None
            assert dlq_task.task_id == task_id
            assert dlq_task.error_message is not None
            assert "Always fails" in dlq_task.error_message
            assert dlq_task.attempts == 1  # failed after 1 attempt
        finally:
            await dlq.disconnect()

    async def test_dlq_retry_via_api(
        self,
        task_queue: Any,
    ) -> None:
        """Task in DLQ can be retried via ``DeadLetterQueue.retry_task()``
        and then successfully processed by a worker."""
        from conductor.dlq.dead_letter_queue import DeadLetterQueue

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit(
            "e2e_dlq_api_retry",
            {"data": "dlq-api-retry"},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-dlq-api-retry-1",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_dlq_api_retry")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Direct to DLQ")

            await worker.run_once()

        # Verify it's in the DLQ
        dlq = DeadLetterQueue(
            database_url=_db_url(),
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )
        try:
            await dlq.connect()
            dlq_task = await dlq.get_task(task_id)
            assert dlq_task is not None

            # Retry via the DLQ API
            await dlq.retry_task(task_id)

            # Verify it's removed from DLQ
            dlq_task_after = await dlq.get_task(task_id)
            assert dlq_task_after is None
        finally:
            await dlq.disconnect()

        # Verify the task is pending again
        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.PENDING
        assert task.attempt == 0

        # Worker should now be able to process it
        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-dlq-api-retry-2",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker2:

            @worker2.task("e2e_dlq_api_retry")
            async def good_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"recovered": True}

            await worker2.run_once()

        task = await task_queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"recovered": True}

    async def test_dlq_discard_via_api(
        self,
        task_queue: Any,
    ) -> None:
        """Task in DLQ can be discarded via ``DeadLetterQueue.discard_task()``."""
        from conductor.dlq.dead_letter_queue import DeadLetterQueue

        rp = RetryPolicy(max_retries=0)
        task_id = await task_queue.submit(
            "e2e_dlq_discard",
            {"data": "discard-me"},
            retry_policy=rp,
        )

        async with Worker(
            database_url=_db_url(),
            worker_id="e2e-dlq-discard",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("e2e_dlq_discard")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                raise RuntimeError("Direct to DLQ")

            await worker.run_once()

        dlq = DeadLetterQueue(
            database_url=_db_url(),
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        )
        try:
            await dlq.connect()

            # Verify it's in the DLQ
            dlq_task = await dlq.get_task(task_id)
            assert dlq_task is not None
            assert dlq_task.discarded is False

            # Discard it
            await dlq.discard_task(task_id, reason="Test discard via API")

            # Verify it's marked as discarded
            dlq_task = await dlq.get_task(task_id)
            assert dlq_task is not None
            assert dlq_task.discarded is True
            assert dlq_task.discard_reason == "Test discard via API"

            # Default list_tasks() should exclude it
            all_tasks = await dlq.list_tasks()
            assert all(task.task_id != task_id for task in all_tasks)
        finally:
            await dlq.disconnect()
