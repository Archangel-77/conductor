"""
Integration tests for the Dead Letter Queue.

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import RetryPolicy, generate_task_id, utc_now
from conductor.dlq.dead_letter_queue import DeadLetterQueue
from conductor.exceptions import ConductorConnectionError, TaskError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="dlq")
async def _dlq_factory() -> Any:
    """Create a DeadLetterQueue connected to the test database.

    Skips if the database is not running.
    """
    from tests.conftest import TEST_DATABASE_URL, db_available

    if not db_available():
        pytest.skip("Test database not available")

    q = DeadLetterQueue(
        database_url=TEST_DATABASE_URL,
        pool_min_size=1,
        pool_max_size=2,
        pool_timeout=5.0,
    )
    try:
        await q.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    yield q

    await q.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def auto_cleanup(dlq: Any) -> Any:
    """Clean all conductor tables after each test."""
    yield
    if dlq.is_connected:
        await dlq._pool.execute("DELETE FROM conductor_retries")
        await dlq._pool.execute("DELETE FROM conductor_dead_letter")
        await dlq._pool.execute("DELETE FROM conductor_tasks")
        await dlq._pool.execute("DELETE FROM conductor_workers")


# ===================================================================
# Helpers
# ===================================================================


async def _insert_failed_task(
    dlq: Any,
    task_id: str,
    task_type: str = "test_dlq",
    payload: dict[str, Any] | None = None,
    error_message: str = "test error",
    attempts: int = 1,
    max_retries: int = 0,
) -> None:
    """Insert a failed task into both conductor_tasks and conductor_dead_letter."""
    now = utc_now()
    rp = RetryPolicy(max_retries=max_retries)
    await dlq._query.insert_task(
        {
            "task_id": task_id,
            "task_type": task_type,
            "payload": payload or {},
            "status": "failed",
            "priority": 0,
            "route": "default",
            "attempt": attempts,
            "max_retries": max_retries,
            "retry_policy": rp.to_dict(),
            "scheduled_for": None,
            "worker_id": None,
            "result": None,
            "error_message": error_message,
            "created_at": now,
            "started_at": now,
            "completed_at": now,
        }
    )
    await dlq._query.insert_dlq_task(
        {
            "task_id": task_id,
            "task_type": task_type,
            "payload": payload or {},
            "error_message": error_message,
            "attempts": attempts,
            "retry_policy": rp.to_dict(),
            "moved_at": now,
        }
    )


# ===================================================================
# DeadLetterQueue integration tests
# ===================================================================


class TestDeadLetterQueue:
    """Integration tests for the ``DeadLetterQueue`` class."""

    async def test_list_empty(self, dlq: Any) -> None:
        """An empty DLQ returns an empty list."""
        tasks = await dlq.list_tasks()
        assert tasks == []

    async def test_list_excludes_discarded_by_default(self, dlq: Any) -> None:
        """``list_tasks()`` excludes discarded tasks unless asked."""
        task_id = generate_task_id()
        await _insert_failed_task(dlq, task_id)
        await dlq._query.discard_dlq_task(task_id, reason="test discard")

        # Default: excluded
        tasks = await dlq.list_tasks()
        assert tasks == []

        # With include_discarded=True: included
        tasks = await dlq.list_tasks(include_discarded=True)
        assert len(tasks) == 1
        assert tasks[0].task_id == task_id
        assert tasks[0].discarded is True

    async def test_get_task_found(self, dlq: Any) -> None:
        """``get_task()`` returns the task if it exists."""
        task_id = generate_task_id()
        await _insert_failed_task(
            dlq,
            task_id,
            payload={"key": "value"},
            error_message="failed after 2 attempts",
            attempts=2,
        )

        result = await dlq.get_task(task_id)
        assert result is not None
        assert result.task_id == task_id
        assert result.task_type == "test_dlq"
        assert result.attempts == 2
        assert "failed after 2 attempts" in (result.error_message or "")

    async def test_get_task_not_found(self, dlq: Any) -> None:
        """``get_task()`` returns ``None`` for missing tasks."""
        result = await dlq.get_task("nonexistent-task-id")
        assert result is None

    async def test_retry_task(self, dlq: Any) -> None:
        """``retry_task()`` removes from DLQ and resets the task to pending."""
        task_id = generate_task_id()
        await _insert_failed_task(
            dlq,
            task_id,
            payload={"data": 123},
            error_message="transient error",
            attempts=2,
            max_retries=2,
        )

        # Set a worker_id so we can verify it gets cleared
        await dlq._pool.execute(
            "UPDATE conductor_tasks SET worker_id = 'worker-1' WHERE task_id = $1",
            task_id,
        )

        # Verify it's in the DLQ
        dlq_entry = await dlq.get_task(task_id)
        assert dlq_entry is not None

        # Retry it
        result = await dlq.retry_task(task_id)
        assert result == task_id

        # Verify it's removed from the DLQ
        dlq_entry = await dlq.get_task(task_id)
        assert dlq_entry is None

        # Verify the task is back to pending with attempt=0 and worker cleared
        task_row = await dlq._query.select_task(task_id)
        assert task_row is not None
        assert task_row["status"] == "pending"
        assert task_row["attempt"] == 0
        assert task_row["worker_id"] is None, "Worker ID should be cleared"
        assert task_row["error_message"] is None

    async def test_retry_task_not_in_dlq(self, dlq: Any) -> None:
        """``retry_task()`` raises ``TaskError`` if not found in DLQ."""
        with pytest.raises(TaskError, match="not found in the dead-letter queue"):
            await dlq.retry_task("nonexistent-task")

    async def test_discard_task(self, dlq: Any) -> None:
        """``discard_task()`` soft-deletes the DLQ entry."""
        task_id = generate_task_id()
        await _insert_failed_task(
            dlq,
            task_id,
            error_message="discard me",
        )

        await dlq.discard_task(task_id, reason="Manually reviewed")

        # Task should still be findable with include_discarded=True
        dlq_entry = await dlq.get_task(task_id)
        assert dlq_entry is not None
        assert dlq_entry.discarded is True
        assert dlq_entry.discard_reason == "Manually reviewed"

    async def test_discard_task_not_in_dlq(self, dlq: Any) -> None:
        """``discard_task()`` raises ``TaskError`` if not found."""
        with pytest.raises(TaskError, match="not found in the dead-letter queue"):
            await dlq.discard_task("nonexistent-task", reason="test")

    async def test_count(self, dlq: Any) -> None:
        """``count()`` returns the correct number of DLQ tasks."""
        assert await dlq.count() == 0

        task_ids = []
        for i in range(3):
            tid = generate_task_id()
            task_ids.append(tid)
            await _insert_failed_task(
                dlq,
                tid,
                task_type=f"test_count_{i}",
                error_message=f"error_{i}",
            )

        assert await dlq.count() == 3

        # Discard one and verify count excludes it
        await dlq.discard_task(task_ids[0], reason="test")
        assert await dlq.count() == 2
        assert await dlq.count(include_discarded=True) == 3
