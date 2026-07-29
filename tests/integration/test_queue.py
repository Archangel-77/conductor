"""
Integration tests for TaskQueue.

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import (
    BackoffStrategyType,
    RetryPolicy,
    TaskStatus,
)
from conductor.core.queue import TaskQueue
from conductor.exceptions import ConductorConnectionError, TaskError

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================

@pytest_asyncio.fixture(scope="module", loop_scope="module", name="queue")
async def _queue_factory() -> Any:
    """Create a TaskQueue connected to the test database.

    Skips the test if the database is not running.
    Waits for module scope teardown.
    """
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
async def _cleanup(queue: Any) -> Any:
    """Clean up all task data after each test."""
    yield
    if queue.is_connected:
        await queue.execute_raw("DELETE FROM conductor_retries")
        await queue.execute_raw("DELETE FROM conductor_dead_letter")
        await queue.execute_raw("DELETE FROM conductor_tasks")
        await queue.execute_raw("DELETE FROM conductor_workers")


# ===================================================================
# Integration tests
# ===================================================================

class TestTaskQueueIntegration:

    async def test_submit_and_get_task(self, queue: Any) -> None:
        """Submit a task, then retrieve it by ID."""
        task_id = await queue.submit(
            "integration_test",
            {"message": "hello world"},
        )
        assert task_id is not None
        assert isinstance(task_id, str)

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.task_id == task_id
        assert task.task_type == "integration_test"
        assert task.payload == {"message": "hello world"}
        assert task.status == TaskStatus.PENDING

    async def test_submit_and_list_pending(self, queue: Any) -> None:
        """Submitted tasks should appear in pending list."""
        await queue.submit("test-type-1", {"n": 1})
        await queue.submit("test-type-2", {"n": 2})

        pending = await queue.list_pending_tasks()
        assert len(pending) >= 2

        types = {t.task_type for t in pending}
        assert "test-type-1" in types
        assert "test-type-2" in types

    async def test_submit_many(self, queue: Any) -> None:
        """submit_many should insert all tasks and return their IDs."""
        tasks = [
            ("batch-a", {"seq": 1}),
            ("batch-b", {"seq": 2}),
            ("batch-c", {"seq": 3}),
        ]
        ids = await queue.submit_many(tasks)
        assert len(ids) == 3
        assert len(set(ids)) == 3  # all unique

        # Verify each exists
        for tid in ids:
            task = await queue.get_task(tid)
            assert task is not None

    async def test_task_status_transition(self, queue: Any) -> None:
        """Update task status and verify."""
        task_id = await queue.submit("status_test", {"x": 1})

        # Move to processing
        qb = queue.query_builder
        await qb.update_task_status(
            task_id, "processing", worker_id="test-worker",
        )

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.PROCESSING
        assert task.worker_id == "test-worker"

        # Move to completed
        await qb.update_task_status(
            task_id, "completed", result={"output": "done"},
        )

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"output": "done"}

    async def test_list_completed_tasks(self, queue: Any) -> None:
        """Completed tasks should appear in the completed list."""
        task_id = await queue.submit("complete_me", {"go": True})

        await queue.query_builder.update_task_status(task_id, "completed", result={"ok": True})

        completed = await queue.list_completed_tasks()
        ids = [t.task_id for t in completed]
        assert task_id in ids

    async def test_list_failed_tasks(self, queue: Any) -> None:
        """Failed tasks should appear in the failed list."""
        task_id = await queue.submit("fail_me", {"bad": True})

        await queue.query_builder.update_task_status(
            task_id, "failed", error_message="Intentional failure",
        )

        failed = await queue.list_failed_tasks()
        ids = [t.task_id for t in failed]
        assert task_id in ids
        task = await queue.get_task(task_id)
        assert task is not None
        assert task.error_message == "Intentional failure"

    async def test_count_tasks_by_status(self, queue: Any) -> None:
        """Count tasks should return accurate numbers."""
        await queue.submit("count-a", {})
        await queue.submit("count-b", {})

        count = await queue.count_tasks_by_status("pending")
        assert count >= 2

    async def test_submit_with_custom_retry_policy(self, queue: Any) -> None:
        """Custom retry policy should be persisted and retrievable."""
        rp = RetryPolicy(
            max_retries=7,
            backoff_strategy=BackoffStrategyType.LINEAR,
            initial_delay=2.0,
            max_delay=60.0,
        )
        task_id = await queue.submit(
            "retry_test", {"attempts": 7}, retry_policy=rp,
        )

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.retry_policy.max_retries == 7
        assert task.retry_policy.backoff_strategy == BackoffStrategyType.LINEAR
        assert task.retry_policy.initial_delay == 2.0

    async def test_submit_scheduled_task(self, queue: Any) -> None:
        """Scheduled tasks should not appear in pending list immediately."""
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)  # noqa: UP017
        task_id = await queue.submit(
            "future_task", {"scheduled": True}, scheduled_for=future,
        )

        # Should not appear in pending (scheduled_for is in the far future)
        pending = await queue.list_pending_tasks()
        pending_ids = [t.task_id for t in pending]
        assert task_id not in pending_ids

        # But should still be retrievable
        task = await queue.get_task(task_id)
        assert task is not None
        assert task.task_type == "future_task"

    async def test_submit_with_custom_task_id(self, queue: Any) -> None:
        """Explicit task_id should be honored."""
        task_id = await queue.submit(
            "custom_id_test", {}, task_id="my-explicit-id",
        )
        assert task_id == "my-explicit-id"

        task = await queue.get_task("my-explicit-id")
        assert task is not None

    async def test_duplicate_task_id_raises(self, queue: Any) -> None:
        """Submitting with an existing task_id should raise."""
        task_id = await queue.submit("dup_test", {})
        with pytest.raises(TaskError):
            await queue.submit(
                "dup_test_again", {}, task_id=task_id,
            )

    async def test_submit_with_route_and_priority(self, queue: Any) -> None:
        """Route and priority should be persisted."""
        task_id = await queue.submit(
            "routed_task", {},
            route="high_priority", priority=50,
        )

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.route == "high_priority"
        assert task.priority == 50

    async def test_task_id_uniqueness(self, queue: Any) -> None:
        """Auto-generated task IDs should be unique."""
        ids = set()
        for i in range(20):
            tid = await queue.submit(f"unique_{i}", {"i": i})
            ids.add(tid)
        assert len(ids) == 20
