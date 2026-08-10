"""
End-to-end tests for the recurring-task workflow.

schedule_recurring → RecurringScheduler fires a task instance → Worker
executes it → the definition's next_run_at advances to the next cron fire.

Requires a running PostgreSQL instance (see ``docker-compose.yml``).
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import TaskStatus, utc_now
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError
from conductor.recurring.scheduler import RecurringScheduler

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
        await task_queue.execute_raw("DELETE FROM conductor_recurring_tasks")


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


class TestRecurringWorkflow:

    async def test_scheduler_fires_and_worker_executes(self, task_queue: Any) -> None:
        """schedule → fire → execute → advance is the full recurring lifecycle."""
        rid = await task_queue.schedule_recurring(
            "recurring_echo",
            {"msg": "hello"},
            "* * * * *",
            route="default",
        )

        # Force the definition to be immediately due.
        await task_queue.query_builder.update_recurring_run(
            rid,
            last_run_at=utc_now(),
            next_run_at=utc_now() - timedelta(seconds=5),
        )

        # Scheduler fires the instance.
        scheduler = RecurringScheduler(database_url=_db_url(), pool_min_size=1, pool_max_size=2)
        await scheduler.connect()
        try:
            await scheduler.run_once()
        finally:
            await scheduler.disconnect()

        # A pending instance should now exist.
        pending = await task_queue.list_pending_tasks()
        fired = [t for t in pending if t.task_type == "recurring_echo"]
        assert len(fired) == 1
        assert fired[0].payload == {"msg": "hello"}

        # Worker executes it.
        async with Worker(
            database_url=_db_url(),
            worker_id="recurring-e2e-worker",
            routes=["default"],
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("recurring_echo")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                return {"echo": payload["msg"]}

            await worker.run_once()

        # The fired task completed.
        task = await task_queue.get_task(fired[0].task_id)
        assert task is not None
        assert task.status == TaskStatus.COMPLETED
        assert task.result == {"echo": "hello"}

        # The definition advanced to a future next_run_at.
        rt = await task_queue.get_recurring_task(rid)
        assert rt is not None
        assert rt.next_run_at > utc_now() - timedelta(minutes=1)
        assert rt.last_run_at is not None
