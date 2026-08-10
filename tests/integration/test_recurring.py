"""
Integration tests for recurring tasks (``schedule_recurring`` + ``RecurringScheduler``).

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import RetryPolicy, generate_task_id, utc_now
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError, TaskError
from conductor.recurring.scheduler import RecurringScheduler

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="queue")
async def _queue_factory() -> Any:
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


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="scheduler")
async def _scheduler_factory() -> Any:
    """Create a connected RecurringScheduler."""
    from tests.conftest import TEST_DATABASE_URL, db_available

    if not db_available():
        pytest.skip("Test database not available")

    s = RecurringScheduler(
        database_url=TEST_DATABASE_URL,
        pool_min_size=1,
        pool_max_size=2,
        pool_timeout=5.0,
        command_timeout=10.0,
    )
    try:
        await s.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    yield s

    await s.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def _cleanup(queue: Any) -> Any:
    """Clean up all conductor tables after each test."""
    yield
    if queue.is_connected:
        await queue.execute_raw("DELETE FROM conductor_retries")
        await queue.execute_raw("DELETE FROM conductor_dead_letter")
        await queue.execute_raw("DELETE FROM conductor_tasks")
        await queue.execute_raw("DELETE FROM conductor_workers")
        await queue.execute_raw("DELETE FROM conductor_recurring_tasks")


async def _force_due(queue: Any, recurring_id: str) -> None:
    """Move a definition's next_run_at into the past so it is immediately due."""
    await queue.query_builder.update_recurring_run(
        recurring_id,
        last_run_at=utc_now(),
        next_run_at=utc_now() - timedelta(seconds=60),
    )


# ===================================================================
# TaskQueue recurring-management API
# ===================================================================


class TestScheduleRecurring:

    async def test_schedule_and_get(self, queue: Any) -> None:
        """schedule_recurring persists a definition retrievable by ID."""
        rid = await queue.schedule_recurring(
            "cleanup",
            {"keep": 30},
            "0 2 * * *",
            route="batch",
            priority=5,
            retry_policy=RetryPolicy(max_retries=2),
        )

        rt = await queue.get_recurring_task(rid)
        assert rt is not None
        assert rt.task_type == "cleanup"
        assert rt.payload == {"keep": 30}
        assert rt.cron_expression == "0 2 * * *"
        assert rt.route == "batch"
        assert rt.priority == 5
        assert rt.retry_policy.max_retries == 2
        assert rt.enabled is True
        # next_run_at should be set to a future cron fire
        assert rt.next_run_at > utc_now() - timedelta(minutes=1)

    async def test_duplicate_id_raises(self, queue: Any) -> None:
        """Re-scheduling with the same ID should raise TaskError."""
        rid = generate_task_id()
        await queue.schedule_recurring("cleanup", {}, "0 2 * * *", recurring_id=rid)
        with pytest.raises(TaskError, match="already exists"):
            await queue.schedule_recurring("cleanup", {}, "0 2 * * *", recurring_id=rid)

    async def test_invalid_cron_raises(self, queue: Any) -> None:
        """An invalid cron expression should raise ValueError."""
        with pytest.raises(ValueError, match="cron"):
            await queue.schedule_recurring("cleanup", {}, "not a cron")

    async def test_list_recurring_tasks(self, queue: Any) -> None:
        """list_recurring_tasks returns all definitions."""
        await queue.schedule_recurring("a", {}, "0 2 * * *")
        await queue.schedule_recurring("b", {}, "0 3 * * *")

        tasks = await queue.list_recurring_tasks()
        types = {t.task_type for t in tasks}
        assert "a" in types
        assert "b" in types

    async def test_pause_resume(self, queue: Any) -> None:
        """pause_recurring/resume_recurring toggle the enabled flag."""
        rid = await queue.schedule_recurring("cleanup", {}, "0 2 * * *")

        await queue.pause_recurring(rid)
        rt = await queue.get_recurring_task(rid)
        assert rt is not None and rt.enabled is False

        await queue.resume_recurring(rid)
        rt = await queue.get_recurring_task(rid)
        assert rt is not None and rt.enabled is True

    async def test_pause_missing_raises(self, queue: Any) -> None:
        """Pausing a nonexistent definition should raise TaskError."""
        with pytest.raises(TaskError, match="not found"):
            await queue.pause_recurring("missing-rec")

    async def test_delete(self, queue: Any) -> None:
        """delete_recurring_task removes the definition."""
        rid = await queue.schedule_recurring("cleanup", {}, "0 2 * * *")
        await queue.delete_recurring_task(rid)
        assert await queue.get_recurring_task(rid) is None


# ===================================================================
# RecurringScheduler sweep
# ===================================================================


class TestRecurringScheduler:

    async def test_run_once_fires_and_advances(self, queue: Any, scheduler: Any) -> None:
        """A due definition fires one task instance and advances next_run_at."""
        rid = await queue.schedule_recurring("cleanup", {"n": 1}, "* * * * *")
        await _force_due(queue, rid)

        await scheduler.run_once()

        rt = await queue.get_recurring_task(rid)
        assert rt is not None
        assert rt.next_run_at > utc_now() - timedelta(minutes=1)
        assert rt.last_run_at is not None

        pending = await queue.list_pending_tasks()
        assert any(t.task_type == "cleanup" and t.payload == {"n": 1} for t in pending)

    async def test_disabled_not_fired(self, queue: Any, scheduler: Any) -> None:
        """A disabled definition is not fired even when due."""
        rid = await queue.schedule_recurring("cleanup", {}, "* * * * *")
        await queue.pause_recurring(rid)
        await _force_due(queue, rid)

        await scheduler.run_once()

        pending = await queue.list_pending_tasks()
        assert all(t.task_type != "cleanup" for t in pending)

    async def test_route_priority_propagated(self, queue: Any, scheduler: Any) -> None:
        """Fired instances inherit route/priority/retry_policy from the definition."""
        rid = await queue.schedule_recurring(
            "cleanup",
            {},
            "* * * * *",
            route="critical",
            priority=50,
            retry_policy=RetryPolicy(max_retries=0),
        )
        await _force_due(queue, rid)

        await scheduler.run_once()

        pending = await queue.list_pending_tasks(route="critical")
        fired = [t for t in pending if t.task_type == "cleanup"]
        assert len(fired) == 1
        assert fired[0].priority == 50
        assert fired[0].route == "critical"
        assert fired[0].max_retries == 0

    async def test_no_refire_until_next_cron(self, queue: Any, scheduler: Any) -> None:
        """A second sweep before the next cron time does not fire again."""
        rid = await queue.schedule_recurring("cleanup", {}, "* * * * *")
        await _force_due(queue, rid)

        await scheduler.run_once()
        await scheduler.run_once()

        pending = await queue.list_pending_tasks()
        assert len([t for t in pending if t.task_type == "cleanup"]) == 1

    async def test_scheduler_status(self, scheduler: Any) -> None:
        """get_status() reports scheduler health."""
        status = scheduler.get_status()
        assert status["connected"] is True
        assert status["tasks_fired_total"] >= 0
        assert status["scheduler_interval"] > 0
        assert "last_sweep_at" in status

    async def test_worker_enable_scheduler_fires(self, queue: Any) -> None:
        """A Worker with enable_scheduler=True runs the recurring scheduler."""
        from tests.conftest import TEST_DATABASE_URL

        handled: list[int] = []

        rid = await queue.schedule_recurring("cleanup", {"w": 1}, "* * * * *")
        await _force_due(queue, rid)

        async with Worker(
            database_url=TEST_DATABASE_URL,
            worker_id="sched-worker-test",
            enable_scheduler=True,
            concurrency=5,
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
            graceful_shutdown_timeout=5.0,
        ) as worker:

            @worker.task("cleanup")
            async def handler(payload: dict[str, Any]) -> dict[str, Any]:
                handled.append(payload["w"])
                return {"ok": True}

            run_task = asyncio.create_task(worker.run())
            await asyncio.sleep(2.5)
            await worker.shutdown()
            await run_task

        # The embedded scheduler should have fired the recurring task
        # and the worker should have executed it.
        assert handled == [1]
