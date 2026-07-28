"""
Unit tests for TaskQueue (conductor/core/queue.py).

Uses mocking to avoid requiring a live PostgreSQL database.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from conductor.core.models import (
    RetryPolicy,
    BackoffStrategyType,
    Task,
    TaskStatus,
)
from conductor.core.queue import TaskQueue, _task_to_db_dict
from conductor.exceptions import TaskError


# ===================================================================
# Fixtures
# ===================================================================

@pytest.fixture
def mock_pool() -> MagicMock:
    """Create a mock DatabasePool."""
    pool = MagicMock()
    pool.is_connected = True
    pool.connect = AsyncMock()
    pool.disconnect = AsyncMock()
    pool.fetchrow = AsyncMock()
    pool.fetch = AsyncMock()
    pool.fetchval = AsyncMock()
    pool.execute = AsyncMock()

    # Mock connection that supports conn.transaction() as async context manager
    mock_transaction_cm = MagicMock()
    mock_transaction_cm.__aenter__ = AsyncMock()
    mock_transaction_cm.__aexit__ = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.transaction.return_value = mock_transaction_cm

    # Mock pool.acquire() as async context manager returning mock_conn
    acquire_cm = MagicMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    acquire_cm.__aexit__ = AsyncMock()
    pool.acquire.return_value = acquire_cm

    return pool


@pytest.fixture
def mock_queries() -> MagicMock:
    """Create a mock QueryBuilder with all needed methods."""
    q = MagicMock()
    q.insert_task = AsyncMock()
    q.select_task = AsyncMock()
    q.select_pending_tasks = AsyncMock()
    q.select_tasks_by_status = AsyncMock()
    q.count_tasks_by_status = AsyncMock()
    return q


@pytest.fixture
def queue(mock_pool: Any, mock_queries: Any) -> TaskQueue:
    """Create a TaskQueue with mocked internals."""
    q = TaskQueue(database_url="postgresql://mock@localhost/db")
    # Replace internal components with mocks
    q._pool = mock_pool
    q._queries = mock_queries
    q._connected = True
    return q


# ===================================================================
# Construction & lifecycle
# ===================================================================

class TestTaskQueueConstruction:

    def test_defaults(self) -> None:
        """TaskQueue should store config values with defaults."""
        q = TaskQueue(database_url="postgresql://u:p@localhost/db")
        assert q._database_url == "postgresql://u:p@localhost/db"
        assert q._task_timeout == 300.0
        assert q._max_task_age == 86400
        assert q.is_connected is False

    def test_custom_values(self) -> None:
        q = TaskQueue(
            database_url="pg://localhost/db",
            task_timeout=120.0,
            max_task_age=3600,
            log_level="DEBUG",
            pool_min_size=1,
            pool_max_size=5,
            pool_timeout=10.0,
            command_timeout=30.0,
        )
        assert q._task_timeout == 120.0
        assert q._max_task_age == 3600

    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, mock_pool: Any) -> None:
        """connect() should create pool and schema."""
        with patch(
            "conductor.core.queue.SchemaManager"
        ) as mock_sm_cls:
            mock_sm = MagicMock()
            mock_sm.ensure_schema = AsyncMock()
            mock_sm_cls.return_value = mock_sm

            q = TaskQueue(database_url="postgresql://mock@localhost/db")
            q._pool = mock_pool
            await q.connect()

            mock_pool.connect.assert_awaited_once()
            mock_sm.ensure_schema.assert_awaited_once()
            assert q.is_connected is True

            await q.disconnect()
            mock_pool.disconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_pool: Any) -> None:
        """async with should connect and disconnect."""
        with patch(
            "conductor.core.queue.SchemaManager"
        ) as mock_sm_cls:
            mock_sm = MagicMock()
            mock_sm.ensure_schema = AsyncMock()
            mock_sm_cls.return_value = mock_sm

            q = TaskQueue(database_url="postgresql://mock@localhost/db")
            q._pool = mock_pool
            async with q as queue:
                assert queue.is_connected is True
                mock_pool.connect.assert_awaited_once()

            mock_pool.disconnect.assert_awaited_once()


# ===================================================================
# submit()
# ===================================================================

class TestSubmit:

    @pytest.mark.asyncio
    async def test_submit_basic(self, queue: Any, mock_queries: Any) -> None:
        """Basic task submission should return a task ID."""
        mock_queries.insert_task.return_value = "abc-123"

        task_id = await queue.submit("email", {"to": "user@example.com"})

        assert task_id == "abc-123"
        mock_queries.insert_task.assert_awaited_once()

        # Verify the dict passed to insert_task
        call_kwargs = mock_queries.insert_task.call_args[0][0]
        assert call_kwargs["task_type"] == "email"
        assert call_kwargs["payload"] == {"to": "user@example.com"}
        assert call_kwargs["status"] == "pending"
        assert call_kwargs["max_retries"] == 3

    @pytest.mark.asyncio
    async def test_submit_with_retry_policy(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """Custom retry policy should be reflected in the insert dict."""
        mock_queries.insert_task.return_value = "tid-1"
        rp = RetryPolicy(max_retries=5, backoff_strategy=BackoffStrategyType.FIXED)

        await queue.submit("test", {"k": "v"}, retry_policy=rp)

        call_kwargs = mock_queries.insert_task.call_args[0][0]
        assert call_kwargs["max_retries"] == 5
        assert call_kwargs["retry_policy"]["backoff_strategy"] == "fixed"

    @pytest.mark.asyncio
    async def test_submit_with_scheduled_for(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """Scheduled-for should be passed through."""
        mock_queries.insert_task.return_value = "tid-sched"
        sched = datetime(2026, 12, 25, 10, 0, 0, tzinfo=timezone.utc)

        await queue.submit("test", {"k": "v"}, scheduled_for=sched)

        call_kwargs = mock_queries.insert_task.call_args[0][0]
        assert call_kwargs["scheduled_for"] == sched

    @pytest.mark.asyncio
    async def test_submit_with_custom_task_id(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """Explicit task_id should be honored."""
        mock_queries.insert_task.return_value = "my-custom-id"

        task_id = await queue.submit("test", {}, task_id="my-custom-id")

        assert task_id == "my-custom-id"
        call_kwargs = mock_queries.insert_task.call_args[0][0]
        assert call_kwargs["task_id"] == "my-custom-id"

    @pytest.mark.asyncio
    async def test_submit_empty_task_type(self, queue: Any) -> None:
        """Empty task_type should raise ValueError."""
        with pytest.raises(ValueError, match="task_type"):
            await queue.submit("", {"k": "v"})

    @pytest.mark.asyncio
    async def test_submit_invalid_payload(self, queue: Any) -> None:
        """Non-dict payload should raise ValueError."""
        with pytest.raises(ValueError, match="payload"):
            await queue.submit("test", "not-a-dict")

    @pytest.mark.asyncio
    async def test_submit_with_route_and_priority(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """Route and priority should be passed to the query."""
        mock_queries.insert_task.return_value = "tid-rp"

        await queue.submit("test", {}, route="critical", priority=50)

        call_kwargs = mock_queries.insert_task.call_args[0][0]
        assert call_kwargs["route"] == "critical"
        assert call_kwargs["priority"] == 50

    @pytest.mark.asyncio
    async def test_submit_when_not_connected(self) -> None:
        """Submitting without connection should raise TaskError."""
        q = TaskQueue(database_url="pg://localhost/db")
        with pytest.raises(TaskError, match="not connected"):
            await q.submit("test", {})


# ===================================================================
# submit_many()
# ===================================================================

class TestSubmitMany:

    @pytest.mark.asyncio
    async def test_submit_many(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """submit_many should insert all tasks."""
        mock_queries.insert_task.side_effect = ["id-1", "id-2", "id-3"]

        tasks = [
            ("email", {"to": "a@b.com"}),
            ("sms", {"phone": "123"}),
            ("report", {"type": "daily"}),
        ]
        ids = await queue.submit_many(tasks)

        assert ids == ["id-1", "id-2", "id-3"]
        assert mock_queries.insert_task.await_count == 3

    @pytest.mark.asyncio
    async def test_submit_many_empty_type(
        self, queue: Any
    ) -> None:
        with pytest.raises(ValueError, match="task_type"):
            await queue.submit_many([("", {})])


# ===================================================================
# Task queries
# ===================================================================

class TestGetTask:

    @pytest.mark.asyncio
    async def test_get_task_found(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """get_task should return a Task when found."""
        mock_queries.select_task.return_value = {
            "task_id": "tid-1",
            "task_type": "email",
            "payload": {"to": "user@example.com"},
            "status": "pending",
            "priority": 0,
            "route": "default",
            "attempt": 0,
            "max_retries": 3,
            "retry_policy": {},
            "scheduled_for": None,
            "worker_id": None,
            "result": None,
            "error_message": None,
            "created_at": datetime.now(timezone.utc),
            "started_at": None,
            "completed_at": None,
        }

        task = await queue.get_task("tid-1")
        assert task is not None
        assert task.task_id == "tid-1"
        assert task.task_type == "email"
        assert task.status == TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_get_task_not_found(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """get_task should return None when not found."""
        mock_queries.select_task.return_value = None

        task = await queue.get_task("nonexistent")
        assert task is None

    @pytest.mark.asyncio
    async def test_get_task_not_connected(self) -> None:
        q = TaskQueue(database_url="pg://localhost/db")
        with pytest.raises(TaskError, match="not connected"):
            await q.get_task("tid-1")


class TestListPendingTasks:

    @pytest.mark.asyncio
    async def test_list_pending(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """list_pending_tasks should return Task objects."""
        mock_queries.select_pending_tasks.return_value = [
            {
                "task_id": "t1", "task_type": "email",
                "payload": {}, "status": "pending",
                "priority": 0, "route": "default", "attempt": 0,
                "max_retries": 3, "retry_policy": {},
                "scheduled_for": None, "worker_id": None,
                "result": None, "error_message": None,
                "created_at": datetime.now(timezone.utc),
                "started_at": None, "completed_at": None,
            },
            {
                "task_id": "t2", "task_type": "sms",
                "payload": {}, "status": "pending",
                "priority": 5, "route": "default", "attempt": 0,
                "max_retries": 3, "retry_policy": {},
                "scheduled_for": None, "worker_id": None,
                "result": None, "error_message": None,
                "created_at": datetime.now(timezone.utc),
                "started_at": None, "completed_at": None,
            },
        ]

        tasks = await queue.list_pending_tasks(limit=5)
        assert len(tasks) == 2
        assert all(isinstance(t, Task) for t in tasks)
        assert tasks[0].task_id == "t1"
        assert tasks[1].task_type == "sms"
        mock_queries.select_pending_tasks.assert_awaited_with(
            limit=5, offset=0,
        )

    @pytest.mark.asyncio
    async def test_list_pending_empty(
        self, queue: Any, mock_queries: Any
    ) -> None:
        """list_pending_tasks should return empty list when no tasks."""
        mock_queries.select_pending_tasks.return_value = []
        tasks = await queue.list_pending_tasks()
        assert tasks == []


class TestListCompletedTasks:

    @pytest.mark.asyncio
    async def test_list_completed(
        self, queue: Any, mock_queries: Any
    ) -> None:
        mock_queries.select_tasks_by_status.return_value = [
            {
                "task_id": "t1", "task_type": "email",
                "payload": {"result": "ok"}, "status": "completed",
                "priority": 0, "route": "default", "attempt": 0,
                "max_retries": 3, "retry_policy": {},
                "scheduled_for": None, "worker_id": "w1",
                "result": {"output": "done"}, "error_message": None,
                "created_at": datetime.now(timezone.utc),
                "started_at": datetime.now(timezone.utc),
                "completed_at": datetime.now(timezone.utc),
            },
        ]

        tasks = await queue.list_completed_tasks()
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.COMPLETED
        mock_queries.select_tasks_by_status.assert_awaited_with(
            "completed", limit=10, offset=0,
        )


class TestListFailedTasks:

    @pytest.mark.asyncio
    async def test_list_failed(
        self, queue: Any, mock_queries: Any
    ) -> None:
        mock_queries.select_tasks_by_status.return_value = [
            {
                "task_id": "t1", "task_type": "email",
                "payload": {}, "status": "failed",
                "priority": 0, "route": "default", "attempt": 3,
                "max_retries": 3, "retry_policy": {},
                "scheduled_for": None, "worker_id": "w1",
                "result": None,
                "error_message": "Connection timeout",
                "created_at": datetime.now(timezone.utc),
                "started_at": datetime.now(timezone.utc),
                "completed_at": datetime.now(timezone.utc),
            },
        ]

        tasks = await queue.list_failed_tasks(limit=5, offset=2)
        assert len(tasks) == 1
        assert tasks[0].status == TaskStatus.FAILED
        assert tasks[0].error_message == "Connection timeout"
        mock_queries.select_tasks_by_status.assert_awaited_with(
            "failed", limit=5, offset=2,
        )


class TestCountTasksByStatus:

    @pytest.mark.asyncio
    async def test_count(self, queue: Any, mock_queries: Any) -> None:
        mock_queries.count_tasks_by_status.return_value = 7

        count = await queue.count_tasks_by_status("pending")
        assert count == 7
        mock_queries.count_tasks_by_status.assert_awaited_with("pending")


# ===================================================================
# _task_to_db_dict helper
# ===================================================================

class TestTaskToDbDict:

    def test_converts_task_to_dict(self) -> None:
        """_task_to_db_dict should produce a dict with datetime objects."""
        now = datetime.now(timezone.utc)
        task = Task(
            task_id="tid-1",
            task_type="email",
            payload={"key": "value"},
            created_at=now,
        )
        result = _task_to_db_dict(task)

        assert result["task_id"] == "tid-1"
        assert result["task_type"] == "email"
        assert result["payload"] == {"key": "value"}
        assert result["status"] == "pending"
        assert result["created_at"] == now  # datetime, not string
        assert result["retry_policy"] == task.retry_policy.to_dict()

    def test_round_trip(self) -> None:
        """A Task -> _task_to_db_dict -> Task.from_dict should round-trip."""
        from conductor.core.queue import _task_to_db_dict

        original = Task(
            task_id="tid-rt",
            task_type="roundtrip",
            payload={"x": 1},
            priority=10,
            route="critical",
            retry_policy=RetryPolicy(max_retries=7),
            scheduled_for=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        db_dict = _task_to_db_dict(original)
        restored = Task.from_dict(db_dict)

        assert restored.task_id == original.task_id
        assert restored.task_type == original.task_type
        assert restored.payload == original.payload
        assert restored.priority == original.priority
        assert restored.route == original.route
        assert restored.retry_policy == original.retry_policy
        assert restored.scheduled_for == original.scheduled_for
