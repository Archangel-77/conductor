"""
Task queue implementation for Conductor.

Provides the public ``TaskQueue`` class for submitting, listing,
and managing tasks in the PostgreSQL-backed task queue.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from conductor.core.models import (
    RetryPolicy,
    Task,
    TaskStatus,
    generate_task_id,
    utc_now,
)
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.exceptions import TaskError

logger = logging.getLogger("conductor.core.queue")


class TaskQueue:
    """High-level interface for submitting and querying tasks.

    Manages a PostgreSQL connection pool internally and provides
    async context manager support.  All public methods are async.

    Typical usage::

        async with TaskQueue(database_url="postgresql://...") as queue:
            task_id = await queue.submit("email", {"to": "user@example.com"})
            task = await queue.get_task(task_id)
            pending = await queue.list_pending_tasks()
    """

    def __init__(
        self,
        database_url: str,
        *,
        task_timeout: float = 300.0,
        max_task_age: int = 86400,
        log_level: str = "INFO",
        pool_min_size: int = 2,
        pool_max_size: int = 10,
        pool_timeout: float = 30.0,
        command_timeout: float = 60.0,
    ) -> None:
        self._database_url = database_url
        # Reserved for Sprint 3 (Worker) — timeout for individual task execution
        self._task_timeout = task_timeout
        # Reserved for Sprint 3 (Worker) — max age before a pending task is dropped
        self._max_task_age = max_task_age

        # Apply log level to the conductor logger hierarchy
        logging.getLogger("conductor").setLevel(log_level.upper())

        self._pool = DatabasePool(
            dsn=database_url,
            min_size=pool_min_size,
            max_size=pool_max_size,
            timeout=pool_timeout,
            command_timeout=command_timeout,
        )
        self._queries: Optional[QueryBuilder] = None
        self._connected = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to the database and ensure the schema exists."""
        await self._pool.connect()
        await SchemaManager(self._pool).ensure_schema()
        self._queries = QueryBuilder(self._pool)
        self._connected = True
        logger.info("TaskQueue connected to database.")

    async def disconnect(self) -> None:
        """Close the database connection."""
        await self._pool.disconnect()
        self._connected = False
        logger.info("TaskQueue disconnected.")

    @property
    def is_connected(self) -> bool:
        """``True`` if the queue is connected to the database."""
        return self._connected and self._pool.is_connected

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> TaskQueue:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------
    # Task submission
    # ------------------------------------------------------------------

    async def submit(
        self,
        task_type: str,
        payload: dict[str, Any],
        *,
        retry_policy: Optional[RetryPolicy] = None,
        scheduled_for: Optional[datetime] = None,
        route: str = "default",
        priority: int = 0,
        task_id: Optional[str] = None,
    ) -> str:
        """Submit a new task to the queue.

        Args:
            task_type: Logical type used to route the task to a handler.
            payload: Arbitrary JSON-serialisable data.
            retry_policy: Retry configuration (uses defaults if omitted).
            scheduled_for: If set, the task won't be picked up before this time.
            route: Route name for selective worker polling (v0.2).
            priority: Task priority, higher = more urgent (v0.2).
            task_id: Optional explicit task ID (auto-generated if omitted).

        Returns:
            The unique task ID.

        Raises:
            ValueError: If ``task_type`` is empty or ``payload`` is not a dict.
            TaskError: If the task already exists or insertion fails.
        """
        self._require_connected()

        if not task_type or not task_type.strip():
            raise ValueError("task_type must not be empty")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")

        rp = retry_policy or RetryPolicy()
        rp.validate()

        tid = task_id or generate_task_id()

        task = Task(
            task_id=tid,
            task_type=task_type,
            payload=payload,
            status=TaskStatus.PENDING,
            priority=priority,
            route=route,
            retry_policy=rp,
            attempt=0,
            max_retries=rp.max_retries,
            scheduled_for=scheduled_for,
            created_at=utc_now(),
        )

        db_dict = _task_to_db_dict(task)
        inserted_id = await self._query.insert_task(db_dict)
        logger.info("Task submitted: %s (%s)", inserted_id, task_type)
        return inserted_id

    async def submit_many(
        self,
        tasks: list[tuple[str, dict[str, Any]]],
        *,
        retry_policy: Optional[RetryPolicy] = None,
        route: str = "default",
        priority: int = 0,
    ) -> list[str]:
        """Submit multiple tasks in a single database transaction.

        All inserts are wrapped in a PostgreSQL transaction for atomicity
        — if any insert fails, the entire batch is rolled back.

        Each tuple is ``(task_type, payload)``.

        Args:
            tasks: List of ``(task_type, payload)`` tuples.
            retry_policy: Shared retry config for all tasks.
            route: Shared route for all tasks.
            priority: Shared priority for all tasks.

        Returns:
            A list of task IDs in the same order as the input.

        Raises:
            ValueError: If any ``task_type`` is empty or ``payload`` is not a dict.
            TaskError: If any task already exists or insertion fails.
        """
        self._require_connected()
        rp = retry_policy or RetryPolicy()
        rp.validate()
        now = utc_now()

        # Validate all inputs up-front before touching the DB
        for task_type, payload in tasks:
            if not task_type or not task_type.strip():
                raise ValueError("task_type must not be empty")
            if not isinstance(payload, dict):
                raise ValueError("payload must be a dict")

        # Build all task dicts up-front
        task_dicts: list[dict[str, Any]] = []
        for task_type, payload in tasks:
            tid = generate_task_id()
            task = Task(
                task_id=tid,
                task_type=task_type,
                payload=payload,
                status=TaskStatus.PENDING,
                priority=priority,
                route=route,
                retry_policy=rp,
                attempt=0,
                max_retries=rp.max_retries,
                created_at=now,
            )
            task_dicts.append(_task_to_db_dict(task))

        # Insert all in a single transaction for atomicity
        task_ids: list[str] = []
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for db_dict in task_dicts:
                    inserted_id = await self._query.insert_task(db_dict)
                    task_ids.append(inserted_id)
                    logger.info(
                        "Task submitted: %s (%s)",
                        inserted_id, db_dict["task_type"],
                    )

        return task_ids

    # ------------------------------------------------------------------
    # Task queries
    # ------------------------------------------------------------------

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Fetch a single task by ID.

        Args:
            task_id: The unique task identifier.

        Returns:
            A ``Task`` object or ``None`` if not found.
        """
        self._require_connected()
        row = await self._query.select_task(task_id)
        if row is None:
            return None
        return Task.from_dict(row)

    async def list_pending_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[Task]:
        """List pending tasks eligible for processing.

        Orders by ``priority DESC, created_at ASC``.

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip (for pagination).

        Returns:
            A list of ``Task`` objects.
        """
        self._require_connected()
        rows = await self._query.select_pending_tasks(
            limit=limit, offset=offset,
        )
        return [Task.from_dict(r) for r in rows]

    async def list_completed_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[Task]:
        """List completed tasks, newest first.

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip (for pagination).

        Returns:
            A list of ``Task`` objects.
        """
        self._require_connected()
        rows = await self._query.select_tasks_by_status(
            "completed", limit=limit, offset=offset,
        )
        return [Task.from_dict(r) for r in rows]

    async def list_failed_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[Task]:
        """List failed tasks, newest first.

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip (for pagination).

        Returns:
            A list of ``Task`` objects.
        """
        self._require_connected()
        rows = await self._query.select_tasks_by_status(
            "failed", limit=limit, offset=offset,
        )
        return [Task.from_dict(r) for r in rows]

    async def count_tasks_by_status(self, status: str) -> int:
        """Count tasks with the given status.

        Args:
            status: One of ``pending``, ``processing``, ``completed``,
                    ``failed``, ``retrying``.

        Returns:
            The number of tasks in that status.
        """
        self._require_connected()
        return await self._query.count_tasks_by_status(status)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _query(self) -> QueryBuilder:
        """Return the ``QueryBuilder``, raising ``TaskError`` if not connected.

        Centralises the ``Optional[QueryBuilder]`` guard so callers don't
        need ``type: ignore[union-attr]`` everywhere.
        """
        if not self._connected or self._pool.is_connected is False:
            raise TaskError(
                "TaskQueue is not connected. Call connect() or use "
                "'async with TaskQueue(...)'."
            )
        assert self._queries is not None  # guaranteed by _connected check
        return self._queries

    def _require_connected(self) -> None:
        """Raise ``TaskError`` if the queue is not connected.

        Used for early validation before expensive work (e.g., building
        a ``Task`` object).  The ``_query`` property also checks this
        on every access.
        """
        if not self._connected or self._pool.is_connected is False:
            raise TaskError(
                "TaskQueue is not connected. Call connect() or use "
                "'async with TaskQueue(...)'."
            )

    # ------------------------------------------------------------------
    # Public helpers for test infrastructure
    # ------------------------------------------------------------------

    async def execute_raw(self, query: str, *args: Any) -> str:
        """Execute a raw SQL statement.

        Primarily intended for test cleanup (``DELETE``, ``TRUNCATE``).
        """
        if not self._connected:
            raise TaskError("TaskQueue is not connected.")
        return await self._pool.execute(query, *args)

    @property
    def query_builder(self) -> QueryBuilder:  # pylint: disable=protected-access
        """Return the ``QueryBuilder`` for direct query access.

        Provides test code with direct access to query methods for
        verification purposes.

        Raises:
            TaskError: If not connected.
        """
        queries = self._queries
        if queries is None:
            raise TaskError(
                "TaskQueue is not connected. Call connect() or use "
                "'async with TaskQueue(...)'."
            )
        return queries


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _task_to_db_dict(task: Task) -> dict[str, Any]:
    """Convert a ``Task`` to the dict format expected by ``QueryBuilder``.

    Keeps datetime fields as native Python ``datetime`` objects (not
    ISO strings) so they can be passed directly to asyncpg.
    """
    return {
        "task_id": task.task_id,
        "task_type": task.task_type,
        "payload": task.payload,
        "status": task.status.value,
        "priority": task.priority,
        "route": task.route,
        "attempt": task.attempt,
        "max_retries": task.max_retries,
        "retry_policy": task.retry_policy.to_dict(),
        "scheduled_for": task.scheduled_for,
        "worker_id": task.worker_id,
        "result": task.result,
        "error_message": task.error_message,
        "created_at": task.created_at,
        "started_at": task.started_at,
        "completed_at": task.completed_at,
    }
