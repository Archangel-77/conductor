"""
Task queue implementation for Conductor.

Provides the public ``TaskQueue`` class for submitting, listing,
and managing tasks in the PostgreSQL-backed task queue.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from croniter import croniter as croniter_cls

from conductor.core.models import (
    DLQTask,
    RecurringTask,
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
from conductor.observability.metrics import inc_tasks_submitted

logger = logging.getLogger("conductor.core.queue")

# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

PRIORITY_MIN = -100
"""Minimum allowed task priority (matches the DB CHECK constraint)."""

PRIORITY_MAX = 100
"""Maximum allowed task priority (matches the DB CHECK constraint)."""


def _validate_priority(priority: int) -> None:
    """Validate a task priority is an integer within the allowed range.

    Raises:
        ValueError: If ``priority`` is not an int or is outside
                    ``[PRIORITY_MIN, PRIORITY_MAX]``.
    """
    if isinstance(priority, bool) or not isinstance(priority, int):
        raise ValueError("priority must be an integer")
    if not (PRIORITY_MIN <= priority <= PRIORITY_MAX):
        raise ValueError(
            f"priority must be between {PRIORITY_MIN} and {PRIORITY_MAX} " f"(got {priority})"
        )


def _validate_route(route: str) -> None:
    """Validate a route name is a non-empty string.

    Raises:
        ValueError: If ``route`` is not a non-empty string.
    """
    if not isinstance(route, str) or not route.strip():
        raise ValueError("route must be a non-empty string")


def _validate_depends_on(depends_on: Optional[list[str]]) -> None:
    """Validate a ``depends_on`` list of task IDs.

    Raises:
        ValueError: If ``depends_on`` is not a list of non-empty strings
            (``None`` is allowed).
    """
    if depends_on is None:
        return
    if not isinstance(depends_on, list):
        raise ValueError("depends_on must be a list of task IDs")
    for dep in depends_on:
        if not isinstance(dep, str) or not dep.strip():
            raise ValueError("depends_on must contain only non-empty task ID strings")


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
        logger.info(
            "TaskQueue connected to database.",
            extra={"component": "TaskQueue"},
        )

    async def disconnect(self) -> None:
        """Close the database connection."""
        await self._pool.disconnect()
        self._connected = False
        logger.info(
            "TaskQueue disconnected.",
            extra={"component": "TaskQueue"},
        )

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
        depends_on: Optional[list[str]] = None,
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
            depends_on: Task IDs that must complete (or be cancelled) before
                this task may run (v0.2).  Forward references are allowed.
            task_id: Optional explicit task ID (auto-generated if omitted).

        Returns:
            The unique task ID.

        Raises:
            ValueError: If ``task_type`` is empty, ``payload`` is not a dict,
                ``depends_on`` is malformed, or the task depends on itself.
            TaskError: If the task already exists or insertion fails.
        """
        self._require_connected()

        if not task_type or not task_type.strip():
            raise ValueError("task_type must not be empty")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        _validate_route(route)
        _validate_priority(priority)
        _validate_depends_on(depends_on)

        rp = retry_policy or RetryPolicy()
        rp.validate()

        tid = task_id or generate_task_id()
        if depends_on and tid in depends_on:
            raise ValueError("a task cannot depend on itself")

        task = Task(
            task_id=tid,
            task_type=task_type,
            payload=payload,
            status=TaskStatus.PENDING,
            priority=priority,
            route=route,
            depends_on=depends_on or [],
            retry_policy=rp,
            attempt=0,
            max_retries=rp.max_retries,
            scheduled_for=scheduled_for,
            created_at=utc_now(),
        )

        db_dict = _task_to_db_dict(task)
        inserted_id = await self._query.insert_task(db_dict)
        inc_tasks_submitted(task_type)
        logger.info(
            "Task submitted: %s (%s)",
            inserted_id,
            task_type,
            extra={
                "task_id": inserted_id,
                "task_type": task_type,
            },
        )
        return inserted_id

    async def submit_many(
        self,
        tasks: list[tuple[str, dict[str, Any]]],
        *,
        retry_policy: Optional[RetryPolicy] = None,
        route: str = "default",
        priority: int = 0,
        scheduled_for: Optional[datetime] = None,
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
            scheduled_for: Shared earliest pickup time for all tasks.

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
        _validate_route(route)
        _validate_priority(priority)

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
                scheduled_for=scheduled_for,
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
                    inc_tasks_submitted(db_dict["task_type"])
                    logger.info(
                        "Task submitted: %s (%s)",
                        inserted_id,
                        db_dict["task_type"],
                        extra={
                            "task_id": inserted_id,
                            "task_type": db_dict["task_type"],
                        },
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
        route: Optional[str] = None,
    ) -> list[Task]:
        """List pending tasks eligible for processing.

        Orders by ``priority DESC, created_at ASC``.

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip (for pagination).
            route: If set, only return tasks submitted to this route.

        Returns:
            A list of ``Task`` objects.

        Raises:
            ValueError: If ``route`` is set but not a non-empty string.
        """
        self._require_connected()
        if route is not None:
            _validate_route(route)
        rows = await self._query.select_pending_tasks(
            limit=limit,
            offset=offset,
            route=route,
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
            "completed",
            limit=limit,
            offset=offset,
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
            "failed",
            limit=limit,
            offset=offset,
        )
        return [Task.from_dict(r) for r in rows]

    async def count_tasks_by_status(self, status: str) -> int:
        """Count tasks with the given status.

        Args:
            status: One of ``pending``, ``processing``, ``completed``,
                    ``failed``, ``retrying``, ``cancelled``.

        Returns:
            The number of tasks in that status.
        """
        self._require_connected()
        return await self._query.count_tasks_by_status(status)

    async def cancel_task(self, task_id: str) -> None:
        """Cancel a pending or retrying task.

        Sets the task's status to ``cancelled`` so it will never be picked
        up by a worker.  Only tasks in ``pending`` or ``retrying`` can be
        cancelled.

        Args:
            task_id: The unique task identifier.

        Raises:
            TaskError: If the task does not exist or is not cancellable
                (``processing``, ``completed``, ``failed`` or ``cancelled``).
        """
        self._require_connected()
        if not await self._query.cancel_task(task_id):
            task = await self._query.select_task(task_id)
            if task is None:
                raise TaskError(f"Task '{task_id}' not found.")
            raise TaskError(
                f"Task '{task_id}' cannot be cancelled from status " f"'{task['status']}'."
            )

    # ------------------------------------------------------------------
    # Dead-letter queue queries
    # ------------------------------------------------------------------

    async def list_dlq_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
        include_discarded: bool = False,
    ) -> list[DLQTask]:
        """List tasks in the dead-letter queue, newest first.

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip (for pagination).
            include_discarded: If ``True``, also include discarded tasks.

        Returns:
            A list of ``DLQTask`` objects.
        """
        self._require_connected()
        rows = await self._query.select_dlq_tasks(
            limit=limit,
            offset=offset,
            include_discarded=include_discarded,
        )
        return [DLQTask.from_dict(r) for r in rows]

    async def get_dlq_task(self, task_id: str) -> Optional[DLQTask]:
        """Fetch a single DLQ task by ID.

        Args:
            task_id: The unique task identifier.

        Returns:
            A ``DLQTask`` object or ``None`` if not found.
        """
        self._require_connected()
        row = await self._query.select_dlq_task(task_id)
        if row is None:
            return None
        return DLQTask.from_dict(row)

    async def retry_dlq_task(self, task_id: str) -> str:
        """Retry a task from the dead-letter queue.

        Removes the task from the DLQ and resets the corresponding
        ``conductor_tasks`` row to ``pending`` with ``attempt=0``.

        Args:
            task_id: The task to retry.

        Returns:
            The task ID that was retried.

        Raises:
            TaskError: If the task is not found in the DLQ.
        """
        self._require_connected()

        dlq_row = await self._query.select_dlq_task(task_id)
        if dlq_row is None:
            raise TaskError(f"Task '{task_id}' not found in the dead-letter queue.")

        now = utc_now()
        await self._query.delete_dlq_task(task_id)

        existing_task = await self._query.select_task(task_id)
        if existing_task is not None:
            await self._query.update_task_status(
                task_id,
                "pending",
                worker_id=None,
                error_message=None,
                attempt=0,
                scheduled_for=now,
            )
        else:
            rp = RetryPolicy.from_dict(dlq_row.get("retry_policy", {}))
            task_dict: dict[str, Any] = {
                "task_id": task_id,
                "task_type": dlq_row["task_type"],
                "payload": dlq_row.get("payload", {}),
                "status": "pending",
                "priority": 0,
                "route": "default",
                "attempt": 0,
                "max_retries": rp.max_retries,
                "retry_policy": rp.to_dict(),
                "scheduled_for": now,
                "worker_id": None,
                "result": None,
                "error_message": None,
                "created_at": now,
                "started_at": None,
                "completed_at": None,
            }
            await self._query.insert_task(task_dict)

        logger.info(
            "Task %s retried from DLQ.",
            task_id,
            extra={"task_id": task_id},
        )
        return task_id

    async def discard_dlq_task(
        self,
        task_id: str,
        reason: Optional[str] = None,
    ) -> None:
        """Mark a DLQ task as permanently discarded.

        Args:
            task_id: The task to discard.
            reason: Optional explanation for the discard.

        Raises:
            TaskError: If the task is not found in the DLQ.
        """
        self._require_connected()

        dlq_row = await self._query.select_dlq_task(task_id)
        if dlq_row is None:
            raise TaskError(f"Task '{task_id}' not found in the dead-letter queue.")

        await self._query.discard_dlq_task(task_id, reason=reason)
        logger.info(
            "Task %s discarded from DLQ (reason: %s).",
            task_id,
            reason or "no reason given",
            extra={
                "task_id": task_id,
                "reason": reason,
            },
        )

    async def count_dlq_tasks(self, include_discarded: bool = False) -> int:
        """Count tasks in the dead-letter queue.

        Args:
            include_discarded: If ``True``, also count discarded tasks.

        Returns:
            The number of tasks in the DLQ.
        """
        self._require_connected()
        return await self._query.count_dlq_tasks(
            include_discarded=include_discarded,
        )

    # ------------------------------------------------------------------
    # Recurring task management
    # ------------------------------------------------------------------

    async def schedule_recurring(
        self,
        task_type: str,
        payload: dict[str, Any],
        cron_expression: str,
        *,
        route: str = "default",
        priority: int = 0,
        retry_policy: Optional[RetryPolicy] = None,
        enabled: bool = True,
        recurring_id: Optional[str] = None,
    ) -> str:
        """Register a cron-driven recurring task definition.

        The scheduler creates one task instance per cron fire.

        Args:
            task_type: Logical type of the task instances to create.
            payload: Payload copied into every generated task.
            cron_expression: Standard 5-field cron expression (UTC).
            route: Route for generated tasks.
            priority: Priority for generated tasks (higher runs first).
            retry_policy: Retry policy applied to generated tasks.
            enabled: Whether the definition starts enabled.
            recurring_id: Optional explicit ID (auto-generated if omitted).

        Returns:
            The recurring definition ID.

        Raises:
            ValueError: On invalid ``task_type``/``payload``/``route``/
                        ``priority``/``cron_expression``.
            TaskError: If the ``recurring_id`` already exists.
        """
        self._require_connected()

        if not task_type or not task_type.strip():
            raise ValueError("task_type must not be empty")
        if not isinstance(payload, dict):
            raise ValueError("payload must be a dict")
        _validate_route(route)
        _validate_priority(priority)

        rp = retry_policy or RetryPolicy()
        rp.validate()

        rid = recurring_id or generate_task_id()
        now = utc_now()
        # Validates the cron expression (raises ValueError) and computes the
        # first future fire time in UTC.
        next_run_at = _next_cron_run(cron_expression, now)

        recurring = RecurringTask(
            id=rid,
            task_type=task_type,
            payload=payload,
            cron_expression=cron_expression,
            route=route,
            priority=priority,
            retry_policy=rp,
            enabled=enabled,
            next_run_at=next_run_at,
            created_at=now,
        )
        inserted_id = await self._query.insert_recurring_task(_recurring_to_db_dict(recurring))
        logger.info(
            "Recurring task scheduled: %s (%s) cron=%s",
            inserted_id,
            task_type,
            cron_expression,
            extra={
                "recurring_id": inserted_id,
                "task_type": task_type,
                "cron_expression": cron_expression,
            },
        )
        return inserted_id

    async def list_recurring_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[RecurringTask]:
        """List recurring definitions, newest first.

        Args:
            limit: Maximum number of definitions to return.
            offset: Number of definitions to skip (for pagination).

        Returns:
            A list of ``RecurringTask`` objects.
        """
        self._require_connected()
        rows = await self._query.select_recurring_tasks(limit=limit, offset=offset)
        return [RecurringTask.from_dict(r) for r in rows]

    async def get_recurring_task(self, recurring_id: str) -> Optional[RecurringTask]:
        """Fetch a single recurring definition by ID.

        Returns:
            A ``RecurringTask`` object or ``None`` if not found.
        """
        self._require_connected()
        row = await self._query.select_recurring_task(recurring_id)
        if row is None:
            return None
        return RecurringTask.from_dict(row)

    async def pause_recurring(self, recurring_id: str) -> None:
        """Disable a recurring definition so it no longer fires.

        Raises:
            TaskError: If the definition does not exist.
        """
        await self._set_recurring_enabled(recurring_id, enabled=False)

    async def resume_recurring(self, recurring_id: str) -> None:
        """Re-enable a paused recurring definition.

        Raises:
            TaskError: If the definition does not exist.
        """
        await self._set_recurring_enabled(recurring_id, enabled=True)

    async def delete_recurring_task(self, recurring_id: str) -> None:
        """Delete a recurring definition permanently.

        Raises:
            TaskError: If the definition does not exist.
        """
        self._require_connected()
        deleted = await self._query.delete_recurring_task(recurring_id)
        if not deleted:
            raise TaskError(f"Recurring task '{recurring_id}' not found.")
        logger.info(
            "Recurring task %s deleted.",
            recurring_id,
            extra={"recurring_id": recurring_id},
        )

    async def _set_recurring_enabled(self, recurring_id: str, enabled: bool) -> None:
        """Toggle a recurring definition's ``enabled`` flag."""
        self._require_connected()
        updated = await self._query.set_recurring_enabled(recurring_id, enabled)
        if not updated:
            raise TaskError(f"Recurring task '{recurring_id}' not found.")
        logger.info(
            "Recurring task %s %s.",
            recurring_id,
            "resumed" if enabled else "paused",
            extra={"recurring_id": recurring_id, "enabled": enabled},
        )

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
                "TaskQueue is not connected. Call connect() or use 'async with TaskQueue(...)'."
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
                "TaskQueue is not connected. Call connect() or use 'async with TaskQueue(...)'."
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
                "TaskQueue is not connected. Call connect() or use 'async with TaskQueue(...)'."
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
        "depends_on": list(task.depends_on),
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


def _recurring_to_db_dict(recurring: RecurringTask) -> dict[str, Any]:
    """Convert a ``RecurringTask`` to the dict format for ``QueryBuilder``.

    Keeps datetime fields as native Python ``datetime`` objects.
    """
    return {
        "id": recurring.id,
        "task_type": recurring.task_type,
        "payload": recurring.payload,
        "cron_expression": recurring.cron_expression,
        "route": recurring.route,
        "priority": recurring.priority,
        "retry_policy": recurring.retry_policy.to_dict(),
        "enabled": recurring.enabled,
        "next_run_at": recurring.next_run_at,
        "last_run_at": recurring.last_run_at,
        "created_at": recurring.created_at,
    }


def _next_cron_run(expression: str, after: datetime) -> datetime:
    """Return the next cron fire time strictly after *after* (UTC).

    Raises:
        ValueError: If ``expression`` is not a valid cron expression.
    """
    try:
        return croniter_cls(expression, after).get_next(datetime)
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Invalid cron expression '{expression}': {exc}") from exc
