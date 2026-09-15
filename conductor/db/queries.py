"""
Type-safe SQL query builders for Conductor.

SQL is rendered through the pool's ``SqlDialect``, so the same builders run on
PostgreSQL, SQLite and (later) MySQL.  Two conventions matter throughout:

* **Placeholders are rendered in the textual order they appear** in the
  statement.  Numbered backends (``$1``) do not care, but positional backends
  (``?``) bind strictly by position, so a statement must never reference a
  higher-numbered parameter before a lower-numbered one.
* **Affected-row counts are normalised** through
  ``SqlDialect.normalize_rowcount``, so backend-specific command tags never leak
  into callers.

Every public method validates its arguments before constructing a query.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional, cast

from croniter import croniter

from conductor.db.backends.base import PoolProtocol, SqlDialect
from conductor.db.backends.postgres import PostgresDialect
from conductor.db.connection import DatabasePool
from conductor.exceptions import TaskError

logger = logging.getLogger("conductor.db.queries")

TASK_COLUMNS: tuple[str, ...] = (
    "task_id",
    "task_type",
    "payload",
    "status",
    "priority",
    "route",
    "attempt",
    "max_retries",
    "retry_policy",
    "depends_on",
    "scheduled_for",
    "worker_id",
    "result",
    "error_message",
    "created_at",
    "started_at",
    "completed_at",
    "traceparent",
)
"""``conductor_tasks`` columns in insert order."""

JSON_TASK_COLUMNS = frozenset({"payload", "retry_policy", "result"})
"""Task columns that must be cast to the backend's JSON type."""

DEAD_LETTER_COLUMNS: tuple[str, ...] = (
    "task_id",
    "task_type",
    "payload",
    "error_message",
    "attempts",
    "retry_policy",
    "route",
    "priority",
    "depends_on",
    "moved_at",
    "discarded",
    "discard_reason",
    "discarded_at",
    "traceparent",
)
"""``conductor_dead_letter`` columns in insert order."""

RECURRING_COLUMNS: tuple[str, ...] = (
    "id",
    "task_type",
    "payload",
    "cron_expression",
    "route",
    "priority",
    "retry_policy",
    "enabled",
    "next_run_at",
    "last_run_at",
    "created_at",
)
"""``conductor_recurring_tasks`` columns in insert order."""


def _task_values(dialect: SqlDialect) -> str:
    """Render the ``VALUES`` list for a task insert (17 columns)."""
    rendered = [
        (dialect.json_param(index) if column in JSON_TASK_COLUMNS else dialect.placeholder(index))
        for index, column in enumerate(TASK_COLUMNS, start=1)
    ]
    return ", ".join(rendered)


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------


def _validate_not_empty(value: Any, name: str) -> None:
    if not value or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"{name} must not be empty")


def _validate_task_status(status: str) -> None:
    valid = {
        "pending",
        "processing",
        "completed",
        "failed",
        "retrying",
        "cancelled",
        "blocked",
    }
    if status not in valid:
        raise ValueError(f"Invalid task status '{status}'. Must be one of {valid}")


def _validate_worker_status(status: str) -> None:
    valid = {"idle", "processing", "unhealthy"}
    if status not in valid:
        raise ValueError(f"Invalid worker status '{status}'. Must be one of {valid}")


def _validate_cron_expression(expression: Any) -> None:
    """Validate a 5-field cron expression using croniter.

    Raises:
        ValueError: If the expression is empty or cannot be parsed.
    """
    _validate_not_empty(expression, "cron_expression")
    try:
        croniter(str(expression))
    except (ValueError, KeyError) as exc:
        raise ValueError(f"Invalid cron expression '{expression}': {exc}") from exc


def _pool_dialect(pool: Any) -> SqlDialect:
    """Return the SQL dialect of *pool*.

    Falls back to PostgreSQL when the pool is a duck-typed double (unit tests
    pass ``AsyncMock`` objects in place of a real pool).
    """
    dialect = getattr(pool, "dialect", None)
    if isinstance(dialect, SqlDialect):
        return dialect
    return PostgresDialect()


# ---------------------------------------------------------------------------
# QueryBuilder
# ---------------------------------------------------------------------------


class QueryBuilder:
    """Collects all database query operations for Conductor.

    Every method accepts and returns plain Python objects (dicts, dataclass
    fields, primitives).  No knowledge of the caller's model classes is
    required – but the helper methods expect dictionaries with the same
    keys used in the schema (see ``schema.py``).

    Args:
        pool: The connection pool (its dialect renders every statement).
        dialect: Optional dialect override; defaults to the pool's dialect.
    """

    def __init__(
        self,
        pool: DatabasePool | PoolProtocol,
        dialect: Optional[SqlDialect] = None,
    ) -> None:
        self._pool: Any = pool
        self._dialect: SqlDialect = dialect or _pool_dialect(pool)

    # ==================================================================
    # Task queries
    # ==================================================================

    async def insert_task(
        self,
        task: dict[str, Any],
        *,
        conn: Optional[Any] = None,
    ) -> str:
        """Insert a new task row and return its ``task_id``.

        Expects a dictionary with at least:
        ``task_id``, ``task_type``, ``payload``, ``status``, ``priority``,
        ``route``, ``attempt``, ``max_retries``, ``retry_policy``,
        ``scheduled_for``, ``created_at``.

        Args:
            task: The task dictionary to insert.
            conn: Optional explicit connection/transaction to use (so the
                caller can hold locks across a transaction).

        Raises:
            TaskError: If a task with the same ``task_id`` already exists.
        """
        _validate_not_empty(task.get("task_id"), "task_id")
        _validate_not_empty(task.get("task_type"), "task_type")

        dialect = self._dialect
        value_list = _task_values(dialect)
        conflict = dialect.insert_ignore(["task_id"])
        columns = ", ".join(TASK_COLUMNS)
        values = (
            task["task_id"],
            task["task_type"],
            _json(task.get("payload", {})),
            task.get("status", "pending"),
            task.get("priority", 0),
            task.get("route", "default"),
            task.get("attempt", 0),
            task.get("max_retries", 3),
            _json(task.get("retry_policy", {})),
            task.get("depends_on") or [],
            task.get("scheduled_for"),
            task.get("worker_id"),
            _json(task.get("result")),
            task.get("error_message"),
            task.get("created_at", datetime.now(timezone.utc)),
            task.get("started_at"),
            task.get("completed_at"),
            task.get("traceparent"),
        )
        target = conn if conn is not None else self._pool
        conflicting = f"Task '{task['task_id']}' already exists"

        if dialect.supports_returning:
            query = f"""
                INSERT INTO conductor_tasks (
                    {columns}
                ) VALUES (
                    {value_list}
                )
                {conflict}
                RETURNING task_id
            """
            row = await target.fetchrow(query, *values)
            if row is None:
                raise TaskError(conflicting)
        else:
            # No RETURNING (MySQL): the conflict clause is a no-op update, so an
            # affected-row count of 0 means the row already existed.
            query = f"""
                INSERT INTO conductor_tasks (
                    {columns}
                ) VALUES (
                    {value_list}
                )
                {conflict}
            """
            result = await target.execute(query, *values)
            if dialect.normalize_rowcount(result) == 0:
                raise TaskError(conflicting)

        return cast(str, task["task_id"])

    async def select_task(self, task_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single task by its ID, returning a dict or ``None``."""
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        query = f"SELECT * FROM conductor_tasks WHERE task_id = {dialect.placeholder(1)}"
        row = await self._pool.fetchrow(query, task_id)
        return self._row_to_dict(row) if row else None

    async def select_pending_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
        route: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch tasks that are eligible for processing.

        Filters by:

        - ``status IN ('pending', 'retrying')`` — a retry is a scheduled task
          waiting for its backoff delay, so it must be claimable again
        - ``scheduled_for`` is ``NULL`` or in the past
        - every entry in ``depends_on`` is satisfied (``completed``/``cancelled``)
        - optional ``route`` filter

        Orders by ``priority DESC, created_at ASC`` and claims the rows with
        ``FOR UPDATE SKIP LOCKED`` (a no-op clause on backends without row
        locking).

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip.
            route: Optional route filter.

        Raises:
            ValueError: If ``limit``/``offset`` are invalid.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        dialect = self._dialect
        deps_gate = (
            f"({dialect.cardinality('t.depends_on')} = 0 OR NOT EXISTS ("
            f"SELECT 1 FROM conductor_tasks d "
            f"WHERE {dialect.array_element_in('d.task_id', 't.depends_on')} "
            f"AND d.status NOT IN ('completed', 'cancelled')))"
        )
        locking = dialect.for_update_skip_locked()

        if route:
            query = f"""
                SELECT * FROM conductor_tasks t
                WHERE status IN ('pending', 'retrying')
                  AND (scheduled_for IS NULL OR scheduled_for <= {dialect.now()})
                  AND {deps_gate}
                  AND route = {dialect.placeholder(1)}
                ORDER BY priority DESC, created_at ASC
                LIMIT {dialect.placeholder(2)} OFFSET {dialect.placeholder(3)}
                {locking}
            """
            rows = await self._pool.fetch(query, route, limit, offset)
        else:
            query = f"""
                SELECT * FROM conductor_tasks t
                WHERE status IN ('pending', 'retrying')
                  AND (scheduled_for IS NULL OR scheduled_for <= {dialect.now()})
                  AND {deps_gate}
                ORDER BY priority DESC, created_at ASC
                LIMIT {dialect.placeholder(1)} OFFSET {dialect.placeholder(2)}
                {locking}
            """
            rows = await self._pool.fetch(query, limit, offset)

        return [self._row_to_dict(r) for r in rows]

    async def select_tasks_by_status(
        self,
        status: str,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch tasks filtered by status, ordered by ``created_at DESC``."""
        _validate_task_status(status)
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        dialect = self._dialect
        query = f"""
            SELECT * FROM conductor_tasks
            WHERE status = {dialect.placeholder(1)}
            ORDER BY created_at DESC
            LIMIT {dialect.placeholder(2)} OFFSET {dialect.placeholder(3)}
        """
        rows = await self._pool.fetch(query, status, limit, offset)
        return [self._row_to_dict(r) for r in rows]

    async def select_completed_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Shorthand for ``select_tasks_by_status('completed', ...)``."""
        return await self.select_tasks_by_status("completed", limit, offset)

    async def select_failed_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Shorthand for ``select_tasks_by_status('failed', ...)``."""
        return await self.select_tasks_by_status("failed", limit, offset)

    async def select_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
        *,
        status: Optional[str] = None,
        route: Optional[str] = None,
        task_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch tasks across all statuses for the dashboard.

        Non-locking read (unlike ``select_pending_tasks``, which uses
        ``FOR UPDATE SKIP LOCKED``); filters are optional and combined
        with ``AND``.  ``search`` matches ``task_id`` or ``task_type``
        case-insensitively (ILIKE).

        Args:
            limit: Maximum number of tasks to return.
            offset: Number of tasks to skip (for pagination).
            status: Optional status filter.
            route: Optional route filter.
            task_type: Optional task-type filter.
            search: Optional free-text search on ``task_id``/``task_type``.

        Returns:
            A list of task dicts ordered by ``created_at DESC``.

        Raises:
            ValueError: If ``limit``/``offset`` are invalid, or a filter
                value is empty.
        """
        conditions, params, idx = self._task_filters(
            status=status,
            route=route,
            task_type=task_type,
            search=search,
        )

        dialect = self._dialect
        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"""
            SELECT * FROM conductor_tasks
            {where_clause}
            ORDER BY created_at DESC
            LIMIT {dialect.placeholder(idx)} OFFSET {dialect.placeholder(idx + 1)}
        """
        rows = await self._pool.fetch(query, *params, limit, offset)
        return [self._row_to_dict(r) for r in rows]

    async def count_tasks(
        self,
        *,
        status: Optional[str] = None,
        route: Optional[str] = None,
        task_type: Optional[str] = None,
        search: Optional[str] = None,
    ) -> int:
        """Count tasks matching the given filters (dashboard pagination).

        Shares the same filter semantics as ``select_tasks``.

        Args:
            status: Optional status filter.
            route: Optional route filter.
            task_type: Optional task-type filter.
            search: Optional free-text search on ``task_id``/``task_type``.

        Returns:
            The number of matching tasks.

        Raises:
            ValueError: If a filter value is empty.
        """
        conditions, params, _ = self._task_filters(
            status=status,
            route=route,
            task_type=task_type,
            search=search,
        )

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        query = f"SELECT COUNT(*) FROM conductor_tasks {where_clause}"
        row = await self._pool.fetchval(query, *params)
        return row or 0

    async def update_task_status(
        self,
        task_id: str,
        new_status: str,
        *,
        worker_id: Optional[str] = None,
        result: Optional[dict[str, Any]] = None,
        error_message: Optional[str] = None,
        attempt: Optional[int] = None,
        scheduled_for: Optional[datetime] = None,
    ) -> bool:
        """Update a task's status and optional metadata.

        Returns ``True`` if a row was updated, ``False`` otherwise.
        """
        _validate_not_empty(task_id, "task_id")
        _validate_task_status(new_status)

        dialect = self._dialect
        set_parts = [f"status = {dialect.placeholder(1)}"]
        params: list[Any] = [new_status]
        idx = 2

        if worker_id is not None:
            set_parts.append(f"worker_id = {dialect.placeholder(idx)}")
            params.append(worker_id)
            idx += 1
        if result is not None:
            set_parts.append(f"result = {dialect.json_param(idx)}")
            params.append(_json(result))
            idx += 1
        if error_message is not None:
            set_parts.append(f"error_message = {dialect.placeholder(idx)}")
            params.append(error_message)
            idx += 1
        if attempt is not None:
            set_parts.append(f"attempt = {dialect.placeholder(idx)}")
            params.append(attempt)
            idx += 1
        if scheduled_for is not None:
            set_parts.append(f"scheduled_for = {dialect.placeholder(idx)}")
            params.append(scheduled_for)
            idx += 1

        # Auto-set timestamps based on the target status.
        if new_status == "processing":
            set_parts.append(f"started_at = {dialect.now()}")
        elif new_status in ("completed", "failed", "blocked"):
            set_parts.append(f"completed_at = {dialect.now()}")

        # The WHERE clause comes last so parameters stay in textual order.
        query = (
            f"UPDATE conductor_tasks SET {', '.join(set_parts)} "
            f"WHERE task_id = {dialect.placeholder(idx)}"
        )
        params.append(task_id)

        result_tag = await self._pool.execute(query, *params)
        return dialect.normalize_rowcount(result_tag, verb="UPDATE") == 1

    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending or retrying task.

        Sets the task's status to ``cancelled`` and stamps ``completed_at``.
        Only tasks in ``pending`` or ``retrying`` can be cancelled.

        Returns ``True`` if the task was cancelled, ``False`` if the task
        was not found or is not in a cancellable state.
        """
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        result_tag = await self._pool.execute(
            "UPDATE conductor_tasks "
            f"SET status = 'cancelled', completed_at = {dialect.now()} "
            f"WHERE task_id = {dialect.placeholder(1)} "
            "AND status IN ('pending', 'retrying')",
            task_id,
        )
        return dialect.normalize_rowcount(result_tag, verb="UPDATE") == 1

    async def mark_dependents_blocked(
        self,
        task_id: str,
        error_message: str,
    ) -> list[str]:
        """Mark pending tasks that depend on *task_id* as ``blocked``.

        Returns the IDs of the tasks that were newly marked (empty when
        nothing was blocked), so callers can propagate the state
        transitively — a blocked task's own dependents are marked next.

        Args:
            task_id: The task that reached a terminal failure.
            error_message: Message stored on each blocked dependent.
        """
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        if dialect.supports_returning:
            rows = await self._pool.fetch(
                "UPDATE conductor_tasks "
                f"SET status = 'blocked', error_message = {dialect.placeholder(1)}, "
                f"completed_at = {dialect.now()} "
                f"WHERE status = 'pending' AND {dialect.array_contains('depends_on', 2)} "
                "RETURNING task_id",
                error_message,
                task_id,
            )
            return [str(r["task_id"]) for r in rows]

        # MySQL has no RETURNING, so the update is split into a locking read
        # and a write.  The write re-checks ``status = 'pending'`` so a
        # dependent that completed in the meantime is not reported as blocked
        # (callers propagate blocking transitively from the returned IDs).
        async with self._pool.transaction() as conn:
            pending = await conn.fetch(
                "SELECT task_id FROM conductor_tasks "
                f"WHERE status = 'pending' AND {dialect.array_contains('depends_on', 1)} "
                "FOR UPDATE",
                task_id,
            )
            blocked_ids = [str(row["task_id"]) for row in pending]
            if not blocked_ids:
                return []
            # Placeholders must be numbered in textual order: the error message
            # is the first parameter of the statement, the IDs follow.
            error_placeholder = dialect.placeholder(1)
            placeholders = dialect.placeholder_list(2, len(blocked_ids))
            await conn.execute(
                "UPDATE conductor_tasks "
                f"SET status = 'blocked', error_message = {error_placeholder}, "
                f"completed_at = {dialect.now()} "
                f"WHERE status = 'pending' AND task_id IN ({placeholders})",
                error_message,
                *blocked_ids,
            )
            return blocked_ids

    async def clear_task_worker_id(self, task_id: str) -> bool:
        """Set ``worker_id`` to ``NULL`` for a task without changing status.

        This is used when retrying a task from the DLQ to disassociate it
        from the worker that failed.

        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        result = await self._pool.execute(
            "UPDATE conductor_tasks SET worker_id = NULL "
            f"WHERE task_id = {dialect.placeholder(1)}",
            task_id,
        )
        return dialect.normalize_rowcount(result, verb="UPDATE") == 1

    async def clear_task_error_message(self, task_id: str) -> bool:
        """Set ``error_message`` to ``NULL`` for a task.

        Used when retrying a task from the DLQ to clear the previous
        failure message.

        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        result = await self._pool.execute(
            "UPDATE conductor_tasks SET error_message = NULL "
            f"WHERE task_id = {dialect.placeholder(1)}",
            task_id,
        )
        return dialect.normalize_rowcount(result, verb="UPDATE") == 1

    # ==================================================================
    # Retry history queries
    # ==================================================================

    async def insert_retry_record(self, record: dict[str, Any]) -> str:
        """Insert a retry history record and return its ``id``."""
        _validate_not_empty(record.get("id"), "id")
        _validate_not_empty(record.get("task_id"), "task_id")

        dialect = self._dialect
        query = f"""
            INSERT INTO conductor_retries
                (id, task_id, attempt, error_message, scheduled_at, created_at)
            VALUES ({dialect.placeholder_list(1, 6)})
            {"RETURNING id" if dialect.supports_returning else ""}
        """
        values = (
            record["id"],
            record["task_id"],
            record["attempt"],
            record.get("error_message"),
            record["scheduled_at"],
            record.get("created_at", datetime.now(timezone.utc)),
        )
        if dialect.supports_returning:
            row = await self._pool.fetchrow(query, *values)
            if row is None:
                raise TaskError(f"Failed to insert retry record '{record['id']}'")
        else:
            result = await self._pool.execute(query, *values)
            if dialect.normalize_rowcount(result) == 0:
                raise TaskError(f"Failed to insert retry record '{record['id']}'")
        return cast(str, record["id"])

    async def select_retries_for_task(self, task_id: str) -> list[dict[str, Any]]:
        """Fetch all retry records for a given task, ordered by attempt."""
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        query = f"""
            SELECT * FROM conductor_retries
            WHERE task_id = {dialect.placeholder(1)}
            ORDER BY attempt ASC
        """
        rows = await self._pool.fetch(query, task_id)
        return [self._row_to_dict(r) for r in rows]

    # ==================================================================
    # Dead-letter queue queries
    # ==================================================================

    async def insert_dlq_task(self, dlq: dict[str, Any]) -> str:
        """Move a task into the dead-letter queue.  Returns the ``task_id``."""
        _validate_not_empty(dlq.get("task_id"), "task_id")
        _validate_not_empty(dlq.get("task_type"), "task_type")

        dialect = self._dialect
        values = ", ".join(
            (
                dialect.json_param(index)
                if column in JSON_TASK_COLUMNS
                else dialect.placeholder(index)
            )
            for index, column in enumerate(DEAD_LETTER_COLUMNS, start=1)
        )
        conflict = dialect.upsert(
            ["task_id"],
            [
                ("error_message", "EXCLUDED.error_message"),
                ("attempts", "EXCLUDED.attempts"),
                ("route", "EXCLUDED.route"),
                ("priority", "EXCLUDED.priority"),
                ("depends_on", "EXCLUDED.depends_on"),
                ("moved_at", "EXCLUDED.moved_at"),
                ("discarded", "FALSE"),
                ("discard_reason", "NULL"),
                ("discarded_at", "NULL"),
                ("traceparent", "EXCLUDED.traceparent"),
            ],
        )
        query = f"""
            INSERT INTO conductor_dead_letter (
                {", ".join(DEAD_LETTER_COLUMNS)}
            ) VALUES (
                {values}
            )
            {conflict}
            {"RETURNING task_id" if dialect.supports_returning else ""}
        """
        dlq_values = (
            dlq["task_id"],
            dlq["task_type"],
            _json(dlq.get("payload", {})),
            dlq.get("error_message"),
            dlq.get("attempts", 0),
            _json(dlq.get("retry_policy", {})),
            dlq.get("route", "default"),
            dlq.get("priority", 0),
            dlq.get("depends_on") or [],
            dlq.get("moved_at", datetime.now(timezone.utc)),
            dlq.get("discarded", False),
            dlq.get("discard_reason"),
            dlq.get("discarded_at"),
            dlq.get("traceparent"),
        )

        if dialect.supports_returning:
            row = await self._pool.fetchrow(query, *dlq_values)
            if row is None:
                raise TaskError(f"Failed to insert DLQ task '{dlq['task_id']}'")
        else:
            # The upsert is idempotent, so a zero affected-row count (MySQL,
            # identical row already present) is a success, not an error.
            await self._pool.execute(query, *dlq_values)

        return cast(str, dlq["task_id"])

    async def select_dlq_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
        include_discarded: bool = False,
    ) -> list[dict[str, Any]]:
        """Fetch tasks from the dead-letter queue, newest first."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        dialect = self._dialect
        where_clause = "" if include_discarded else "WHERE discarded = FALSE"
        query = f"""
            SELECT * FROM conductor_dead_letter
            {where_clause}
            ORDER BY moved_at DESC
            LIMIT {dialect.placeholder(1)} OFFSET {dialect.placeholder(2)}
        """
        rows = await self._pool.fetch(query, limit, offset)
        return [self._row_to_dict(r) for r in rows]

    async def select_dlq_task(self, task_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single DLQ entry by task ID."""
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        query = "SELECT * FROM conductor_dead_letter " f"WHERE task_id = {dialect.placeholder(1)}"
        row = await self._pool.fetchrow(query, task_id)
        return self._row_to_dict(row) if row else None

    async def delete_dlq_task(self, task_id: str) -> bool:
        """Remove a task from the dead-letter queue entirely.

        Returns ``True`` if a row was deleted.
        """
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        result = await self._pool.execute(
            "DELETE FROM conductor_dead_letter " f"WHERE task_id = {dialect.placeholder(1)}",
            task_id,
        )
        return dialect.normalize_rowcount(result, verb="DELETE") == 1

    async def discard_dlq_task(
        self,
        task_id: str,
        reason: Optional[str] = None,
    ) -> bool:
        """Mark a DLQ task as discarded (soft-delete).

        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(task_id, "task_id")

        dialect = self._dialect
        result = await self._pool.execute(
            "UPDATE conductor_dead_letter "
            f"SET discarded = TRUE, discard_reason = {dialect.placeholder(1)}, "
            f"discarded_at = {dialect.now()} "
            f"WHERE task_id = {dialect.placeholder(2)}",
            reason,
            task_id,
        )
        return dialect.normalize_rowcount(result, verb="UPDATE") == 1

    # ==================================================================
    # Recurring task queries
    # ==================================================================

    async def insert_recurring_task(
        self,
        recurring: dict[str, Any],
        *,
        conn: Optional[Any] = None,
    ) -> str:
        """Insert a recurring-task definition.  Returns its ``id``.

        Args:
            recurring: Dictionary with ``id``, ``task_type``, ``payload``,
                ``cron_expression``, ``route``, ``priority``, ``retry_policy``,
                ``enabled``, ``next_run_at``, ``last_run_at``, ``created_at``.
            conn: Optional explicit connection/transaction to use.

        Raises:
            ValueError: If ``cron_expression`` is invalid.
            TaskError: If the ``id`` already exists.
        """
        _validate_not_empty(recurring.get("id"), "id")
        _validate_not_empty(recurring.get("task_type"), "task_type")
        _validate_cron_expression(recurring.get("cron_expression"))

        dialect = self._dialect
        values = ", ".join(
            (
                dialect.json_param(index)
                if column in JSON_TASK_COLUMNS
                else dialect.placeholder(index)
            )
            for index, column in enumerate(RECURRING_COLUMNS, start=1)
        )
        conflict = dialect.insert_ignore(["id"])
        query = f"""
            INSERT INTO conductor_recurring_tasks (
                {", ".join(RECURRING_COLUMNS)}
            ) VALUES (
                {values}
            )
            {conflict}
            {"RETURNING id" if dialect.supports_returning else ""}
        """
        target = conn if conn is not None else self._pool
        recurring_values = (
            recurring["id"],
            recurring["task_type"],
            _json(recurring.get("payload", {})),
            recurring["cron_expression"],
            recurring.get("route", "default"),
            recurring.get("priority", 0),
            _json(recurring.get("retry_policy", {})),
            recurring.get("enabled", True),
            recurring.get("next_run_at", datetime.now(timezone.utc)),
            recurring.get("last_run_at"),
            recurring.get("created_at", datetime.now(timezone.utc)),
        )
        if dialect.supports_returning:
            row = await target.fetchrow(query, *recurring_values)
            inserted = row is not None
        else:
            # ``ON DUPLICATE KEY UPDATE id = id`` reports 0 affected rows when
            # the row already exists (MySQL has no ``ON CONFLICT DO NOTHING``).
            result = await target.execute(query, *recurring_values)
            inserted = dialect.normalize_rowcount(result) != 0
        if not inserted:
            raise TaskError(f"Recurring task '{recurring['id']}' already exists")
        return cast(str, recurring["id"])

    async def select_recurring_task(
        self,
        recurring_id: str,
        *,
        conn: Optional[Any] = None,
    ) -> Optional[dict[str, Any]]:
        """Fetch a single recurring definition by ID."""
        _validate_not_empty(recurring_id, "recurring_id")

        dialect = self._dialect
        query = "SELECT * FROM conductor_recurring_tasks " f"WHERE id = {dialect.placeholder(1)}"
        target = conn if conn is not None else self._pool
        row = await target.fetchrow(query, recurring_id)
        return self._row_to_dict(row) if row else None

    async def select_recurring_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
        *,
        conn: Optional[Any] = None,
    ) -> list[dict[str, Any]]:
        """Fetch recurring definitions, newest first."""
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        dialect = self._dialect
        query = f"""
            SELECT * FROM conductor_recurring_tasks
            ORDER BY created_at DESC
            LIMIT {dialect.placeholder(1)} OFFSET {dialect.placeholder(2)}
        """
        target = conn if conn is not None else self._pool
        rows = await target.fetch(query, limit, offset)
        return [self._row_to_dict(r) for r in rows]

    async def select_due_recurring_tasks(
        self,
        now: datetime,
        limit: int = 50,
        *,
        conn: Optional[Any] = None,
    ) -> list[dict[str, Any]]:
        """Fetch enabled recurring definitions due at or before *now*.

        Locks the rows with ``FOR UPDATE SKIP LOCKED`` so multiple schedulers
        can safely claim definitions without double-firing.  The caller must
        hold the enclosing transaction until the next-run advancement commits.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")

        dialect = self._dialect
        query = f"""
            SELECT * FROM conductor_recurring_tasks
            WHERE enabled = TRUE AND next_run_at <= {dialect.placeholder(1)}
            ORDER BY next_run_at ASC
            LIMIT {dialect.placeholder(2)}
            {dialect.for_update_skip_locked()}
        """
        target = conn if conn is not None else self._pool
        rows = await target.fetch(query, now, limit)
        return [self._row_to_dict(r) for r in rows]

    async def update_recurring_run(
        self,
        recurring_id: str,
        *,
        last_run_at: datetime,
        next_run_at: datetime,
        conn: Optional[Any] = None,
    ) -> bool:
        """Record a fired run and advance the next run time.

        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(recurring_id, "recurring_id")

        dialect = self._dialect
        query = (
            "UPDATE conductor_recurring_tasks "
            f"SET last_run_at = {dialect.placeholder(1)}, "
            f"next_run_at = {dialect.placeholder(2)} "
            f"WHERE id = {dialect.placeholder(3)}"
        )
        target = conn if conn is not None else self._pool
        result = await target.execute(query, last_run_at, next_run_at, recurring_id)
        return dialect.normalize_rowcount(result, verb="UPDATE") == 1

    async def set_recurring_enabled(
        self,
        recurring_id: str,
        enabled: bool,
        *,
        conn: Optional[Any] = None,
    ) -> bool:
        """Enable or disable a recurring definition.

        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(recurring_id, "recurring_id")

        dialect = self._dialect
        query = (
            "UPDATE conductor_recurring_tasks "
            f"SET enabled = {dialect.placeholder(1)} "
            f"WHERE id = {dialect.placeholder(2)}"
        )
        target = conn if conn is not None else self._pool
        result = await target.execute(query, enabled, recurring_id)
        return dialect.normalize_rowcount(result, verb="UPDATE") == 1

    async def delete_recurring_task(
        self,
        recurring_id: str,
        *,
        conn: Optional[Any] = None,
    ) -> bool:
        """Delete a recurring definition.

        Returns ``True`` if a row was deleted.
        """
        _validate_not_empty(recurring_id, "recurring_id")

        dialect = self._dialect
        query = "DELETE FROM conductor_recurring_tasks " f"WHERE id = {dialect.placeholder(1)}"
        target = conn if conn is not None else self._pool
        result = await target.execute(query, recurring_id)
        return dialect.normalize_rowcount(result, verb="DELETE") == 1

    # ==================================================================
    # Worker queries
    # ==================================================================

    async def upsert_worker(self, worker: dict[str, Any]) -> str:
        """Insert or update a worker record.  Returns the ``worker_id``."""
        _validate_not_empty(worker.get("worker_id"), "worker_id")

        dialect = self._dialect
        conflict = dialect.upsert(
            ["worker_id"],
            [
                ("status", "EXCLUDED.status"),
                ("current_task_id", "EXCLUDED.current_task_id"),
                ("uptime_seconds", "EXCLUDED.uptime_seconds"),
                ("tasks_processed_total", "EXCLUDED.tasks_processed_total"),
                ("tasks_failed_total", "EXCLUDED.tasks_failed_total"),
                ("last_heartbeat", "EXCLUDED.last_heartbeat"),
            ],
        )
        query = f"""
            INSERT INTO conductor_workers (
                worker_id, status, current_task_id, hostname, pid,
                uptime_seconds, tasks_processed_total, tasks_failed_total,
                last_heartbeat, started_at
            ) VALUES (
                {dialect.placeholder_list(1, 10)}
            )
            {conflict}
            {"RETURNING worker_id" if dialect.supports_returning else ""}
        """
        values = (
            worker["worker_id"],
            worker.get("status", "idle"),
            worker.get("current_task_id"),
            worker.get("hostname", ""),
            worker.get("pid", 0),
            worker.get("uptime_seconds", 0.0),
            worker.get("tasks_processed_total", 0),
            worker.get("tasks_failed_total", 0),
            worker.get("last_heartbeat", datetime.now(timezone.utc)),
            worker.get("started_at", datetime.now(timezone.utc)),
        )
        if dialect.supports_returning:
            row = await self._pool.fetchrow(query, *values)
            if row is None:
                raise TaskError(f"Failed to upsert worker '{worker['worker_id']}'")
        else:
            # The upsert is idempotent, so no affected-row count check is made.
            await self._pool.execute(query, *values)
        return cast(str, worker["worker_id"])

    async def select_worker(self, worker_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single worker by ID."""
        _validate_not_empty(worker_id, "worker_id")

        dialect = self._dialect
        query = f"SELECT * FROM conductor_workers WHERE worker_id = {dialect.placeholder(1)}"
        row = await self._pool.fetchrow(query, worker_id)
        return self._row_to_dict(row) if row else None

    async def select_active_workers(self, heartbeat_timeout: float = 30.0) -> list[dict[str, Any]]:
        """Fetch workers with heartbeat within *heartbeat_timeout* seconds."""
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be > 0")

        dialect = self._dialect
        query = f"""
            SELECT * FROM conductor_workers
            WHERE {dialect.interval_ago("last_heartbeat", 1)}
            ORDER BY last_heartbeat DESC
        """
        rows = await self._pool.fetch(query, heartbeat_timeout)
        return [self._row_to_dict(r) for r in rows]

    async def select_all_workers(
        self,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch all registered workers, newest heartbeat first.

        Unlike ``select_active_workers``, this includes workers whose
        heartbeat has lapsed (the dashboard's "all workers" view).

        Args:
            limit: Maximum number of workers to return.
            offset: Number of workers to skip (for pagination).

        Returns:
            A list of worker dicts ordered by ``last_heartbeat DESC``.

        Raises:
            ValueError: If ``limit``/``offset`` are invalid.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        dialect = self._dialect
        query = f"""
            SELECT * FROM conductor_workers
            ORDER BY {dialect.nulls_last("last_heartbeat DESC")}
            LIMIT {dialect.placeholder(1)} OFFSET {dialect.placeholder(2)}
        """
        rows = await self._pool.fetch(query, limit, offset)
        return [self._row_to_dict(r) for r in rows]

    async def update_worker_heartbeat(
        self,
        worker_id: str,
        *,
        status: Optional[str] = None,
        current_task_id: Optional[str] = None,
        uptime_seconds: Optional[float] = None,
        tasks_processed_total: Optional[int] = None,
        tasks_failed_total: Optional[int] = None,
    ) -> bool:
        """Update a worker's heartbeat and optionally other fields.

        ``last_heartbeat`` is always set to the current time.
        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(worker_id, "worker_id")
        if status is not None:
            _validate_worker_status(status)

        dialect = self._dialect
        set_parts = [f"last_heartbeat = {dialect.now()}"]
        params: list[Any] = []
        idx = 1

        if status is not None:
            set_parts.append(f"status = {dialect.placeholder(idx)}")
            params.append(status)
            idx += 1
        if current_task_id is not None:
            set_parts.append(f"current_task_id = {dialect.placeholder(idx)}")
            params.append(current_task_id)
            idx += 1
        if uptime_seconds is not None:
            set_parts.append(f"uptime_seconds = {dialect.placeholder(idx)}")
            params.append(uptime_seconds)
            idx += 1
        if tasks_processed_total is not None:
            set_parts.append(f"tasks_processed_total = {dialect.placeholder(idx)}")
            params.append(tasks_processed_total)
            idx += 1
        if tasks_failed_total is not None:
            set_parts.append(f"tasks_failed_total = {dialect.placeholder(idx)}")
            params.append(tasks_failed_total)
            idx += 1

        query = (
            f"UPDATE conductor_workers SET {', '.join(set_parts)} "
            f"WHERE worker_id = {dialect.placeholder(idx)}"
        )
        params.append(worker_id)

        result = await self._pool.execute(query, *params)
        return dialect.normalize_rowcount(result, verb="UPDATE") == 1

    # ==================================================================
    # Maintenance queries
    # ==================================================================

    async def count_tasks_by_status(self, status: str) -> int:
        """Count tasks with the given status."""
        _validate_task_status(status)

        dialect = self._dialect
        row = await self._pool.fetchval(
            "SELECT COUNT(*) FROM conductor_tasks " f"WHERE status = {dialect.placeholder(1)}",
            status,
        )
        return row or 0

    async def count_dlq_tasks(self, include_discarded: bool = False) -> int:
        """Count tasks in the dead-letter queue."""
        if include_discarded:
            row = await self._pool.fetchval("SELECT COUNT(*) FROM conductor_dead_letter")
        else:
            row = await self._pool.fetchval(
                "SELECT COUNT(*) FROM conductor_dead_letter WHERE discarded = FALSE",
            )
        return row or 0

    async def count_active_workers(self, heartbeat_timeout: float = 30.0) -> int:
        """Count workers with a recent heartbeat."""
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be > 0")

        dialect = self._dialect
        row = await self._pool.fetchval(
            "SELECT COUNT(*) FROM conductor_workers "
            f"WHERE {dialect.interval_ago('last_heartbeat', 1)}",
            heartbeat_timeout,
        )
        return row or 0

    async def delete_completed_tasks(self, older_than: datetime) -> int:
        """Delete completed tasks older than *older_than*.

        Returns the number of deleted rows.
        """
        dialect = self._dialect
        result = await self._pool.execute(
            "DELETE FROM conductor_tasks "
            f"WHERE status = 'completed' AND completed_at < {dialect.placeholder(1)}",
            older_than,
        )
        return dialect.normalize_rowcount(result, verb="DELETE")

    # ==================================================================
    # Internal helpers
    # ==================================================================

    def _task_filters(
        self,
        *,
        status: Optional[str],
        route: Optional[str],
        task_type: Optional[str],
        search: Optional[str],
    ) -> tuple[list[str], list[Any], int]:
        """Build the shared ``WHERE`` fragments for the dashboard task queries.

        Returns:
            A ``(conditions, params, next_index)`` tuple, where the conditions
            and parameters are in textual order.
        """
        if status is not None:
            _validate_task_status(status)
        if route is not None:
            _validate_not_empty(route, "route")
        if task_type is not None:
            _validate_not_empty(task_type, "task_type")
        if search is not None:
            _validate_not_empty(search, "search")

        dialect = self._dialect
        conditions: list[str] = []
        params: list[Any] = []
        idx = 1

        if status is not None:
            conditions.append(f"status = {dialect.placeholder(idx)}")
            params.append(status)
            idx += 1
        if route is not None:
            conditions.append(f"route = {dialect.placeholder(idx)}")
            params.append(route)
            idx += 1
        if task_type is not None:
            conditions.append(f"task_type = {dialect.placeholder(idx)}")
            params.append(task_type)
            idx += 1
        if search is not None:
            conditions.append(
                f"({dialect.ilike('task_id', idx)}" f" OR {dialect.ilike('task_type', idx + 1)})"
            )
            params.append(f"%{search}%")
            params.append(f"%{search}%")
            idx += 2

        return conditions, params, idx

    def _row_to_dict(self, row: Any) -> dict[str, Any]:
        """Normalise a backend row into a plain, dialect-independent dict.

        JSON columns are decoded (asyncpg can return JSONB as text on newer
        Pythons) and timestamp columns are turned into aware UTC datetimes
        (SQLite stores them as text).
        """
        if row is None:
            return {}
        return self._dialect.normalize_row(row)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _json(value: Any) -> Optional[str]:
    """Serialize a value to a JSON string, or return ``None``."""
    if value is None:
        return None
    return json.dumps(value, default=str, ensure_ascii=False)
