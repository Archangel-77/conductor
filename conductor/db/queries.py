"""
Type-safe SQL query builders for Conductor.

Uses raw SQL with asyncpg parameter placeholders (``$1``, ``$2``, …)
wrapped in small builder classes.  Every public method validates its
arguments before constructing a query.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional, cast

from conductor.db.connection import DatabasePool
from conductor.exceptions import TaskError

logger = logging.getLogger("conductor.db.queries")


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _validate_not_empty(value: Any, name: str) -> None:
    if not value or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"{name} must not be empty")


def _validate_task_status(status: str) -> None:
    valid = {"pending", "processing", "completed", "failed", "retrying"}
    if status not in valid:
        raise ValueError(
            f"Invalid task status '{status}'. Must be one of {valid}"
        )


def _validate_worker_status(status: str) -> None:
    valid = {"idle", "processing", "unhealthy"}
    if status not in valid:
        raise ValueError(
            f"Invalid worker status '{status}'. Must be one of {valid}"
        )


# ---------------------------------------------------------------------------
# QueryBuilder
# ---------------------------------------------------------------------------

class QueryBuilder:
    """Collects all database query operations for Conductor.

    Every method accepts and returns plain Python objects (dicts, dataclass
    fields, primitives).  No knowledge of the caller's model classes is
    required – but the helper methods expect dictionaries with the same
    keys used in the schema (see ``schema.py``).
    """

    def __init__(self, pool: DatabasePool) -> None:
        self._pool = pool

    # ==================================================================
    # Task queries
    # ==================================================================

    async def insert_task(self, task: dict[str, Any]) -> str:
        """Insert a new task row and return its ``task_id``.

        Expects a dictionary with at least:
        ``task_id``, ``task_type``, ``payload``, ``status``, ``priority``,
        ``route``, ``attempt``, ``max_retries``, ``retry_policy``,
        ``scheduled_for``, ``created_at``.
        """
        _validate_not_empty(task.get("task_id"), "task_id")
        _validate_not_empty(task.get("task_type"), "task_type")

        query = """
            INSERT INTO conductor_tasks (
                task_id, task_type, payload, status, priority, route,
                attempt, max_retries, retry_policy, scheduled_for,
                worker_id, result, error_message, created_at,
                started_at, completed_at
            ) VALUES (
                $1, $2, $3::jsonb, $4, $5, $6,
                $7, $8, $9::jsonb, $10,
                $11, $12::jsonb, $13, $14,
                $15, $16
            )
            ON CONFLICT (task_id) DO NOTHING
            RETURNING task_id
        """

        row = await self._pool.fetchrow(
            query,
            task["task_id"],
            task["task_type"],
            _json(task.get("payload", {})),
            task.get("status", "pending"),
            task.get("priority", 0),
            task.get("route", "default"),
            task.get("attempt", 0),
            task.get("max_retries", 3),
            _json(task.get("retry_policy", {})),
            task.get("scheduled_for"),
            task.get("worker_id"),
            _json(task.get("result")),
            task.get("error_message"),
            task.get("created_at", datetime.now(timezone.utc)),
            task.get("started_at"),
            task.get("completed_at"),
        )

        if row is None:
            raise TaskError(f"Task '{task['task_id']}' already exists")

        return cast(str, row["task_id"])

    async def select_task(self, task_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single task by its ID, returning a dict or ``None``."""
        _validate_not_empty(task_id, "task_id")

        query = "SELECT * FROM conductor_tasks WHERE task_id = $1"
        row = await self._pool.fetchrow(query, task_id)
        return _row_to_dict(row) if row else None

    async def select_pending_tasks(
        self,
        limit: int = 10,
        offset: int = 0,
        route: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Fetch pending tasks that are eligible for processing.

        Filters by:
        - ``status = 'pending'``
        - ``scheduled_for`` is ``NULL`` or in the past
        - optional ``route`` filter

        Orders by ``priority DESC, created_at ASC``.
        Uses ``FOR UPDATE SKIP LOCKED`` for atomic poll semantics.
        """
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if offset < 0:
            raise ValueError("offset must be >= 0")

        if route:
            query = """
                SELECT * FROM conductor_tasks
                WHERE status = 'pending'
                  AND (scheduled_for IS NULL OR scheduled_for <= NOW())
                  AND route = $3
                ORDER BY priority DESC, created_at ASC
                LIMIT $1 OFFSET $2
                FOR UPDATE SKIP LOCKED
            """
            rows = await self._pool.fetch(query, limit, offset, route)
        else:
            query = """
                SELECT * FROM conductor_tasks
                WHERE status = 'pending'
                  AND (scheduled_for IS NULL OR scheduled_for <= NOW())
                ORDER BY priority DESC, created_at ASC
                LIMIT $1 OFFSET $2
                FOR UPDATE SKIP LOCKED
            """
            rows = await self._pool.fetch(query, limit, offset)

        return [_row_to_dict(r) for r in rows]

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

        query = """
            SELECT * FROM conductor_tasks
            WHERE status = $1
            ORDER BY created_at DESC
            LIMIT $2 OFFSET $3
        """
        rows = await self._pool.fetch(query, status, limit, offset)
        return [_row_to_dict(r) for r in rows]

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

        # Build SET clauses dynamically
        set_parts = ["status = $2"]
        params: list[Any] = [task_id, new_status]
        idx = 3

        if worker_id is not None:
            set_parts.append(f"worker_id = ${idx}")
            params.append(worker_id)
            idx += 1
        if result is not None:
            set_parts.append(f"result = ${idx}::jsonb")
            params.append(_json(result))
            idx += 1
        if error_message is not None:
            set_parts.append(f"error_message = ${idx}")
            params.append(error_message)
            idx += 1
        if attempt is not None:
            set_parts.append(f"attempt = ${idx}")
            params.append(attempt)
            idx += 1
        if scheduled_for is not None:
            set_parts.append(f"scheduled_for = ${idx}")
            params.append(scheduled_for)
            idx += 1

        # Auto-set timestamps based on status
        if new_status == "processing":
            set_parts.append(f"started_at = ${idx}")
            params.append(datetime.now(timezone.utc))
            idx += 1
        elif new_status in ("completed", "failed"):
            set_parts.append(f"completed_at = ${idx}")
            params.append(datetime.now(timezone.utc))
            idx += 1

        query = (
            f"UPDATE conductor_tasks SET {', '.join(set_parts)} "
            f"WHERE task_id = $1"
        )

        result_tag = await self._pool.execute(query, *params)
        return "UPDATE 1" in result_tag

    # ==================================================================
    # Retry history queries
    # ==================================================================

    async def insert_retry_record(self, record: dict[str, Any]) -> str:
        """Insert a retry history record and return its ``id``."""
        _validate_not_empty(record.get("id"), "id")
        _validate_not_empty(record.get("task_id"), "task_id")

        query = """
            INSERT INTO conductor_retries
                (id, task_id, attempt, error_message, scheduled_at, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id
        """
        row = await self._pool.fetchrow(
            query,
            record["id"],
            record["task_id"],
            record["attempt"],
            record.get("error_message"),
            record["scheduled_at"],
            record.get("created_at", datetime.now(timezone.utc)),
        )
        if row is None:
            raise TaskError(f"Failed to insert retry record '{record['id']}'")
        return cast(str, row["id"])

    async def select_retries_for_task(
        self, task_id: str
    ) -> list[dict[str, Any]]:
        """Fetch all retry records for a given task, ordered by attempt."""
        _validate_not_empty(task_id, "task_id")

        query = """
            SELECT * FROM conductor_retries
            WHERE task_id = $1
            ORDER BY attempt ASC
        """
        rows = await self._pool.fetch(query, task_id)
        return [_row_to_dict(r) for r in rows]

    # ==================================================================
    # Dead-letter queue queries
    # ==================================================================

    async def insert_dlq_task(self, dlq: dict[str, Any]) -> str:
        """Move a task into the dead-letter queue.  Returns the ``task_id``."""
        _validate_not_empty(dlq.get("task_id"), "task_id")
        _validate_not_empty(dlq.get("task_type"), "task_type")

        query = """
            INSERT INTO conductor_dead_letter (
                task_id, task_type, payload, error_message, attempts,
                retry_policy, moved_at, discarded, discard_reason, discarded_at
            ) VALUES (
                $1, $2, $3::jsonb, $4, $5,
                $6::jsonb, $7, $8, $9, $10
            )
            ON CONFLICT (task_id) DO UPDATE SET
                error_message = EXCLUDED.error_message,
                attempts     = EXCLUDED.attempts,
                moved_at     = EXCLUDED.moved_at,
                discarded    = FALSE,
                discard_reason = NULL,
                discarded_at = NULL
            RETURNING task_id
        """
        row = await self._pool.fetchrow(
            query,
            dlq["task_id"],
            dlq["task_type"],
            _json(dlq.get("payload", {})),
            dlq.get("error_message"),
            dlq.get("attempts", 0),
            _json(dlq.get("retry_policy", {})),
            dlq.get("moved_at", datetime.now(timezone.utc)),
            dlq.get("discarded", False),
            dlq.get("discard_reason"),
            dlq.get("discarded_at"),
        )
        if row is None:
            raise TaskError(f"Failed to insert DLQ task '{dlq['task_id']}'")
        return cast(str, row["task_id"])

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

        if include_discarded:
            query = """
                SELECT * FROM conductor_dead_letter
                ORDER BY moved_at DESC
                LIMIT $1 OFFSET $2
            """
            rows = await self._pool.fetch(query, limit, offset)
        else:
            query = """
                SELECT * FROM conductor_dead_letter
                WHERE discarded = FALSE
                ORDER BY moved_at DESC
                LIMIT $1 OFFSET $2
            """
            rows = await self._pool.fetch(query, limit, offset)

        return [_row_to_dict(r) for r in rows]

    async def select_dlq_task(self, task_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single DLQ entry by task ID."""
        _validate_not_empty(task_id, "task_id")

        query = "SELECT * FROM conductor_dead_letter WHERE task_id = $1"
        row = await self._pool.fetchrow(query, task_id)
        return _row_to_dict(row) if row else None

    async def delete_dlq_task(self, task_id: str) -> bool:
        """Remove a task from the dead-letter queue entirely.

        Returns ``True`` if a row was deleted.
        """
        _validate_not_empty(task_id, "task_id")

        result = await self._pool.execute(
            "DELETE FROM conductor_dead_letter WHERE task_id = $1",
            task_id,
        )
        return "DELETE 1" in result

    async def discard_dlq_task(
        self,
        task_id: str,
        reason: Optional[str] = None,
    ) -> bool:
        """Mark a DLQ task as discarded (soft-delete).

        Returns ``True`` if a row was updated.
        """
        _validate_not_empty(task_id, "task_id")

        result = await self._pool.execute(
            """
            UPDATE conductor_dead_letter
            SET discarded = TRUE,
                discard_reason = $2,
                discarded_at = NOW()
            WHERE task_id = $1
            """,
            task_id,
            reason,
        )
        return "UPDATE 1" in result

    # ==================================================================
    # Worker queries
    # ==================================================================

    async def upsert_worker(self, worker: dict[str, Any]) -> str:
        """Insert or update a worker record.  Returns the ``worker_id``."""
        _validate_not_empty(worker.get("worker_id"), "worker_id")

        query = """
            INSERT INTO conductor_workers (
                worker_id, status, current_task_id, hostname, pid,
                uptime_seconds, tasks_processed_total, tasks_failed_total,
                last_heartbeat, started_at
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7, $8,
                $9, $10
            )
            ON CONFLICT (worker_id) DO UPDATE SET
                status               = EXCLUDED.status,
                current_task_id      = EXCLUDED.current_task_id,
                uptime_seconds       = EXCLUDED.uptime_seconds,
                tasks_processed_total = EXCLUDED.tasks_processed_total,
                tasks_failed_total   = EXCLUDED.tasks_failed_total,
                last_heartbeat       = EXCLUDED.last_heartbeat
            RETURNING worker_id
        """
        row = await self._pool.fetchrow(
            query,
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
        if row is None:
            raise TaskError(f"Failed to upsert worker '{worker['worker_id']}'")
        return cast(str, row["worker_id"])

    async def select_worker(self, worker_id: str) -> Optional[dict[str, Any]]:
        """Fetch a single worker by ID."""
        _validate_not_empty(worker_id, "worker_id")

        query = "SELECT * FROM conductor_workers WHERE worker_id = $1"
        row = await self._pool.fetchrow(query, worker_id)
        return _row_to_dict(row) if row else None

    async def select_active_workers(
        self, heartbeat_timeout: float = 30.0
    ) -> list[dict[str, Any]]:
        """Fetch workers with heartbeat within *heartbeat_timeout* seconds."""
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be > 0")

        query = """
            SELECT * FROM conductor_workers
            WHERE last_heartbeat >= NOW() - MAKE_INTERVAL(secs => $1)
            ORDER BY last_heartbeat DESC
        """
        rows = await self._pool.fetch(query, heartbeat_timeout)
        return [_row_to_dict(r) for r in rows]

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

        set_parts = ["last_heartbeat = NOW()"]
        params: list[Any] = []
        idx = 2

        if status is not None:
            set_parts.append(f"status = ${idx}")
            params.append(status)
            idx += 1
        if current_task_id is not None:
            set_parts.append(f"current_task_id = ${idx}")
            params.append(current_task_id)
            idx += 1
        if uptime_seconds is not None:
            set_parts.append(f"uptime_seconds = ${idx}")
            params.append(uptime_seconds)
            idx += 1
        if tasks_processed_total is not None:
            set_parts.append(f"tasks_processed_total = ${idx}")
            params.append(tasks_processed_total)
            idx += 1
        if tasks_failed_total is not None:
            set_parts.append(f"tasks_failed_total = ${idx}")
            params.append(tasks_failed_total)
            idx += 1

        if not params:
            # Only updating the heartbeat timestamp
            result = await self._pool.execute(
                "UPDATE conductor_workers"
                " SET last_heartbeat = NOW() WHERE worker_id = $1",
                worker_id,
            )
            return "UPDATE 1" in result

        query = (
            f"UPDATE conductor_workers SET {', '.join(set_parts)} "
            f"WHERE worker_id = $1"
        )
        result = await self._pool.execute(query, worker_id, *params)
        return "UPDATE 1" in result

    # ==================================================================
    # Maintenance queries
    # ==================================================================

    async def count_tasks_by_status(self, status: str) -> int:
        """Count tasks with the given status."""
        _validate_task_status(status)

        row = await self._pool.fetchval(
            "SELECT COUNT(*) FROM conductor_tasks WHERE status = $1",
            status,
        )
        return row or 0

    async def count_dlq_tasks(
        self, include_discarded: bool = False
    ) -> int:
        """Count tasks in the dead-letter queue."""
        if include_discarded:
            row = await self._pool.fetchval(
                "SELECT COUNT(*) FROM conductor_dead_letter"
            )
        else:
            row = await self._pool.fetchval(
                "SELECT COUNT(*) FROM conductor_dead_letter"
                " WHERE discarded = FALSE",
            )
        return row or 0

    async def count_active_workers(
        self, heartbeat_timeout: float = 30.0
    ) -> int:
        """Count workers with a recent heartbeat."""
        if heartbeat_timeout <= 0:
            raise ValueError("heartbeat_timeout must be > 0")

        row = await self._pool.fetchval(
            "SELECT COUNT(*) FROM conductor_workers "
            "WHERE last_heartbeat >= NOW() - MAKE_INTERVAL(secs => $1)",
            heartbeat_timeout,
        )
        return row or 0

    async def delete_completed_tasks(self, older_than: datetime) -> int:
        """Delete completed tasks older than *older_than*.

        Returns the number of deleted rows.
        """
        result = await self._pool.execute(
            "DELETE FROM conductor_tasks"
            " WHERE status = 'completed' AND completed_at < $1",
            older_than,
        )
        # Extract the integer from the "DELETE N" tag
        parts = result.split()
        return int(parts[1]) if len(parts) == 2 else 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _json(value: Any) -> Optional[str]:
    """Serialize a value to a JSON string, or return ``None``."""
    if value is None:
        return None
    return json.dumps(value, default=str, ensure_ascii=False)


def _row_to_dict(row: Any) -> dict[str, Any]:
    """Convert an asyncpg ``Record`` to a plain dict.

    asyncpg ``Record`` objects are dict-like but not serialisable via
    ``json.dumps`` out-of-the-box.  This normalises them.

    On Python 3.14+, asyncpg 0.31 returns ``JSON`` / ``JSONB`` columns
    as plain strings rather than parsed ``dict`` objects.  We
    auto-deserialise any string value that looks like JSON so that
    callers always receive ``dict`` for JSONB fields.
    """
    result: dict[str, Any] = dict(row) if row is not None else {}
    for key, val in result.items():
        if isinstance(val, str) and len(val) > 0 and val[0] in ("{", "["):
            try:
                result[key] = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                pass  # keep original string
    return result
