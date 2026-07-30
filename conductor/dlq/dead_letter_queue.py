"""
Dead Letter Queue implementation for Conductor.

Provides the ``DeadLetterQueue`` class — a high-level API for inspecting,
retrying, and discarding tasks that have exhausted their retry attempts.

Typical usage::

    async with DeadLetterQueue(database_url="postgresql://...") as dlq:
        tasks = await dlq.list_tasks(limit=10)
        for task in tasks:
            if task.attempts < 5:
                await dlq.retry_task(task.task_id)
            else:
                await dlq.discard_task(
                    task.task_id, reason="Too many failures"
                )
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from conductor.core.models import DLQTask, utc_now
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.exceptions import TaskError

logger = logging.getLogger("conductor.dlq.dead_letter_queue")


class DeadLetterQueue:
    """High-level interface for managing dead-letter queue tasks.

    Manages a PostgreSQL connection pool internally and provides
    async context manager support.  All public methods are async.

    Typical usage::

        async with DeadLetterQueue(database_url="postgresql://...") as dlq:
            tasks = await dlq.list_tasks()
            await dlq.retry_task(task_id)
            await dlq.discard_task(task_id, reason="Fixed upstream")
    """

    def __init__(
        self,
        database_url: str,
        *,
        log_level: str = "INFO",
        pool_min_size: int = 2,
        pool_max_size: int = 10,
        pool_timeout: float = 30.0,
        command_timeout: float = 60.0,
    ) -> None:
        self._database_url = database_url

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
            "DeadLetterQueue connected to database.",
            extra={"component": "DeadLetterQueue"},
        )

    async def disconnect(self) -> None:
        """Close the database connection."""
        await self._pool.disconnect()
        self._connected = False
        logger.info(
            "DeadLetterQueue disconnected.",
            extra={"component": "DeadLetterQueue"},
        )

    @property
    def is_connected(self) -> bool:
        """``True`` if the DLQ is connected to the database."""
        return self._connected and self._pool.is_connected

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> DeadLetterQueue:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------
    # DLQ queries
    # ------------------------------------------------------------------

    async def list_tasks(
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

    async def get_task(self, task_id: str) -> Optional[DLQTask]:
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

    async def retry_task(self, task_id: str) -> str:
        """Retry a task from the dead-letter queue.

        Removes the task from the DLQ and resets the corresponding
        ``conductor_tasks`` row to ``pending`` with ``attempt=0``,
        ``worker_id=NULL``, and ``scheduled_for`` set to now so it
        can be picked up by a worker immediately.

        Args:
            task_id: The task to retry.

        Returns:
            The task ID that was retried.

        Raises:
            TaskError: If the task is not found in the DLQ.
        """
        self._require_connected()

        # Verify the task exists in the DLQ
        dlq_row = await self._query.select_dlq_task(task_id)
        if dlq_row is None:
            raise TaskError(f"Task '{task_id}' not found in the dead-letter queue.")

        now = utc_now()

        # Remove from DLQ
        await self._query.delete_dlq_task(task_id)

        # Reset the task in conductor_tasks to pending
        # If the task was hard-deleted from conductor_tasks (CASCADE from
        # retries), we re-insert it with a minimal record.
        existing_task = await self._query.select_task(task_id)
        if existing_task is not None:
            await self._query.update_task_status(
                task_id,
                "pending",
                error_message=None,
                attempt=0,
                scheduled_for=now,
            )
            # Explicitly clear worker_id and error_message since
            # update_task_status skips fields set to None.
            await self._query.clear_task_worker_id(task_id)
            await self._query.clear_task_error_message(task_id)
        else:
            # Re-insert the task with original data
            from conductor.core.models import RetryPolicy

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
            "Task %s retried from DLQ (resubmitted as pending).",
            task_id,
            extra={"task_id": task_id},
        )
        return task_id

    async def discard_task(
        self,
        task_id: str,
        reason: Optional[str] = None,
    ) -> None:
        """Mark a DLQ task as permanently discarded.

        This is a soft-delete — the task remains in the database but
        will be excluded from ``list_tasks()`` by default.

        Args:
            task_id: The task to discard.
            reason: Optional explanation for the discard.

        Raises:
            TaskError: If the task is not found in the DLQ.
        """
        self._require_connected()

        # Verify the task exists
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

    async def count(self, include_discarded: bool = False) -> int:
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
    # Internal helpers
    # ------------------------------------------------------------------

    @property
    def _query(self) -> QueryBuilder:
        """Return the ``QueryBuilder``, raising ``TaskError`` if not connected."""
        if not self._connected or self._pool.is_connected is False:
            raise TaskError(
                "DeadLetterQueue is not connected. Call connect() or use "
                "'async with DeadLetterQueue(...)'."
            )
        assert self._queries is not None
        return self._queries

    def _require_connected(self) -> None:
        """Raise ``TaskError`` if the DLQ is not connected."""
        if not self._connected or self._pool.is_connected is False:
            raise TaskError(
                "DeadLetterQueue is not connected. Call connect() or use "
                "'async with DeadLetterQueue(...)'."
            )
