"""
Recurring task scheduler for Conductor.

The ``RecurringScheduler`` polls ``conductor_recurring_tasks`` for due
definitions (``enabled`` and ``next_run_at <= now``), creates one ``Task``
instance per fire, then advances the definition's ``next_run_at`` to the
next cron fire time (UTC, skipping missed occurrences).

It can run as a standalone process or be embedded in a ``Worker`` via
``enable_scheduler=True``.

Typical usage::

    from conductor.recurring import RecurringScheduler

    scheduler = RecurringScheduler(database_url="postgresql://...")
    async with scheduler:
        await scheduler.run()   # runs until SIGTERM/SIGINT
"""

from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime
from typing import Any, Optional

from conductor.core.models import (
    RecurringTask,
    Task,
    TaskStatus,
    generate_task_id,
    utc_now,
)
from conductor.core.queue import _next_cron_run, _task_to_db_dict
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.observability.metrics import inc_recurring_fired

logger = logging.getLogger("conductor.recurring.scheduler")


class RecurringScheduler:
    """Creates task instances for due recurring definitions.

    Manages its own PostgreSQL connection pool and supports async context
    manager usage.  All public methods are async.

    Typical usage::

        async with RecurringScheduler(database_url="postgresql://...") as sched:
            await sched.run()
    """

    def __init__(
        self,
        database_url: str,
        *,
        scheduler_interval: float = 1.0,
        poll_batch_size: int = 50,
        log_level: str = "INFO",
        pool_min_size: int = 1,
        pool_max_size: int = 5,
        pool_timeout: float = 30.0,
        command_timeout: float = 60.0,
    ) -> None:
        self._database_url = database_url
        self._scheduler_interval = scheduler_interval
        self._poll_batch_size = poll_batch_size

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

        # Lifecycle
        self._shutdown_requested = False
        self._started_at: Optional[datetime] = None

        # Statistics
        self._tasks_fired_total = 0
        self._last_sweep_at: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def tasks_fired_total(self) -> int:
        """Total number of task instances created so far."""
        return self._tasks_fired_total

    @property
    def is_running(self) -> bool:
        """``True`` if the scheduler loop is currently active."""
        return self._started_at is not None and not self._shutdown_requested

    @property
    def is_connected(self) -> bool:
        """``True`` if the scheduler is connected to the database."""
        return self._connected and self._pool.is_connected

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Connect to the database and ensure the schema exists."""
        await self._pool.connect()
        await SchemaManager(self._pool).ensure_schema()
        self._queries = QueryBuilder(self._pool)
        self._connected = True
        logger.info("RecurringScheduler connected to database.")

    async def disconnect(self) -> None:
        """Close the database connection."""
        await self._pool.disconnect()
        self._connected = False
        logger.info("RecurringScheduler disconnected.")

    async def __aenter__(self) -> RecurringScheduler:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the scheduler loop until shutdown.

        Polls due recurring definitions every ``scheduler_interval`` seconds
        and fires their task instances.  Handles ``SIGTERM`` and ``SIGINT``
        gracefully.  Does **not** return until shutdown completes.
        """
        if not self.is_connected:
            await self.connect()

        self._shutdown_requested = False
        self._started_at = utc_now()

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._request_shutdown)
            except NotImplementedError:
                logger.warning(
                    "Signal handler not supported for %s on this platform.",
                    sig,
                )

        logger.info(
            "RecurringScheduler started. Polling every %.2fs, batch=%d.",
            self._scheduler_interval,
            self._poll_batch_size,
        )

        try:
            while not self._shutdown_requested:
                try:
                    await self._sweep()
                except Exception:
                    logger.exception("Recurring scheduler sweep failed; will retry next interval.")
                await asyncio.sleep(self._scheduler_interval)
        finally:
            await self.shutdown()

    def _request_shutdown(self) -> None:
        """Signal the run loop to shut down gracefully."""
        self._shutdown_requested = True

    async def run_once(self) -> None:
        """Run a single sweep (useful for testing and debugging).

        Connects to the database if not already connected, performs one
        sweep, and returns.  Does **not** start signal handlers.
        """
        if not self.is_connected:
            await self.connect()

        if self._started_at is None:
            self._started_at = utc_now()

        await self._sweep()

    async def shutdown(self) -> None:
        """Graceful shutdown: stop the loop and close the database."""
        self._shutdown_requested = True
        if self.is_connected:
            await self.disconnect()
        logger.info("RecurringScheduler shutdown complete.")

    # ------------------------------------------------------------------
    # Sweep logic
    # ------------------------------------------------------------------

    async def _sweep(self) -> None:
        """Claim due recurring definitions and fire their task instances.

        Runs inside a single transaction: due rows are locked with
        ``FOR UPDATE SKIP LOCKED`` (safe for multiple schedulers), one task
        instance is created per definition, and ``next_run_at`` is advanced
        to the next cron fire time.  If anything fails the transaction is
        rolled back, so no definition is left half-fired.
        """
        queries = self._queries
        assert queries is not None

        now = utc_now()
        fired = 0

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                due_rows = await queries.select_due_recurring_tasks(
                    now,
                    limit=self._poll_batch_size,
                    conn=conn,
                )
                for row in due_rows:
                    recurring = RecurringTask.from_dict(row)
                    await self._fire(recurring, now=now, conn=conn)
                    fired += 1

        self._last_sweep_at = now
        if fired:
            self._tasks_fired_total += fired
            logger.info("Recurring scheduler fired %d task(s).", fired)

    async def _fire(
        self,
        recurring: RecurringTask,
        *,
        now: datetime,
        conn: Any,
    ) -> None:
        """Create one task instance for *recurring* and advance its run time."""
        queries = self._queries
        assert queries is not None

        task = Task(
            task_id=generate_task_id(),
            task_type=recurring.task_type,
            payload=recurring.payload,
            status=TaskStatus.PENDING,
            priority=recurring.priority,
            route=recurring.route,
            retry_policy=recurring.retry_policy,
            attempt=0,
            max_retries=recurring.retry_policy.max_retries,
            created_at=now,
        )
        await queries.insert_task(_task_to_db_dict(task), conn=conn)

        # Advance to the next cron fire after now (UTC, skip missed runs).
        next_run_at = _next_cron_run(recurring.cron_expression, now)
        await queries.update_recurring_run(
            recurring.id,
            last_run_at=now,
            next_run_at=next_run_at,
            conn=conn,
        )

        inc_recurring_fired(recurring.task_type)
        logger.info(
            "Fired recurring task %s (%s) -> task %s. Next run %s.",
            recurring.id,
            recurring.task_type,
            task.task_id,
            next_run_at.isoformat(),
            extra={
                "recurring_id": recurring.id,
                "task_id": task.task_id,
                "task_type": recurring.task_type,
                "next_run_at": next_run_at.isoformat(),
            },
        )

    # ------------------------------------------------------------------
    # Status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Return a snapshot of the scheduler's health and statistics.

        Returns:
            A dictionary with uptime, tasks fired, last sweep time,
            configuration, and connection state.
        """
        uptime = 0.0
        if self._started_at is not None:
            uptime = (utc_now() - self._started_at).total_seconds()

        return {
            "uptime_seconds": uptime,
            "tasks_fired_total": self._tasks_fired_total,
            "last_sweep_at": (self._last_sweep_at.isoformat() if self._last_sweep_at else None),
            "scheduler_interval": self._scheduler_interval,
            "poll_batch_size": self._poll_batch_size,
            "connected": self.is_connected,
        }
