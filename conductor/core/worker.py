"""
Worker implementation for Conductor.

Provides the ``Worker`` class — a PostgreSQL-backed task worker that polls
for pending tasks, dispatches them to registered handlers, manages
concurrency, sends heartbeats, and supports graceful shutdown.

Typical usage::

    from conductor.core.worker import Worker

    worker = Worker(database_url="postgresql://...")

    @worker.task("send_email")
    async def send_email(payload: dict) -> dict:
        # send the email ...
        return {"status": "sent"}

    await worker.run()
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Optional

from conductor.core.models import (
    Task,
    TaskStatus,
    WorkerStatus,
    generate_task_id,
    get_hostname,
    utc_now,
)
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.observability.metrics import (
    inc_tasks_completed,
    inc_tasks_failed,
    inc_tasks_retried,
    observe_task_duration,
)
from conductor.observability.health import HealthChecker
from conductor.observability.metrics import MetricsExporter

logger = logging.getLogger("conductor.core.worker")

# Type alias for a task handler — an async callable that receives the
# task payload and returns an optional result dict.
HandlerFunc = Callable[[dict[str, Any]], Awaitable[Optional[dict[str, Any]]]]


class Worker:
    """Poll-based task worker that dispatches work to registered handlers.

    Manages its own PostgreSQL connection pool.  Can be used as an async
    context manager::

        async with Worker(database_url="postgresql://...") as worker:
            @worker.task("process")
            async def process(payload: dict) -> dict:
                ...
            await worker.run()
    """

    def __init__(
        self,
        database_url: str,
        *,
        worker_id: Optional[str] = None,
        concurrency: int = 10,
        poll_interval: float = 0.5,
        routes: Optional[list[str]] = None,
        log_level: str = "INFO",
        pool_min_size: int = 2,
        pool_max_size: int = 10,
        pool_timeout: float = 30.0,
        command_timeout: float = 60.0,
        heartbeat_interval: float = 10.0,
        graceful_shutdown_timeout: float = 30.0,
        metrics_port: int = 8000,
        metrics_enabled: bool = True,
        health_enabled: bool = True,
    ) -> None:
        # Worker identity
        hostname = get_hostname()
        pid = os.getpid()
        self._worker_id: str = worker_id or f"{hostname}-{pid}"
        self._hostname = hostname
        self._pid = pid

        # Configuration
        self._concurrency = concurrency
        self._poll_interval = poll_interval
        self._routes = routes or ["default"]
        self._heartbeat_interval = heartbeat_interval
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._metrics_port = metrics_port
        self._metrics_enabled = metrics_enabled
        self._health_enabled = health_enabled

        # Apply log level
        logging.getLogger("conductor").setLevel(log_level.upper())

        # Database
        self._database_url = database_url
        self._pool = DatabasePool(
            dsn=database_url,
            min_size=pool_min_size,
            max_size=pool_max_size,
            timeout=pool_timeout,
            command_timeout=command_timeout,
        )
        self._queries: Optional[QueryBuilder] = None
        self._connected = False

        # Handler registry: task_type -> HandlerFunc
        self._handlers: dict[str, HandlerFunc] = {}

        # Concurrency
        self._semaphore: Optional[asyncio.Semaphore] = None

        # Lifecycle
        self._shutdown_requested = False
        self._in_flight_tasks: set[asyncio.Task[Any]] = set()
        self._heartbeat_task: Optional[asyncio.Task[None]] = None

        # Statistics
        self._started_at: Optional[datetime] = None
        self._tasks_processed_total = 0
        self._tasks_failed_total = 0
        self._current_task_id: Optional[str] = None

        # Observability
        self._metrics_exporter: Optional[MetricsExporter] = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def worker_id(self) -> str:
        """The unique identifier for this worker."""
        return self._worker_id

    @property
    def is_running(self) -> bool:
        """``True`` if the worker is currently running."""
        return self._started_at is not None and not self._shutdown_requested

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
            "Worker '%s' connected to database.",
            self._worker_id,
            extra={"worker_id": self._worker_id},
        )

    async def disconnect(self) -> None:
        """Close the database connection."""
        await self._pool.disconnect()
        self._connected = False
        logger.info(
            "Worker '%s' disconnected.",
            self._worker_id,
            extra={"worker_id": self._worker_id},
        )

    @property
    def is_connected(self) -> bool:
        """``True`` if the worker is connected to the database."""
        return self._connected and self._pool.is_connected

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> Worker:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.disconnect()

    # ------------------------------------------------------------------
    # Task handler registration
    # ------------------------------------------------------------------

    def task(
        self,
        task_type: str,
    ) -> Callable[[HandlerFunc], HandlerFunc]:
        """Decorator that registers an async handler for *task_type*.

        The decorated function **must** be an async callable that accepts a
        single ``dict`` argument (the task payload) and returns an optional
        ``dict`` (the result).

        Example::

            @worker.task("send_email")
            async def handle_email(payload: dict) -> dict:
                ...

        Args:
            task_type: The task type string to register this handler for.

        Returns:
            A decorator that registers the handler and returns it unchanged.

        Raises:
            ValueError: If ``task_type`` is empty or a handler is already
                        registered for this type.
        """
        if not task_type or not task_type.strip():
            raise ValueError("task_type must not be empty")

        def decorator(func: HandlerFunc) -> HandlerFunc:
            if not inspect.iscoroutinefunction(func):
                raise ValueError(
                    f"Handler for '{task_type}' must be an async function. "
                    f"Got {type(func).__name__}."
                )

            if task_type in self._handlers:
                raise ValueError(
                    f"A handler for task_type '{task_type}' is already " f"registered."
                )

            # Validate handler signature: it must accept exactly 1 positional
            # argument (payload).  We do a best-effort check here – some
            # callable signatures are hard to inspect at runtime.
            sig = inspect.signature(func)
            params = list(sig.parameters.values())
            # Must have at least one parameter (payload)
            if len(params) < 1:
                raise ValueError(
                    f"Handler for '{task_type}' must accept at least one "
                    f"argument (the payload dict). Found {len(params)}."
                )
            # The first parameter should accept a dict-like argument
            # (we don't enforce types strictly, just check it exists)

            self._handlers[task_type] = func
            logger.debug(
                "Handler registered for task_type '%s': %s",
                task_type,
                func.__name__,
            )
            return func

        return decorator

    # ------------------------------------------------------------------
    # Worker event loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Start the worker event loop.

        Connects to the database, registers the worker, begins polling for
        tasks, and continues indefinitely until a shutdown signal is received.
        Handles ``SIGTERM`` and ``SIGINT`` gracefully.

        This method does **not** return until shutdown is complete.
        """
        if not self.is_connected:
            await self.connect()

        self._shutdown_requested = False
        self._started_at = utc_now()
        self._semaphore = asyncio.Semaphore(self._concurrency)

        # Register worker in the database
        await self._register_worker()

        # Start heartbeat background task
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"worker-heartbeat-{self._worker_id}",
        )

        # Start metrics/health HTTP server
        if self._metrics_enabled or self._health_enabled:
            health_checker = HealthChecker(self._pool)
            self._metrics_exporter = MetricsExporter(
                pool=self._pool,
                health_checker=health_checker,
                port=self._metrics_port,
            )
            try:
                await self._metrics_exporter.start()
            except OSError as exc:
                logger.warning(
                    "Failed to start metrics/health server on port %d: %s. "
                    "Worker will continue without it.",
                    self._metrics_port,
                    exc,
                )
                self._metrics_exporter = None

        # Set up signal handlers for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:

                def _make_handler(sig_h: signal.Signals) -> Callable[[], None]:
                    def _handler() -> None:
                        asyncio.ensure_future(self._handle_signal(sig_h))

                    return _handler

                loop.add_signal_handler(sig, _make_handler(sig))
            except NotImplementedError:
                # Signal handlers not supported on this platform (e.g., Windows)
                logger.warning(
                    "Signal handler not supported for %s on this platform.",
                    sig,
                )

        logger.info(
            "Worker '%s' started. Polling every %.2fs, concurrency=%d, routes=%s",
            self._worker_id,
            self._poll_interval,
            self._concurrency,
            self._routes,
        )

        try:
            while not self._shutdown_requested:
                await self._poll_and_execute()
                # Wait for the poll interval before next poll
                await asyncio.sleep(self._poll_interval)
        finally:
            await self._shutdown()

    async def run_once(self) -> None:
        """Run a single poll-and-execute cycle.

        Useful for testing and debugging.  Connects to the database
        if not already connected, polls for tasks once, executes them,
        and returns.

        This method does **not** start the heartbeat task or register
        signal handlers.
        """
        if not self.is_connected:
            await self.connect()

        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._concurrency)

        if self._started_at is None:
            self._started_at = utc_now()

        await self._register_worker()
        # Execute tasks synchronously in run_once (not via background tasks)
        # so the worker's pool stays open until all tasks complete.
        batch = await self._poll_tasks()
        if batch:
            # Acquire semaphore for each task in sequence, execute, release
            for task_item in batch:
                await self._semaphore.acquire()
                try:
                    await self._execute_task(task_item)
                finally:
                    self._semaphore.release()

    # ------------------------------------------------------------------
    # Task polling
    # ------------------------------------------------------------------

    async def _poll_and_execute(self) -> None:
        """Poll for pending tasks and execute them.

        This is the inner loop body: 1) query the database for pending
        tasks, 2) acquire the semaphore for each, 3) spawn execution
        tasks.
        """
        tasks = await self._poll_tasks()
        if not tasks:
            return

        logger.debug("Polled %d pending task(s).", len(tasks))

        for task in tasks:
            # Acquire the semaphore before spawning
            await self._semaphore.acquire()  # type: ignore[union-attr]

            exec_task = asyncio.create_task(
                self._execute_task(task),
                name=f"exec-{task.task_id}",
            )
            self._in_flight_tasks.add(exec_task)
            exec_task.add_done_callback(self._on_execution_done)

    async def _poll_tasks(self) -> list[Task]:
        """Query the database for pending tasks eligible for processing.

        Returns:
            A list of ``Task`` objects (may be empty).
        """
        queries = self._queries
        assert queries is not None

        all_tasks: list[Task] = []
        for route in self._routes:
            rows = await queries.select_pending_tasks(
                limit=10,
                offset=0,
                route=route,
            )
            for row in rows:
                all_tasks.append(Task.from_dict(row))

        # Sort all tasks by priority DESC, created_at ASC (mimicking DB order)
        all_tasks.sort(key=lambda t: (-t.priority, t.created_at))

        # Limit batch size (cap at 10 per poll cycle)
        batch_size = min(len(all_tasks), 10)
        return all_tasks[:batch_size]

    # ------------------------------------------------------------------
    # Task execution
    # ------------------------------------------------------------------

    async def _execute_task(self, task: Task) -> None:
        """Execute a single task: update status, call handler, record result.

        Args:
            task: The ``Task`` to execute.
        """
        queries = self._queries
        assert queries is not None

        self._current_task_id = task.task_id
        task_type = task.task_type

        # Update task status to "processing"
        await self._update_status(task.task_id, TaskStatus.PROCESSING)

        # Find the registered handler
        handler = self._handlers.get(task_type)
        if handler is None:
            error_msg = (
                f"No handler registered for task_type '{task_type}'. "
                f"Registered types: {list(self._handlers.keys())}"
            )
            await self._handle_task_failure(task, error_msg)
            self._tasks_failed_total += 1
            self._current_task_id = None
            return

        # Execute the handler with the task payload
        start_time = time.monotonic()
        result, handler_exc = await _call_handler(handler, task.payload)
        if handler_exc is not None:
            duration_ms = (time.monotonic() - start_time) * 1000
            duration_sec = duration_ms / 1000.0
            error_msg = str(handler_exc)
            logger.error(
                "Task %s (%s) failed after %.0fms: %s",
                task.task_id,
                task_type,
                duration_ms,
                error_msg,
                extra={
                    "task_id": task.task_id,
                    "task_type": task_type,
                    "duration_ms": duration_ms,
                    "error": error_msg,
                },
            )
            inc_tasks_failed(task_type)
            observe_task_duration(task_type, duration_sec)
            await self._handle_task_failure(task, error_msg)
            self._tasks_failed_total += 1
            self._current_task_id = None
            return

        duration_ms = (time.monotonic() - start_time) * 1000
        duration_sec = duration_ms / 1000.0

        # Update task status to "completed"
        await queries.update_task_status(
            task.task_id,
            "completed",
            worker_id=self._worker_id,
            result=result or {},
        )

        self._tasks_processed_total += 1
        self._current_task_id = None

        inc_tasks_completed(task_type)
        observe_task_duration(task_type, duration_sec)

        logger.info(
            "Task %s (%s) completed in %.0fms.",
            task.task_id,
            task_type,
            duration_ms,
            extra={
                "task_id": task.task_id,
                "task_type": task_type,
                "duration_ms": duration_ms,
            },
        )

    async def _handle_task_failure(
        self,
        task: Task,
        error_message: str,
    ) -> None:
        """Handle a task failure by recording it and potentially retrying.

        If retries are exhausted, the task is moved to the dead-letter queue.
        """
        queries = self._queries
        assert queries is not None

        new_attempt = task.attempt + 1

        if new_attempt <= task.max_retries:
            # Schedule a retry
            delay = calculate_backoff_delay(
                attempt=new_attempt,
                strategy=task.retry_policy.backoff_strategy,
                initial_delay=task.retry_policy.initial_delay,
                max_delay=task.retry_policy.max_delay,
            )
            scheduled_for_ts = utc_now().timestamp() + delay
            scheduled_dt = datetime.fromtimestamp(scheduled_for_ts, tz=timezone.utc)

            # Record the retry in the retries table
            # Build the dict manually so datetime objects stay native
            # (to_dict() serializes them to strings, which asyncpg rejects).
            await queries.insert_retry_record(
                {
                    "id": generate_task_id(),
                    "task_id": task.task_id,
                    "attempt": new_attempt,
                    "error_message": error_message,
                    "scheduled_at": scheduled_dt,
                    "created_at": utc_now(),
                }
            )

            # Update the task status to "retrying" and set scheduled_for
            await queries.update_task_status(
                task.task_id,
                "retrying",
                worker_id=self._worker_id,
                error_message=error_message,
                attempt=new_attempt,
                scheduled_for=scheduled_dt,
            )

            inc_tasks_retried(task.task_type)

            logger.info(
                "Task %s failed (attempt %d/%d). Retrying in %.2fs.",
                task.task_id,
                new_attempt,
                task.max_retries,
                delay,
                extra={
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "attempt": new_attempt,
                    "delay": delay,
                },
            )
        else:
            # Max retries exceeded — move to dead-letter queue
            now = utc_now()
            # Build the dict manually so datetime objects stay native
            await queries.insert_dlq_task(
                {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "payload": task.payload,
                    "error_message": error_message,
                    "attempts": new_attempt,
                    "retry_policy": task.retry_policy.to_dict(),
                    "moved_at": now,
                }
            )

            # Update the task status to "failed"
            await queries.update_task_status(
                task.task_id,
                "failed",
                worker_id=self._worker_id,
                error_message=error_message,
                attempt=new_attempt,
            )

            logger.warning(
                "Task %s (%s) moved to DLQ after %d attempts. Last error: %s",
                task.task_id,
                task.task_type,
                new_attempt,
                error_message,
                extra={
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "attempts": new_attempt,
                    "error": error_message,
                },
            )

    async def _update_status(
        self,
        task_id: str,
        status: TaskStatus,
    ) -> None:
        """Update a task's status in the database.

        Args:
            task_id: The task to update.
            status: The new status.
        """
        queries = self._queries
        assert queries is not None
        await queries.update_task_status(
            task_id,
            status.value,
            worker_id=self._worker_id,
        )

    # ------------------------------------------------------------------
    # Callback helpers
    # ------------------------------------------------------------------

    def _on_execution_done(self, task: asyncio.Task[Any]) -> None:
        """Callback invoked when an execution task completes.

        Releases the semaphore and removes the task from the in-flight set.
        """
        self._in_flight_tasks.discard(task)
        self._semaphore.release()  # type: ignore[union-attr]

    async def _wait_for_in_flight(self) -> None:
        """Wait for all in-flight execution tasks to complete."""
        if self._in_flight_tasks:
            await asyncio.gather(*self._in_flight_tasks, return_exceptions=True)

    # ------------------------------------------------------------------
    # Worker heartbeat
    # ------------------------------------------------------------------

    async def _heartbeat_loop(self) -> None:
        """Background coroutine that sends heartbeats at regular intervals.

        Runs concurrently with the polling loop.  Updates the worker record
        in the database every ``heartbeat_interval`` seconds.
        """
        queries = self._queries
        assert queries is not None

        while not self._shutdown_requested:
            uptime = (utc_now() - self._started_at).total_seconds() if self._started_at else 0.0

            # Determine current status
            if self._current_task_id is not None:
                current_status = WorkerStatus.PROCESSING.value
            else:
                current_status = WorkerStatus.IDLE.value

            # Send heartbeat — a single failure shouldn't kill the loop
            hb_error = await _send_heartbeat(
                queries,
                self._worker_id,
                current_status,
                self._current_task_id,
                uptime,
                self._tasks_processed_total,
                self._tasks_failed_total,
            )
            if hb_error is None:
                logger.debug(
                    "Heartbeat sent for worker '%s' (status=%s, "
                    "processed=%d, failed=%d, uptime=%.0fs)",
                    self._worker_id,
                    current_status,
                    self._tasks_processed_total,
                    self._tasks_failed_total,
                    uptime,
                    extra={
                        "worker_id": self._worker_id,
                        "status": current_status,
                        "tasks_processed": self._tasks_processed_total,
                        "tasks_failed": self._tasks_failed_total,
                        "uptime_seconds": uptime,
                    },
                )
            else:
                logger.error(
                    "Heartbeat failed for worker '%s': %s",
                    self._worker_id,
                    hb_error,
                    extra={
                        "worker_id": self._worker_id,
                        "error": str(hb_error),
                    },
                )

            await asyncio.sleep(self._heartbeat_interval)

    # ------------------------------------------------------------------
    # Graceful shutdown
    # ------------------------------------------------------------------

    async def shutdown(self) -> None:
        """Initiate a graceful shutdown.

        Sets the shutdown flag, stops accepting new tasks, waits for
        in-flight tasks to complete (with configurable timeout), and
        disconnects from the database.
        """
        if self._shutdown_requested:
            return
        self._shutdown_requested = True
        await self._shutdown()

    async def _shutdown(self) -> None:
        """Internal shutdown routine.

        Called from both ``shutdown()`` and the ``run()`` finally block.
        """
        logger.info("Worker '%s' shutting down...", self._worker_id)

        # Stop the heartbeat task
        if self._heartbeat_task is not None and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # Wait for in-flight tasks to complete (with timeout)
        if self._in_flight_tasks:
            logger.info(
                "Waiting for %d in-flight task(s) to complete (timeout: %.0fs)...",
                len(self._in_flight_tasks),
                self._graceful_shutdown_timeout,
            )
            _done, pending = await asyncio.wait(
                self._in_flight_tasks,
                timeout=self._graceful_shutdown_timeout,
                return_when=asyncio.ALL_COMPLETED,
            )

            if pending:
                logger.warning(
                    "%d task(s) did not complete within the shutdown timeout. Cancelling...",
                    len(pending),
                )
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        # Send a final heartbeat with "unhealthy" status
        queries = self._queries
        if queries is not None:
            uptime = (utc_now() - self._started_at).total_seconds() if self._started_at else 0.0
            final_error = await _send_heartbeat(
                queries,
                self._worker_id,
                WorkerStatus.UNHEALTHY.value,
                None,
                uptime,
                self._tasks_processed_total,
                self._tasks_failed_total,
            )
            if final_error is not None:
                logger.debug(
                    "Failed to send final heartbeat for worker '%s': %s",
                    self._worker_id,
                    final_error,
                )

        # Stop metrics/health HTTP server
        if self._metrics_exporter is not None:
            await self._metrics_exporter.stop()

        # Disconnect from database
        await self.disconnect()

        logger.info(
            "Worker '%s' shut down. Processed=%d, Failed=%d",
            self._worker_id,
            self._tasks_processed_total,
            self._tasks_failed_total,
        )

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle a termination signal by initiating graceful shutdown.

        Args:
            sig: The signal received (``SIGTERM`` or ``SIGINT``).
        """
        sig_name = signal.Signals(sig).name
        logger.info(
            "Worker '%s' received signal %s. Initiating graceful shutdown...",
            self._worker_id,
            sig_name,
        )
        await self.shutdown()

    # ------------------------------------------------------------------
    # Worker registration
    # ------------------------------------------------------------------

    async def _register_worker(self) -> None:
        """Register this worker in the ``conductor_workers`` table.

        Creates or updates the worker record with identity information.
        """
        queries = self._queries
        assert queries is not None

        worker_dict = _worker_info_to_db_dict(
            worker_id=self._worker_id,
            hostname=self._hostname,
            pid=self._pid,
            started_at=self._started_at or utc_now(),
        )
        await queries.upsert_worker(worker_dict)
        logger.debug(
            "Worker '%s' registered (hostname=%s, pid=%d).",
            self._worker_id,
            self._hostname,
            self._pid,
        )

    # ------------------------------------------------------------------
    # Worker status
    # ------------------------------------------------------------------

    def get_status(self) -> dict[str, Any]:
        """Return a snapshot of the worker's current health and statistics.

        Returns:
            A dictionary with the following keys:

            - ``worker_id`` — unique worker identifier
            - ``status`` — ``idle``, ``processing``, or ``unhealthy``
            - ``uptime_seconds`` — seconds since the worker started
            - ``tasks_processed_total`` — total successfully processed tasks
            - ``tasks_failed_total`` — total failed tasks
            - ``current_task_id`` — task currently being processed (or ``None``)
            - ``concurrency`` — maximum concurrent tasks
            - ``in_flight`` — number of tasks currently being executed
            - ``routes`` — routes this worker polls
            - ``registered_handlers`` — list of registered task types
            - ``connected`` — whether the database is connected
        """
        uptime = 0.0
        if self._started_at is not None:
            uptime = (utc_now() - self._started_at).total_seconds()

        if self._current_task_id is not None:
            status = WorkerStatus.PROCESSING.value
        elif self._shutdown_requested:
            status = WorkerStatus.UNHEALTHY.value
        else:
            status = WorkerStatus.IDLE.value

        return {
            "worker_id": self._worker_id,
            "status": status,
            "uptime_seconds": uptime,
            "tasks_processed_total": self._tasks_processed_total,
            "tasks_failed_total": self._tasks_failed_total,
            "current_task_id": self._current_task_id,
            "concurrency": self._concurrency,
            "in_flight": len(self._in_flight_tasks),
            "routes": list(self._routes),
            "registered_handlers": list(self._handlers.keys()),
            "connected": self.is_connected,
        }


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


async def _call_handler(
    handler: HandlerFunc,
    payload: dict[str, Any],
) -> tuple[Optional[dict[str, Any]], Optional[Exception]]:
    """Call ``handler(payload)`` and return ``(result, exception)``."""
    result: Optional[dict[str, Any]] = None
    error: Optional[Exception] = None
    try:
        result = await handler(payload)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        error = exc
    return (result, error)


async def _send_heartbeat(
    queries: QueryBuilder,
    worker_id: str,
    status: str,
    current_task_id: Optional[str],
    uptime_seconds: float,
    tasks_processed_total: int,
    tasks_failed_total: int,
) -> Optional[Exception]:
    """Send a worker heartbeat and return any exception that was raised."""
    try:
        await queries.update_worker_heartbeat(
            worker_id,
            status=status,
            current_task_id=current_task_id,
            uptime_seconds=uptime_seconds,
            tasks_processed_total=tasks_processed_total,
            tasks_failed_total=tasks_failed_total,
        )
        return None
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return exc


def _worker_info_to_db_dict(
    *,
    worker_id: str,
    hostname: str,
    pid: int,
    started_at: datetime,
) -> dict[str, Any]:
    """Convert worker identity fields to a DB-ready dict.

    Keeps ``datetime`` objects as native Python objects (not ISO strings)
    so that asyncpg can bind them to ``TIMESTAMPTZ`` columns.
    """
    return {
        "worker_id": worker_id,
        "status": WorkerStatus.IDLE.value,
        "current_task_id": None,
        "hostname": hostname,
        "pid": pid,
        "uptime_seconds": 0.0,
        "tasks_processed_total": 0,
        "tasks_failed_total": 0,
        "last_heartbeat": None,
        "started_at": started_at,
    }


# ---------------------------------------------------------------------------
# Backoff delay calculation
# ---------------------------------------------------------------------------


def calculate_backoff_delay(
    attempt: int,
    strategy: str = "exponential",
    initial_delay: float = 1.0,
    max_delay: float = 3600.0,
) -> float:
    """Calculate the delay before the given retry *attempt*.

    Args:
        attempt: The retry attempt number (1-based).
        strategy: One of ``"exponential"``, ``"linear"``, or ``"fixed"``.
        initial_delay: Base delay in seconds.
        max_delay: Maximum delay cap in seconds.

    Returns:
        The delay in seconds, capped at *max_delay*.

    Raises:
        ValueError: If *strategy* is unknown.
    """
    if strategy == "exponential":
        delay: float = initial_delay * float(2 ** (attempt - 1))
    elif strategy == "linear":
        delay = initial_delay + (initial_delay * float(attempt - 1))
    elif strategy == "fixed":
        delay = initial_delay
    else:
        raise ValueError(
            f"Unknown backoff strategy '{strategy}'. "
            f"Expected one of: exponential, linear, fixed."
        )
    return min(delay, max_delay)
