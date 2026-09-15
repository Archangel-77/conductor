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
from conductor.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitState,
)
from conductor.core.queue import _task_to_db_dict
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.exceptions import TracingError, WorkerError
from conductor.observability.metrics import (
    inc_tasks_completed,
    inc_tasks_failed,
    inc_tasks_rejected,
    inc_tasks_retried,
    observe_task_duration,
    set_circuit_breaker_open,
)
from conductor.observability.health import HealthChecker
from conductor.observability.metrics import MetricsExporter
from conductor.observability import tracing
from conductor.observability.tracing import TracingConfig

logger = logging.getLogger("conductor.core.worker")

# Type alias for a task handler — an async callable that receives the
# task payload and returns an optional result dict.
HandlerFunc = Callable[[dict[str, Any]], Awaitable[Optional[dict[str, Any]]]]

_POLL_BATCH_SIZE = 10
"""Maximum number of tasks claimed in a single poll cycle."""

_STALE_CLAIM_FLOOR = 30.0
"""Minimum age before an unowned/orphaned claim is reclaimed (seconds)."""


class Worker:
    """Poll-based task worker that dispatches work to registered handlers.

    Manages its own database connection pool.  Can be used as an async
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
        busy_timeout: float = 5.0,
        heartbeat_interval: float = 10.0,
        graceful_shutdown_timeout: float = 30.0,
        metrics_port: int = 8000,
        metrics_enabled: bool = True,
        health_enabled: bool = True,
        enable_scheduler: bool = False,
        grpc_port: int = 50051,
        grpc_enabled: bool = False,
        grpc_max_message_size: Optional[int] = None,
        api_port: int = 8080,
        api_enabled: bool = False,
        api_key: Optional[str] = None,
        circuit_breaker_enabled: bool = False,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
        circuit_breaker_overrides: Optional[dict[str, CircuitBreakerConfig]] = None,
        tracing_config: Optional[TracingConfig] = None,
        stale_claim_timeout: Optional[float] = None,
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
        # Claims are taken atomically, so a crashed worker can strand a task in
        # 'processing'; anything older than this (with no heartbeat from its
        # owner) is returned to 'pending' by the next poll.
        self._stale_claim_timeout = (
            stale_claim_timeout
            if stale_claim_timeout is not None
            else max(3.0 * heartbeat_interval, _STALE_CLAIM_FLOOR)
        )
        self._next_reclaim_at = 0.0
        # ``None`` means the worker polls *all* routes (no route filter).
        self._routes: Optional[list[str]] = list(routes) if routes else None
        self._heartbeat_interval = heartbeat_interval
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._metrics_port = metrics_port
        self._metrics_enabled = metrics_enabled
        self._health_enabled = health_enabled
        self._enable_scheduler = enable_scheduler
        self._grpc_port = grpc_port
        self._grpc_enabled = grpc_enabled
        self._grpc_max_message_size = grpc_max_message_size
        self._api_port = api_port
        self._api_enabled = api_enabled
        self._api_key = api_key
        self._log_level = log_level

        # Circuit breaker (per-task-type, in-memory)
        self._circuit_breaker_enabled = circuit_breaker_enabled
        self._circuit_breaker_config = circuit_breaker_config or CircuitBreakerConfig()
        self._circuit_breaker_registry: Optional[CircuitBreakerRegistry] = (
            CircuitBreakerRegistry(
                enabled=circuit_breaker_enabled,
                default_config=self._circuit_breaker_config,
                overrides=circuit_breaker_overrides,
            )
            if circuit_breaker_enabled
            else None
        )

        # Tracing (optional; the span path is a no-op when disabled)
        self._tracing_config = tracing_config

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
            busy_timeout=busy_timeout,
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
        self._recurring_task: Optional[asyncio.Task[None]] = None
        self._recurring_scheduler: Optional[Any] = None
        self._grpc_server: Optional[Any] = None
        self._dashboard_server: Optional[Any] = None

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

        # Start the recurring scheduler background task (optional)
        if self._enable_scheduler:
            from conductor.recurring.scheduler import RecurringScheduler

            self._recurring_scheduler = RecurringScheduler(
                database_url=self._database_url,
            )
            self._recurring_task = asyncio.create_task(
                self._recurring_scheduler.run(),
                name=f"recurring-scheduler-{self._worker_id}",
            )

        # Start distributing tracing (optional)
        if self._tracing_config is not None and self._tracing_config.enabled:
            try:
                tracing.setup_tracing(self._tracing_config)
            except TracingError as exc:
                logger.warning(
                    "Tracing requested but unavailable: %s. Worker will continue "
                    "without tracing.",
                    exc,
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

        # Start the gRPC server (optional)
        if self._grpc_enabled:
            from conductor.grpc.server import GrpcWorkerServer

            self._grpc_server = GrpcWorkerServer(
                self,
                port=self._grpc_port,
                max_message_size=self._grpc_max_message_size,
            )
            try:
                await self._grpc_server.start()
            except OSError as exc:
                logger.warning(
                    "Failed to start gRPC server on port %d: %s. "
                    "Worker will continue without it.",
                    self._grpc_port,
                    exc,
                )
                self._grpc_server = None

        # Start the dashboard server (optional)
        if self._api_enabled:
            from conductor.api.server import DashboardServer

            self._dashboard_server = DashboardServer(
                self._pool,
                health_checker=HealthChecker(self._pool),
                api_key=self._api_key,
                port=self._api_port,
                log_level=self._log_level,
            )
            try:
                await self._dashboard_server.start()
            except OSError as exc:
                logger.warning(
                    "Failed to start dashboard server on port %d: %s. "
                    "Worker will continue without it.",
                    self._api_port,
                    exc,
                )
                self._dashboard_server = None

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
    # Public handler / execution helpers
    # ------------------------------------------------------------------

    def get_handler(self, task_type: str) -> Optional[HandlerFunc]:
        """Return the registered handler for *task_type* (or ``None``).

        Args:
            task_type: The task type whose handler to fetch.
        """
        return self._handlers.get(task_type)

    def has_handler(self, task_type: str) -> bool:
        """Return whether a handler is registered for *task_type*."""
        return task_type in self._handlers

    def register_remote_handler(self, task_type: str) -> bool:
        """Register a remote (non-executable) handler for *task_type*.

        Idempotent — returns ``True`` if newly registered.  The marker handler
        raises a clear error if invoked in-process (e.g. via gRPC
        ``ProcessTask``).

        Args:
            task_type: The task type to declare as handled remotely.
        """
        if task_type in self._handlers:
            return False

        async def _remote_handler(_payload: dict[str, Any]) -> dict[str, Any]:
            raise WorkerError(
                f"task_type '{task_type}' is registered remotely and "
                "cannot be executed in-process"
            )

        self._handlers[task_type] = _remote_handler
        return True

    async def persist_and_execute(
        self,
        *,
        task_id: str,
        task_type: str,
        payload: dict[str, Any],
        traceparent: Optional[str] = None,
    ) -> Optional[Task]:
        """Insert a task row (if needed) and execute it through the lifecycle.

        Runs the normal ``_execute_task`` path (status transitions, metrics,
        retry/DLQ handling) and returns the resulting
        :class:`~conductor.core.models.Task`, or ``None`` if the row cannot
        be found afterwards.

        Args:
            task_id: The task ID (must not collide with an existing row that
                belongs to a different task).
            task_type: The task type to execute.
            payload: The payload to store and pass to the handler.
            traceparent: Optional W3C ``traceparent`` from the caller (gRPC),
                so the execution continues the caller's trace.
        """
        queries = self._queries
        assert queries is not None
        now = utc_now()

        existing = await queries.select_task(task_id)
        if existing is None:
            task = Task(
                task_id=task_id,
                task_type=task_type,
                payload=payload,
                status=TaskStatus.PENDING,
                created_at=now,
                traceparent=traceparent,
            )
            await queries.insert_task(_task_to_db_dict(task))
        else:
            task = Task.from_dict(existing)

        await self._execute_task(task)

        row = await queries.select_task(task_id)
        return Task.from_dict(row) if row else None

    # ------------------------------------------------------------------
    # Task polling
    # ------------------------------------------------------------------

    async def _poll_and_execute(self) -> None:
        """Claim eligible tasks and execute them.

        This is the inner loop body: 1) reclaim claims left behind by dead
        workers (throttled), 2) atomically claim eligible tasks, 3) acquire the
        semaphore for each, 4) spawn execution tasks.
        """
        await self._maybe_reclaim_stale_claims()

        # Never claim more rows than this worker can start right now: a claim
        # moves the row to 'processing', so over-claiming would hide work from
        # other workers while it waits for a free slot.
        capacity = self._concurrency - len(self._in_flight_tasks)
        if capacity <= 0:
            return

        tasks = await self._poll_tasks(limit=min(_POLL_BATCH_SIZE, capacity))
        if not tasks:
            return

        logger.debug("Claimed %d task(s).", len(tasks))

        for task in tasks:
            # Acquire the semaphore before spawning
            await self._semaphore.acquire()  # type: ignore[union-attr]

            exec_task = asyncio.create_task(
                self._execute_task(task),
                name=f"exec-{task.task_id}",
            )
            self._in_flight_tasks.add(exec_task)
            exec_task.add_done_callback(self._on_execution_done)

    async def _poll_tasks(self, limit: int = _POLL_BATCH_SIZE) -> list[Task]:
        """Atomically claim up to *limit* tasks that are eligible to run.

        Claiming moves the rows to ``processing`` in the same statement (or
        transaction) that selects them, so two workers polling concurrently can
        never receive the same task.

        If the worker polls *all* routes (``routes=None``) a single unfiltered
        claim is used; otherwise each configured route is claimed in turn, never
        claiming more than *limit* rows in total.

        Args:
            limit: Maximum number of tasks to claim in this poll cycle.

        Returns:
            A list of ``Task`` objects (may be empty).
        """
        queries = self._queries
        assert queries is not None

        if self._routes is None:
            rows = await queries.claim_pending_tasks(
                limit=limit,
                worker_id=self._worker_id,
            )
            return [Task.from_dict(row) for row in rows]

        claimed: list[Task] = []
        for route in self._routes:
            remaining = limit - len(claimed)
            if remaining <= 0:
                break
            rows = await queries.claim_pending_tasks(
                limit=remaining,
                worker_id=self._worker_id,
                route=route,
            )
            claimed.extend(Task.from_dict(row) for row in rows)

        # Sort all tasks by priority DESC, created_at ASC (mimicking DB order)
        claimed.sort(key=lambda t: (-t.priority, t.created_at))
        return claimed[:limit]

    async def _maybe_reclaim_stale_claims(self) -> None:
        """Reclaim stranded claims, at most once per stale-claim window."""
        if self._queries is None:
            return
        now = time.monotonic()
        if now < self._next_reclaim_at:
            return
        self._next_reclaim_at = now + self._stale_claim_timeout
        reclaimed = await self._queries.reclaim_stale_tasks(self._stale_claim_timeout)
        if reclaimed:
            logger.warning(
                "Reclaimed %d task(s) stranded by dead workers.",
                reclaimed,
                extra={"count": reclaimed},
            )

    # ------------------------------------------------------------------
    # Task execution
    # ------------------------------------------------------------------

    async def _execute_task(self, task: Task) -> None:
        """Execute a task inside a trace span.

        The span continues the submitting trace when the task carries a W3C
        ``traceparent`` (schema v6), so every attempt is a child of the
        submission span — attempts show up as siblings, which matches reality:
        they are independent executions of one submitted task.

        Args:
            task: The ``Task`` to execute.
        """
        with tracing.span(
            tracing.SPAN_EXECUTE,
            parent=tracing.extract_traceparent(task.traceparent),
            attributes={
                tracing.ATTR_TASK_ID: task.task_id,
                tracing.ATTR_TASK_TYPE: task.task_type,
                tracing.ATTR_TASK_ROUTE: task.route,
                tracing.ATTR_TASK_PRIORITY: task.priority,
                tracing.ATTR_TASK_ATTEMPT: task.attempt,
                tracing.ATTR_WORKER_ID: self._worker_id,
            },
        ):
            await self._execute_task_traced(task)

    async def _execute_task_traced(self, task: Task) -> None:
        """Execute a single task: update status, call handler, record result.

        Args:
            task: The ``Task`` to execute.
        """
        queries = self._queries
        assert queries is not None

        self._current_task_id = task.task_id
        task_type = task.task_type

        # Skip execution while the circuit for this task type is open.
        breaker = self._get_circuit_breaker(task_type)
        if breaker is not None and not breaker.allow_request():
            inc_tasks_rejected(task_type)
            self._set_circuit_breaker_metric(task_type, breaker)
            # The task was claimed (status 'processing') while polling, so the
            # claim has to be handed back — otherwise it would be stranded.
            await queries.release_claim(task.task_id)
            logger.warning(
                "Circuit open for task_type '%s'; releasing claim on task %s (left pending).",
                task_type,
                task.task_id,
                extra={"task_id": task.task_id, "task_type": task_type},
            )
            self._current_task_id = None
            return

        # Update task status to "processing"
        await self._update_status(task.task_id, TaskStatus.PROCESSING)

        # Find the registered handler
        handler = self._handlers.get(task_type)
        if handler is None:
            error_msg = (
                f"No handler registered for task_type '{task_type}'. "
                f"Registered types: {list(self._handlers.keys())}"
            )
            tracing.record_error(error_msg)
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
            tracing.record_error(error_msg, handler_exc)
            if breaker is not None:
                breaker.record_failure()
                self._set_circuit_breaker_metric(task_type, breaker)
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
        tracing.mark_success()

        if breaker is not None:
            breaker.record_success()
            self._set_circuit_breaker_metric(task_type, breaker)

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

    # ------------------------------------------------------------------
    # Circuit breaker helpers
    # ------------------------------------------------------------------

    def _get_circuit_breaker(self, task_type: str) -> Optional[CircuitBreaker]:
        """Return the circuit breaker for *task_type* (``None`` if disabled)."""
        if self._circuit_breaker_registry is None:
            return None
        return self._circuit_breaker_registry.get(task_type)

    def _set_circuit_breaker_metric(self, task_type: str, breaker: CircuitBreaker) -> None:
        """Update the circuit-breaker-open gauge for *task_type*."""
        set_circuit_breaker_open(
            task_type,
            1 if breaker.state is not CircuitState.CLOSED else 0,
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

            tracing.add_event(
                "retry.scheduled",
                {
                    tracing.ATTR_TASK_ATTEMPT: new_attempt,
                    "retry.delay_seconds": delay,
                },
            )

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
            tracing.add_event(
                "task.dlq",
                {
                    tracing.ATTR_TASK_ATTEMPT: new_attempt,
                    "dlq.reason": error_message,
                },
            )
            # Build the dict manually so datetime objects stay native
            await queries.insert_dlq_task(
                {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "payload": task.payload,
                    "error_message": error_message,
                    "attempts": new_attempt,
                    "retry_policy": task.retry_policy.to_dict(),
                    "route": task.route,
                    "priority": task.priority,
                    "depends_on": task.depends_on,
                    "traceparent": task.traceparent,
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

            # Tasks that depend on this one can never run — mark them blocked.
            await self._propagate_terminal_dependency(task.task_id)

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

    async def _propagate_terminal_dependency(self, task_id: str) -> None:
        """Mark pending tasks that depend on *task_id* as ``blocked``.

        Propagates transitively: tasks blocked here may themselves have
        dependents, which are marked in turn.  The loop is bounded to
        protect against pathological chains.

        Args:
            task_id: The task that reached a terminal failure.
        """
        queries = self._queries
        assert queries is not None

        error_message = f"dependency '{task_id}' failed"
        frontier = [task_id]
        seen: set[str] = set()
        with tracing.span(
            tracing.SPAN_BLOCK_DEPENDENTS,
            attributes={"dependency.task_id": task_id},
        ) as active:
            for _ in range(200):
                newly_blocked: list[str] = []
                for tid in frontier:
                    for dep in await queries.mark_dependents_blocked(tid, error_message):
                        if dep not in seen:
                            seen.add(dep)
                            newly_blocked.append(dep)
                            logger.warning(
                                "Task %s blocked because its dependency '%s' failed.",
                                dep,
                                tid,
                                extra={"task_id": dep, "error": error_message},
                            )
                if not newly_blocked:
                    break
                frontier = newly_blocked
            if seen:
                active.set_attribute("blocked.count", len(seen))
                active.set_attribute("blocked.task_ids", sorted(seen))

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

        # Stop the recurring scheduler background task (if enabled)
        if self._recurring_task is not None and not self._recurring_task.done():
            self._recurring_task.cancel()
            try:
                await self._recurring_task
            except asyncio.CancelledError:
                pass

        # Stop the gRPC server (if enabled)
        if self._grpc_server is not None:
            await self._grpc_server.stop()
            self._grpc_server = None

        # Stop the dashboard server (if enabled)
        if self._dashboard_server is not None:
            await self._dashboard_server.stop()
            self._dashboard_server = None

        # Flush pending spans (no-op when tracing is disabled)
        if self._tracing_config is not None:
            tracing.shutdown_tracing()

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
            - ``routes`` — routes this worker polls (``None`` = all routes)
            - ``registered_handlers`` — list of registered task types
            - ``connected`` — whether the database is connected
            - ``grpc_enabled`` — whether a gRPC server is configured
            - ``grpc_port`` — the configured gRPC port
            - ``grpc_serving`` — whether the gRPC server is currently up
            - ``api_enabled`` — whether a dashboard server is configured
            - ``api_port`` — the configured dashboard port
            - ``api_serving`` — whether the dashboard server is up
            - ``circuit_breaker_enabled`` — whether a circuit breaker is configured
            - ``circuit_breaker_open`` — task types whose circuit is open/half-open
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
            "routes": list(self._routes) if self._routes else None,
            "registered_handlers": list(self._handlers.keys()),
            "connected": self.is_connected,
            "grpc_enabled": self._grpc_enabled,
            "grpc_port": self._grpc_port,
            "grpc_serving": (
                self._grpc_server.is_serving if self._grpc_server is not None else False
            ),
            "api_enabled": self._api_enabled,
            "api_port": self._api_port,
            "api_serving": (
                self._dashboard_server.get_status()["serving"]
                if self._dashboard_server is not None
                else False
            ),
            "circuit_breaker_enabled": self._circuit_breaker_enabled,
            "circuit_breaker_open": (
                self._circuit_breaker_registry.open_types()
                if self._circuit_breaker_registry is not None
                else []
            ),
            "tracing_enabled": tracing.is_enabled(),
            "tracing_exporter": (
                self._tracing_config.exporter
                if self._tracing_config is not None and tracing.is_enabled()
                else "none"
            ),
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
