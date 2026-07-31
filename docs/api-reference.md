# API Reference

This reference documents Conductor's public API. All public names are
re-exported from the top-level `conductor` package unless noted.

```python
from conductor import (
    TaskQueue, Worker, DeadLetterQueue,
    Task, TaskStatus, RetryPolicy, DLQTask,
    WorkerInfo, WorkerStatus, RetryRecord,
    BackoffStrategyType, ExponentialBackoff, LinearBackoff, FixedBackoff,
    HealthChecker, HealthResult, HealthStatus,
)
```

---

## TaskQueue

Submit and query tasks. All methods are `async`.

```python
async with TaskQueue(database_url="postgresql://...") as queue:
    task_id = await queue.submit("send_email", {"to": "x@example.com"})
```

### `submit(task_type, payload, *, retry_policy=None, scheduled_for=None, route="default", priority=0, task_id=None)`

Submit a new task.

- `task_type` (`str`): logical type used to route the task to a handler.
- `payload` (`dict`): arbitrary JSON-serialisable data.
- `retry_policy` (`RetryPolicy | None`): retry configuration.
- `scheduled_for` (`datetime | None`): earliest pickup time.
- `route` (`str`): route name for selective worker polling.
- `priority` (`int`): higher runs first.
- `task_id` (`str | None`): explicit ID (auto-generated otherwise).

**Returns** `str` — the task ID. **Raises** `ValueError` on bad input,
`TaskError` on duplicate ID / insert failure.

```python
task_id = await queue.submit(
    "send_email",
    {"to": "user@example.com"},
    retry_policy=RetryPolicy(max_retries=3),
)
```

### `submit_many(tasks, *, retry_policy=None, route="default", priority=0)`

Submit many `(task_type, payload)` tuples in a single transaction.

**Returns** `list[str]` — task IDs in input order.

### Queries

| Method | Returns |
|---|---|
| `get_task(task_id)` | `Task | None` |
| `list_pending_tasks(limit=10, offset=0)` | `list[Task]` |
| `list_completed_tasks(limit=10, offset=0)` | `list[Task]` |
| `list_failed_tasks(limit=10, offset=0)` | `list[Task]` |
| `count_tasks_by_status(status)` | `int` |

### DLQ convenience methods

| Method | Returns |
|---|---|
| `list_dlq_tasks(limit=10, offset=0, include_discarded=False)` | `list[DLQTask]` |
| `get_dlq_task(task_id)` | `DLQTask | None` |
| `retry_dlq_task(task_id)` | `str` |
| `discard_dlq_task(task_id, reason=None)` | `None` |
| `count_dlq_tasks()` | `int` |

---

## Worker

Poll-based worker that dispatches tasks to registered handlers.

```python
async with Worker(database_url="postgresql://...") as worker:
    @worker.task("send_email")
    async def handler(payload: dict) -> dict:
        return {"status": "sent"}

    await worker.run()
```

### Constructor

`Worker(database_url, *, worker_id=None, concurrency=10, poll_interval=0.5,
routes=None, log_level="INFO", pool_min_size=2, pool_max_size=10,
pool_timeout=30.0, command_timeout=60.0, heartbeat_interval=10.0,
graceful_shutdown_timeout=30.0, metrics_port=8000, metrics_enabled=True,
health_enabled=True)`

### `@worker.task(task_type)`

Decorator that registers an async handler for `task_type`. The handler must
accept a single `dict` (the payload) and return an optional `dict` (result).
Raises `ValueError` if the type is empty, already registered, or the handler
is not async.

### Methods

| Method | Description |
|---|---|
| `async run()` | Start the event loop; polls and executes until shutdown. Handles `SIGTERM`/`SIGINT`. Starts heartbeat + metrics/health server. |
| `async run_once()` | Run a single poll-and-execute cycle (testing/debugging; no heartbeat/metrics). |
| `async shutdown()` | Graceful shutdown: stop polling, wait for in-flight tasks (with timeout), send final heartbeat, disconnect. |
| `get_status()` | Dict with worker health info (uptime, processed/failed counts, current task, etc.). |

### Properties

`worker_id`, `is_running`, `is_connected`.

---

## DeadLetterQueue

Manage tasks that exhausted their retries.

```python
async with DeadLetterQueue(database_url="postgresql://...") as dlq:
    tasks = await dlq.list_tasks()
```

| Method | Returns |
|---|---|
| `list_tasks(limit=10, offset=0, include_discarded=False)` | `list[DLQTask]` |
| `get_task(task_id)` | `DLQTask | None` |
| `retry_task(task_id)` | `str` — removes from DLQ, resets to `pending` |
| `discard_task(task_id, reason=None)` | `None` — soft-delete |
| `count(include_discarded=False)` | `int` |

---

## Models

### `Task`

Frozen dataclass. Key fields: `task_id`, `task_type`, `payload`, `status`,
`priority`, `route`, `retry_policy`, `attempt`, `max_retries`,
`scheduled_for`, `worker_id`, `result`, `error_message`, `created_at`,
`started_at`, `completed_at`. Supports `to_dict()` / `from_dict()`.

### `TaskStatus`

`(str, Enum)`: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`.

### `RetryPolicy`

Frozen dataclass: `max_retries=3`, `backoff_strategy="exponential"`,
`initial_delay=1.0`, `max_delay=3600.0`. `validate()`, `to_dict()`,
`from_dict()`.

### `DLQTask`

Frozen dataclass: `task_id`, `task_type`, `payload`, `error_message`,
`attempts`, `retry_policy`, `moved_at`, `discarded`, `discard_reason`,
`discarded_at`. `to_dict()` / `from_dict()`.

### `WorkerInfo`, `WorkerStatus`, `RetryRecord`

Worker heartbeat info (`WorkerStatus`: `IDLE`, `PROCESSING`, `UNHEALTHY`)
and retry-history records. `to_dict()` / `from_dict()`.

### Backoff strategies

`BackoffStrategyType` (`EXPONENTIAL`, `LINEAR`, `FIXED`) plus
`ExponentialBackoff`, `LinearBackoff`, `FixedBackoff` with
`calculate_delay(attempt_number)`.

---

## Exceptions

All inherit from `ConductorException`:

| Exception | Raised for |
|---|---|
| `ConductorException` | Base class |
| `DatabaseError` | Database operation failures |
| `WorkerError` | Worker unrecoverable errors |
| `TaskError` | Task submit/fetch/update failures |
| `RetryPolicyError` | Invalid retry policy |
| `ConductorConnectionError` | Failed DB connection |

---

## Observability

- `HealthChecker(pool, dlq_size_threshold=100)` — `async check()` returns
  `HealthResult` (`status`, `database`, `pending_tasks`, `dead_letter_queue`,
  `workers_active`, `uptime_seconds`, `last_check`); `HealthStatus` is
  `HEALTHY` / `DEGRADED` / `UNHEALTHY`.
- `MetricsExporter(pool, health_checker, port=8000)` — serves `/metrics`
  (Prometheus) and `/health` (JSON). Started automatically by `Worker.run()`.
- `JsonFormatter` / `setup_logging(level="INFO", fmt="json")` — structured
  JSON logging (`conductor.observability.logging`).

---

## Configuration & CLI

- `WorkerSettings.from_env()` — build settings from environment variables
  (see [Configuration](configuration.md)); `.build_worker()` constructs a
  `Worker`.
- `conductor worker [--handlers MODULE] [--env-file PATH]` — run a worker
  from the CLI (also `python -m conductor worker`).
