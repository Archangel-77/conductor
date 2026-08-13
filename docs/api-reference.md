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

### `submit(task_type, payload, *, retry_policy=None, scheduled_for=None, route="default", priority=0, depends_on=None, task_id=None)`

Submit a new task.

- `task_type` (`str`): logical type used to route the task to a handler.
- `payload` (`dict`): arbitrary JSON-serialisable data.
- `retry_policy` (`RetryPolicy | None`): retry configuration.
- `scheduled_for` (`datetime | None`): earliest pickup time.
- `route` (`str`, default `"default"`): route name for selective worker polling.
- `priority` (`int`, default `0`): higher runs first; range **-100..100**.
- `depends_on` (`list[str] | None`): task IDs that must complete (or be
  cancelled) before this task runs; forward references are allowed.
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

### `submit_many(tasks, *, retry_policy=None, route="default", priority=0, scheduled_for=None)`

Submit many `(task_type, payload)` tuples in a single transaction.

**Returns** `list[str]` — task IDs in input order.

### Queries

| Method | Returns |
|---|---|
| `get_task(task_id)` | `Task | None` |
| `list_pending_tasks(limit=10, offset=0, route=None)` | `list[Task]` |
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

### Recurring-task management

```python
rid = await queue.schedule_recurring(
    "cleanup", {"keep": 30}, "0 2 * * *",   # task_type, payload, cron (UTC)
    route="default", priority=0, retry_policy=None, enabled=True,
    recurring_id=None,
)
```

| Method | Returns |
|---|---|
| `schedule_recurring(task_type, payload, cron_expression, *, route="default", priority=0, retry_policy=None, enabled=True, recurring_id=None)` | `str` — the definition ID |
| `list_recurring_tasks(limit=10, offset=0)` | `list[RecurringTask]` |
| `get_recurring_task(recurring_id)` | `RecurringTask | None` |
| `pause_recurring(recurring_id)` | `None` — disables firing |
| `resume_recurring(recurring_id)` | `None` — re-enables firing |
| `delete_recurring_task(recurring_id)` | `None` |

`cron_expression` is a standard 5-field cron evaluated in **UTC**; missed
occurrences are skipped (no backfill). `schedule_recurring` raises `ValueError`
on an invalid cron/priority/route and `TaskError` on a duplicate `recurring_id`.

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
health_enabled=True, enable_scheduler=False, grpc_port=50051,
grpc_enabled=False, grpc_max_message_size=None, api_port=8080,
api_enabled=False, api_key=None, circuit_breaker_enabled=False,
circuit_breaker_config=None, circuit_breaker_overrides=None)`

> `routes=None` polls **all** routes (no route filter).  Pass a list such as
> `routes=["critical"]` to poll only those routes.  The CLI/`ROUTES` env var
> defaults to `["default"]` for predictability — see
> [Configuration](configuration.md).

### `@worker.task(task_type)`

Decorator that registers an async handler for `task_type`. The handler must
accept a single `dict` (the payload) and return an optional `dict` (result).
Raises `ValueError` if the type is empty, already registered, or the handler
is not async.

### Methods

| Method | Description |
|---|---|
| `async run()` | Start the event loop; polls and executes until shutdown. Handles `SIGTERM`/`SIGINT`. Starts heartbeat + metrics/health + optional gRPC server. |
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

## RecurringScheduler

Creates task instances for due recurring definitions.

```python
from conductor import RecurringScheduler

async with RecurringScheduler(database_url="postgresql://...") as sched:
    await sched.run()   # runs until SIGTERM/SIGINT
```

| Method | Description |
|---|---|
| `async run()` | Perpetual loop: sweep every `scheduler_interval` seconds until shutdown. Handles `SIGTERM`/`SIGINT`. |
| `async run_once()` | Single sweep (testing/debugging; no signal handlers). |
| `async shutdown()` | Graceful shutdown: stop the loop and close the pool. |
| `get_status()` | Dict with uptime, `tasks_fired_total`, `last_sweep_at`, config, connection state. |

Constructor: `RecurringScheduler(database_url, *, scheduler_interval=1.0, poll_batch_size=50,
log_level="INFO", pool_min_size=1, pool_max_size=5, pool_timeout=30.0, command_timeout=60.0)`.

Each sweep claims due definitions with `FOR UPDATE SKIP LOCKED` (safe for
multiple schedulers), creates one task instance per fire, and advances
`next_run_at` to the next cron time (UTC, skipping missed runs).  A
`Worker(enable_scheduler=True)` embeds a scheduler in-process.

---

## gRPC API

A Worker can embed an async gRPC server (`Worker(grpc_enabled=True)`) exposing
the `ConductorWorker` service, so polyglot clients (Go, Rust, Node.js, Python)
can execute tasks and inspect worker state.

```python
from conductor import Worker

worker = Worker(database_url="...", grpc_enabled=True, grpc_port=50051)
```

### Service — `conductor.proto`

```proto
service ConductorWorker {
  rpc ProcessTask(TaskRequest) returns (TaskResponse);
  rpc RegisterHandler(RegisterRequest) returns (RegisterResponse);
  rpc GetWorkerStatus(StatusRequest) returns (WorkerStatus);
}
```

- `TaskRequest { task_id, task_type, payload /* JSON bytes */, persist }`
- `TaskResponse { task_id, success, result /* JSON bytes */, error }`
- `RegisterRequest { task_type }` / `RegisterResponse { registered, task_type, error }`
- `WorkerStatus { worker_id, status, uptime_seconds, tasks_processed_total,
  tasks_failed_total, registered_handlers, grpc_enabled, grpc_port }`

### `ProcessTask`

Executes a task through the worker's registered handler and returns the result
or error.  By default this is **pure execution** — no writes to
`conductor_tasks`.  Set `persist=true` to record the outcome (processing →
completed/failed, with metrics and retry/DLQ handling) exactly like a polled
task.

### `RegisterHandler`

Registers a `task_type` on the worker (idempotent).  A remotely registered
type appears in `GetWorkerStatus`; executing it in-process reports a clear
error.  It cannot be re-registered with the `@worker.task()` decorator.

### `GetWorkerStatus`

Returns the worker's health and statistics (mirrors `Worker.get_status()`).

### Error mapping

Transport-level errors (empty `task_type`, malformed JSON payload) abort with
gRPC status codes (`INVALID_ARGUMENT`).  Handler failures are returned in-band
as `success=false` with the error message.

### Client example

```python
import grpc.aio as grpc_aio
from conductor.grpc import conductor_pb2, conductor_pb2_grpc

channel = grpc_aio.insecure_channel("localhost:50051")
stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
resp = await stub.ProcessTask(conductor_pb2.TaskRequest(
    task_type="echo", payload=b'{"message":"hi"}',
))
```

See `examples/8_grpc_client.py` (working Python client) and
`examples/grpc/` (Go/Rust/Node reference stubs).

---

## Web Dashboard API

A **FastAPI** app (`conductor/api/`) serves a JSON API plus the built React
frontend. Run it standalone with `conductor api` or embed it in a worker with
`Worker(api_enabled=True)`.

```python
from conductor import DashboardServer
from conductor.db.connection import DatabasePool

pool = DatabasePool(dsn="postgresql://...")
await pool.connect()
server = DashboardServer(pool, api_key=None, host="0.0.0.0", port=8080)
await server.start()
...
await server.stop()
await pool.disconnect()
```

### Endpoints

All endpoints are under `/api` and (when an API key is configured) require an
`X-API-Key` header; unset key = open access. The built SPA is served at `/`.

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/tasks` | List tasks (`status`, `route`, `task_type`, `search`, `limit`, `offset`) → `{items, total, limit, offset}` |
| `GET` | `/api/tasks/{id}` | Task details plus `retries` history (404 if missing) |
| `POST` | `/api/tasks/{id}/cancel` | Cancel a pending/retrying task → `{status:"cancelled"}` (404 missing, 409 not cancellable) |
| `GET` | `/api/workers` | All registered workers, newest heartbeat first |
| `GET` | `/api/metrics` | Prometheus metrics as JSON (`{metrics:[{name,type,help,samples}]}`) |
| `GET` | `/api/dlq` | Dead-letter tasks (`limit`, `offset`, `include_discarded`) |
| `POST` | `/api/dlq/{id}/retry` | Requeue a DLQ task as pending → `{task_id}` (404 missing) |
| `POST` | `/api/dlq/{id}/discard` | Soft-delete a DLQ task → `{status:"discarded", task_id}` (404 missing) |
| `GET` | `/api/health` | `HealthResult.to_dict()` |

### `TaskQueue.cancel_task(task_id)`

Cancels a pending or retrying task, setting its status to `cancelled` and
stamping `completed_at`. Raises `TaskError` if the task is missing or is not
in a cancellable state (`processing`/`completed`/`failed`/`cancelled`). The
`CANCELLED` status is enforced by the `chk_task_status` CHECK constraint
(schema **v4**); `SchemaManager.ensure_schema()` migrates v3 databases.

---

## Circuit Breaker

A per-task-type **circuit breaker** protects workers from repeatedly executing
a handler that keeps failing. It is **worker-side and in-memory** — each
worker tracks its own *consecutive* failures per task type (state is not
shared across workers; a DB-backed registry is future work).

Enable it on the worker:

```python
from conductor import CircuitBreakerConfig, Worker

worker = Worker(
    database_url="...",
    circuit_breaker_enabled=True,
    circuit_breaker_config=CircuitBreakerConfig(
        threshold=5,              # open after 5 consecutive failures
        timeout=60.0,             # stay open 60s, then allow half-open probes
        half_open_attempts=2,     # probe executions allowed while half-open
    ),
)
```

### State machine

- **CLOSED** — normal operation. After `threshold` *consecutive* failures the
  circuit trips **OPEN**.
- **OPEN** — the worker **skips** execution of that task type (tasks stay
  pending — no false failures/retries/DLQ) until `timeout` seconds elapse.
- **HALF_OPEN** — after the timeout, up to `half_open_attempts` probe
  executions are allowed. A successful probe closes the circuit; all probes
  failing re-opens it.

Only real handler exceptions trip the breaker — a task whose handler is not
registered does **not** count as a circuit failure.

### Models

- `CircuitState` (`(str, Enum)`): `CLOSED`, `OPEN`, `HALF_OPEN`.
- `CircuitBreakerConfig` (frozen dataclass): `threshold=5`, `timeout=60.0`,
  `half_open_attempts=2`. Validates via `CircuitBreakerError`;
  `to_dict()`/`from_dict()`.
- `CircuitBreaker` — per-type state machine: `allow_request()`,
  `record_success()`, `record_failure()`, `reset()`, `snapshot()`,
  `state`/`is_open`.
- `CircuitBreakerRegistry` — hands out one `CircuitBreaker` per task type;
  `get(task_type)`, `open_types()`, `snapshot()`, `reset()`.

Per-task-type `circuit_breaker_overrides` are programmatic only; global
defaults come from the `CONDUCTOR_CIRCUIT_BREAKER_*` env vars (see
[Configuration](configuration.md)).

### Observability

- `conductor_tasks_rejected_total{task_type=...}` — tasks skipped while open.
- `conductor_circuit_breaker_open{task_type=...}` — 1 while open/half-open.
- `Worker.get_status()` reports `circuit_breaker_enabled` and
  `circuit_breaker_open` (task types currently open/half-open).

---

## Task Dependencies & Chaining

Native task chaining via `TaskQueue.submit(..., depends_on=[...])`: a task
with dependencies is **not polled** until every dependency is satisfied, so
chains (A → B → C) run in order automatically.

```python
a = await queue.submit("download", {"url": "..."})
b = await queue.submit("process", {}, depends_on=[a])
c = await queue.submit("publish", {}, depends_on=[b])
```

### Semantics

- **Gating** — a task whose dependencies are not all in `completed` or
  `cancelled` is excluded from polling (it stays `pending`). This handles the
  transient "waiting for dependencies" case.
- **`BLOCKED`** — when a dependency reaches a terminal failure, its pending
  dependents are marked `BLOCKED` (error: `dependency '<id>' failed`). This is
  terminal (no retry) and propagates **transitively**: if A fails and B depends
  on A and C depends on B, both B and C become `BLOCKED`. It is driven by the
  worker when a task exhausts its retries (and covers the gRPC `persist=true`
  path).
- **Cancelled dependencies** — count as satisfied, so dependents are released
  to run (a cancelled step does not hang its chain).
- **Forward references** — you may reference task IDs that don't exist yet; the
  gating simply keeps the dependent pending until they do. Self-references are
  rejected at submit time (`ValueError`).
- **DLQ preservation** — `depends_on` is stored on `conductor_dead_letter` and
  restored when a task is retried (schema v2 route/priority precedent).

Schema is at **v5** (`depends_on TEXT[]` + GIN index + `blocked` status);
`SchemaManager.ensure_schema()` migrates v4 databases.

---

## Models

### `Task`

Frozen dataclass. Key fields: `task_id`, `task_type`, `payload`, `status`,
`priority`, `route`, `depends_on`, `retry_policy`, `attempt`, `max_retries`,
`scheduled_for`, `worker_id`, `result`, `error_message`, `created_at`,
`started_at`, `completed_at`. Supports `to_dict()` / `from_dict()`.

### `TaskStatus`

`(str, Enum)`: `PENDING`, `PROCESSING`, `COMPLETED`, `FAILED`, `RETRYING`,
`CANCELLED`, `BLOCKED`.

### `RetryPolicy`

Frozen dataclass: `max_retries=3`, `backoff_strategy="exponential"`,
`initial_delay=1.0`, `max_delay=3600.0`. `validate()`, `to_dict()`,
`from_dict()`.

### `DLQTask`

Frozen dataclass: `task_id`, `task_type`, `payload`, `error_message`,
`attempts`, `retry_policy`, `route`, `priority`, `depends_on`, `moved_at`,
`discarded`, `discard_reason`, `discarded_at`. `to_dict()` / `from_dict()`.

The `route`/`priority` fields preserve how the task was submitted so that
`retry_task()` restores it to the correct route and priority.

### `RecurringTask`

Frozen dataclass describing a cron definition: `id`, `task_type`, `payload`,
`cron_expression`, `route`, `priority`, `retry_policy`, `enabled`,
`next_run_at`, `last_run_at`, `created_at`. `to_dict()` / `from_dict()`.

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
