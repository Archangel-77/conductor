# Configuration

Conductor is configured either **programmatically** (constructor arguments)
or via **environment variables** (used by the `conductor` CLI and
`WorkerSettings.from_env()`). This page documents every option.

## Environment Variables

The full env-var contract that `WorkerSettings.from_env()` reads
(`conductor/config.py`). `DATABASE_URL` is required; everything else has a
default. Values are parsed to the correct types (`CONCURRENCY` is an int,
`POLL_INTERVAL` a float, `METRICS_ENABLED` a bool accepting
`true/1/yes/on`, `ROUTES` a comma-separated list).

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | *(required)* | Database URL — the scheme selects the backend: `postgresql://user:pass@host:5432/conductor`, `sqlite:///conductor.db`, or `mysql://user:pass@host:3306/conductor` |
| `WORKER_ID` | `hostname-pid` | Unique worker identifier |
| `CONCURRENCY` | `10` | Maximum concurrent tasks per worker |
| `POLL_INTERVAL` | `0.5` | Seconds between task polls |
| `ROUTES` | `default` | Comma-separated route names to poll |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `LOG_FORMAT` | `json` | `json` (structured) or `text` |
| `DB_MIN_SIZE` | `2` | Minimum connection pool size |
| `DB_MAX_SIZE` | `10` | Maximum connection pool size |
| `DB_TIMEOUT` | `30` | Connection acquire timeout (seconds) |
| `DB_COMMAND_TIMEOUT` | `60` | SQL command timeout (seconds) |
| `DB_BUSY_TIMEOUT` | `5` | SQLite only: seconds to wait for a locked database before failing |
| `HEARTBEAT_INTERVAL` | `10` | Worker heartbeat frequency (seconds) |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | `30` | Seconds to wait for in-flight tasks on shutdown |
| `METRICS_PORT` | `8000` | Port for the metrics/health HTTP server |
| `METRICS_ENABLED` | `true` | Serve the Prometheus `/metrics` endpoint |
| `HEALTH_ENABLED` | `true` | Serve the JSON `/health` endpoint |
| `CONDUCTOR_ENABLE_SCHEDULER` | `false` | Also run the recurring-task scheduler in the worker |
| `GRPC_ENABLED` | `false` | Serve the `ConductorWorker` gRPC API |
| `GRPC_PORT` | `50051` | gRPC server port |
| `GRPC_MAX_MESSAGE_SIZE` | *(none)* | Max gRPC message size in bytes (empty = gRPC default) |
| `CONDUCTOR_API_ENABLED` | `false` | Serve the web dashboard (FastAPI + built frontend) in the worker |
| `CONDUCTOR_API_PORT` | `8080` | Dashboard server port |
| `CONDUCTOR_API_KEY` | *(none)* | Optional API key; when set, `/api/*` requires an `X-API-Key` header (empty = open) |
| `CONDUCTOR_CIRCUIT_BREAKER_ENABLED` | `false` | Enable the per-task-type circuit breaker (worker-side, in-memory) |
| `CONDUCTOR_CIRCUIT_BREAKER_THRESHOLD` | `5` | Consecutive failures before the circuit opens |
| `CONDUCTOR_CIRCUIT_BREAKER_TIMEOUT` | `60` | Seconds the circuit stays open before half-open probes |
| `CONDUCTOR_CIRCUIT_BREAKER_HALF_OPEN_ATTEMPTS` | `2` | Probe executions allowed while half-open |
| `TRACING_ENABLED` | `false` | Emit OpenTelemetry spans (requires the `otel` extra) |
| `TRACING_EXPORTER` | `otlp` | `none`, `console`, or `otlp` |
| `OTEL_SERVICE_NAME` | `conductor` | Value reported as `service.name` on every span |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | *(none)* | OTLP/HTTP endpoint, e.g. `http://localhost:4318/v1/traces` |
| `TRACING_SAMPLE_RATIO` | `1.0` | Fraction of traces to sample (0.0–1.0) |
| `CONDUCTOR_HANDLERS_MODULE` | *(none)* | Dotted path to a module exposing `register(worker)` |

> **Note:** The CLI/`ROUTES` env default is `["default"]`.  Programmatically,
> `Worker(routes=None)` polls **all** routes (see
> [Routing & Priorities](#routing--priorities)).

> **Note:** There is no separate `HEALTH_PORT` — `/metrics` and `/health`
> are served on the same `METRICS_PORT` (default `8000`).

## Database Backends

The backend is selected by the **DSN scheme** in `DATABASE_URL` / the
`database_url` argument — there is no separate setting:

| DSN | Backend | Install |
|---|---|---|
| `postgresql://user:pass@host:5432/db` | PostgreSQL (asyncpg) | core dependency (default) |
| `sqlite:///conductor.db` | SQLite (aiosqlite) | `pip install "conductor-task-queue[sqlite]"` |
| `mysql://user:pass@host:3306/db` | MySQL/MariaDB (asyncmy) | `pip install "conductor-task-queue[mysql]"` |

Everything else (`CONCURRENCY`, routes, retry policies, circuit breaker, gRPC,
dashboard, …) behaves identically on every backend: `QueryBuilder`, the schema
manager and the migrations render their SQL through a per-backend dialect, and
the core flows are parity-tested across backends.

Backend-specific notes:

- **PostgreSQL** — the reference backend. Pending rows are claimed with
  `FOR UPDATE SKIP LOCKED`, so any number of worker processes can share one
  database.
- **SQLite** — embedded and **single-process**: exactly one worker process per
  database file (see
  [Single-process contract](installation.md#single-process-contract)).
  `DB_MIN_SIZE`, `DB_MAX_SIZE` and `DB_COMMAND_TIMEOUT` are ignored;
  `DB_BUSY_TIMEOUT` controls how long a locked database is waited on.
- **MySQL/MariaDB** — **MySQL 8.0+** (8.0.16+ for `CHECK` constraints) or
  **MariaDB 10.6+**. Pending rows are claimed with `FOR UPDATE SKIP LOCKED`,
  so workers scale horizontally like they do on PostgreSQL. `DB_BUSY_TIMEOUT`
  is ignored (SQLite-only). Connection query parameters are passed through to
  the driver, e.g.
  `mysql://user:pass@host:3306/conductor?charset=utf8mb4&connect_timeout=10`.

## Distributed Tracing

Tracing is an **optional extra** — without it every tracing call is a no-op and
no spans are produced:

```bash
pip install "conductor-task-queue[otel]"
```

```python
from conductor import TaskQueue
from conductor.observability import TracingConfig, setup_tracing

setup_tracing(
    TracingConfig(
        enabled=True,
        exporter="otlp",
        endpoint="http://localhost:4318/v1/traces",
        service_name="my-service",
        sample_ratio=1.0,
    )
)

async with TaskQueue(database_url="postgresql://...") as queue:
    await queue.submit("send_email", {"to": "user@example.com"})
```

Or via the CLI / `WorkerSettings`, using the env vars above:

```bash
TRACING_ENABLED=true TRACING_EXPORTER=otlp \
  OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318/v1/traces \
  conductor worker --handlers myapp.handlers
```

### Spans

| Span | Emitted by | Notes |
|---|---|---|
| `conductor.task.submit` | `TaskQueue.submit` | attributes: `task.id`, `task.type`, `task.route`, `task.priority` |
| `conductor.task.submit_many` | `TaskQueue.submit_many` | one span per batch (`task.count`); every task shares its trace |
| `conductor.task.execute` | `Worker._execute_task` | child of the submit span; attributes: `task.attempt`, `conductor.worker_id`; events: `retry.scheduled`, `task.dlq` |
| `conductor.task.cancel` | `TaskQueue.cancel_task` | |
| `conductor.dlq.retry` / `conductor.dlq.discard` | DLQ operations | retry links back to the original execution |
| `conductor.task.block_dependents` | terminal-failure propagation | attributes: `blocked.count`, `blocked.task_ids` |
| `conductor.recurring.fire` | `RecurringScheduler` | each generated task gets a child trace |

### Cross-process propagation

The submitter stores a W3C `traceparent` on the task row (schema **v6**), so a
worker in another process (or a gRPC client, via `TaskRequest.traceparent`)
continues the same trace. Every attempt of a task is a **child of the
submission span**, i.e. attempts appear as siblings — they are independent
executions of one submitted task. The `traceparent` is preserved through the
dead-letter queue, so a DLQ retry links back to the original execution.

With tracing enabled, log records also carry `trace_id`/`span_id`
(`SpanContextFilter`), so logs and traces can be correlated.

> Conductor keeps its tracer provider **private** rather than installing it as
the global OpenTelemetry provider, so it never overrides a host application's
setup.

## Programmatic Configuration

### TaskQueue

```python
from conductor import TaskQueue

queue = TaskQueue(
    database_url="postgresql://user:pass@localhost:5432/conductor",
    task_timeout=300.0,       # reserved: task execution timeout (seconds)
    max_task_age=86400,       # max age before a pending task is dropped
    log_level="INFO",
    pool_min_size=2,
    pool_max_size=10,
    pool_timeout=30.0,
    command_timeout=60.0,
)
```

### Worker

```python
from conductor import Worker

worker = Worker(
    database_url="postgresql://user:pass@localhost:5432/conductor",
    worker_id="worker-1",           # default: hostname-pid
    concurrency=10,                  # max concurrent tasks
    poll_interval=0.5,               # poll every 500ms
    routes=None,                     # None -> polls ALL routes
    log_level="INFO",
    pool_min_size=2,
    pool_max_size=10,
    pool_timeout=30.0,
    command_timeout=60.0,
    heartbeat_interval=10.0,
    graceful_shutdown_timeout=30.0,
    metrics_port=8000,               # metrics/health HTTP server
    metrics_enabled=True,            # serve /metrics
    health_enabled=True,             # serve /health
    enable_scheduler=False,          # also run the recurring-task scheduler
    grpc_enabled=False,              # serve the ConductorWorker gRPC API
    grpc_port=50051,                 # gRPC server port
    grpc_max_message_size=None,      # optional max gRPC message size (bytes)
    api_enabled=False,               # serve the web dashboard (FastAPI + SPA)
    api_port=8080,                   # dashboard server port
    api_key=None,                    # optional API key (X-API-Key header)
    circuit_breaker_enabled=False,   # per-task-type circuit breaker
    circuit_breaker_config=None,     # optional CircuitBreakerConfig (global)
    circuit_breaker_overrides=None,  # optional per-task-type configs
)
```

Circuit breakers are enabled per worker with
`circuit_breaker_enabled=True`; global defaults come from the
`CONDUCTOR_CIRCUIT_BREAKER_*` env vars, and per-task-type overrides are passed
programmatically via `circuit_breaker_overrides` (a
`dict[task_type, CircuitBreakerConfig]`).  See
[Circuit Breaker](api-reference.md#circuit-breaker).

### DeadLetterQueue

```python
from conductor import DeadLetterQueue

dlq = DeadLetterQueue(
    database_url="postgresql://user:pass@localhost:5432/conductor",
    log_level="INFO",
    pool_min_size=2,
    pool_max_size=10,
    pool_timeout=30.0,
    command_timeout=60.0,
)
```

### RetryPolicy

```python
from conductor import RetryPolicy

RetryPolicy(max_retries=3, backoff_strategy="exponential", initial_delay=1.0, max_delay=3600.0)
RetryPolicy(max_retries=5, backoff_strategy="linear", initial_delay=5.0)
RetryPolicy(max_retries=10, backoff_strategy="fixed", initial_delay=10.0)
RetryPolicy(max_retries=0)  # no retries
```

## Routing & Priorities

Tasks are submitted with an optional `route` and `priority`:

```python
await queue.submit("send_sms", {"to": "+123"}, route="critical", priority=50)
```

- **`route`** (default `"default"`) — a logical channel.  Workers poll only
  the routes they subscribe to via `Worker(routes=[...])` or the `ROUTES`
  env var.  Pass `routes=None` programmatically to poll **all** routes.
- **`priority`** (range **-100..100**, default `0`) — higher values are
  dispatched first.  The polling query orders by `priority DESC, created_at ASC`,
  so priority wins over submission time.

Routed/prioritized tasks keep their `route`/`priority` when they land in
the dead-letter queue and are later retried via
`DeadLetterQueue.retry_task()`.

## Scheduled & Recurring Tasks

### One-off scheduled tasks (`scheduled_for`)

A task can be deferred to a specific time with `scheduled_for`; it is not
polled until then.

```python
from datetime import datetime, timedelta, timezone

await queue.submit(
    "cleanup",
    {"keep_days": 30},
    scheduled_for=datetime.now(timezone.utc) + timedelta(hours=1),
)
```

`submit_many(..., scheduled_for=...)` applies a shared `scheduled_for` to a
whole batch.

### Recurring (cron) tasks

Register a cron-driven definition that creates one task instance per fire:

```python
rid = await queue.schedule_recurring(
    "cleanup", {"keep_days": 30}, "0 2 * * *",   # UTC, 5-field cron
    route="default", priority=0,
)
```

A `RecurringScheduler` (standalone process, or embedded via
`Worker(enable_scheduler=True)`) polls due definitions, fires instances, and
advances `next_run_at` to the next cron fire (UTC, **skipping missed runs**
— no backfill).  Multiple schedulers are safe: due definitions are claimed
with `FOR UPDATE SKIP LOCKED`.

```python
from conductor import RecurringScheduler

scheduler = RecurringScheduler(
    database_url="postgresql://user:pass@localhost:5432/conductor",
    scheduler_interval=1.0,   # seconds between sweeps
    poll_batch_size=50,
)
async with scheduler:
    await scheduler.run()
```

Manage definitions via `schedule_recurring()`, `list_recurring_tasks()`,
`get_recurring_task()`, `pause_recurring()`, `resume_recurring()`, and
`delete_recurring_task()`.

## Task Dependencies & Chaining

Chain tasks with `TaskQueue.submit(..., depends_on=[...])` — a dependent task
is not polled until its dependencies complete (or are cancelled):

```python
a = await queue.submit("download", {"url": "..."})
b = await queue.submit("process", {}, depends_on=[a])
```

- A task whose dependencies are not all `completed`/`cancelled` stays
  `pending` (excluded from polling).
- If a dependency **fails**, dependents are marked **`blocked`** (terminal,
  `dependency '<id>' failed`) — propagating transitively (A→B→C).
- `depends_on` is preserved across DLQ retries. No environment variables are
  involved — chaining is a submit-time API (schema **v5**).

## WorkerSettings

`WorkerSettings` bridges environment variables and the `Worker` constructor:

```python
from conductor.config import WorkerSettings

settings = WorkerSettings.from_env()   # raises ConductorException if DATABASE_URL unset
worker = settings.build_worker()
```

## CLI

The `conductor` console script (installed with the package) runs a worker
from the environment:

```bash
conductor worker                          # reads env vars / ./.env
conductor worker --handlers myapp.handlers
conductor worker --env-file /path/to/.env
python -m conductor worker                # equivalent
```

- `--handlers MODULE` overrides `CONDUCTOR_HANDLERS_MODULE`.
- The handlers module must expose `register(worker)` that attaches task
  handlers (see [Installation](installation.md)).

The `conductor api` subcommand runs a **standalone dashboard server** (no
worker):

```bash
conductor api                             # binds 0.0.0.0:8080
conductor api --host 127.0.0.1 --port 9000
conductor api --api-key s3cret            # require X-API-Key on /api/*
conductor api --env-file /path/to/.env
```

- `--port`/`--api-key` fall back to `CONDUCTOR_API_PORT`/`CONDUCTOR_API_KEY`
  when not given. `DATABASE_URL` is required.
- The same server can be embedded in a worker via
  `Worker(api_enabled=True)` / `CONDUCTOR_API_ENABLED` (port and key defaults
  come from the worker's `api_port`/`api_key` / the `CONDUCTOR_API_*` env
  vars).
