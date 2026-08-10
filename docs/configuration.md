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
| `DATABASE_URL` | *(required)* | PostgreSQL connection URI, e.g. `postgresql://user:pass@host:5432/conductor` |
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
| `CONDUCTOR_HANDLERS_MODULE` | *(none)* | Dotted path to a module exposing `register(worker)` |

> **Note:** The CLI/`ROUTES` env default is `["default"]`.  Programmatically,
> `Worker(routes=None)` polls **all** routes (see
> [Routing & Priorities](#routing--priorities)).

> **Note:** There is no separate `HEALTH_PORT` — `/metrics` and `/health`
> are served on the same `METRICS_PORT` (default `8000`).

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
)
```

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
