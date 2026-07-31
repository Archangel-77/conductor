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
| `CONDUCTOR_HANDLERS_MODULE` | *(none)* | Dotted path to a module exposing `register(worker)` |

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
    routes=None,                     # None -> ["default"]
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
