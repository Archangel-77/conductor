# Troubleshooting

Common issues and debugging tips for Conductor.

## Database connection failures

**Symptom:** `ConductorConnectionError: Could not connect to PostgreSQL...`

1. Verify PostgreSQL is running: `pg_isready -h localhost`
2. Check `DATABASE_URL` — format `postgresql://user:pass@host:port/database`
3. Check network/firewall rules and credentials
4. The pool retries with backoff (default 3 attempts), then raises

For MySQL/MariaDB the same flow applies; the message names the backend and the
DSN form is `mysql://user:pass@host:3306/database`. Verify with
`mysqladmin ping -h localhost -u conductor -p`.

## SQLite issues

### `sqlite3.OperationalError: database is locked`

Another connection held a write lock for longer than `DB_BUSY_TIMEOUT`
(default 5 seconds).

1. Make sure only **one worker process** targets the file — a second worker is
   the most common cause (see below).
2. Raise the timeout: `export DB_BUSY_TIMEOUT=30`.
3. Keep the database on a local filesystem. SQLite over NFS/SMB or a network
   volume can produce spurious locking errors and is not supported.

### Two workers never share the queue

This is by design. SQLite has no `SKIP LOCKED`, so Conductor's SQLite backend
contracts for **exactly one worker process per database file**; it serialises
access inside that process (`BEGIN IMMEDIATE` + WAL) and provides the same
exactly-once guarantee as PostgreSQL only within it. For horizontal scaling,
use a PostgreSQL DSN — the task API is identical.

A separate producer (`TaskQueue`) in the same process is fine.

### Nothing is durable with `sqlite://:memory:`

An in-memory database lives and dies with the connection; every restart starts
empty. This is intentional (it is the fastest way to run the test suite) — use
a file DSN for anything that must persist.

### Are threads involved now?

The SQLite driver (`aiosqlite`) runs a dedicated worker thread per connection.
Conductor itself remains asyncio-only: no user code runs on that thread and no
blocking calls are made from the event loop. There is no asyncio-native SQLite
driver in the standard library, so this is an accepted, documented trade-off.

### Missing `json_each` / `json_array_length`

The dependency checks use the JSON1 functions, available in SQLite 3.38+
(2022). Older system SQLite builds fail with `no such function: json_each` —
upgrade SQLite (the CPython bundled version is fine on Python 3.11+).

## MySQL / MariaDB issues

### `The mysql backend requires an optional dependency`

The driver lives in an extra:

```bash
pip install "conductor-task-queue[mysql]"
```

### `Check constraint 'chk_task_status' is not supported` / `Unknown collation`

Use **MySQL 8.0.16+** (named `CHECK` constraints) or **MariaDB 10.6+**, and a
server that knows the `utf8mb4` / `utf8mb4_unicode_ci` collations (every modern
build does). MySQL 5.7 is not supported.

### `Unsupported server version '10.5.x-MariaDB-…'`

The server is older than the minimum Conductor requires: pending rows are
claimed with `FOR UPDATE SKIP LOCKED` (MySQL 8.0+, **MariaDB 10.6+**) and the
schema uses named `CHECK` constraints (MySQL 8.0.16+). Upgrade the server — on
MariaDB 10.5 the polling query fails with a raw `SKIP LOCKED` syntax error, so
Conductor refuses to connect instead.

### `OperationalError: (2013, 'Lost connection')` on long tasks

The driver aborts a read after `DB_COMMAND_TIMEOUT` seconds (default 60) — this
becomes `read_timeout`. Raise it for handlers that run for minutes:

```bash
export DB_COMMAND_TIMEOUT=600
```

### `Incorrect string value` when storing emoji or CJK text

The connection charset must be `utf8mb4` (Conductor's default). If you
overrode it in the DSN with `?charset=utf8`, non-BMP characters fail: use
`?charset=utf8mb4`.

### Tasks execute twice / workers idle

Verify `sql_mode` and the server version: row claiming relies on
`FOR UPDATE SKIP LOCKED`, which needs **InnoDB** and MySQL 8.0+. On a 5.7
server no rows are ever claimed, and tasks stay `pending` forever. Run
`SELECT VERSION();` and check the schemas with
`SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE();`.

### Benign driver warnings in the logs

Both of these are harmless and expected; they are emitted by the server through
the `asyncmy` logger:

- `Table 'conductor_version' already exists` — the schema statements use
  `CREATE TABLE IF NOT EXISTS`, so MySQL reports an existing table as a warning.
  It appears once per `ensure_schema()` call (startup) and nothing is recreated.
- `'VALUES function' is deprecated and will be removed in a future release …` —
  Conductor renders `ON DUPLICATE KEY UPDATE col = VALUES(col)` for DLQ moves and
  worker heartbeats. MySQL 8.0.20+ deprecates `VALUES()` in favour of a row alias
  (`INSERT … VALUES (…) AS new …`), **but MariaDB rejects the alias form with a
  syntax error**, so the portable form is used. The warning can be ignored.

To reduce the noise without hiding real problems, raise the level of the driver
logger in your logging configuration:

```python
import logging

logging.getLogger("asyncmy").setLevel(logging.ERROR)
```

## Tasks not processing

**Symptom:** tasks submitted but never executed.

Checklist:

1. **Is a worker running?** Start one (see [Installation](installation.md)).
2. **Is the handler registered?** A task whose `task_type` has no handler is
   failed and moved to the DLQ after retries:
   ```python
   @worker.task("my_task")          # must match the submitted task_type
   async def handler(payload: dict) -> dict:
       return {"status": "done"}
   ```
3. **Routes match?** A worker polls only the routes it subscribes to — the
   CLI's `ROUTES` env var (default `["default"]`) or programmatic
   `Worker(routes=[...])`. A task submitted with a different `route` won't be
   picked up. A programmatic `Worker(routes=None)` polls **all** routes.
4. **Is `scheduled_for` in the past?** Tasks scheduled in the future are not
   polled until then.
5. **Check the worker logs** (see Logging below) for poll/execute messages.

## High latency

**Symptom:** tasks take a long time from submit to execution.

- **Polling interval** is the dominant factor (default `0.5s`). Lower it for
  faster pickup at the cost of more DB load:
  ```python
  Worker(database_url="...", poll_interval=0.1)
  ```
- **Concurrency limit reached** — raise `concurrency`.
- **Scale horizontally** — run more worker processes; they share the queue
  via `FOR UPDATE SKIP LOCKED`.

## Dead letter queue buildup

**Symptom:** tasks keep failing and accumulate in the DLQ.

```python
import asyncio
from conductor import DeadLetterQueue


async def inspect() -> None:
    async with DeadLetterQueue(database_url="postgresql://...") as dlq:
        for task in await dlq.list_tasks(limit=100):
            print(task.task_id, task.task_type, task.error_message)
        # Fix the cause, then:
        # await dlq.retry_task(task_id)      # requeue a task
        # await dlq.discard_task(task_id)    # permanently discard


asyncio.run(inspect())
```

Also see the Grafana dashboard (`docs/grafana/`) for a DLQ-size gauge and
error-rate graphs.

## Metrics / health endpoint unavailable

**Symptom:** `curl localhost:8000/health` refuses or the worker logs
`Metrics exporter could not bind to port ...`.

- The metrics/health server binds `METRICS_PORT` (default 8000). If the port
  is already in use, the worker logs a warning and **continues without the
  server** — pick a different port:
  ```bash
  export METRICS_PORT=9100
  ```
- The server only starts inside `Worker.run()` (not `run_once()`).
- Disable either endpoint: `METRICS_ENABLED=false`, `HEALTH_ENABLED=false`.

## Logging

Conductor uses Python's standard `logging` module (`conductor.*` loggers).

- **Level:** `LOG_LEVEL` (DEBUG, INFO, WARNING, ERROR) or
  `logging.getLogger("conductor").setLevel(...)`.
- **Format:** `LOG_FORMAT=json` (default, structured) or `text`.
- **Structured fields:** every event includes `timestamp`, `level`, `logger`,
  `message`, plus task context (`task_id`, `task_type`, `worker_id`,
  `duration_ms`, `error`) where applicable.

```bash
export LOG_LEVEL=DEBUG LOG_FORMAT=json
conductor worker --handlers myapp.handlers
```

Example JSON line:

```json
{"timestamp": "...", "level": "INFO", "logger": "conductor.core.worker",
 "message": "Task <id> (<type>) completed in 1ms.",
 "task_id": "...", "task_type": "...", "duration_ms": 1}
```

## Debug tips

- Use `await worker.run_once()` to execute a single poll-and-execute cycle
  deterministically (no heartbeat/metrics server).
- Inspect task state directly:
  ```python
  async with TaskQueue(database_url="...") as queue:
      task = await queue.get_task(task_id)
      print(task.status, task.attempt, task.error_message)
  ```
- The schema is auto-managed on first `connect()` — there is no separate
  migration command to run.
