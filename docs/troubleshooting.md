# Troubleshooting

Common issues and debugging tips for Conductor.

## Database connection failures

**Symptom:** `ConductorConnectionError: Could not connect to PostgreSQL...`

1. Verify PostgreSQL is running: `pg_isready -h localhost`
2. Check `DATABASE_URL` — format `postgresql://user:pass@host:port/database`
3. Check network/firewall rules and credentials
4. The pool retries with backoff (default 3 attempts), then raises

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
3. **Routes match?** A worker polls only its `routes` (`ROUTES` env var,
   default `["default"]`). A task submitted with a different `route` won't be
   picked up.
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
