# Conductor Examples

Twelve runnable examples that demonstrate real-world Conductor patterns.
Each script is self-contained, uses the public API, and exits cleanly.

| # | File | Demonstrates |
|---|---|---|
| 1 | `1_basic_queue.py` | submit → poll → execute → inspect result |
| 2 | `2_email_notifications.py` | retry/backoff + aiohttp SendGrid (mock by default) |
| 3 | `3_data_processing.py` | multi-step pipeline + manual task chaining |
| 4 | `4_scheduled_cleanup.py` | scheduled tasks (`scheduled_for`) + cron pattern |
| 5 | `5_error_handling.py` | custom exceptions, idempotency, DLQ recovery |
| 6 | `6_routing_priority.py` | task routing (`route`) + priority queues |
| 7 | `7_recurring_tasks.py` | recurring cron tasks (`schedule_recurring` + scheduler) |
| 8 | `8_grpc_client.py` | gRPC API (`ProcessTask`/`RegisterHandler`/`GetWorkerStatus`) |
| 9 | `9_web_dashboard.py` | web dashboard (FastAPI API + built React SPA, cancel task) |
| 10 | `10_circuit_breaker.py` | per-task-type circuit breaker (trip → skip → recover) |
| 11 | `11_task_chaining.py` | task dependencies (`depends_on`: gate execution, block on failure) |
| 12 | `12_distributed_tracing.py` | OpenTelemetry tracing (submit → execute in one trace, retry/DLQ events) |

Polyglot **reference client stubs** (Go/Rust/Node) for the gRPC service live in
[`grpc/`](grpc/README.md).

## Prerequisites

- Python 3.11+
- PostgreSQL 12+ reachable from `DATABASE_URL` — **or** MySQL/MariaDB
  (`pip install "conductor-task-queue[mysql]"`, DSN
  `mysql://conductor:conductor@localhost:3306/conductor`) — **or** SQLite,
  which needs no server at all: `pip install "conductor-task-queue[sqlite]"`
  and `DATABASE_URL=sqlite:///conductor.db` (one worker process per file).
- The package installed: `pip install -e .`

## Database setup

```bash
createdb conductor            # or: docker compose up -d postgres
```

Conductor creates its schema automatically on first `connect()`.

## Run an example

```bash
DATABASE_URL=postgresql://conductor:conductor@localhost:5432/conductor \
    python examples/1_basic_queue.py
```

Each script prints a short expected-output report and returns. The
`DATABASE_URL` env var is optional (defaults to the value above).

## What each example shows

### 1. Basic queue

`queue.submit(...)` → a worker polls and executes via `run_once()` → the
stored task shows `status=completed` with the result, worker id, and
timestamps.

### 2. Email notifications with retry

A `send_email` handler with `RetryPolicy(max_retries=3, backoff_strategy="exponential")`.
The first attempt simulates a transient network failure, so the task is
retried and succeeds on the second attempt. If `SENDGRID_API_KEY` is set,
the handler posts to the SendGrid v3 API with aiohttp; otherwise a mock
transport prints the email (no external credentials needed).

### 3. Data processing pipeline

A `process_upload` handler runs a mocked pipeline (download → process →
store) and then submits a follow-up `send_notification` task before
returning — the manual chaining pattern. Native chaining is planned for
v0.2.

### 4. Scheduled cleanup

A task submitted with a future `scheduled_for` is not polled before its
time; the script shows it staying `pending`, then being executed once due.
For repeating runs, prefer the native cron scheduler (`examples/7`); an
external crontab still works as a manual alternative:

```cron
# crontab — submit the cleanup task daily at 2 AM
0 2 * * * cd /opt/conductor && DATABASE_URL=... python examples/4_scheduled_cleanup.py
```

### 5. Error handling & idempotency

- Custom exceptions (`TransientError` retryable, `PaymentError` permanent)
- Retry policy + backoff; a task that fails transiently then succeeds
- An idempotency guard: submitting the same order again is a no-op
  (no double charge)
- A task with `max_retries=0` fails immediately into the dead-letter
  queue, then is recovered with `DeadLetterQueue.retry_task()`

### 6. Routing & priority queues

Two v0.2 features in one script:

- **Routing** — tasks are submitted to named routes (`route="critical"`,
  `route="batch"`).  A worker polls only the routes it subscribes to
  (`routes=["critical"]`), so `worker-critical` never sees `batch` tasks.
  Pass `routes=None` to a worker to poll *all* routes.
- **Priority** — tasks with a higher `priority` (range -100..100) are
  executed before lower-priority ones, even when submitted later.  The
  script submits ranks out of order and prints the execution order.

### 7. Recurring cron tasks

Registers a cron definition with `queue.schedule_recurring(task_type, payload,
cron_expression)`, then runs a `RecurringScheduler` once to fire a due
instance and a `Worker` to execute it.  The definition's `next_run_at`
advances to the next cron fire (UTC).  In production, run the scheduler
continuously (`scheduler.run()`) — optionally embedded in a worker with
`Worker(enable_scheduler=True)`.

> Sections for examples **8–12** live in the numbered files themselves; this
> page keeps the summary table above up to date.  Example 12 needs the tracing
> extra: `pip install "conductor-task-queue[otel]"`.

## Notes

- Scripts use `worker.run_once()` (not `run()`), so they complete one
  poll-and-execute cycle and exit — no infinite loop, no signal handling.
- Examples are excluded from the built package (`find_packages` excludes
  `examples*`).
- See `docs/installation.md` and `docs/api-reference.md` for the full API.
