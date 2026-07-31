# Conductor Examples

Five runnable examples that demonstrate real-world Conductor patterns.
Each script is self-contained, uses the public API, and exits cleanly.

| # | File | Demonstrates |
|---|---|---|
| 1 | `1_basic_queue.py` | submit → poll → execute → inspect result |
| 2 | `2_email_notifications.py` | retry/backoff + aiohttp SendGrid (mock by default) |
| 3 | `3_data_processing.py` | multi-step pipeline + manual task chaining |
| 4 | `4_scheduled_cleanup.py` | scheduled tasks (`scheduled_for`) + cron pattern |
| 5 | `5_error_handling.py` | custom exceptions, idempotency, DLQ recovery |

## Prerequisites

- Python 3.11+
- PostgreSQL 12+ reachable from `DATABASE_URL`
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
For recurring work, trigger submissions from cron or a systemd timer
(native cron is planned for v0.2):

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

## Notes

- Scripts use `worker.run_once()` (not `run()`), so they complete one
  poll-and-execute cycle and exit — no infinite loop, no signal handling.
- Examples are excluded from the built package (`find_packages` excludes
  `examples*`).
- See `docs/installation.md` and `docs/api-reference.md` for the full API.
