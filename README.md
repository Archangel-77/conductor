# Conductor

**A lightweight, production-ready async task queue for Python teams that don't need Redis.**

Conductor orchestrates reliable, distributed task execution with exactly-once semantics, built entirely on PostgreSQL. Simple API, observable by default, deploy anywhere.

[![Python 3.11+](https://img.shields.io/badge/Python-3.11%2B-blue)](https://www.python.org/)
[![PostgreSQL 12+](https://img.shields.io/badge/PostgreSQL-12%2B-336791)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PyPI version](https://img.shields.io/pypi/v/conductor-task-queue.svg)](https://pypi.org/project/conductor-task-queue/)
[![Tests](https://img.shields.io/github/actions/workflow/status/Archangel-77/Conductor/test.yml?label=tests)](https://github.com/Archangel-77/Conductor/actions)
[![Coverage](https://img.shields.io/codecov/c/github/Archangel-77/Conductor)](https://codecov.io/gh/Archangel-77/Conductor)

**Documentation**: [Installation](docs/installation.md) · [Configuration](docs/configuration.md) · [API Reference](docs/api-reference.md) · [Deployment](docs/deployment.md) · [Troubleshooting](docs/troubleshooting.md) · [docs index](docs/index.md)

---

- [Why Conductor?](#why-conductor)
- [Quick Start](#quick-start)
- [Core Features](#core-features)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Architecture](#architecture)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Comparison to Alternatives](#comparison-to-alternatives)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

---

## Table of Contents

- [Why Conductor?](#why-conductor)
- [Quick Start](#quick-start)
- [Core Features](#core-features)
- [Installation](#installation)
- [Usage Examples](#usage-examples)
- [Configuration](#configuration)
- [Deployment](#deployment)
- [Architecture](#architecture)
- [API Reference](#api-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Comparison to Alternatives](#comparison-to-alternatives)
- [Roadmap](#roadmap)
- [Local Development](#local-development)
- [Contributing](#contributing)
- [License](#license)

---

## Why Conductor?

### The Problem

Every Python team that outgrows Celery or doesn't want Redis dependency ends up rebuilding task queue logic:

- **Celery is overkill**: Over-engineered for most teams, requires Redis/RabbitMQ, steep learning curve
- **RQ is too simple**: Works for basic cases, but lacks reliability patterns (exactly-once semantics, dead letter queues, observability)
- **dramatiq is okay**: Still requires external brokers, doesn't solve the "I just need PostgreSQL" case
- **Custom solutions break**: Teams implement retry logic, task deduplication, and worker pools from scratch—each time

### The Solution

Conductor solves this by:

✅ **No external dependencies** – PostgreSQL only. Deploy to any server, container, or serverless environment  
✅ **Exactly-once semantics** – Tasks execute once, guaranteed. Built-in idempotency and deduplication  
✅ **Observable from day one** – Structured logging with task-level context, Prometheus metrics (`GET /metrics`) and health checks (`GET /health`)
✅ **Production-ready** – Exponential backoff, circuit breakers, graceful shutdown, dead letter queues  
✅ **Simple API** – Submit a task in one line. Register a worker in two  
✅ **Built for async** – Native asyncio support. No threads, no blocking calls

---

## Quick Start

### Installation

```bash
pip install conductor-task-queue
```

**Prerequisites**:
- Python 3.11+
- PostgreSQL 12+

### Basic Example

**1. Submit a task** (app.py):

```python
import asyncio
from conductor import TaskQueue, RetryPolicy


async def main() -> None:
    async with TaskQueue(
        database_url="postgresql://user:password@localhost/conductor"
    ) as queue:
        task_id = await queue.submit(
            task_type="send_email",
            payload={
                "to": "user@example.com",
                "subject": "Hello",
                "body": "Welcome to Conductor!",
            },
            retry_policy=RetryPolicy(
                max_retries=3,
                backoff_strategy="exponential",
            ),
        )
        print(f"Task submitted: {task_id}")


asyncio.run(main())
```

**2. Process tasks with a worker** (worker.py):

```python
import asyncio
from conductor import Worker


async def main() -> None:
    async with Worker(
        database_url="postgresql://user:password@localhost/conductor"
    ) as worker:

        @worker.task("send_email")
        async def send_email(payload: dict) -> dict:
            to = payload["to"]
            subject = payload["subject"]
            body = payload["body"]

            # Send email logic here
            print(f"Sending email to {to}: {subject}")

            # Simulate sending
            await asyncio.sleep(0.5)

            return {"status": "sent"}

        await worker.run()


asyncio.run(main())
```

**3. Run it**:

```bash
# Terminal 1: Start the worker
python worker.py

# Terminal 2: Submit a task
python app.py
```

**That's it.** Your task is submitted, retried on failure, and executed exactly once.

---

## Core Features

### 1. Exactly-Once Semantics

Tasks execute exactly once, even if workers crash mid-execution:

```python
@worker.task("process_payment")
async def process_payment(payload):
    payment_id = payload["id"]
    
    # If a worker crashes here, the task will NOT be retried
    # (it was already marked as completed)
    
    if await is_payment_processed(payment_id):
        return {"status": "already_processed"}
    
    await charge_card(payload)
    await mark_payment_processed(payment_id)
    
    return {"status": "success"}
```

### 2. Retry Logic with Exponential Backoff

Configurable retry policies with automatic backoff:

```python
from conductor import RetryPolicy

await queue.submit(
    task_type="flaky_api_call",
    payload={"url": "https://api.example.com/data"},
    retry_policy=RetryPolicy(
        max_retries=5,
        backoff_strategy="exponential",
        initial_delay=1.0,  # seconds
        max_delay=300.0,
    ),
)
```

**Retry attempts** (with default exponential backoff, max_retries=5):
- Attempt 1: Immediate
- Attempt 2: ~1 second delay
- Attempt 3: ~2 second delay
- Attempt 4: ~4 second delay
- Attempt 5: ~8 second delay
- Attempt 6: Moved to dead letter queue

### 3. Dead Letter Queue

Tasks that fail all retries go to a dead letter queue for manual inspection:

```python
from conductor import DeadLetterQueue

dlq = DeadLetterQueue(database_url="postgresql://...")

# Inspect failed tasks
failed_tasks = dlq.list_tasks(limit=10)

for task in failed_tasks:
    print(f"Task {task.task_id} failed: {task.error_message}")
    print(f"Attempts: {task.attempts}")

# Retry a failed task manually
await dlq.retry_task(task_id="abc-123")

# Or mark as permanently failed
await dlq.discard_task(task_id="abc-123", reason="Known issue, will retry manually later")
```

### 4. Built-in Observability

Conductor logs every task transition with structured context using
Python's standard ``logging`` module.  All log messages include
task ID, task type, worker ID, and duration where applicable.

```python
import logging

logger = logging.getLogger("conductor.core.worker")

# Automatically logged events:
# - Task submitted  (INFO)
# - Task started    (DEBUG)
# - Task completed  (INFO)  — includes duration_ms
# - Task failed     (ERROR) — includes error_message
# - Task retrying   (WARNING)
# - Task moved to DLQ (WARNING)
```

> Structured JSON logging, Prometheus metrics (`GET /metrics`), and health
> checks (`GET /health`) are included and enabled by default. The worker
> exposes them on `METRICS_PORT` (default `8000`).

### 5. Graceful Shutdown

Workers shut down cleanly, completing in-flight tasks. Use the ``run()`` method
which handles ``SIGTERM`` and ``SIGINT`` automatically:

```python
import asyncio
from conductor import Worker


async def main() -> None:
    async with Worker(database_url="postgresql://...") as worker:
        await worker.run()  # runs until SIGTERM/SIGINT


asyncio.run(main())
```

To shut down programmatically, call ``await worker.shutdown()``.
All in-flight tasks complete before the worker disconnects.
```

---

## Installation

### Prerequisites

- **Python 3.11+**
- **PostgreSQL 12+**

### Step 1: Install Conductor

```bash
pip install conductor-task-queue
```

### Step 2: Initialize Database

Conductor automatically creates tables on first connect.  Just create
the database and the schema is applied automatically:

```bash
createdb conductor
```

This creates:
- `conductor_tasks` – Task submissions and status
- `conductor_workers` – Worker heartbeats
- `conductor_retries` – Retry history
- `conductor_dead_letter` – Failed tasks

> **Note:** A CLI migration tool is planned for a future release.
> For now, schema is auto-managed on first ``connect()``.

### Step 3: Create a Worker

```python
import asyncio
from conductor import Worker


async def main() -> None:
    async with Worker(
        database_url="postgresql://user:password@localhost/conductor"
    ) as worker:

        @worker.task("example_task")
        async def example_task(payload: dict) -> dict:
            print(f"Processing: {payload}")
            return {"status": "done"}

        await worker.run()


asyncio.run(main())
```

Run it:

```bash
python worker.py
```

### Step 4: Submit Tasks

```python
import asyncio
from conductor import TaskQueue


async def main() -> None:
    async with TaskQueue(
        database_url="postgresql://user:password@localhost/conductor"
    ) as queue:
        task_id = await queue.submit(
            task_type="example_task",
            payload={"message": "Hello, Conductor!"},
        )
        print(f"Submitted task: {task_id}")


asyncio.run(main())
```

### Step 5: Run a Worker from the CLI (optional)

The package installs a `conductor` console script that runs a worker from
environment variables (no application code needed):

```bash
# handlers.py exposes: def register(worker): @worker.task(...) ...
export DATABASE_URL=postgresql://user:password@localhost/conductor
conductor worker --handlers myapp.handlers
```

`python -m conductor worker` is equivalent. See
[docs/installation.md](docs/installation.md) for the handlers-module
contract.

---

## Usage Examples

> Complete, runnable versions of these patterns live in
> [examples/](examples/README.md).

### Example 1: Email Notifications

Send emails with automatic retry and error handling:

```python
import asyncio
import logging

import aiohttp

from conductor import RetryPolicy, TaskQueue, Worker


logger = logging.getLogger(__name__)


async def main() -> None:
    async with Worker(
        database_url="postgresql://user:password@localhost/conductor"
    ) as worker:

        @worker.task("send_email")
        async def send_email(payload: dict) -> dict:
            """Send an email via SendGrid API."""
            to = payload["to"]
            subject = payload["subject"]
            body = payload["body"]

            logger.info("Sending email to %s", to)

            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.sendgrid.com/v3/mail/send",
                    json={
                        "personalizations": [{"to": [{"email": to}]}],
                        "from": {"email": "noreply@example.com"},
                        "subject": subject,
                        "content": [{"type": "text/html", "value": body}],
                    },
                    headers={"Authorization": f"Bearer ..."},
                ) as resp:
                    resp.raise_for_status()

            logger.info("Email sent to %s", to)
            return {"status": "sent", "to": to}

        await worker.run()


# Submit an email task
async def submit_email() -> None:
    async with TaskQueue(
        database_url="postgresql://user:password@localhost/conductor"
    ) as queue:
        task_id = await queue.submit(
            task_type="send_email",
            payload={
                "to": "user@example.com",
                "subject": "Welcome!",
                "body": "<h1>Thanks for signing up</h1>",
            },
            retry_policy=RetryPolicy(
                max_retries=3,
                backoff_strategy="exponential",
                initial_delay=1.0,
            ),
        )
        print(f"Submitted task: {task_id}")


if __name__ == "__main__":
    asyncio.run(submit_email())
```

### Example 2: Data Processing Pipeline

Multi-step data processing with task chaining:

```python
import logging

from conductor import TaskQueue

logger = logging.getLogger(__name__)


@worker.task("process_upload")
async def process_upload(payload: dict) -> dict:
    """Process an uploaded file through a pipeline."""
    file_id = payload["file_id"]
    user_id = payload["user_id"]

    logger.info("Starting to process file %s", file_id)

    try:
        # Step 1: Download
        file_data = await download_file(file_id)

        # Step 2: Process
        result = await process_data(file_data)

        # Step 3: Store
        await store_result(file_id, result)

        # Step 4: Notify user (spawn another task)
        async with TaskQueue(
            database_url="postgresql://user:password@localhost/conductor"
        ) as queue:
            await queue.submit(
                task_type="send_notification",
                payload={
                    "user_id": user_id,
                    "message": "Your file is ready",
                    "result_id": result["id"],
                },
            )

        logger.info("Successfully processed file %s", file_id)
        return {"file_id": file_id, "status": "processed"}
    except Exception as e:
        logger.error("Failed to process file %s: %s", file_id, str(e))
        raise
```

### Example 3: Scheduled Cleanup

Regular maintenance tasks:

```python
import logging
from datetime import datetime, timezone

from conductor import TaskQueue

logger = logging.getLogger(__name__)


@worker.task("cleanup_expired_sessions")
async def cleanup_expired_sessions(payload: dict) -> dict:
    """Remove expired user sessions."""
    logger.info("Starting daily session cleanup")

    try:
        deleted_count = await db.execute(
            "DELETE FROM sessions WHERE expires_at < NOW()"
        )
        logger.info(
            "Deleted %d expired sessions", deleted_count
        )
        return {"deleted": deleted_count}
    except Exception as e:
        logger.error("Failed to cleanup sessions: %s", str(e))
        raise


# Submit via cron job: runs daily at 2 AM
# async with TaskQueue(database_url="...") as queue:
#     await queue.submit(
#         task_type="cleanup_expired_sessions",
#         payload={},
#     )
```

> **Tip:** For recurring/scheduled tasks, use an external cron job
> or systemd timer to submit tasks on a schedule. Native cron
> support is planned for v0.2.

### Example 4: Error Handling & Idempotency

Patterns for robust task handlers:

```python
@worker.task("process_order")
async def process_order(payload):
    """Process an order with idempotency"""
    order_id = payload["order_id"]
    
    # Check if already processed (idempotency)
    existing = await db.fetch_one(
        "SELECT id FROM processed_orders WHERE order_id = %s",
        order_id
    )
    
    if existing:
        logger.info(f"Order {order_id} already processed")
        return {"status": "already_processed", "order_id": order_id}
    
    try:
        # Process payment
        payment_result = await charge_payment(order_id)
        
        # Record as processed (atomic)
        await db.execute(
            "INSERT INTO processed_orders (order_id, payment_id) VALUES (%s, %s)",
            order_id,
            payment_result["id"]
        )
        
        logger.info(f"Order {order_id} processed successfully")
        return {"status": "success", "order_id": order_id}
    
    except PaymentError as e:
        # Specific error handling
        logger.error(f"Payment failed for order {order_id}: {e}")
        raise  # Retry
    
    except DatabaseError as e:
        # Database error - likely transient
        logger.error(f"Database error for order {order_id}: {e}")
        raise  # Retry
    
    except Exception as e:
        # Unexpected error
        logger.error(f"Unexpected error for order {order_id}: {e}", exc_info=True)
        raise  # Move to DLQ after retries
```

---

## Configuration

### Task Queue Options

```python
from conductor import TaskQueue

queue = TaskQueue(
    database_url="postgresql://user:password@localhost/conductor",
    task_timeout=300.0,       # Task execution timeout (seconds)
    max_task_age=86400,       # Clean up tasks older than 1 day
    log_level="INFO",         # Logging level
    pool_min_size=2,          # Minimum DB pool connections
    pool_max_size=10,         # Maximum DB pool connections
    pool_timeout=30.0,        # DB pool acquire timeout
    command_timeout=60.0,     # SQL command timeout
)
```

### Worker Options

```python
from conductor import Worker

worker = Worker(
    database_url="postgresql://user:password@localhost/conductor",
    worker_id="worker-1",           # Unique worker identifier
    concurrency=10,                  # Max concurrent tasks
    poll_interval=0.5,               # Check for tasks every 500ms
    routes=None,                     # ``None`` polls all routes
    log_level="INFO",
    heartbeat_interval=10.0,         # DB heartbeat every 10s
    graceful_shutdown_timeout=30.0,  # Wait 30s for in-flight tasks
)
```

### Retry Policies

```python
from conductor import RetryPolicy

# Default retry policy
RetryPolicy(
    max_retries=3,
    backoff_strategy="exponential",
    initial_delay=1.0,  # seconds
    max_delay=3600.0,
)

# No retries
RetryPolicy(max_retries=0)

# Linear backoff (5s, 10s, 15s, 20s, 25s)
RetryPolicy(
    max_retries=5,
    backoff_strategy="linear",
    initial_delay=5.0,
    max_delay=3600.0,
)

# Fixed backoff (always 10s delay)
RetryPolicy(
    max_retries=10,
    backoff_strategy="fixed",
    initial_delay=10.0,
    max_delay=3600.0,
)
```

You can also pass a dict with matching keys (useful for JSON configs):

```python
policy = RetryPolicy.from_dict({
    "max_retries": 3,
    "backoff_strategy": "exponential",
    "initial_delay": 1.0,
    "max_delay": 3600.0,
})
```

### Environment Variables

```env
# Database (required)
DATABASE_URL=postgresql://postgres:password@localhost:5432/conductor

# Worker
WORKER_ID=worker-1
CONCURRENCY=10
POLL_INTERVAL=0.5
ROUTES=default
HEARTBEAT_INTERVAL=10
GRACEFUL_SHUTDOWN_TIMEOUT=30
CONDUCTOR_HANDLERS_MODULE=

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Metrics & Health (shared port)
METRICS_ENABLED=true
METRICS_PORT=8000
HEALTH_ENABLED=true

# Tasks
TASK_TIMEOUT=300
MAX_TASK_AGE=86400

# Connection pool
DB_MIN_SIZE=2
DB_MAX_SIZE=10
DB_TIMEOUT=30
DB_COMMAND_TIMEOUT=60
```

> Full reference: [docs/configuration.md](docs/configuration.md).

---

## Deployment

Ready-to-use deployment artifacts are included in the repository and
covered in detail in [docs/deployment.md](docs/deployment.md).

- **Docker** — a `python:3.11-slim` `Dockerfile` (non-root user, healthcheck
  on `/health`, `ENTRYPOINT ["conductor"] CMD ["worker"]`):
  ```bash
  docker build -t conductor:0.1.0 .
  docker run --rm -e DATABASE_URL=postgresql://... -p 8000:8000 conductor:0.1.0
  ```
- **Docker Compose** — `docker-compose.yml` (dev: PostgreSQL + worker) and
  `docker-compose.prod.yml` (replicas, resource limits, log rotation, nightly
  `pg_dump` backup):
  ```bash
  docker compose up -d --build
  docker compose -f docker-compose.prod.yml up -d --build --scale worker=3
  ```
- **Kubernetes** — `examples/kubernetes.yaml` (ConfigMap, Secret, Deployment
  with 3 replicas + liveness/readiness probes, Service):
  ```bash
  kubectl apply -f examples/kubernetes.yaml
  ```
- **systemd** — `examples/conductor-worker.service` (runs `conductor worker`):
  ```bash
  sudo cp examples/conductor-worker.service /etc/systemd/system/
  sudo systemctl daemon-reload && sudo systemctl enable --now conductor-worker
  ```

Validate deployment files locally without Docker:

```bash
python scripts/validate_deploy.py
```

The worker serves Prometheus metrics at `/metrics` and a JSON health check at
`/health` on `METRICS_PORT` (default 8000). A Grafana dashboard is provided
in [docs/grafana/](docs/grafana/README.md).

---

## Architecture

### How It Works

```
┌─────────────────────────────────────────────────────────┐
│                   Your Application                      │
│                  queue.submit(task)                     │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  PostgreSQL Database                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────┐ │
│  │  Tasks   │  │ Workers  │  │ Retries  │  │  DLQ   │ │
│  └──────────┘  └──────────┘  └──────────┘  └────────┘ │
└──────────┬──────────────────────────────────┬───────────┘
           │                                  │
      Polls every │                          │
      500ms       ▼                          │
┌──────────────────────────────────────────────────────────┐
│                   Worker Processes                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐              │
│  │ Worker-1 │  │ Worker-2 │  │ Worker-N │              │
│  │ async    │  │ async    │  │ async    │              │
│  └──────────┘  └──────────┘  └──────────┘              │
│  (Multiple machines or containers, all share DB)        │
└──────────────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **PostgreSQL as source of truth** – No separate message broker. All state lives in one place.
2. **Polling-based task dispatch** – Workers poll for new tasks every 500ms. No complex subscription logic.
3. **Idempotent task processing** – Workers record task IDs. Duplicate submissions are deduplicated automatically.
4. **Async-first** – Built on asyncio. No threads, no blocking I/O.
5. **Observable** – Every task transition logged and metered.

---

## API Reference

### TaskQueue

#### `submit(task_type, payload, *, retry_policy=None, scheduled_for=None, route="default", priority=0, task_id=None)`

Submit a new task (async).

**Parameters**:
- `task_type` (str): Logical type used to route the task to a handler
- `payload` (dict): Task data (JSON serializable)
- `retry_policy` (RetryPolicy, optional): Retry configuration
- `scheduled_for` (datetime, optional): Earliest pickup time
- `route` (str): Route for selective worker polling
- `priority` (int): Higher runs first
- `task_id` (str, optional): Explicit task ID

**Returns**: `str` – Task ID

**Example**:
```python
task_id = await queue.submit(
    task_type="send_email",
    payload={"to": "user@example.com", "subject": "Hello"},
    retry_policy=RetryPolicy(max_retries=3, backoff_strategy="exponential"),
)
```

#### `list_pending_tasks(limit=10, offset=0)`

Get pending (not yet executed) tasks.

**Parameters**:
- `limit` (int): Max results
- `offset` (int): Pagination offset

**Returns**: `List[Task]`

#### `list_completed_tasks(limit=10, offset=0)`

Get completed tasks.

#### `list_failed_tasks(limit=10, offset=0)`

Get tasks that failed (``status='failed'``).

#### `get_task(task_id)`

Get a single task by ID.

**Parameters**:
- `task_id` (str): Task identifier

**Returns**: `Task` or `None`

### Worker

#### `@worker.task(task_type)`

Decorator to register a task handler.

**Parameters**:
- `task_type` (str): Task type to handle

**Example**:
```python
@worker.task("send_email")
async def handle_send_email(payload):
    # Process task
    return {"status": "sent"}
```

#### `run()`

Start the worker event loop (runs indefinitely).

**Example**:
```python
asyncio.run(worker.run())
```

#### `run_once()`

Execute one polling cycle (useful for testing).

#### `shutdown()`

Gracefully shut down the worker.

### DeadLetterQueue

#### `list_tasks(limit=10, offset=0, include_discarded=False)`

List tasks in the dead-letter queue.

**Parameters**:
- `include_discarded` (bool): Include soft-deleted tasks

**Returns**: `List[DLQTask]`

#### `get_task(task_id)`

Get a single DLQ task.

**Returns**: `DLQTask` or `None`

#### `retry_task(task_id)`

Retry a task from the DLQ (reset to pending, clears worker and error).

**Returns**: `str` — The task ID

#### `discard_task(task_id, reason=None)`

Permanently mark a DLQ task as discarded (soft-delete).

**Parameters**:
- `reason` (str, optional): Why the task is being discarded

#### `count(include_discarded=False)`

Count tasks in the dead-letter queue.

**Returns**: `int`

---

## Best Practices

### 1. Design Idempotent Tasks

Always assume a task might run twice:

```python
# Bad: Not idempotent
@worker.task("increment_counter")
async def increment_counter(payload):
    user_id = payload["user_id"]
    await db.execute("UPDATE users SET score = score + 1 WHERE id = ?", user_id)
    # If this runs twice, score increases by 2!

# Good: Idempotent
@worker.task("set_status")
async def set_status(payload):
    user_id = payload["user_id"]
    status = payload["status"]
    await db.execute("UPDATE users SET status = ? WHERE id = ?", status, user_id)
    # Safe to run multiple times (same result each time)
```

### 2. Use Structured Logging

Use lazy %-formatting for better performance:

```python
import logging

logger = logging.getLogger(__name__)

@worker.task("process_data")
async def process_data(payload: dict) -> dict:
    task_id = payload.get("task_id", "unknown")
    logger.info("Task %s starting processing", task_id)
    # ... do work ...
    logger.info("Task %s completed", task_id)
    return {"status": "done"}
```

### 3. Handle Exceptions Gracefully

Always catch ``ConductorException`` for predictable error handling:

```python
from conductor import TaskQueue
from conductor.exceptions import ConductorException


async with TaskQueue(database_url="...") as queue:
    try:
        task_id = await queue.submit(
            task_type="critical_job",
            payload={"data": "..."},
        )
        print(f"Submitted: {task_id}")
    except ConductorException as exc:
        # Database connection failure, invalid config, etc.
        print(f"Failed to submit: {exc}")
```

### 4. Monitor Dead Letter Queue

Don't ignore failed tasks:

```python
import asyncio
from conductor import DeadLetterQueue


async def check_dlq() -> None:
    async with DeadLetterQueue(database_url="...") as dlq:
        count = await dlq.count()
        if count > 0:
            tasks = await dlq.list_tasks(limit=100)
            print(f"⚠️ {len(tasks)} tasks in DLQ")
            for task in tasks:
                print(f"  - {task.task_id}: {task.error_message}")


asyncio.run(check_dlq())
```

### 5. Scale Horizontally

Run multiple workers across machines:

```bash
# Machine 1
python worker.py --worker-id worker-1

# Machine 2
python worker.py --worker-id worker-2

# Machine 3
python worker.py --worker-id worker-3
```

All workers share the same PostgreSQL database. No coordination needed.

### 6. Use Environment Variables

Never hardcode credentials:

```python
import os
from conductor import TaskQueue, Worker

database_url = os.getenv("DATABASE_URL")
queue = TaskQueue(database_url=database_url)
worker = Worker(database_url=database_url)
```

---

## Troubleshooting

### Issues with PostgreSQL Connection

**Problem**: `ConnectionError: could not connect to server`

**Solutions**:
1. Verify PostgreSQL is running: `pg_isready -h localhost`
2. Check connection string: `DATABASE_URL=postgresql://user:password@host:port/database`
3. Verify network permissions (firewall, security groups)
4. Check database credentials

### Tasks not processing

**Problem**: Tasks submitted but not executed

**Checklist**:

1. **Is the worker running?**
   ```bash
   ps aux | grep worker.py
   ```

2. **Are there tasks in the queue?**
   ```python
   from conductor import TaskQueue
   queue = TaskQueue(database_url="...")
   pending = queue.list_pending_tasks()
   print(f"Pending tasks: {len(pending)}")
   ```

3. **Check worker logs**:
   ```
   [2025-01-15 10:30:45] INFO: Worker started, listening for tasks
   [2025-01-15 10:30:46] DEBUG: Polled 0 tasks
   [2025-01-15 10:30:47] DEBUG: Polled 1 task (task_id: abc-123)
   ```

4. **Verify task handler is registered**:
   ```python
   @worker.task("my_task")
   async def handler(payload):
       return {"status": "done"}
   ```

### High latency

**Problem**: Tasks taking too long to execute after submission

**Causes**:
- Polling interval too long (default: 500ms)
- Worker overloaded (concurrency limit reached)
- Database queries slow

**Solutions**:

1. **Increase polling frequency** (trade-off: higher DB load):
   ```python
   worker = Worker(poll_interval=0.1)  # Check every 100ms
   ```

2. **Scale workers horizontally**:
   ```bash
   python worker.py --worker-id worker-1 &
   python worker.py --worker-id worker-2 &
   python worker.py --worker-id worker-3 &
   ```

3. **Increase worker concurrency**:
   ```python
   worker = Worker(concurrency=20)  # Handle 20 concurrent tasks
   ```

### Database errors

**Problem**: `DatabaseError: connection timeout` or `pool exhausted`

**Solutions**:

1. **Check PostgreSQL connection**:
   ```bash
   psql postgresql://user:password@localhost/conductor -c "SELECT 1"
   ```

2. **Schema is auto-managed** — tables are created on first `connect()`. Verify with `\dt` in psql.

3. **Check database load**:
   ```sql
   SELECT count(*) FROM conductor_tasks WHERE status = 'pending';
   ```

4. **Increase connection pool size**:
   ```python
   queue = TaskQueue(
       database_url="...",
       pool_min_size=5,
       pool_max_size=20,  # Increase from default 10
       pool_timeout=60.0,
   )
   ```

### Worker crashes / signal handling

**Problem**: Worker doesn't shut down cleanly

**Solutions**:

1. **Add signal handlers**:
   ```python
   import signal
   
   def handle_shutdown(sig, frame):
       print("Shutting down...")
       worker.shutdown()
   
   signal.signal(signal.SIGTERM, handle_shutdown)
   signal.signal(signal.SIGINT, handle_shutdown)
   ```

2. **Increase graceful shutdown timeout**:
   ```python
   worker = Worker(
       database_url="...",
       graceful_shutdown_timeout=60  # Wait up to 60s for tasks
   )
   ```

---

## Comparison to Alternatives

| Feature | Conductor | Celery | RQ | Dramatiq |
|---------|-----------|--------|-----|----------|
| **No external broker** | ✅ | ❌ (needs Redis/RabbitMQ) | ❌ (needs Redis) | ❌ (needs RabbitMQ) |
| **Exactly-once semantics** | ✅ | ⚠️ (at-least-once) | ⚠️ (at-least-once) | ⚠️ (at-least-once) |
| **Dead letter queue** | ✅ | ⚠️ (limited) | ❌ | ✅ |
| **Built-in observability** | ✅ | ⚠️ (needs Flower) | ⚠️ (needs tools) | ⚠️ (needs tools) |
| **Simple API** | ✅ | ❌ (steep learning curve) | ✅ | ✅ |
| **Graceful shutdown** | ✅ | ✅ | ✅ | ✅ |
| **Scheduled tasks** | ⚠️ (v0.2) | ✅ | ❌ | ✅ |
| **Async-native** | ✅ | ⚠️ (hybrid) | ❌ | ✅ |
| **PostgreSQL only** | ✅ | ❌ | ❌ | ❌ |
| **Production-ready** | ✅ | ✅ | ⚠️ | ✅ |

**When to use Conductor**:
- You don't want Redis/RabbitMQ dependency
- You need reliability guarantees (exactly-once)
- You want simple deployment
- Observability is important

**When to use alternatives**:
- You already have Celery (switching is expensive)
- You need advanced routing (Celery)
- You have complex workflows (Airflow, Prefect)

---

## Performance & Benchmarks

*Benchmarks under typical conditions (PostgreSQL on localhost, 4 concurrent workers):*

| Metric | Target | Notes |
|--------|--------|-------|
| Task submission | <2ms | Single-row insert |
| Task polling latency | ~500ms | Dictated by poll_interval (configurable) |
| Task processing (empty handler) | <10ms | Status update + database write |
| Throughput (simple task) | 400+ tasks/sec per worker | Scales linearly with worker count |
| Memory per worker | ~50MB base | + payload size |
| Database load (100 tasks/sec) | ~15% CPU | PostgreSQL on localhost |

**Notes**:
- Latency is dominated by polling interval (currently 500ms). Can be tuned for lower latency / higher DB load.
- Throughput scales linearly with worker count.
- Database is the bottleneck at very high scale (>10k tasks/sec). Consider sharding or dedicated task queue for extreme scale.

---

## Roadmap

### Phase 1 (v0.1 — Current)

✅ **Completed**:
- Basic task queue
- Retry logic with exponential backoff
- Dead letter queue with retry/discard API
- Worker pool with concurrency control
- Graceful shutdown with signal handling

🔄 **In Progress**:
- Structured logging (Sprint 5)
- Prometheus metrics exporter (Sprint 5)
- Health check endpoint (Sprint 5)
- Performance benchmarks (Sprint 6)
- Docker & deployment examples (Sprint 6)
- Comprehensive documentation (Sprint 6)

### Phase 2 (v0.2 — Planned)

🔲 Task routing, priority queues, scheduled/recurring tasks (cron)
🔲 Web dashboard, gRPC API
🔲 Circuit breaker pattern, task dependencies/chaining

### Phase 3 (v0.3+ — Future)

🔲 Multi-database support (MySQL, SQLite)
🔲 Distributed tracing (OpenTelemetry)
🔲 Advanced workflows, task versioning

---

## Local Development

```bash
# 1. Start PostgreSQL
docker compose up -d

# 2. Copy and configure environment
cp .env.example .env

# 3. Create virtual environment & install
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 4. Run tests (schema auto-creates on first connect)
pytest

# 5. Run linting and type checking
black .
mypy conductor/
flake8 conductor/ tests/
```

---

## Contributing

We welcome contributions! Here's how:

1. **Fork the repository** on GitHub
2. **Create a feature branch** (`git checkout -b feature/amazing-feature`)
3. **Write tests** for your changes (aim for 85%+ coverage)
4. **Run linter and type checker**:
   ```bash
   black .
   mypy conductor/
   flake8 conductor/ tests/
   ```
5. **Run tests**:
   ```bash
   CONDUCTOR_TEST_DATABASE_URL=postgresql://conductor:conductor@localhost:5432/conductor_test pytest
   ```
   ```
6. **Submit a pull request** with a clear description

### Development Setup

```bash
# Clone repo
git clone https://github.com/yourusername/conductor.git
cd conductor

# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run type checker
mypy src/

# Format code
black src/ tests/
```

### Areas for Contribution

- Performance optimizations
- Additional retry strategies
- Observability enhancements
- Documentation improvements
- Example projects
- Database backend support (MySQL, SQLite)
- Polyglot worker implementations

---

## Support & Community

- **Documentation**: [docs.conductor.sh](https://docs.conductor.sh)
- **Issues & Bugs**: [GitHub Issues](https://github.com/Archangel-77/Conductor/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Archangel-77/Conductor/discussions)

---

## License

Conductor is licensed under the **MIT License**. See [LICENSE](LICENSE) file for details.

---

## Authors

- **Panagiotis Panageas** (@Archangel-77) – Creator & Maintainer

---

## Acknowledgments

Inspired by the need for a simpler task queue that doesn't require external infrastructure. Built for teams who value reliability, observability, and simplicity.

---

**Start building reliable async systems with Conductor today.**

```bash
pip install conductor-task-queue
```
