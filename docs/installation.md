# Installation

## Prerequisites

- **Python 3.11+**
- **PostgreSQL 12+** (local, container, or managed — Conductor uses only
  PostgreSQL; no Redis or message broker is required)

## Install the Package

```bash
pip install conductor-task-queue
```

For development (tests, linting, docs tooling):

```bash
pip install -e ".[dev]"
```

## Database Setup

Conductor creates its schema automatically on first `connect()` (tables,
indexes, and schema-version tracking). You only need an empty database:

```bash
createdb conductor
```

Or, if you use the included development environment:

```bash
docker compose up -d postgres
```

Tables created automatically:

- `conductor_tasks` – task submissions and status
- `conductor_workers` – worker heartbeats
- `conductor_retries` – retry history
- `conductor_dead_letter` – failed tasks

## Worker Startup

There are two ways to run a worker: embed Conductor in your own asyncio
program, or use the bundled `conductor` CLI.

### Option 1: Embed in your application

```python
import asyncio
from conductor import Worker


async def main() -> None:
    async with Worker(
        database_url="postgresql://user:pass@localhost:5432/conductor"
    ) as worker:

        @worker.task("send_email")
        async def send_email(payload: dict) -> dict:
            # ... send the email ...
            return {"status": "sent"}

        await worker.run()


asyncio.run(main())
```

### Option 2: Use the `conductor` CLI

Create a handlers module that exposes a `register(worker)` function:

```python
# myapp/handlers.py
from conductor.core.worker import Worker


def register(worker: Worker) -> None:
    @worker.task("send_email")
    async def send_email(payload: dict) -> dict:
        return {"status": "sent"}
```

Set your environment (see [Configuration](configuration.md)) and run:

```bash
export DATABASE_URL=postgresql://user:pass@localhost:5432/conductor
conductor worker --handlers myapp.handlers
```

The same works with `CONDUCTOR_HANDLERS_MODULE` instead of `--handlers`,
or via `python -m conductor worker`. A `.env` file is loaded automatically
if present (install `python-dotenv`, already a dependency).

## Verify

Submit a task and confirm the worker completes it:

```python
import asyncio
from conductor import TaskQueue


async def main() -> None:
    async with TaskQueue(database_url="postgresql://user:pass@localhost:5432/conductor") as queue:
        task_id = await queue.submit("send_email", {"to": "user@example.com"})
        print(f"Submitted: {task_id}")


asyncio.run(main())
```

With a running worker, the health endpoint reports the queue state:

```bash
curl http://localhost:8000/health
```

```json
{"status": "healthy", "database": "connected", "pending_tasks": 0, ...}
```

Metrics are available at `http://localhost:8000/metrics` (Prometheus text
format).
