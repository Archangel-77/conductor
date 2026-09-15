# Installation

## Prerequisites

- **Python 3.11+**
- **PostgreSQL 12+** (local, container, or managed), **MySQL 8.0+ / MariaDB
  10.6+**, or **SQLite** for embedded single-process deployments
- No Redis, no message broker — the database *is* the queue

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

## SQLite (embedded, no server)

For local development, tests, desktop apps, and single-process services,
Conductor runs entirely on an embedded SQLite database:

```bash
pip install "conductor-task-queue[sqlite]"
export DATABASE_URL=sqlite:///conductor.db
conductor worker --handlers myapp.handlers
```

DSN forms:

| DSN | Database |
|---|---|
| `sqlite:///conductor.db` | relative to the working directory |
| `sqlite:////var/lib/conductor.db` | absolute path |
| `sqlite://:memory:` | in-memory, discarded on exit (handy for tests) |

The schema is created on first `connect()` exactly as for PostgreSQL, and
retries, the DLQ, dependencies, recurring tasks, routing/priority, and the
dashboard all work unchanged.

### Single-process contract

**Exactly one worker process may target a given SQLite file.**

SQLite has no `SELECT … FOR UPDATE SKIP LOCKED`, so Conductor cannot reproduce
cross-process row claiming. Within one process the SQLite backend serialises
database access and opens write transactions with `BEGIN IMMEDIATE` (WAL
journal mode), which provides the same exactly-once guarantee as PostgreSQL —
but **only** within that contract. Two worker processes on the same file can
execute the same task twice.

Use PostgreSQL when you need to scale workers horizontally. A single-process
service may still submit tasks (`TaskQueue`) while a worker runs in the same
process.

Tuning: `DB_BUSY_TIMEOUT` (default `5` seconds) is how long SQLite waits for a
lock before raising `database is locked`.

## MySQL / MariaDB

Conductor supports MySQL 8.0+ (8.0.16 or newer for `CHECK` constraints) and
MariaDB 10.6+ through the `asyncmy` driver:

```bash
pip install "conductor-task-queue[mysql]"
export DATABASE_URL=mysql://conductor:secret@localhost:3306/conductor
conductor worker --handlers myapp.handlers
```

```bash
# create the database (the schema itself is created on first connect)
mysql -u root -p -e "CREATE DATABASE conductor CHARACTER SET utf8mb4;"
```

DSN form: `mysql://user:password@host:port/database?charset=utf8mb4`. Query
parameters other than `charset` are passed straight to the driver
(`connect_timeout`, `read_timeout`, …).

Like PostgreSQL — and unlike SQLite — the MySQL backend claims pending rows
with `FOR UPDATE SKIP LOCKED`, so **workers scale horizontally**: run as many
worker processes against one database as you like, and each task runs exactly
once.

Notes:

- Tables are created as `InnoDB` with `utf8mb4` / `utf8mb4_unicode_ci`.
- Timestamps are stored as `DATETIME(6)` **in UTC** and read back as
  timezone-aware datetimes, matching the other backends.
- Arrays (`depends_on`) are stored as `JSON` and queried with `JSON_CONTAINS`;
  the column is not indexed (MySQL cannot index a `JSON` column, and the
  containment predicate could not use such an index anyway).

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
