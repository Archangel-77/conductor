# Deployment

Conductor ships as a plain Python package plus an optional `conductor` CLI.
This page covers running it with Docker, Docker Compose, Kubernetes, and
systemd. All artifacts referenced here are committed in the repository and
can be validated locally without Docker via `scripts/validate_deploy.py`.

## Docker

A production-ready `Dockerfile` is included:

```bash
docker build -t conductor:0.3.0 .
docker run --rm \
  -e DATABASE_URL=postgresql://user:pass@pg-host:5432/conductor \
  -p 8000:8000 \
  conductor:0.3.0
```

The image:

- uses `python:3.11-slim` and runs as a non-root `conductor` user;
- installs the package and its `conductor` console script;
- exposes port `8000` and a `HEALTHCHECK` against `/health`;
- runs `conductor worker` (reads env vars / `.env`).

Add task handlers by mounting a module and pointing the worker at it:

```bash
docker run --rm \
  -v "$PWD/myapp:/app/handlers" \
  -e DATABASE_URL=postgresql://... \
  -e CONDUCTOR_HANDLERS_MODULE=handlers.myapp \
  conductor:0.3.0
```

## Docker Compose

### Development

`docker-compose.yml` runs PostgreSQL + a worker:

```bash
docker compose up -d --build
```

- Worker: `http://localhost:8000/health` and `/metrics`.
- Config is set via `environment:` (see [Configuration](configuration.md)).

### Production

`docker-compose.prod.yml` adds worker replicas, resource limits, log
rotation, and a nightly `pg_dump` backup sidecar:

```bash
export POSTGRES_PASSWORD='<strong-password>'
docker compose -f docker-compose.prod.yml up -d --build --scale worker=3
```

> `deploy.replicas` is honoured under Docker Swarm; under plain Compose use
> `--scale worker=N` (as above).

Validate both files:

```bash
docker compose config --quiet
docker compose -f docker-compose.prod.yml config --quiet
python scripts/validate_deploy.py   # static checks, no Docker needed
```

## Kubernetes

A complete manifest is in `examples/kubernetes.yaml`:

```bash
kubectl apply -f examples/kubernetes.yaml
```

It defines a `ConfigMap` (non-secret config), a `Secret` (DATABASE_URL),
a `Deployment` with 3 replicas plus liveness/readiness probes on `/health`,
and a `ClusterIP` `Service` on port 8000. PostgreSQL is assumed to be
external/managed — set `DATABASE_URL` in the Secret accordingly.

## Systemd

A unit file is provided at `examples/conductor-worker.service`:

```bash
sudo cp examples/conductor-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now conductor-worker
```

It runs `conductor worker` under a dedicated `conductor` user with
`EnvironmentFile` pointing at your `.env` file.

## Embedded SQLite (single process)

SQLite removes the database server entirely, which suits appliances, desktop
apps, and small services that run one worker:

```bash
pip install "conductor-task-queue[sqlite]"

DATABASE_URL=sqlite:////var/lib/conductor/conductor.db \
  conductor worker --handlers myapp.handlers
```

> **One worker process per database file.** SQLite has no
> `FOR UPDATE SKIP LOCKED`, so cross-process row claiming is impossible. Run
> two workers against the same file and a task can execute twice. Use
> PostgreSQL whenever you need horizontal scaling.

Operational notes:

- Create the directory first and make it writable by the worker user; the file
  is created on first `connect()`.
- The journal runs in WAL mode, so `conductor.db-wal` and `conductor.db-shm`
  appear alongside the database. **Back up all three** (or use
  `sqlite3 conductor.db "VACUUM INTO '/backup/conductor.db'"` for a consistent
  online copy) — copying only the main file can lose recent commits.
- `DB_BUSY_TIMEOUT` (default `5s`) bounds how long a writer waits for a lock.
- `/metrics` and `/health` work unchanged; the dashboard's read queries do too.

## MySQL / MariaDB

MySQL 8.0+ (or MariaDB 10.6+) behaves like PostgreSQL — workers claim rows with
`FOR UPDATE SKIP LOCKED`, so you can scale out by running more worker processes
against the same database:

```bash
pip install "conductor-task-queue[mysql]"

DATABASE_URL=mysql://conductor:secret@mysql.internal:3306/conductor \
  conductor worker --handlers myapp.handlers
```

A throwaway server for local experiments:

```bash
docker run -d --name conductor-mysql -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=conductor \
  -e MYSQL_USER=conductor \
  -e MYSQL_PASSWORD=secret \
  mysql:8.4
```

Operational notes:

- The schema (InnoDB, `utf8mb4_unicode_ci`) is created on first `connect()`.
  Grant the worker user DDL rights for that first start, or pre-apply the schema
  with a privileged account.
- Timestamps are stored as `DATETIME(6)` in **UTC**; keep the server timezone
  and the application timezone aligned (`time_zone = '+00:00'`) so
  `CURRENT_TIMESTAMP(6)` defaults match the values Conductor writes.
- `DB_COMMAND_TIMEOUT` becomes the driver's `read_timeout`; raise it for
  handlers that run for minutes.
- `DB_MIN_SIZE` / `DB_MAX_SIZE` map to the driver's `minsize` / `maxsize`;
  `DB_BUSY_TIMEOUT` is SQLite-only and ignored.
- **MySQL 5.7 is not supported** (`CHECK` constraints and `SKIP LOCKED` are
  missing); MariaDB needs 10.6+ (`FOR UPDATE SKIP LOCKED`). Older servers are
  rejected at `connect()` with the detected version in the message.

## Web Dashboard

The dashboard (FastAPI + built React frontend) ships inside the wheel, so no
Node.js toolchain is required at deploy time — the committed `conductor/web/dist`
bundle is served automatically.

**Standalone** (separate process, shares the same PostgreSQL):

```bash
conductor api --host 0.0.0.0 --port 8080
CONDUCTOR_API_KEY=s3cret conductor api   # require X-API-Key on /api/*
```

**Embedded in a worker** (serves on the worker's `CONDUCTOR_API_PORT`, default
8080):

```bash
CONDUCTOR_API_ENABLED=true conductor worker
```

The dashboard binds its own port (`CONDUCTOR_API_PORT` / `--port`, default
8080) and is independent of the metrics/health port (`METRICS_PORT`, default
8000). The built SPA is served at `/`; the JSON API lives under `/api`.

**Rebuilding the frontend** (only needed when the React sources change):

```bash
scripts/build_frontend.sh      # npm ci && npm run build -> conductor/web/dist
```

The CI `frontend` job runs this and fails if the committed `dist/` is stale.

## Observability

The worker serves Prometheus metrics at `/metrics` and a JSON health check
at `/health` on `METRICS_PORT` (default 8000). Scrape it with Prometheus and
visualize with the bundled Grafana dashboard (`docs/grafana/`):

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "conductor"
    metrics_path: /metrics
    static_configs:
      - targets: ["<worker-host>:8000"]
```

See `docs/grafana/README.md` for dashboard import instructions.
