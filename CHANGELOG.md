# Changelog

All notable changes to **Conductor** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-13

### Added

- **Task routing** — tasks are submitted with a `route` (default `"default"`);
  workers poll only the routes they subscribe to via `Worker(routes=[...])` or
  the `ROUTES` env var. `Worker(routes=None)` polls **all** routes (no filter).
- **Priority queues** — tasks carry a `priority` in the range **-100..100**
  (default `0`). The polling query orders by `priority DESC, created_at ASC`,
  so higher-priority tasks are dispatched first even when submitted later.
- **`TaskQueue.list_pending_tasks(limit, offset, route=None)`** — optional route
  filter on the pending-tasks listing.
- **DLQ routing preservation (schema v2)** — `conductor_dead_letter` now stores
  `route`/`priority`, so routed/prioritized tasks keep their routing metadata
  when retried via `DeadLetterQueue.retry_task()` (including re-inserted tasks).
  `SchemaManager` migrates existing v1 databases incrementally to v2.
- **Fail-fast validation** — `submit()`/`submit_many()` validate `priority`
  (int, -100..100) and `route` (non-empty string) up-front, raising `ValueError`
  instead of failing later at the database layer.
- **Recurring (cron) tasks** — `TaskQueue.schedule_recurring(task_type, payload,
  cron_expression, ...)` registers a cron definition; `list_recurring_tasks()`,
  `get_recurring_task()`, `pause_recurring()`, `resume_recurring()`, and
  `delete_recurring_task()` manage them.
- **`RecurringScheduler`** (`conductor/recurring/`) — a daemon that polls due
  definitions and creates one task instance per fire, advancing `next_run_at`
  to the next cron fire (UTC, skipping missed runs). Due definitions are claimed
  with `FOR UPDATE SKIP LOCKED` (safe for multiple schedulers). Can be embedded
  in a worker via `Worker(enable_scheduler=True)` / `CONDUCTOR_ENABLE_SCHEDULER`.
- **`submit_many(scheduled_for=...)`** — shared earliest-pickup time for a batch
  (previously only single `submit()` supported it).
- **Schema v3** — composite `idx_recurring_polling (enabled, next_run_at)` index
  for the scheduler's hot query; `SchemaManager` migrates v2 databases to v3.
- **New dependency** — `croniter>=1.4` for cron expression parsing.
- **Metrics** — `conductor_recurring_fired_total{task_type=...}` counter.
- **gRPC API (polyglot workers)** — a Worker can embed an async `grpc.aio`
  server (`Worker(grpc_enabled=True)` / `GRPC_ENABLED`) exposing the
  `ConductorWorker` service: `ProcessTask` (execute through a registered
  handler; optional `persist=true` records the outcome), `RegisterHandler`
  (idempotent runtime handler registration), and `GetWorkerStatus`.
- **`conductor/grpc/`** — committed generated stubs (`conductor_pb2`,
  `conductor_pb2_grpc`) plus hand-maintained `.pyi` type stubs; regenerate via
  `python scripts/generate_grpc.py`. No `protoc` needed at install.
- **Dependencies** — `grpcio>=1.60` (runtime); `grpcio-tools`, `grpc-stubs`,
  `types-protobuf` (dev).
- **Config** — `GRPC_ENABLED`, `GRPC_PORT` (default 50051),
  `GRPC_MAX_MESSAGE_SIZE` env vars; `Worker(grpc_port, grpc_enabled,
  grpc_max_message_size)`; `get_status()` reports gRPC state.
- **Examples** — `examples/8_grpc_client.py` (Python client) and
  `examples/grpc/` Go/Rust/Node reference stubs.
- **Web dashboard** — a FastAPI app (`conductor/api/`) exposes a JSON API
  (`/api/tasks`, `/api/tasks/{id}`, `/api/tasks/{id}/cancel`, `/api/workers`,
  `/api/metrics`, `/api/dlq` + retry/discard, `/api/health`) and serves a
  built React + Vite frontend (`conductor/web/`) with five screens (Tasks,
  Task details, Workers, Metrics, DLQ). Optional API-key auth
  (`CONDUCTOR_API_KEY` → `X-API-Key` header; unset = open).
- **`TaskStatus.CANCELLED` (schema v4)** — new task status for cancelled
  tasks; `TaskQueue.cancel_task()` cancels pending/retrying tasks only
  (`TaskError` otherwise); `chk_task_status` CHECK rebuilt by the v3→v4
  migration.
- **Dashboard read queries** — non-locking `QueryBuilder.select_tasks()`
  (cross-status, ILIKE search, pagination), `count_tasks()`, and
  `select_all_workers()` for the dashboard (never reuse the locked
  `select_pending_tasks` for reads).
- **`DashboardServer`** (`conductor/api/server.py`) — uvicorn-backed embeddable
  server; standalone via `conductor api [--host --port --api-key --env-file]`
  or embedded in a worker (`Worker(api_enabled=True)` /
  `CONDUCTOR_API_ENABLED` / `CONDUCTOR_API_PORT` / `CONDUCTOR_API_KEY`).
- **Committed frontend bundle** — `conductor/web/dist` is committed and
  packaged (`[tool.setuptools.package-data]`), so no Node.js is needed at
  install; regenerate with `scripts/build_frontend.sh` (CI `frontend` job
  verifies freshness).
- **Dependencies** — `fastapi>=0.110`, `uvicorn>=0.29` (runtime); `httpx`
  (dev).
- **Example** — `examples/9_web_dashboard.py` (standalone dashboard server).
- **Circuit breaker** — a per-task-type, worker-side in-memory breaker
  (`conductor/circuit_breaker/`) that tracks *consecutive* failures: after
  `threshold` failures the circuit trips `CLOSED → OPEN` and the worker
  **skips** execution of that type (tasks stay pending — no false
  failures/DLQ); after `timeout` it becomes `HALF_OPEN` and allows
  `half_open_attempts` probe executions — a probe success closes the circuit,
  all probes failing re-opens it.
- **Models** — `CircuitState` enum, `CircuitBreakerConfig` (frozen dataclass,
  validates via `CircuitBreakerError`), `CircuitBreaker` (state machine with
  injectable clock), `CircuitBreakerRegistry` (per-task-type, with overrides).
- **Worker integration** — `Worker(circuit_breaker_enabled,
  circuit_breaker_config, circuit_breaker_overrides)`; `_execute_task` skips
  open types and records success/failure; `get_status()` reports
  `circuit_breaker_enabled` and `circuit_breaker_open`.
- **Metrics** — `conductor_tasks_rejected_total{task_type=...}` counter and
  `conductor_circuit_breaker_open{task_type=...}` gauge.
- **Config** — `CONDUCTOR_CIRCUIT_BREAKER_ENABLED` / `_THRESHOLD` / `_TIMEOUT` /
  `_HALF_OPEN_ATTEMPTS` env vars; `WorkerSettings` fields; `.env.example`.
- **Example** — `examples/10_circuit_breaker.py` (breaker trip → skip →
  recovery).
- **Task dependencies & chaining** — `TaskQueue.submit(..., depends_on=[...])`
  lets a task reference prerequisite task IDs; the polling query excludes
  tasks with unmet dependencies (they stay pending) until each dependency is
  `completed`/`cancelled`.
- **`TaskStatus.BLOCKED`** — when a dependency fails, its pending dependents
  are marked `blocked` (`dependency '<id>' failed`, terminal, no retry),
  propagating **transitively** (A→B→C) from the worker's failure path.
- **Schema v5** — `depends_on TEXT[]` column on `conductor_tasks` +
  `conductor_dead_letter`, GIN index `idx_tasks_depends_on`, and `blocked`
  added to `chk_task_status`; `SchemaManager` migrates v4 databases.
- **DLQ preservation** — `depends_on` is stored on the dead-letter table and
  restored when a task is retried (schema-v2 route/priority precedent).
- **Dashboard** — task detail shows `depends_on`; `blocked` status filter +
  badge; rebuilt committed `conductor/web/dist`.
- **Example** — `examples/11_task_chaining.py` (chained execution + failure
  propagation).

## [0.1.0] - 2026-07-31

### Added

- **Core task queue** — lightweight, async-native, PostgreSQL-backed task queue with
  exactly-once semantics and idempotent processing. No Redis or external broker.
- **`TaskQueue`** (`conductor/core/queue.py`):
  - `submit()` / `submit_many()` with UUID v4 task IDs, payload, and `RetryPolicy`
  - `get_task()`, `list_pending_tasks()`, `list_completed_tasks()`, `list_failed_tasks()`
  - Retry-policy validation on submit; optional `scheduled_for`, `route`, `priority`
  - DLQ convenience methods: `list_dlq_tasks()`, `get_dlq_task()`, `retry_dlq_task()`,
    `discard_dlq_task()`, `count_dlq_tasks()`
- **`Worker`** (`conductor/core/worker.py`):
  - `@worker.task()` handler decorator with signature validation
  - Atomic task acquisition via `FOR UPDATE SKIP LOCKED`, route filtering, batch polling
  - Concurrency limiting with `asyncio.Semaphore`
  - Heartbeat loop and graceful shutdown (SIGTERM/SIGINT) with in-flight drain
  - `run_once()` for deterministic single-cycle execution; `get_status()` reporting
- **Retry logic** (`conductor/retry/` + `conductor/core/models.py`):
  - `RetryPolicy` with `max_retries`, `initial_delay`, `max_delay`
  - Exponential, linear, and fixed backoff strategies; `calculate_backoff_delay()` helper
  - Retry history in the `conductor_retries` table; `retrying` task status
- **Dead Letter Queue** (`conductor/dlq/dead_letter_queue.py`):
  - `DeadLetterQueue` with `list_tasks()`, `get_task()`, `retry_task()`, `discard_task()`
  - Discard tracking (soft-delete with reason and timestamp)
- **Observability** (`conductor/observability/`):
  - JSON structured logging with `task_id`, `task_type`, `worker_id`, `duration_ms`
  - Prometheus metrics (counters, histograms, gauges) exported over HTTP on `:8000`
  - `/health` endpoint with `healthy` / `degraded` / `unhealthy` status and DB checks
- **CLI & configuration** (`conductor/cli.py`, `conductor/config.py`):
  - `conductor worker [--handlers MODULE]` console script; `python -m conductor`
  - `WorkerSettings.from_env()` mapping all environment variables to worker options
- **Deployment**:
  - `Dockerfile` (python:3.11-slim, non-root, healthcheck), `.dockerignore`
  - `docker-compose.yml` (dev) and `docker-compose.prod.yml` (replicas, pg backup)
  - Kubernetes manifest, systemd unit, `scripts/validate_deploy.py`
- **Documentation**: README overhaul, `docs/` (installation, configuration, API
  reference, deployment, troubleshooting, index), Grafana dashboard + usage README
- **Examples**: five runnable scripts (`examples/`) covering basic queueing, email
  notifications with retry, data-processing pipelines, scheduled cleanup, and error
  handling / idempotency / DLQ recovery
- **Testing**: unit, integration, E2E, and performance suites; GitHub Actions CI
  (`test.yml`: lint + tests against PostgreSQL + Codecov + perf) and release workflow
  (`release.yml`: build, publish to PyPI, GitHub Release)
- **Packaging**: MIT license, `py.typed` marker for downstream type checking

### Fixed

- `RetryPolicy(backoff_strategy="...")` accepted the documented string form but
  crashed in `to_dict()` — the field is now normalized to the enum in
  `__post_init__`, and invalid strategies raise `RetryPolicyError`
- `SchemaManager.rollback()` to v0 kept the `conductor_version` table despite the
  "no tables" contract — it is now dropped (and recreated idempotently by
  `ensure_schema()`)
- DB unit tests failed under `pytest-asyncio` 1.x due to an event-loop scope
  mismatch — aligned via `asyncio_default_test_loop_scope = "session"` and
  session-scoped fixtures; disconnect tests now use private pools so they no longer
  corrupt the shared session fixture
- Multi-worker tests were flaky (one worker could grab the whole poll batch) — made
  deterministic by alternating `run_once()` per submitted task

### Removed

- Nothing (no breaking changes in v0.1).

[0.1.0]: https://github.com/Archangel-77/Conductor/releases/tag/v0.1.0
[0.2.0]: https://github.com/Archangel-77/Conductor/releases/tag/v0.2.0
