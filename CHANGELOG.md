# Changelog

All notable changes to **Conductor** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Nothing yet.

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
