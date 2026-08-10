# Conductor — Agent Instructions

## Project Identity
- **Package**: `conductor-task-queue` v0.1.0, MIT license, published to PyPI
- **Python**: 3.11+ only, asyncio-native, **no threads**
- **Database**: PostgreSQL 12+ only, **no Redis**, no external message brokers
- **Architecture**: Polling-based task dispatch against PostgreSQL; exactly-once semantics; idempotent task processing
- **Status**: v0.1.0 released to PyPI (2026-07-31). v0.2 Sprint 1 🔄 Routing & Priority in progress.

## Code Style & Formatting
- **Line length**: 100 characters (enforced by black and flake8)
- **Formatter**: `black` with `target_version = ["py311"]`
- **Linter**: `flake8` (ignore E203, W503)
- **Type checker**: `mypy` with `--strict` mode; `strict = true` in config
- **Import sorting**: standard library → third-party → local (grouped with blank lines)
- **`__init__.py` files**: Use `"""docstring"""` for module-level docs; re-export public API via `__all__: list[str] = [...]`
- **String quotes**: Double quotes `"` for all strings (black default)

## Documentation
- Every module file: `"""One-line summary.\n\nDetailed description.\n"""` (Google-style docstrings, summary line, blank line, then body)
- Every public class/method: Google-style docstrings with `Args:`, `Returns:`, `Raises:` sections
- `Args:` use the format: `arg_name: Description.` on the next line, indented
- Docstrings use backtick-delimited inline code for parameter/class references (e.g., `` ``Task`` ``)
- Logging statements: use lazy %-formatting, e.g. `logger.info("Task %s submitted.", task_id)`

## Typing & Models
- Add `from __future__ import annotations` at the top of every module
- Use `Optional[X]` (not `X | None`) for consistency with current codebase
- Use `Any` sparingly — prefer specific types
- **Models**: immutable `@dataclass(frozen=True)` with `field(default_factory=...)` for mutable defaults
- All model classes: implement `to_dict()` and `from_dict()` for JSON serialization
- **Enums**: inherit from `(str, Enum)` with `__str__` returning `self.value`
- Type aliases for complex callable signatures (e.g., `HandlerFunc = Callable[[dict[str, Any]], Awaitable[Optional[dict[str, Any]]]]`)
- All dataclass fields must have type hints and (where applicable) inline docstrings using `""" """`

## Exception Hierarchy
- Base: `ConductorException(Exception)`
- Subclasses: `DatabaseError`, `WorkerError`, `TaskError`, `RetryPolicyError`, `ConductorConnectionError`
- All defined in `conductor/exceptions.py`

## Database Patterns
- **Driver**: `asyncpg` only (connection pool via `DatabasePool` in `conductor/db/connection.py`)
- **No ORM** — use raw SQL with asyncpg parameter placeholders (`$1`, `$2`, …)
- **Polling**: Use `FOR UPDATE SKIP LOCKED` for atomic task acquisition
- **Schema versioning**: Track via `conductor_version` table; migrations in `conductor/db/schema.py`; idempotent (`CREATE IF NOT EXISTS`)
- **Query methods**: Defined in `QueryBuilder` class in `conductor/db/queries.py`; validate inputs with private helpers (`_validate_not_empty`, `_validate_task_status`)
- **Connection management**: Use `DatabasePool.acquire()` as async context manager; always use transactions for batch operations

## Async Patterns
- All public methods are `async def` — no synchronous wrappers
- Async context managers (`__aenter__`/`__aexit__`) for both `TaskQueue` and `Worker`
- Concurrency control via `asyncio.Semaphore` (not thread-based)
- Graceful shutdown: set flag → stop accepting → wait for in-flight → cancel with timeout → cleanup
- Signal handlers via `loop.add_signal_handler()` (graceful fallback on platforms without support)
- Background tasks via `asyncio.create_task()` with named tasks and done callbacks

## Logging
- Module-level logger idiom: `logger = logging.getLogger("conductor.<module>")` (e.g., `"conductor.core.worker"`)
- Log levels: DEBUG for heartbeat/trace, INFO for lifecycle events, WARNING for retries/DLQ, ERROR for failures
- Structured context: include `task_id`, `task_type`, `worker_id`, `duration_ms` in extra/log messages

## Testing
- **Framework**: `pytest` + `pytest-asyncio` (asyncio_mode = "auto")
- **Coverage**: `pytest-cov` with HTML report (`--cov=conductor --cov-report=term-missing --cov-report=html`)
- **Markers**: `unit` (fast, no DB), `integration` (requires PostgreSQL), `e2e` (full workflow), `perf` (benchmarks)
- **Test DB URL**: `CONDUCTOR_TEST_DATABASE_URL` env var (default: `postgresql://conductor:conductor@localhost:5432/conductor_test`)
- **Fixtures**: session-scoped `db_pool` + `schema_manager`; per-test `auto_cleanup` fixture truncates all tables
- **Integration tests**: Use `pytest-asyncio` fixtures with `AsyncGenerator`; skip gracefully via `pytest.skip()` when DB unavailable
- Test files follow `tests/{unit,integration,e2e,perf}/test_<module>.py` convention

## Package Structure
```
conductor/
├── __init__.py           # Public API re-exports + __all__
├── exceptions.py         # Exception hierarchy
├── core/
│   ├── __init__.py
│   ├── models.py         # Dataclasses: Task, RetryPolicy, WorkerInfo, etc.
│   ├── queue.py          # TaskQueue: submit, list, get tasks
│   └── worker.py         # Worker: poll, execute, heartbeat, shutdown
├── db/
│   ├── __init__.py
│   ├── connection.py     # DatabasePool (asyncpg pool + health checks)
│   ├── schema.py         # SchemaManager (idempotent migrations)
│   └── queries.py        # QueryBuilder (type-safe SQL methods)
├── retry/
│   ├── __init__.py        # To implement: policies.py, backoff.py
├── dlq/
│   ├── __init__.py        # To implement: dead_letter_queue.py
└── observability/
    └── __init__.py        # To implement: logging.py, metrics.py, health.py
```

## Development Workflow
- **Setup**: `docker compose up -d` → `cp .env.example .env` → `python3 -m venv .venv` → `pip install -e ".[dev]"`
- **Run tests**: `pytest` (all), `pytest -m unit` (fast), `pytest -m integration` (DB needed)
- **Schema migration**: Auto-runs on first `connect()`, or manually via `SchemaManager(pool).ensure_schema()`
- **Before committing**: Ensure `pytest` passes, `mypy conductor/` is clean, `black --check .` passes

## Correctness — Fix All Diagnostics

Before finishing ANY task (feature, bug fix, refactor, or doc change), the agent MUST leave the workspace clean:

- **Run the checks** applicable to the change: `mypy conductor/`, `black --check conductor/ tests/ examples/`, `flake8` (E203, W503 ignored), and the relevant `pytest` subset (unit / integration / e2e; DB-gated tests skip gracefully when PostgreSQL is down).
- **Fix every error, warning, and info** the checks and the editor's Problems panel report — compiler/linter/type-checker diagnostics, test failures, and deprecation notices — then **re-run until clean**.
- **Escalation**: a remaining item is acceptable only if it is (a) pre-existing and unrelated to the change, or (b) intentional and explicitly justified. Call it out in the response; never leave it silently.
- **No masking**: don't silence diagnostics with broad `# type: ignore`, `noqa`, or `pragma: no cover` comments to make them disappear. Resolve the underlying cause, or use a narrowly-scoped, commented suppression.
- **Type-check authority**: `mypy --strict` is authoritative for typing, but visible Pylance/editor errors on touched files must also be addressed (re-analyze if a diagnostic looks stale).
- **Keep tests green**: update tests when a fix changes behavior, then re-run the affected suite.

## Phase Awareness (v0.1 → v0.2 → v0.3)
- **v0.1.0**: RELEASED (2026-07-31) — all 6 sprints complete, published to PyPI (`conductor-task-queue`), GitHub Release `v0.1.0`.
- **v0.2 Sprint 1**: COMPLETE — Task routing + priority queues (schema v2, DLQ preserves route/priority).
- **v0.2 Sprint 2**: COMPLETE — Scheduled & recurring (cron) tasks (`RecurringScheduler`, schema v3).
- **v0.2 Sprint 3**: COMPLETE — gRPC API (polyglot workers).
- **v0.2 Sprint 4**: IN PROGRESS — Web dashboard (FastAPI + React/Vite; schema v4 `CANCELLED`).
- **v0.2 Sprint 5+** (planned): Circuit breakers, task chaining.
- **v0.3+** (future): webhook callbacks, batch operations, multi-region support.
- Do **not** implement later v0.2/v0.3 features before the current sprint is complete — follow the plan.

### Codified decisions — v0.2 Sprint 1 (routing & priority)
- `Worker(routes=None)` (programmatic default) polls **all** routes (no route filter). The CLI / `ROUTES` env var defaults to `["default"]` — this asymmetry is intentional and documented.
- `submit()`/`submit_many()` validate `priority` (int, **-100..100**) and `route` (non-empty string) up-front, raising `ValueError` (mirrors the DB CHECK constraint).
- DLQ preserves `route`/`priority`: `conductor_dead_letter` has `route`/`priority` columns. Never revert to `routes or ["default"]` semantics.

### Codified decisions — v0.2 Sprint 2 (scheduled & recurring)
- Cron expressions are **5-field**, evaluated in **UTC**; missed occurrences are **skipped** (no backfill). `croniter>=1.4` is a runtime dependency.
- Recurring definitions live in `conductor_recurring_tasks`; `TaskQueue.schedule_recurring()` registers them and `list_recurring_tasks()`/`get_recurring_task()`/`pause_recurring()`/`resume_recurring()`/`delete_recurring_task()` manage them.
- `RecurringScheduler` (`conductor/recurring/scheduler.py`) is a standalone daemon (`run()`/`run_once()`/`shutdown()`), optionally embedded via `Worker(enable_scheduler=True)` / `CONDUCTOR_ENABLE_SCHEDULER`. Due definitions are claimed with `FOR UPDATE SKIP LOCKED` inside one transaction — multiple schedulers never double-fire.
- Schema is at **v3** (`idx_recurring_polling (enabled, next_run_at)`); `SchemaManager.ensure_schema()` runs incremental migrations (v0→v1→v2→v3), one `conductor_version` row per step.

### Codified decisions — v0.2 Sprint 3 (gRPC API)
- The gRPC server is **embedded in the Worker** (`Worker(grpc_enabled=True)` / `GRPC_ENABLED`); polyglot stubs are gRPC *clients* that call it. `conductor/grpc/server.py` (`GrpcWorkerServer`, `ConductorWorkerServicer`) uses `grpc.aio`.
- `ProcessTask` is **pure execution** by default (handler in → result out, no queue writes). `TaskRequest.persist=true` opts into the full lifecycle (status/metrics/retry/DLQ) via the worker's `_execute_task`.
- Generated stubs (`conductor/grpc/conductor_pb2*`) are **committed** and typed by hand-maintained `.pyi` files; regenerate with `python scripts/generate_grpc.py`. `grpcio>=1.60` is runtime; `grpcio-tools`, `grpc-stubs`, `types-protobuf` are dev deps. Never edit the generated `_pb2*.py` by hand.

### Codified decisions — v0.2 Sprint 4 (web dashboard)
- Backend is **FastAPI + uvicorn** (`conductor/api/`: `create_app`, `DashboardServer`, Pydantic response models); frontend is **React + Vite** (`conductor/web/`, HashRouter, polling, hand-rolled SVG charts — no heavy chart lib).
- The built frontend (`conductor/web/dist`) is **committed** and shipped in the wheel (`[tool.setuptools.package-data] conductor = ["py.typed", "web/dist/**/*"]`); no Node.js at install — rebuild with `npm run build` (`scripts/build_frontend.sh`); CI `frontend` job verifies committed `dist/` is fresh.
- Dashboard reads use new **non-locking** queries (`QueryBuilder.select_tasks`/`count_tasks`/`select_all_workers`); never reuse the locked `select_pending_tasks` (or `select_due_recurring_tasks`) for reads.
- `TaskStatus.CANCELLED` + schema **v4** — the v3→v4 migration rebuilds `chk_task_status` to include `'cancelled'`; `TaskQueue.cancel_task()` cancels pending/retrying only (`TaskError` otherwise); `QueryBuilder.cancel_task()` sets `cancelled` + `completed_at`.
- The dashboard runs standalone (`conductor api [--host --port --api-key --env-file]`) **or** embedded (`Worker(api_enabled=True)` / `CONDUCTOR_API_ENABLED` / `CONDUCTOR_API_PORT` / `CONDUCTOR_API_KEY`); bind failures are OSError-non-fatal (like metrics/gRPC); `get_status()` reports `api_*`.
- API key is **optional** (`X-API-Key` header, unset = open); metrics-as-JSON via `prometheus_client.parser.text_string_to_metric_families`; `conductor.observability.metrics` is imported in `app.py` so conductor families are always registered; WebSocket real-time is deferred (polling first).

