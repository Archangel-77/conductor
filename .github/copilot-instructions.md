# Conductor — Agent Instructions

## Project Identity
- **Package**: `conductor-task-queue` v0.3.0, MIT license, published to PyPI
- **Python**: 3.11+ only, asyncio-native, **no threads**
- **Database**: PostgreSQL 12+ (default; `asyncpg`), **MySQL 8.0.16+/MariaDB 10.6+** (`asyncmy`, extra `mysql`) or **SQLite** (`aiosqlite`, extra `sqlite`, single-process contract). **No Redis**, no external message brokers
- **Architecture**: Polling-based task dispatch against the database; exactly-once semantics; idempotent task processing. The DSN scheme selects the backend and all SQL is rendered through a per-backend `SqlDialect`
- **Status**: v0.2.0 released to PyPI (2026-08-13). v0.3.0 (SQLite, MySQL/MariaDB, OpenTelemetry tracing) is prepared and committed — tag/publish pending.

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
- **Driver**: `asyncpg` (PostgreSQL, core), `aiosqlite` (extra `sqlite`) or `asyncmy` (extra `mysql`); `DatabasePool` in `conductor/db/connection.py` is a facade over `backends/registry.create_pool()`
- **No ORM** — use raw SQL rendered through `SqlDialect` (never hardcode `$1`/`?`/`%s`, `RETURNING`, `ON CONFLICT` or array syntax in a query)
- **Polling**: Use `FOR UPDATE SKIP LOCKED` for atomic task acquisition (SQLite has no row locking and is single-process)
- **Schema versioning**: Track via `conductor_version` table; migrations live per backend in `conductor/db/ddl/`; idempotent (`CREATE IF NOT EXISTS`)
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
│   ├── connection.py     # DatabasePool (facade over the backend pool + dialect)
│   ├── schema.py         # SchemaManager (idempotent migrations; no SQL of its own)
│   ├── queries.py        # QueryBuilder (type-safe SQL methods, rendered per dialect)
│   ├── backends/         # base.py (protocols + SqlDialect), postgres.py, sqlite.py, mysql.py, registry.py
│   └── ddl/              # __init__.py (SchemaDDL + get_ddl), postgres.py, sqlite.py, mysql.py
├── retry/
│   ├── __init__.py        # To implement: policies.py, backoff.py
├── dlq/
│   ├── __init__.py        # To implement: dead_letter_queue.py
└── observability/
    └── __init__.py        # To implement: logging.py, metrics.py, health.py
```

## Development Workflow
- **Setup**: `docker compose up -d` → `cp .env.example .env` → `python3 -m venv .venv` → `pip install -e ".[dev,sqlite,mysql,otel]"` (the unit suite imports every backend module, so the optional extras must be installed)
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
- **v0.2 Sprint 4**: COMPLETE — Web dashboard (FastAPI + React/Vite; schema v4 `CANCELLED`).
- **v0.2 Sprint 5**: COMPLETE — Circuit breaker (worker-side, per-task-type).
- **v0.2 Sprint 6**: COMPLETE — Task chaining/dependencies (schema v5 `BLOCKED`).
- **v0.2.0**: RELEASED (2026-08-13) — Sprints 1–6 complete, published to PyPI (`conductor-task-queue`), GitHub Release `v0.2.0`.
- **v0.3 Track A**: COMPLETE (2026-09-15) — pluggable DB backends (`conductor/db/backends/` + `conductor/db/ddl/`) and the **SQLite** backend. Schema stays **v5** (no migration). Plan of record: `todo_p3.md` (git-untracked) — v0.3.0 (SQLite + OpenTelemetry, schema v6), v0.4.0 (MySQL), v0.5.0 (workflows, v7), v0.6.0 (webhooks/batch, v8 + tenancy/auth, v9).
- **v0.3 Track B**: COMPLETE (2026-09-15) — OpenTelemetry tracing + cross-process trace context (schema **v6** `traceparent`). Optional extra `otel`.
- **v0.3.0 Task A2**: COMPLETE (2026-09-15) — **MySQL/MariaDB backend** (optional extra `mysql` = `asyncmy`). Schema stays **v6** (no migration). Live-verified against MySQL 8.0.46/8.4 and MariaDB 11.8.9 in Docker (680 passed / 53 skipped each, plus an 81-test three-backend parity matrix); MariaDB 10.5.29 is correctly rejected at `connect()`.
- **v0.4.0 Tracks C/D/E** (planned, renumbered after v0.3.0 absorbed MySQL): advanced workflows (v7), webhook callbacks + batch operations (v8), tenancy/auth/quota enablers (v9), multi-region docs.
- Do **not** implement later v0.3 features before the plan calls for them — follow `todo_p3.md`.

### Codified decisions — v0.3.0 Task A2 (MySQL/MariaDB backend)
- **Driver**: `asyncmy` only, in the optional extra `mysql` (declared in **both** `pyproject.toml` and `setup.py`). `conductor/db/backends/mysql.py` is the only module importing `asyncmy`; `backends/__init__.py` must **not** re-export `MySqlPool` (optional drivers stay lazily imported by `registry.create_pool()`, exactly like `SqlitePool`). Minimum server: **MySQL 8.0.16+** (named `CHECK` constraints) / **MariaDB 10.6+** (`FOR UPDATE SKIP LOCKED`; MariaDB 10.5 fails the polling query, so `validate_server_version()` rejects it at `connect()` with the detected version); MySQL 5.7 is unsupported.
- **Verified live (2026-09-15, Docker)**: full non-perf suite green on MySQL 8.0.46, MySQL 8.4 and MariaDB 11.8.9 (680 passed / 53 skipped each; 733 collected, the 26-test `test_db_schema.py` skip is PostgreSQL-only by design) and on PostgreSQL 16 (706 passed / 27 skipped). MariaDB 10.5.29 is correctly rejected. CI runs the parity matrix against MySQL 8.0 + 8.4 + MariaDB 11.
- **`execute()` returns `Any`** in `ConnectionProtocol`/`PoolProtocol` and `DatabasePool`: PostgreSQL/SQLite return a command tag (`"UPDATE 1"`), MySQL returns an **int** rowcount. Always go through `SqlDialect.normalize_rowcount()`; `MySqlConnection.execute` coerces asyncmy's loosely-typed `cursor.rowcount` and reports `0` for DDL/SELECT.
- **No `RETURNING`** (`supports_returning = False`). Conflict detection uses the affected-row count: `dialect.insert_ignore([...])` renders the **no-op** `ON DUPLICATE KEY UPDATE col = col` (0 rows ⇒ duplicate ⇒ `TaskError`), while idempotent upserts (DLQ, worker heartbeat) deliberately **ignore** the rowcount because MySQL reports 0 for "row already identical". `mark_dependents_blocked()` falls back to a locking `SELECT … FOR UPDATE` + `UPDATE` inside `pool.transaction()`, re-checking `status = 'pending'` so a concurrently completed dependent is never reported as blocked — **its parameters must be passed in textual order (`error_message` first, then the IDs)**; getting that backwards silently updates nothing while still reporting the dependents as blocked. Never make these paths unconditional — PostgreSQL must keep its single-statement `RETURNING` form.
- **`ON DUPLICATE KEY UPDATE` uses `VALUES(col)`, never the MySQL 8.0.20+ row alias.** The alias form (`INSERT … VALUES (…) AS new … = new.col`) is **verified to be a MariaDB syntax error** (10.5 and 11.8), so keep `VALUES()` even though MySQL ≥ 8.0.20 logs deprecation warning 1287 (documented as benign in `docs/troubleshooting.md`).
- **Transactions**: asyncmy exposes `begin()`/`commit()`/`rollback()` **as coroutines** (no transaction context manager), so `MySqlConnection.transaction()` wraps them explicitly. `autocommit=True` in the DSN means statements outside a transaction commit immediately.
- **Types**: IDs `VARCHAR(64)`, name-like columns `VARCHAR(255)` (MySQL cannot index `TEXT`), `JSON` for `payload`/`result`/`retry_policy`/`depends_on`, `DATETIME(6)` in **UTC** (`CURRENT_TIMESTAMP(6)` defaults; `NOW(6)`; `DATE_SUB(NOW(6), INTERVAL %s SECOND)`), `TINYINT(1)` booleans coerced on read, tables `ENGINE=InnoDB CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`. `depends_on` is queried with `JSON_CONTAINS` (`JSON_QUOTE` the bound value) and is **not** indexed (a `JSON` column cannot be indexed usefully).
- **DDL/indexes**: `ddl/mysql.py` embeds every index as a `KEY` clause **inside** the `CREATE TABLE` statements (`index_statements` is empty) — MySQL has no `CREATE INDEX IF NOT EXISTS`, and `CREATE TABLE IF NOT EXISTS` already makes the v0 → v1 step idempotent. All historic migration steps are empty but recorded (new backend ⇒ ships at the latest shape). `DROP TABLE IF EXISTS` takes no `CASCADE`.
- **Tests**: MySQL needs no server for `tests/unit/test_dialect.py` + `tests/unit/test_mysql_ddl.py`, which pin (a) the rendered SQL never contains a PostgreSQL-only construct (`RETURNING`, `$n`, `ON CONFLICT`, `ILIKE`, `NULLS LAST`, `ARRAY[`) and every `%s` has a matching parameter (in the right order), (b) the DDL covers every column in `TASK_COLUMNS`/`DEAD_LETTER_COLUMNS`/`RECURRING_COLUMNS`, and (c) the server-version guard. `tests/integration/test_backend_matrix.py` lists `("postgresql", "sqlite", "mysql")` and skips unavailable backends. Local MySQL/MariaDB servers (Docker): `docker run -d --name conductor-mysql -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=conductor_test -e MYSQL_USER=conductor -e MYSQL_PASSWORD=conductor -p 127.0.0.1:3306:3306 mysql:8.4`.

### Codified decisions — v0.3 Track B (distributed tracing)
- Tracing is an **optional extra** (`otel` = opentelemetry-api/sdk/otlp-http). `conductor/observability/tracing.py` has **no import-time OpenTelemetry dependency**; `conductor/observability/_otel.py` is the only module importing the SDK and is imported lazily — it doubles as the availability canary for `is_available()`. Without the extra every public call is a no-op, so call sites never need guards.
- The tracer provider is **private** (Conductor never calls `trace.set_tracer_provider`), so a host application's OpenTelemetry setup is not overridden. `Worker.run()` installs it; `Worker._shutdown()` flushes it.
- `setup_tracing()` **raises `TracingError`** when tracing is enabled without the extra; `Worker.run()` catches it and logs a WARNING (the metrics/gRPC/dashboard degradation pattern). Never crash a worker over telemetry.
- Core modules (queue/worker/scheduler/gRPC) must **not import OpenTelemetry**: they use `tracing.span()`, `add_event()`, `mark_success()`, `record_error()`, `set_task_attributes()`, `current_traceparent()`, `extract_traceparent()`, `span_link()`.
- **Propagation model**: the submitter persists the W3C `traceparent` on the task row (schema v6, preserved through the DLQ), and every attempt is a **child of the submission span** — attempts are siblings, not a nested chain. `links` are used for the DLQ-retry *operation* span. Per-attempt linking would need a `traceparent` column on `conductor_retries` (not implemented).
- Schema v6 adds `traceparent TEXT` to `conductor_tasks` **and** `conductor_dead_letter`. New task columns go into `TASK_COLUMNS`/`DEAD_LETTER_COLUMNS` (queries.py) — those tuples drive the insert placeholders — plus the DDL of every backend and `_task_to_db_dict`.
- gRPC carries the context in `TaskRequest.traceparent` / `TaskResponse.traceparent`; regenerate stubs via `scripts/generate_grpc.py` **and** update the hand-maintained `.pyi`.
- Logs: `SpanContextFilter` adds `trace_id`/`span_id` while a span is active (records are untouched otherwise).
- **Behaviour change (bug fix, v0.1/v0.2 regression)**: `select_pending_tasks` claims `status IN ('pending', 'retrying')` once `scheduled_for` has elapsed — previously retries were marked `retrying` but never re-polled, so a retryable failure never ran again. **Never narrow this back to `status = 'pending'`** (regression test: `test_backend_matrix.py::TestRetryAndDlq::test_due_retry_is_reexecuted`).
- Tracing tests: install **one** in-memory exporter per module and never reinstall tracing mid-module — `setup_tracing()` builds a new provider, which orphans the fixture's exporter (monkeypatch to simulate "tracing off" instead).

### Codified decisions — v0.3 Track A (portable DB core + SQLite)
- The backend is chosen from the **DSN scheme** (`postgresql://`, `sqlite:///`, `mysql://`); there is no separate setting. `conductor/db/connection.py` is a facade over `registry.create_pool()`, and backend construction is **lazy** (a bad DSN must fail at `connect()`, not at construction).
- Only `conductor/db/backends/postgres.py` imports `asyncpg`. DDL text lives in `conductor/db/ddl/<backend>.py`; `SchemaManager` contains no SQL of its own. New backends declare the full latest shape as migration **step 1** and leave historic steps empty (they still record a version row).
- `QueryBuilder` renders every statement through `self._dialect`. **Placeholders must appear in textual order** (= parameter order): SQLite binds `?` positionally, so the `WHERE id` parameter goes **last** in dynamic `UPDATE`s.
- Affected-row counts go through `dialect.normalize_rowcount()`; the SQLite adapter **synthesises asyncpg-style command tags** (`"UPDATE 1"`) so `DatabasePool.execute()` keeps returning `str`.
- **JSON decoding belongs in `SqlDialect.decode_json_value`** — asyncpg returns `JSONB` as *text* on Python 3.14. Removing it breaks the whole PostgreSQL suite.
- SQLite specifics: single-process contract (one worker per file), re-entrant `ContextVar`-guarded `asyncio.Lock` + `BEGIN IMMEDIATE`, WAL, timestamps stored as `YYYY-MM-DD HH:MM:SS.ffffff` UTC (`strftime('%Y-%m-%d %H:%M:%f000','now')` — **SQLite's `%f` is `SS.SSS`, not microseconds**), `depends_on` as JSON `TEXT` + JSON1 functions, booleans as `INTEGER` coerced on read, `for_update_skip_locked()` returns `""`, `DROP TABLE` takes no `CASCADE`.
- Optional drivers never import at package-import time: `backends/__init__.py` does not re-export `SqlitePool`; `registry.create_pool()` imports the driver lazily and raises `ConductorConnectionError` naming the extra to install. Extras (`sqlite` = aiosqlite) are declared in **both** `pyproject.toml` and `setup.py`.
- Tests: new backends must join `tests/integration/test_backend_matrix.py` (parametrized by backend, skips when unavailable). `db_available()` validates the DSN scheme; clean up with `truncate_all()`; async fixtures that touch session-scoped pools **must** set `loop_scope="session"`.

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

### Codified decisions — v0.2 Sprint 5 (circuit breaker)
- The breaker is **worker-side and in-memory** (`conductor/circuit_breaker/`): per-worker consecutive failures per `task_type`; state is **not shared across workers** (a DB-backed registry is future work / schema v5). "Reject when open" is enforced at the execution layer, not at `submit()` time.
- While OPEN the worker **skips & leaves pending** (no false failures/retries/DLQ). Only **real handler exceptions** trip the breaker — a missing-handler error does not.
- State machine: CLOSED → (`threshold` consecutive failures) OPEN → (`timeout`) HALF_OPEN (`half_open_attempts` probes) → probe success CLOSED / all probes fail OPEN. The clock is injectable for deterministic tests.
- Config: global defaults via `WorkerSettings`/env (`CONDUCTOR_CIRCUIT_BREAKER_*`); per-task-type `circuit_breaker_overrides` are programmatic only.
- Observability: `conductor_tasks_rejected_total` counter + `conductor_circuit_breaker_open` gauge + `Worker.get_status()` (`circuit_breaker_enabled`, `circuit_breaker_open`).

### Codified decisions — v0.2 Sprint 6 (task dependencies)
- `depends_on` is a `TEXT[]` column (schema **v5**) with a GIN index (`idx_tasks_depends_on`); `conductor_dead_letter` also carries it so a DLQ retry restores dependencies (schema-v2 route/priority precedent). The v4→v5 migration adds the columns, index, and rebuilds `chk_task_status` to include `'blocked'`.
- A dependency is satisfied when its status is `completed` or `cancelled`; tasks with unmet deps stay `pending` and are excluded by the polling query (`select_pending_tasks`, both route/no-route branches — never relax this).
- `TaskStatus.BLOCKED` marks dependents of a **failed** dependency (`dependency '<id>' failed`, terminal, no retry); propagation is **transitive** (A→B→C) via the worker's terminal failure path (`_propagate_terminal_dependency`, bounded loop) and covers the gRPC `persist=true` path.
- `TaskQueue.submit(..., depends_on=[...])` is the chaining primitive (`submit_many` unchanged); forward references are allowed, self-references rejected at submit time. Full DAG cycle detection is future work.
- Dashboard: task detail exposes `depends_on`; `blocked` is a first-class status (badge + filter).

