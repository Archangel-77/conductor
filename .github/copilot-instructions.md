# Conductor — Agent Instructions

## Project Identity
- **Package**: `conductor-task-queue` v0.1.0, MIT license, published to PyPI
- **Python**: 3.11+ only, asyncio-native, **no threads**
- **Database**: PostgreSQL 12+ only, **no Redis**, no external message brokers
- **Architecture**: Polling-based task dispatch against PostgreSQL; exactly-once semantics; idempotent task processing
- **Status**: v0.1 MVP (Sprints 1-3 ✅ completed; Sprint 4 🔄 Retry/DLQ in progress; Sprint 5 ❌ Observability not started; Sprint 6 🔄 Integration/Docs in progress)

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

## Phase Awareness (v0.1 → v0.2 → v0.3)
- **Sprint 4** (in progress): Retry policies (`conductor/retry/`), backoff strategies (exponential/linear/fixed), DLQ class (`conductor/dlq/`)
- **Sprint 5** (not started): Structured logging, Prometheus metrics exporter, health check endpoint
- **Sprint 6** (in progress): E2E tests, perf benchmarks, docs, Docker Compose, examples
- **v0.2** (planned): Task routing, priority queues, scheduled/recurring (cron), web dashboard, circuit breakers, task chaining
- **v0.3+** (future): gRPC API, webhook callbacks, batch operations, multi-region support, CLI tool
- Do **not** implement v0.2/v0.3 features before v0.1 is complete — follow the plan
