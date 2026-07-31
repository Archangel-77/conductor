# Contributing to Conductor

Thanks for your interest in contributing! Conductor is a lightweight,
PostgreSQL-backed async task queue for Python. This guide covers how to set up
your environment, what to keep in mind while coding, and how to get your changes
merged.

By participating in this project you agree to abide by our
[Code of Conduct](CODE_OF_CONDUCT.md).

---

## Table of Contents

- [Development Setup](#development-setup)
- [Project Layout](#project-layout)
- [Running Tests](#running-tests)
- [Code Style](#code-style)
- [Type Checking](#type-checking)
- [Pull Request Process](#pull-request-process)
- [Areas for Contribution](#areas-for-contribution)

---

## Development Setup

**Prerequisites**: Python 3.11+, PostgreSQL 12+ (a `docker-compose.yml` is
provided for the database), and `git`.

```bash
# 1. Clone and enter the repo
git clone https://github.com/Archangel-77/Conductor.git
cd conductor

# 2. Start PostgreSQL (optional but recommended; skip if you have a local one)
docker compose up -d postgres

# 3. Configure environment
cp .env.example .env

# 4. Create a virtualenv and install the package with dev extras
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

> **No Docker?** Point `DATABASE_URL` / `CONDUCTOR_TEST_DATABASE_URL` at any
> reachable PostgreSQL instance (see `.env.example` and `docs/configuration.md`).

---

## Project Layout

```
conductor/
├── core/            # TaskQueue, Worker, models
├── db/              # asyncpg pool, schema migrations, query builder
├── retry/           # retry policies & backoff (public API in core/models)
├── dlq/             # DeadLetterQueue
├── observability/   # JSON logging, Prometheus metrics, health checks
├── config.py        # WorkerSettings.from_env()
├── cli.py           # `conductor worker` console script
└── __main__.py      # `python -m conductor`
tests/
├── unit/            # fast, no DB
├── integration/     # require PostgreSQL
├── e2e/             # full workflows
└── perf/            # benchmarks
examples/            # runnable example scripts
docs/                # documentation (installation, configuration, API, ...)
```

---

## Running Tests

The test suite uses `pytest` with markers to separate fast tests from those that
need a database:

```bash
# Everything except perf benchmarks (requires PostgreSQL)
CONDUCTOR_TEST_DATABASE_URL=postgresql://conductor:conductor@localhost:5432/conductor_test \
  pytest tests/unit tests/integration tests/e2e -m "not perf"

# Fast unit tests only (no DB required)
pytest tests/unit -m "not integration"

# Integration + E2E (DB required)
pytest tests/integration tests/e2e -m "integration or e2e"

# Performance benchmarks (coverage disabled so timings aren't skewed)
pytest tests/perf -m perf --no-cov
```

- Tests that need a database are skipped automatically if it is unreachable.
- `CONDUCTOR_TEST_DATABASE_URL` defaults to
  `postgresql://conductor:conductor@localhost:5432/conductor_test` (matches the
  compose file).
- Aim for **85%+ coverage** on `conductor/` when adding features:
  ```bash
  pytest --cov=conductor --cov-report=term-missing
  ```

---

## Code Style

We use **black** + **flake8** + **mypy --strict** and enforce them in CI. Before
submitting, make sure everything is clean:

```bash
# Formatting (line length 100)
black --check conductor/ tests/ scripts/ examples/

# Lint
flake8 conductor/ tests/ scripts/ examples/

# Types (strict)
mypy conductor/
```

Conventions to follow:

- **Line length**: 100 characters.
- **Quotes**: double quotes `"` everywhere.
- **Imports**: standard library → third-party → local, grouped with blank lines.
- **`from __future__ import annotations`** at the top of every module.
- Use `Optional[X]` (not `X | None`) for consistency.
- Every module, public class, and public method gets a Google-style docstring
  (`Args:` / `Returns:` / `Raises:`).
- Models are immutable `@dataclass(frozen=True)` with `to_dict()` / `from_dict()`.
- Enums inherit from `(str, Enum)` and return `self.value` from `__str__`.
- Logging uses lazy `%`-formatting, e.g. `logger.info("Task %s submitted.", id)`.
- Async everywhere — no threads, no blocking calls in the hot path.
- Exceptions derive from `ConductorException` (see `conductor/exceptions.py`).

---

## Type Checking

`mypy --strict` is part of CI:

```bash
mypy conductor/
```

New public functions and methods must have full type hints. Avoid `Any` where a
specific type is known.

---

## Pull Request Process

1. **Fork** the repository and create a feature branch:
   `git checkout -b feature/my-change`.
2. Make your changes, **add tests** for them (happy path + error path), and
   update documentation (`docs/`, `README.md`, `CHANGELOG.md`) if behaviour or
   the public API changed.
3. Run the checks from [Code Style](#code-style) and
   [Running Tests](#running-tests) locally — they must pass.
4. Open a pull request against `master` with a clear title and description.
   Link any related issue.
5. A maintainer will review; address feedback (updating the PR, not stacking new
   commits where avoidable). CI runs on every push and must stay green.

Small, focused PRs are much easier to review and land.

---

## Areas for Contribution

- Performance optimizations (polling, batching, pool tuning)
- Additional retry/backoff strategies
- Observability enhancements (metrics, tracing)
- Documentation improvements and fixes
- Example projects and integrations
- Database backend support (v0.3+ roadmap)
- v0.2 roadmap items: routing, priority queues, scheduled/recurring tasks, gRPC,
  web dashboard, circuit breakers, task dependencies

> Before starting a large feature, open an issue or discussion so we can agree
> on the design and scope — it avoids wasted effort on both sides.
