"""
Shared test fixtures and configuration for Conductor tests.

Divided into two tiers:

- **Unit-test fixtures** – no database required
- **Integration-test fixtures** – require a running PostgreSQL instance
  (skipped automatically if the database is unavailable)
"""

from __future__ import annotations

import os
from typing import Any

import pytest
import pytest_asyncio

# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = os.environ.get(
    "CONDUCTOR_TEST_DATABASE_URL",
    "postgresql://conductor:conductor@localhost:5432/conductor_test",
)
"""Connection string used by integration tests.

Override via the ``CONDUCTOR_TEST_DATABASE_URL`` environment variable.
"""


def db_available() -> bool:
    """Return ``True`` if the test database appears reachable.

    This is a lightweight check that tests whether the env variable
    looks reasonable.  Individual fixtures will fail with a clear
    skip message when the database is not running.
    """
    return bool(
        TEST_DATABASE_URL
        and TEST_DATABASE_URL.startswith("postgresql")
    )


# ---------------------------------------------------------------------------
# Unit-test fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_task_dict() -> dict[str, Any]:
    """Return a minimal task dictionary suitable for model construction."""
    from conductor.core.models import generate_task_id, utc_now

    return {
        "task_id": generate_task_id(),
        "task_type": "test_task",
        "payload": {"key": "value"},
        "status": "pending",
        "priority": 0,
        "route": "default",
        "attempt": 0,
        "max_retries": 3,
        "retry_policy": {
            "max_retries": 3,
            "backoff_strategy": "exponential",
            "initial_delay": 1.0,
            "max_delay": 3600.0,
        },
        "scheduled_for": None,
        "worker_id": None,
        "result": None,
        "error_message": None,
        "created_at": utc_now(),
        "started_at": None,
        "completed_at": None,
    }


@pytest.fixture
def sample_worker_dict() -> dict[str, Any]:
    """Return a minimal worker dictionary."""
    return {
        "worker_id": "test-worker-1",
        "status": "idle",
        "current_task_id": None,
        "hostname": "test-host",
        "pid": 12345,
        "uptime_seconds": 0.0,
        "tasks_processed_total": 0,
        "tasks_failed_total": 0,
        "last_heartbeat": None,
    }


@pytest.fixture
def sample_retry_record_dict() -> dict[str, Any]:
    """Return a minimal retry-record dictionary."""
    from conductor.core.models import utc_now

    return {
        "id": "test-retry-1",
        "task_id": "test-task-1",
        "attempt": 1,
        "error_message": "Something went wrong",
        "scheduled_at": utc_now(),
    }


# ---------------------------------------------------------------------------
# Integration-test fixtures (database required)
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="session")
async def db_pool():
    """Create a :class:`DatabasePool` connected to the test database.

    Skips the test if the database is not running.
    """
    from conductor.db.connection import DatabasePool
    from conductor.exceptions import ConductorConnectionError

    if not db_available():
        pytest.skip("Test database not available")

    pool = DatabasePool(
        dsn=TEST_DATABASE_URL,
        min_size=1,
        max_size=2,
        timeout=5.0,
        max_retries=1,
    )
    try:
        await pool.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect to test database: {exc}")

    yield pool

    await pool.disconnect()


@pytest_asyncio.fixture(scope="session")
async def schema_manager(db_pool):
    """Create a :class:`SchemaManager` and run migrations."""
    from conductor.db.schema import SchemaManager

    mgr = SchemaManager(db_pool)
    await mgr.ensure_schema()
    return mgr


@pytest_asyncio.fixture
async def auto_cleanup(db_pool):
    """Clean all rows from conductor tables after each test.

    Tests that need per-test isolation should request this fixture explicitly.
    """
    from conductor.db.queries import QueryBuilder

    yield

    if db_pool.is_connected:
        qb = QueryBuilder(db_pool)
        await qb._pool.execute("DELETE FROM conductor_retries")
        await qb._pool.execute("DELETE FROM conductor_dead_letter")
        await qb._pool.execute("DELETE FROM conductor_tasks")
        await qb._pool.execute("DELETE FROM conductor_workers")
