"""
Shared test fixtures and configuration for Conductor tests.

Divided into two tiers:

- **Unit-test fixtures** – no database required
- **Integration-test fixtures** – require a reachable database
  (skipped automatically if the configured database is unavailable)

The backend is selected by the DSN in ``CONDUCTOR_TEST_DATABASE_URL``: the
PostgreSQL default for the main suite, or ``sqlite:///…`` to run against an
embedded database with no server at all.
"""

# pylint: disable=import-outside-toplevel

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from typing import Any, TYPE_CHECKING

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    from conductor.db.connection import DatabasePool
    from conductor.db.schema import SchemaManager

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
    """Return ``True`` if the configured test database looks usable.

    The DSN scheme is validated only – this is a cheap check that keeps the
    integration suite skipped on machines without a database.  Any supported
    backend (PostgreSQL, SQLite, MySQL) passes; the fixtures themselves report
    unreachable databases with a clear skip message.
    """
    if not TEST_DATABASE_URL:
        return False

    from conductor.db.backends.registry import detect_backend
    from conductor.exceptions import ConductorConnectionError

    try:
        detect_backend(TEST_DATABASE_URL)
    except ConductorConnectionError:
        return False
    return True


CONDUCTOR_TABLES: tuple[str, ...] = (
    "conductor_retries",
    "conductor_dead_letter",
    "conductor_tasks",
    "conductor_workers",
    "conductor_recurring_tasks",
)
"""Every table holding test data, in foreign-key-safe deletion order."""


async def truncate_all(pool: Any) -> None:
    """Delete every row from the Conductor tables.

    ``conductor_retries`` has a foreign key onto ``conductor_tasks``, so rows
    are removed dependents-first.  Portable across backends (no ``TRUNCATE``).

    Args:
        pool: A connected pool – any backend.
    """
    for table in CONDUCTOR_TABLES:
        await pool.execute(f"DELETE FROM {table}")


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


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def db_pool() -> (  # noqa: N802  # pylint: disable=redefined-outer-name
    AsyncGenerator[Any, None]
):
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


@pytest_asyncio.fixture(scope="session", loop_scope="session")
async def schema_manager(  # noqa: N802  # pylint: disable=redefined-outer-name
    db_pool: DatabasePool,
) -> SchemaManager:
    """Create a :class:`SchemaManager` and run migrations."""
    from conductor.db.schema import SchemaManager

    mgr = SchemaManager(db_pool)
    await mgr.ensure_schema()
    return mgr


@pytest_asyncio.fixture
async def auto_cleanup(  # noqa: N802  # pylint: disable=redefined-outer-name
    db_pool: DatabasePool,
) -> AsyncGenerator[None, None]:
    """Clean all rows from conductor tables after each test.

    Tests that need per-test isolation should request this fixture explicitly.
    """
    yield

    if db_pool.is_connected:
        await truncate_all(db_pool)
