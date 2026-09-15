"""
Unit tests for SchemaManager (database schema creation & migrations).

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.

They are also **PostgreSQL-specific by design**: the assertions read
``pg_catalog``/``pg_indexes``, drive the PostgreSQL migration ladder
(``ALTER TABLE … DROP CONSTRAINT``, the v3→v6 simulation) and use ``$n``
placeholders.  Cross-backend schema behaviour is covered by
``tests/integration/test_backend_matrix.py``, which calls ``ensure_schema()``
and exercises every table on each configured backend.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

from conductor.db.schema import SCHEMA_VERSION, CREATE_VERSION_TABLE


def _is_postgres_backend() -> bool:
    """``True`` when the configured test database is PostgreSQL.

    The catalogue queries and migration simulations below only make sense for
    PostgreSQL; on any other backend the module is skipped rather than failed.
    """
    from tests.conftest import TEST_DATABASE_URL

    from conductor.db.backends.registry import detect_backend
    from conductor.exceptions import ConductorConnectionError

    try:
        return detect_backend(TEST_DATABASE_URL) == "postgresql"
    except ConductorConnectionError:
        return False


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not _is_postgres_backend(),
        reason=(
            "PostgreSQL-specific schema/catalogue assertions; the cross-backend "
            "schema contract is covered by tests/integration/test_backend_matrix.py"
        ),
    ),
]


# ===================================================================
# Schema constants
# ===================================================================


class TestSchemaConstants:

    def test_schema_version(self) -> None:
        assert SCHEMA_VERSION == 6

    def test_version_table_sql(self) -> None:
        assert "conductor_version" in CREATE_VERSION_TABLE


# ===================================================================
# Table creation
# ===================================================================


class TestTableCreation:

    async def test_all_tables_exist(
        self,
        schema_manager: Any,  # pylint: disable=unused-argument
        db_pool: Any,
    ) -> None:
        """Verify that all expected tables were created."""
        tables = [
            "conductor_version",
            "conductor_tasks",
            "conductor_workers",
            "conductor_retries",
            "conductor_dead_letter",
            "conductor_recurring_tasks",
        ]
        for table in tables:
            row = await db_pool.fetchrow(
                "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = $1",
                table,
            )
            assert row is not None, f"Table '{table}' not found"

    async def test_version_tracked(self, db_pool: Any) -> None:
        """The conductor_version table should record the latest version (6)."""
        row = await db_pool.fetchrow("SELECT MAX(version) AS version FROM conductor_version")
        assert row is not None
        assert row["version"] == 6


# ===================================================================
# Constraints & checks
# ===================================================================


class TestConstraints:

    async def test_task_status_check(self, db_pool: Any) -> None:
        """Inserting an invalid status should fail."""
        with pytest.raises(Exception):
            await db_pool.execute(
                "INSERT INTO conductor_tasks "
                "(task_id, task_type, status) "
                "VALUES ($1, $2, $3)",
                "bad-status-task",
                "test",
                "invalid_status",
            )

    async def test_cancelled_status_allowed(self, db_pool: Any) -> None:
        """The ``cancelled`` status should be accepted by the CHECK."""
        result = await db_pool.execute(
            "INSERT INTO conductor_tasks (task_id, task_type, status) " "VALUES ($1, $2, $3)",
            "cancelled-status-task",
            "test",
            "cancelled",
        )
        assert "INSERT" in result

    async def test_blocked_status_allowed(self, db_pool: Any) -> None:
        """The ``blocked`` status should be accepted by the CHECK."""
        result = await db_pool.execute(
            "INSERT INTO conductor_tasks (task_id, task_type, status) VALUES ($1, $2, $3)",
            "blocked-status-task",
            "test",
            "blocked",
        )
        assert "INSERT" in result

    async def test_task_priority_range(self, db_pool: Any) -> None:
        """Priority outside the allowed range should fail."""
        with pytest.raises(Exception):
            await db_pool.execute(
                "INSERT INTO conductor_tasks "
                "(task_id, task_type, payload, priority) "
                "VALUES ($1, $2, '{}', $3)",
                "bad-priority-task",
                "test",
                200,
            )

    async def test_valid_task_insert(self, db_pool: Any) -> None:
        """A valid task insert should succeed."""
        result = await db_pool.execute(
            "INSERT INTO conductor_tasks (task_id, task_type, payload) VALUES ($1, $2, '{}')",
            "valid-task-1",
            "test",
        )
        assert "INSERT" in result


# ===================================================================
# Indexes
# ===================================================================


class TestIndexes:

    async def _index_exists(self, db_pool: Any, index_name: str) -> bool:
        row = await db_pool.fetchrow(
            "SELECT indexname FROM pg_indexes "
            "WHERE indexname = $1 AND tablename LIKE 'conductor_%'",
            index_name,
        )
        return row is not None

    async def test_tasks_status_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_tasks_status")

    async def test_tasks_polling_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_tasks_polling")

    async def test_tasks_depends_on_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_tasks_depends_on")

    async def test_workers_heartbeat_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_workers_last_heartbeat")

    async def test_retries_task_id_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_retries_task_id")

    async def test_dead_letter_discarded_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_dead_letter_discarded")

    async def test_recurring_next_run_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_recurring_next_run")

    async def test_recurring_polling_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_recurring_polling")


# ===================================================================
# Idempotent migrations
# ===================================================================


class TestIdempotentMigrations:

    async def test_ensure_schema_twice(self, schema_manager: Any) -> None:
        """Running ensure_schema twice should not raise."""
        await schema_manager.ensure_schema()  # second run

    async def test_versions_recorded_once(self, db_pool: Any) -> None:
        """Each migration step version should be recorded exactly once."""
        rows = await db_pool.fetch("SELECT version FROM conductor_version")
        versions = [r["version"] for r in rows]
        # No duplicate version rows (unique constraint holds)
        assert len(versions) == len(set(versions))
        # All migration steps are recorded
        assert 1 in versions
        assert 2 in versions
        assert 3 in versions
        assert 4 in versions
        assert 5 in versions

    async def test_dead_letter_route_priority_columns(self, db_pool: Any) -> None:
        """The dead-letter table should expose route/priority columns."""
        rows = await db_pool.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'conductor_dead_letter' "
            "AND column_name IN ('route', 'priority')"
        )
        cols = {r["column_name"] for r in rows}
        assert {"route", "priority"} <= cols

    async def test_dead_letter_depends_on_column(self, db_pool: Any) -> None:
        """The dead-letter table should carry ``depends_on`` forward."""
        rows = await db_pool.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'conductor_dead_letter' "
            "AND column_name = 'depends_on'"
        )
        assert rows


# ===================================================================
# Rollback
# ===================================================================


class TestRollback:

    async def test_rollback_drops_tables(self, schema_manager: Any, db_pool: Any) -> None:
        """Rollback to v0 should drop all conductor tables."""
        await schema_manager.rollback(target_version=0)

        # Tables should be gone
        row = await db_pool.fetchrow(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE tablename LIKE 'conductor_%'"
        )
        assert row is None

        # Re-create for subsequent tests
        await schema_manager.ensure_schema()


class TestMigrationUpgrade:

    @pytest_asyncio.fixture(autouse=True, loop_scope="session")
    async def _restore_schema(
        self, schema_manager: Any  # pylint: disable=unused-argument
    ) -> AsyncIterator[None]:
        """Rebuild the schema after each destructive test.

        These tests simulate older databases by dropping columns/indexes and
        deleting version rows.  Without this teardown, a failing assertion would
        leave the shared database degraded for later tests *and* for the next
        run (the database file/persists between runs).
        """
        yield
        await schema_manager.rollback(0)
        await schema_manager.ensure_schema()

    async def test_migrates_older_db_to_latest(self, schema_manager: Any, db_pool: Any) -> None:
        """An older (v2) database is upgraded to the latest version by ensure_schema()."""
        # Simulate a v2 database: drop the v3-only index and the v3 version row
        await db_pool.execute("DROP INDEX IF EXISTS idx_recurring_polling")
        await db_pool.execute("DELETE FROM conductor_version WHERE version >= 3")

        current = await schema_manager.get_current_version()
        assert current == 2

        # Upgrade back to the latest version
        await schema_manager.ensure_schema()

        current = await schema_manager.get_current_version()
        assert current == 6

        # Index is back
        row = await db_pool.fetchrow(
            "SELECT indexname FROM pg_indexes WHERE indexname = 'idx_recurring_polling'"
        )
        assert row is not None

        # No duplicate version rows
        versions = [
            r["version"] for r in await db_pool.fetch("SELECT version FROM conductor_version")
        ]
        assert len(versions) == len(set(versions))

    async def test_migrates_v3_to_v4(self, schema_manager: Any, db_pool: Any) -> None:
        """A v3 database gains the ``cancelled`` status via the v3→v4 migration."""
        # Simulate a v3 database: re-add the v3 CHECK (no ``cancelled``)
        # and drop the version rows above 3.
        await db_pool.execute("ALTER TABLE conductor_tasks DROP CONSTRAINT chk_task_status")
        await db_pool.execute(
            "ALTER TABLE conductor_tasks ADD CONSTRAINT chk_task_status CHECK ("
            "status IN ('pending', 'processing', 'completed', 'failed', 'retrying')"
            ")"
        )
        await db_pool.execute("DELETE FROM conductor_version WHERE version >= 4")

        current = await schema_manager.get_current_version()
        assert current == 3

        # Upgrading should re-add ``cancelled`` to the constraint
        await schema_manager.ensure_schema()

        current = await schema_manager.get_current_version()
        assert current == 6

        # The new status is accepted by the migrated constraint
        result = await db_pool.execute(
            "INSERT INTO conductor_tasks (task_id, task_type, status) " "VALUES ($1, $2, $3)",
            "v3-migrated-cancelled",
            "test",
            "cancelled",
        )
        assert "INSERT" in result

    async def test_migrates_v4_to_v5(self, schema_manager: Any, db_pool: Any) -> None:
        """A v4 database gains ``depends_on`` + the ``blocked`` status."""
        # Simulate a v4 database: drop the v5 column/index + the v5/v6 rows.
        await db_pool.execute("ALTER TABLE conductor_tasks DROP COLUMN IF EXISTS depends_on")
        await db_pool.execute("DROP INDEX IF EXISTS idx_tasks_depends_on")
        await db_pool.execute("DELETE FROM conductor_version WHERE version >= 5")

        current = await schema_manager.get_current_version()
        assert current == 4

        # Upgrade back to the latest version.
        await schema_manager.ensure_schema()

        current = await schema_manager.get_current_version()
        assert current == 6

        # The depends_on column exists and accepts a task with dependencies.
        rows = await db_pool.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'conductor_tasks' AND column_name = 'depends_on'"
        )
        assert rows

        result = await db_pool.execute(
            "INSERT INTO conductor_tasks (task_id, task_type, status, depends_on) "
            "VALUES ($1, $2, 'pending', $3)",
            "v4-migrated-dep",
            "test",
            ["some-dep"],
        )
        assert "INSERT" in result

    async def test_migrates_v5_to_v6(self, schema_manager: Any, db_pool: Any) -> None:
        """A v5 database gains the tracing ``traceparent`` columns."""
        # Simulate a v5 database: drop the v6 columns and their version row.
        await db_pool.execute("ALTER TABLE conductor_tasks DROP COLUMN IF EXISTS traceparent")
        await db_pool.execute("ALTER TABLE conductor_dead_letter DROP COLUMN IF EXISTS traceparent")
        await db_pool.execute("DELETE FROM conductor_version WHERE version = 6")

        current = await schema_manager.get_current_version()
        assert current == 5

        # Upgrade back to the latest version.
        await schema_manager.ensure_schema()

        current = await schema_manager.get_current_version()
        assert current == 6

        # Both tables carry the column again and it round-trips.
        for table in ("conductor_tasks", "conductor_dead_letter"):
            rows = await db_pool.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = $1 AND column_name = 'traceparent'",
                table,
            )
            assert rows, f"{table} is missing traceparent"

        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        await db_pool.execute(
            "INSERT INTO conductor_tasks (task_id, task_type, status, traceparent) "
            "VALUES ($1, $2, 'pending', $3)",
            "v5-migrated-trace",
            "test",
            traceparent,
        )
        row = await db_pool.fetchrow(
            "SELECT traceparent FROM conductor_tasks WHERE task_id = $1",
            "v5-migrated-trace",
        )
        assert row is not None
        assert row["traceparent"] == traceparent
