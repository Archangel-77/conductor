"""
Unit tests for SchemaManager (database schema creation & migrations).

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

from __future__ import annotations

from typing import Any

import pytest

from conductor.db.schema import SCHEMA_VERSION, CREATE_VERSION_TABLE

pytestmark = pytest.mark.integration


# ===================================================================
# Schema constants
# ===================================================================


class TestSchemaConstants:

    def test_schema_version(self) -> None:
        assert SCHEMA_VERSION == 1

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
                "SELECT tablename FROM pg_catalog.pg_tables " "WHERE tablename = $1",
                table,
            )
            assert row is not None, f"Table '{table}' not found"

    async def test_version_tracked(self, db_pool: Any) -> None:
        """The conductor_version table should record version 1."""
        row = await db_pool.fetchrow("SELECT version FROM conductor_version")
        assert row is not None
        assert row["version"] == 1


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
            "INSERT INTO conductor_tasks " "(task_id, task_type, payload) " "VALUES ($1, $2, '{}')",
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

    async def test_workers_heartbeat_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_workers_last_heartbeat")

    async def test_retries_task_id_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_retries_task_id")

    async def test_dead_letter_discarded_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_dead_letter_discarded")

    async def test_recurring_next_run_index(self, db_pool: Any) -> None:
        assert await self._index_exists(db_pool, "idx_recurring_next_run")


# ===================================================================
# Idempotent migrations
# ===================================================================


class TestIdempotentMigrations:

    async def test_ensure_schema_twice(self, schema_manager: Any) -> None:
        """Running ensure_schema twice should not raise."""
        await schema_manager.ensure_schema()  # second run

    async def test_version_not_duplicated(self, db_pool: Any) -> None:
        """Version row should not be duplicated after re-migration."""
        rows = await db_pool.fetch("SELECT version FROM conductor_version")
        assert len(rows) == 1


# ===================================================================
# Rollback
# ===================================================================


class TestRollback:

    async def test_rollback_drops_tables(self, schema_manager: Any, db_pool: Any) -> None:
        """Rollback to v0 should drop all conductor tables."""
        await schema_manager.rollback(target_version=0)

        # Tables should be gone
        row = await db_pool.fetchrow(
            "SELECT tablename FROM pg_catalog.pg_tables " "WHERE tablename LIKE 'conductor_%'"
        )
        assert row is None

        # Re-create for subsequent tests
        await schema_manager.ensure_schema()
