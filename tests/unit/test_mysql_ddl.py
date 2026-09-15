"""
Unit tests for the MySQL/MariaDB DDL plan.

MySQL has no service in the local development environment, so these tests pin
the *rendered* DDL: they require no database and catch the drift that matters
most — a query-layer column (``TASK_COLUMNS`` and friends drive the insert
placeholders) that never made it into the MySQL schema.
"""

from __future__ import annotations

import pytest

from conductor.db.ddl import SCHEMA_VERSION, get_ddl
from conductor.db.ddl import mysql as mysql_ddl
from conductor.db.queries import (
    DEAD_LETTER_COLUMNS,
    RECURRING_COLUMNS,
    TASK_COLUMNS,
)
from conductor.db.backends.registry import detect_backend

pytestmark = pytest.mark.unit

DDL = get_ddl("mysql")

WORKER_COLUMNS: tuple[str, ...] = (
    "worker_id",
    "status",
    "current_task_id",
    "hostname",
    "pid",
    "uptime_seconds",
    "tasks_processed_total",
    "tasks_failed_total",
    "last_heartbeat",
    "started_at",
)
"""``conductor_workers`` columns (see ``QueryBuilder.upsert_worker``)."""

RETRY_COLUMNS: tuple[str, ...] = (
    "id",
    "task_id",
    "attempt",
    "error_message",
    "scheduled_at",
    "created_at",
)
"""``conductor_retries`` columns (see ``QueryBuilder.insert_retry_record``)."""


def _create_statement(table: str) -> str:
    """Return the ``CREATE TABLE`` statement for *table* (leading whitespace trimmed)."""
    for statement in DDL.create_statements:
        if f"conductor_{table} (" in statement:
            return statement.strip()
    raise AssertionError(f"No CREATE TABLE statement found for '{table}'")


# ===================================================================
# Plan shape
# ===================================================================


class TestDdlPlan:

    def test_registry_returns_the_mysql_plan(self) -> None:
        assert DDL.backend == "mysql"
        assert DDL is mysql_ddl.DDL

    def test_unknown_backend_raises(self) -> None:
        with pytest.raises(KeyError, match="No DDL defined"):
            get_ddl("oracle")

    def test_creates_every_table(self) -> None:
        for table in ("tasks", "workers", "retries", "dead_letter", "recurring_tasks"):
            assert _create_statement(table).startswith("CREATE TABLE IF NOT EXISTS")

    def test_version_table_created(self) -> None:
        assert "CREATE TABLE IF NOT EXISTS conductor_version" in DDL.version_table

    def test_historic_migrations_are_empty_but_recorded(self) -> None:
        """MySQL ships at the current schema: every step is a recorded no-op."""
        assert set(DDL.migrations) == set(range(1, SCHEMA_VERSION + 1))
        assert all(statements == [] for statements in DDL.migrations.values())

    def test_rollback_drops_newest_first_without_cascade(self) -> None:
        order = [statement.split()[4].rstrip(";") for statement in DDL.rollback_statements]
        assert order[0] == "conductor_recurring_tasks"
        assert order[-1] == "conductor_version"
        assert "CASCADE" not in " ".join(DDL.rollback_statements)

    def test_indexes_are_embedded_in_the_create_statements(self) -> None:
        """MySQL has no ``CREATE INDEX IF NOT EXISTS``, so ``KEY`` clauses do the job."""
        assert DDL.index_statements == []
        assert "KEY idx_tasks_polling" in _create_statement("tasks")

    def test_no_returning_clause_anywhere(self) -> None:
        rendered = " ".join(DDL.create_statements) + " ".join(DDL.rollback_statements)
        assert "RETURNING" not in rendered.upper()

    def test_tables_are_innodb_utf8mb4(self) -> None:
        for statement in DDL.create_statements:
            assert "ENGINE=InnoDB" in statement
            assert "CHARSET=utf8mb4" in statement


# ===================================================================
# Column coverage (query-layer ↔ schema drift)
# ===================================================================


class TestColumnCoverage:

    @pytest.mark.parametrize("column", TASK_COLUMNS)
    def test_task_column_present(self, column: str) -> None:
        assert f"    {column} " in _create_statement("tasks")

    @pytest.mark.parametrize("column", DEAD_LETTER_COLUMNS)
    def test_dead_letter_column_present(self, column: str) -> None:
        assert f"    {column} " in _create_statement("dead_letter")

    @pytest.mark.parametrize("column", RECURRING_COLUMNS)
    def test_recurring_column_present(self, column: str) -> None:
        assert f"    {column} " in _create_statement("recurring_tasks")

    @pytest.mark.parametrize("column", WORKER_COLUMNS)
    def test_worker_column_present(self, column: str) -> None:
        assert f"    {column} " in _create_statement("workers")

    @pytest.mark.parametrize("column", RETRY_COLUMNS)
    def test_retry_column_present(self, column: str) -> None:
        assert f"    {column} " in _create_statement("retries")


# ===================================================================
# MySQL-specific type choices
# ===================================================================


class TestColumnTypes:

    def test_identifiers_are_indexable_varchars(self) -> None:
        statement = _create_statement("tasks")
        assert "task_id         VARCHAR(64)  NOT NULL" in statement
        assert "task_type       VARCHAR(255) NOT NULL" in statement

    def test_json_columns_use_the_json_type(self) -> None:
        statement = _create_statement("tasks")
        for column in ("payload", "retry_policy", "depends_on"):
            assert f"{column}" in statement
        assert "JSON         NOT NULL" in statement

    def test_timestamps_use_datetime_6(self) -> None:
        assert "created_at      DATETIME(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6)" in (
            _create_statement("tasks")
        )

    def test_booleans_are_tinyint(self) -> None:
        assert "discarded       TINYINT(1)   NOT NULL DEFAULT 0" in _create_statement("dead_letter")
        assert "enabled         TINYINT(1)   NOT NULL DEFAULT 1" in _create_statement(
            "recurring_tasks"
        )

    def test_status_check_accepts_every_status(self) -> None:
        statement = _create_statement("tasks")
        for status in (
            "pending",
            "processing",
            "completed",
            "failed",
            "retrying",
            "cancelled",
            "blocked",
        ):
            assert f"'{status}'" in statement

    def test_priority_check_matches_the_validation_range(self) -> None:
        assert "priority >= -100 AND priority <= 100" in _create_statement("tasks")

    def test_retries_cascade_on_task_delete(self) -> None:
        statement = _create_statement("retries")
        assert "FOREIGN KEY (task_id)" in statement
        assert "ON DELETE CASCADE" in statement

    def test_depends_on_has_no_index(self) -> None:
        """A JSON column cannot be indexed directly and cannot use one anyway."""
        assert "depends_on" not in _create_statement("tasks").split("CONSTRAINT")[-1]


# ===================================================================
# DSN detection for the new backend
# ===================================================================


class TestBackendDetection:

    @pytest.mark.parametrize(
        "dsn",
        ["mysql://user:pass@localhost:3306/db", "mariadb://user:pass@localhost/db"],
    )
    def test_mysql_schemes_route_to_the_mysql_backend(self, dsn: str) -> None:
        assert detect_backend(dsn) == "mysql"
