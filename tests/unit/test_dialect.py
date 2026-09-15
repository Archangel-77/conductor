"""
Unit tests for the backend/dialect layer.

Covers SQL rendering per dialect, value encoding/decoding, DSN detection and
pool construction.  No database is required.
"""

from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import pytest

from conductor.db.backends.base import SqlDialect
from conductor.db.backends.mysql import (
    MIN_MARIADB_VERSION,
    MIN_MYSQL_VERSION,
    MySqlConnection,
    MySqlDialect,
    MySqlPool,
    format_timestamp as mysql_format_timestamp,
    parse_dsn as mysql_parse_dsn,
    parse_server_version,
    parse_timestamp as mysql_parse_timestamp,
    validate_server_version,
)
from conductor.db.backends.postgres import PostgresDialect, PostgresPool
from conductor.db.backends.registry import create_pool, detect_backend
from conductor.db.backends.sqlite import (
    MEMORY_PATH,
    SqliteDialect,
    SqlitePool,
    format_timestamp,
    parse_dsn,
    parse_timestamp,
)
from conductor.db.connection import DatabasePool, PoolConfig
from conductor.db.queries import QueryBuilder
from conductor.exceptions import ConductorConnectionError, TaskError

pytestmark = pytest.mark.unit

PG = PostgresDialect()
SQLITE = SqliteDialect()
MYSQL = MySqlDialect()


# ===================================================================
# Placeholders and simple fragments
# ===================================================================


class TestPlaceholders:

    def test_postgres_numbered(self) -> None:
        assert PG.placeholder(1) == "$1"
        assert PG.placeholder(17) == "$17"
        assert PG.placeholder_list(1, 3) == "$1, $2, $3"

    def test_sqlite_positional(self) -> None:
        assert SQLITE.placeholder(1) == "?"
        assert SQLITE.placeholder(9) == "?"
        assert SQLITE.placeholder_list(1, 3) == "?, ?, ?"

    def test_json_param_casts_only_on_postgres(self) -> None:
        assert PG.json_param(3) == "$3::jsonb"
        assert SQLITE.json_param(3) == "?"
        assert MYSQL.json_param(3) == "%s"

    def test_mysql_uses_percent_s_placeholders(self) -> None:
        assert MYSQL.placeholder(1) == "%s"
        assert MYSQL.placeholder(42) == "%s"
        assert MYSQL.placeholder_list(1, 3) == "%s, %s, %s"

    def test_backend_names(self) -> None:
        assert PG.name == "postgresql"
        assert SQLITE.name == "sqlite"
        assert MYSQL.name == "mysql"

    def test_only_mysql_lacks_returning(self) -> None:
        assert PG.supports_returning is True
        assert SQLITE.supports_returning is True
        assert MYSQL.supports_returning is False

    def test_now_expressions(self) -> None:
        assert MYSQL.now() == "NOW(6)"
        assert MYSQL.interval_ago("scheduled_for", 1) == (
            "scheduled_for >= DATE_SUB(NOW(6), INTERVAL %s SECOND)"
        )


class TestArrayRendering:

    def test_postgres_uses_native_arrays(self) -> None:
        assert PG.array_type() == "TEXT[]"
        assert PG.array_contains("depends_on", 1) == "depends_on @> ARRAY[$1]"
        assert PG.array_element_in("d.task_id", "t.depends_on") == "d.task_id = ANY(t.depends_on)"
        assert PG.cardinality("t.depends_on") == "COALESCE(cardinality(t.depends_on), 0)"

    def test_sqlite_uses_json1(self) -> None:
        assert SQLITE.array_type() == "TEXT"
        assert "json_each(depends_on)" in SQLITE.array_contains("depends_on", 1)
        assert "json_each(t.depends_on)" in SQLITE.array_element_in("d.task_id", "t.depends_on")
        assert SQLITE.cardinality("t.depends_on") == "COALESCE(json_array_length(t.depends_on), 0)"

    def test_mysql_uses_json_functions(self) -> None:
        assert MYSQL.array_type() == "JSON"
        assert MYSQL.array_contains("depends_on", 1) == (
            "JSON_CONTAINS(depends_on, JSON_QUOTE(%s))"
        )
        assert MYSQL.array_element_in("d.task_id", "t.depends_on") == (
            "JSON_CONTAINS(t.depends_on, JSON_QUOTE(d.task_id))"
        )
        assert MYSQL.cardinality("t.depends_on") == "COALESCE(JSON_LENGTH(t.depends_on), 0)"


class TestLockingAndUpsert:

    def test_row_locking_clause(self) -> None:
        assert PG.for_update_skip_locked() == "FOR UPDATE SKIP LOCKED"
        assert MYSQL.for_update_skip_locked() == "FOR UPDATE SKIP LOCKED"
        assert SQLITE.for_update_skip_locked() == ""

    def test_upsert_targets_conflict_columns(self) -> None:
        clause = PG.upsert(["worker_id"], [("status", "EXCLUDED.status"), ("x", "NULL")])
        assert clause == (
            "ON CONFLICT (worker_id) DO UPDATE SET status = EXCLUDED.status, x = NULL"
        )
        assert SQLITE.upsert(["id"], [("a", "EXCLUDED.a")]) == PG.upsert(
            ["id"], [("a", "EXCLUDED.a")]
        )

    def test_insert_ignore(self) -> None:
        assert PG.insert_ignore(["version"]) == "ON CONFLICT (version) DO NOTHING"

    def test_mysql_upsert_uses_duplicate_key(self) -> None:
        clause = MYSQL.upsert(
            ["worker_id"],
            [("status", "EXCLUDED.status"), ("discarded", "FALSE"), ("x", "NULL")],
        )
        assert clause == (
            "ON DUPLICATE KEY UPDATE status = VALUES(status), " "discarded = FALSE, x = NULL"
        )

    def test_mysql_insert_ignore_is_a_noop_update(self) -> None:
        """MySQL has no ``ON CONFLICT DO NOTHING``: the self-assignment yields 0 rows."""
        assert MYSQL.insert_ignore(["task_id"]) == "ON DUPLICATE KEY UPDATE task_id = task_id"

    def test_case_insensitive_and_ordering(self) -> None:
        assert PG.ilike("task_id", 2) == "task_id ILIKE $2"
        assert SQLITE.ilike("task_id", 2) == "LOWER(task_id) LIKE LOWER(?)"
        assert MYSQL.ilike("task_id", 2) == "LOWER(task_id) LIKE LOWER(%s)"
        assert PG.nulls_last("last_heartbeat DESC") == "last_heartbeat DESC NULLS LAST"
        assert SQLITE.nulls_last("last_heartbeat DESC") == "last_heartbeat DESC NULLS LAST"
        assert MYSQL.nulls_last("last_heartbeat DESC") == (
            "last_heartbeat IS NULL, last_heartbeat DESC"
        )

    def test_constraint_helpers(self) -> None:
        assert PG.drop_constraint("conductor_tasks", "chk_task_status") == (
            "ALTER TABLE conductor_tasks DROP CONSTRAINT chk_task_status;"
        )
        assert MYSQL.drop_constraint("conductor_tasks", "chk_task_status") == (
            "ALTER TABLE conductor_tasks DROP CHECK chk_task_status;"
        )
        assert PG.add_check("t", "c", "a > 0") == ("ALTER TABLE t ADD CONSTRAINT c CHECK (a > 0);")


class TestRowcountNormalization:

    @pytest.mark.parametrize(
        ("tag", "expected"),
        [
            ("UPDATE 1", 1),
            ("UPDATE 0", 0),
            ("DELETE 3", 3),
            ("INSERT 0 1", 1),
            ("SELECT 1", 1),
            ("CREATE -1", 0),
            ("", 0),
            (None, 0),
            (7, 7),
        ],
    )
    def test_normalizes_tags_and_integers(self, tag: Any, expected: int) -> None:
        assert PG.normalize_rowcount(tag) == expected
        assert SQLITE.normalize_rowcount(tag) == expected
        assert MYSQL.normalize_rowcount(tag) == expected

    def test_mysql_reports_plain_rowcounts(self) -> None:
        assert MYSQL.normalize_rowcount(1, verb="INSERT") == 1
        assert MYSQL.normalize_rowcount(0, verb="UPDATE") == 0


# ===================================================================
# Value encoding / decoding
# ===================================================================


class TestValueEncoding:

    def test_postgres_binds_natively(self) -> None:
        moment = datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
        assert PG.encode_param(moment) is moment
        assert PG.encode_param(["a"]) == ["a"]

    def test_sqlite_encodes_datetimes_and_arrays(self) -> None:
        moment = datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
        assert SQLITE.encode_param(moment) == "2026-09-15 10:00:00.000000"
        assert SQLITE.encode_param(["a", "b"]) == '["a", "b"]'
        assert SQLITE.encode_param(5) == 5

    def test_mysql_encodes_datetimes_arrays_and_booleans(self) -> None:
        moment = datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
        assert MYSQL.encode_param(moment) == "2026-09-15 10:00:00.000000"
        assert MYSQL.encode_param(["a", "b"]) == '["a", "b"]'
        assert MYSQL.encode_param(True) == 1
        assert MYSQL.encode_param("plain") == "plain"

    def test_mysql_timestamp_round_trip(self) -> None:
        moment = datetime(2026, 9, 15, 10, 30, 15, 123456, tzinfo=timezone.utc)
        stored = mysql_format_timestamp(moment)
        assert stored == format_timestamp(moment)
        assert mysql_parse_timestamp(stored) == moment
        assert mysql_parse_timestamp(None) is None

    def test_mysql_parses_naive_datetimes_as_utc(self) -> None:
        naive = datetime(2026, 9, 15, 10, 0)
        assert mysql_parse_timestamp(naive) == datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)

    def test_timestamp_round_trip(self) -> None:
        moment = datetime(2026, 9, 15, 10, 30, 15, 123456, tzinfo=timezone.utc)
        stored = format_timestamp(moment)
        assert stored == "2026-09-15 10:30:15.123456"
        assert parse_timestamp(stored) == moment

    def test_timestamp_parsing_is_lexicographically_sortable(self) -> None:
        early = format_timestamp(datetime(2026, 9, 15, 9, 0, tzinfo=timezone.utc))
        late = format_timestamp(datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc))
        assert early < late

    def test_timestamp_parsing_tolerates_other_formats(self) -> None:
        assert parse_timestamp("2026-09-15T10:00:00+00:00") == datetime(
            2026, 9, 15, 10, 0, tzinfo=timezone.utc
        )
        assert parse_timestamp("not a timestamp") == "not a timestamp"
        assert parse_timestamp(None) is None


class TestRowNormalization:

    def test_postgres_decodes_json_text(self) -> None:
        row = {
            "payload": '{"a": 1}',
            "depends_on": '["x"]',
            "error_message": "plain text",
            "status": "pending",
        }
        normalized = PG.normalize_row(row)
        assert normalized["payload"] == {"a": 1}
        assert normalized["depends_on"] == ["x"]
        assert normalized["error_message"] == "plain text"

    def test_sqlite_decodes_json_timestamps_and_booleans(self) -> None:
        row = {
            "payload": '{"a": 1}',
            "depends_on": '["x", "y"]',
            "created_at": "2026-09-15 10:00:00.000000",
            "discarded": 0,
            "enabled": 1,
            "status": "pending",
        }
        normalized = SQLITE.normalize_row(row)
        assert normalized["payload"] == {"a": 1}
        assert normalized["depends_on"] == ["x", "y"]
        assert normalized["created_at"] == datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
        assert normalized["discarded"] is False
        assert normalized["enabled"] is True

    def test_decode_json_value_keeps_plain_strings(self) -> None:
        assert PG.decode_json_value("hello") == "hello"
        assert PG.decode_json_value("{broken") == "{broken"

    def test_mysql_normalises_rows(self) -> None:
        row: dict[str, Any] = {
            "payload": b'{"a": 1}',
            "depends_on": '["x", "y"]',
            "created_at": datetime(2026, 9, 15, 10, 0),
            "discarded": 0,
            "enabled": 1,
            "status": "pending",
        }
        normalized = MYSQL.normalize_row(row)
        assert normalized["payload"] == {"a": 1}
        assert normalized["depends_on"] == ["x", "y"]
        assert normalized["created_at"] == datetime(2026, 9, 15, 10, 0, tzinfo=timezone.utc)
        assert normalized["discarded"] is False
        assert normalized["enabled"] is True

    def test_mysql_keeps_null_booleans(self) -> None:
        assert MYSQL.normalize_row({"discarded": None})["discarded"] is None


# ===================================================================
# MySQL statement rendering (no server required)
# ===================================================================


class _RecordingPool:
    """A pool double that records every statement and its parameters."""

    dialect: SqlDialect = MYSQL

    def __init__(
        self,
        *,
        rowcount: int = 1,
        rows: list[dict[str, Any]] | None = None,
        dialect: SqlDialect = MYSQL,
    ) -> None:
        self.dialect = dialect
        self.statements: list[tuple[str, tuple[Any, ...]]] = []
        self.rowcount = rowcount
        self.rows = rows if rows is not None else []
        self.transactions = 0

    def _record(self, query: str, args: tuple[Any, ...]) -> None:
        self.statements.append((query, args))

    async def execute(self, query: str, *args: Any) -> int:
        self._record(query, args)
        return self.rowcount

    async def fetch(self, query: str, *args: Any) -> list[Any]:
        self._record(query, args)
        return self.rows

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self._record(query, args)
        return None

    async def fetchval(self, query: str, *args: Any) -> Any:
        self._record(query, args)
        return None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[_RecordingPool]:
        self.transactions += 1
        yield self

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_RecordingPool]:
        yield self


def _task_row(task_id: str = "t1") -> dict[str, Any]:
    """A minimal task dictionary accepted by ``insert_task``."""
    return {"task_id": task_id, "task_type": "unit", "payload": {"a": 1}}


class TestMySqlStatementRendering:
    """Render the dialect-sensitive statements and check for PostgreSQL-isms.

    MySQL has no server in this environment, so these tests pin the *rendered*
    SQL: a PostgreSQL-only construct (``RETURNING``, ``$n``, ``ON CONFLICT``,
    native arrays) must never reach a MySQL connection, and every ``%s`` must
    have a matching parameter in the same order.
    """

    @staticmethod
    def _assert_mysql_safe(pool: _RecordingPool) -> None:
        assert pool.statements, "expected at least one statement"
        for query, args in pool.statements:
            assert "RETURNING" not in query.upper(), query
            assert "ON CONFLICT" not in query.upper(), query
            assert "ILIKE" not in query.upper(), query
            assert "NULLS LAST" not in query.upper(), query
            assert "ARRAY[" not in query.upper(), query
            assert "$1" not in query and "::jsonb" not in query, query
            assert query.count("%s") == len(args), query

    async def test_insert_task(self) -> None:
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        assert await builder.insert_task(_task_row()) == "t1"
        self._assert_mysql_safe(pool)
        assert "INSERT INTO conductor_tasks" in pool.statements[0][0]
        assert "ON DUPLICATE KEY UPDATE task_id = task_id" in pool.statements[0][0]

    async def test_insert_task_duplicate_raises(self) -> None:
        """MySQL reports 0 affected rows when the no-op upsert changes nothing."""
        pool = _RecordingPool(rowcount=0)
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        with pytest.raises(TaskError, match="already exists"):
            await builder.insert_task(_task_row())

    async def test_insert_dlq_task(self) -> None:
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        dlq = {"task_id": "t1", "task_type": "unit", "payload": {}, "error_message": "boom"}
        assert await builder.insert_dlq_task(dlq) == "t1"
        self._assert_mysql_safe(pool)
        assert "ON DUPLICATE KEY UPDATE" in pool.statements[0][0]
        assert "VALUES(moved_at)" in pool.statements[0][0]

    async def test_insert_retry_record(self) -> None:
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        record = {
            "id": "r1",
            "task_id": "t1",
            "attempt": 1,
            "scheduled_at": datetime.now(timezone.utc),
        }
        assert await builder.insert_retry_record(record) == "r1"
        self._assert_mysql_safe(pool)

    async def test_insert_recurring_task_duplicate_raises(self) -> None:
        pool = _RecordingPool(rowcount=0)
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        with pytest.raises(TaskError, match="already exists"):
            await builder.insert_recurring_task(
                {"id": "r1", "task_type": "unit", "cron_expression": "*/5 * * * *"}
            )
        self._assert_mysql_safe(pool)

    async def test_upsert_worker(self) -> None:
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        assert await builder.upsert_worker({"worker_id": "w1"}) == "w1"
        self._assert_mysql_safe(pool)

    async def test_mark_dependents_blocked_selects_then_updates(self) -> None:
        """Without ``RETURNING`` the update is a locking read plus a write."""
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        assert await builder.mark_dependents_blocked("parent", "dependency failed") == []
        self._assert_mysql_safe(pool)
        assert "FOR UPDATE" in pool.statements[0][0]
        assert "JSON_CONTAINS" in pool.statements[0][0]
        # Nothing pending: no update is issued at all.
        assert len(pool.statements) == 1

    async def test_mark_dependents_blocked_binds_the_error_message_first(self) -> None:
        """Parameters must be passed in textual order (error message, then IDs).

        MySQL binds positionally: passing the IDs first silently updated no rows
        (``task_id IN ('dependency failed')``) while still reporting them as
        blocked — caught by the parity matrix against a live server.
        """
        pool = _RecordingPool(rows=[{"task_id": "c1"}, {"task_id": "c2"}])
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        blocked = await builder.mark_dependents_blocked("parent", "dependency failed")
        assert blocked == ["c1", "c2"]
        self._assert_mysql_safe(pool)
        update_query, update_args = pool.statements[1]
        assert update_args == ("dependency failed", "c1", "c2")
        assert update_query.index("error_message") < update_query.index("task_id IN")
        assert "task_id IN (%s, %s)" in update_query
        # The write re-checks the status so a concurrently completed dependent
        # is never reported as blocked.
        assert "WHERE status = 'pending' AND task_id IN" in update_query

    async def test_polling_and_worker_queries(self) -> None:
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        await builder.select_pending_tasks(limit=5, route="default")
        await builder.select_active_workers(heartbeat_timeout=30.0)
        await builder.cancel_task("t1")
        await builder.delete_completed_tasks(datetime.now(timezone.utc))
        self._assert_mysql_safe(pool)

    async def test_polling_still_filters_retrying_and_unmet_dependencies(self) -> None:
        pool = _RecordingPool()
        builder = QueryBuilder(pool)  # type: ignore[arg-type]
        await builder.select_pending_tasks(limit=5)
        query = pool.statements[0][0]
        # The retry-claiming behaviour and the dependency gate must survive the
        # dialect rendering (see the backend-matrix regression test).
        assert "status IN ('pending', 'retrying')" in query
        assert "d.status NOT IN ('completed', 'cancelled')" in query
        assert "scheduled_for <= NOW(6)" in query
        assert "FOR UPDATE SKIP LOCKED" in query


# ===================================================================
# Atomic task claiming (all backends)
# ===================================================================


class TestAtomicClaimRendering:
    """Claiming must move rows to ``processing`` *as part of* taking the claim.

    ``FOR UPDATE SKIP LOCKED`` on its own is not enough: those locks are
    released the moment the statement ends, so the status transition has to
    happen in the same statement (``RETURNING`` backends) or inside one
    transaction — otherwise two workers claim the same task.
    """

    @staticmethod
    def _builder(dialect: SqlDialect, **kwargs: Any) -> tuple[QueryBuilder, _RecordingPool]:
        pool = _RecordingPool(dialect=dialect, **kwargs)
        return QueryBuilder(pool), pool  # type: ignore[arg-type]

    async def test_postgres_claims_in_a_single_materialised_statement(self) -> None:
        builder, pool = self._builder(PostgresDialect())
        await builder.claim_pending_tasks(limit=5, worker_id="w1", route="r1")
        assert len(pool.statements) == 1
        query, args = pool.statements[0]
        # The locking read must be *materialised*: inside an UPDATE subquery
        # PostgreSQL ignores the LIMIT for the rows it actually updates.
        assert "WITH claimed AS MATERIALIZED" in query
        assert "UPDATE conductor_tasks" in query
        assert "SET status = 'processing'" in query
        assert "RETURNING *" in query
        assert "FOR UPDATE SKIP LOCKED" in query
        assert "LIMIT $2" in query
        assert "worker_id = $3" in query
        # Bound in textual order: route, limit, worker_id.
        assert args == ("r1", 5, "w1")
        assert pool.transactions == 0

    async def test_postgres_claim_without_route_binds_limit_then_worker(self) -> None:
        builder, pool = self._builder(PostgresDialect())
        await builder.claim_pending_tasks(limit=3, worker_id="w1")
        assert pool.statements[0][1] == (3, "w1")

    async def test_mysql_claims_inside_one_transaction(self) -> None:
        builder, pool = self._builder(MYSQL, rows=[{"task_id": "t1"}, {"task_id": "t2"}])
        claimed = await builder.claim_pending_tasks(limit=2, worker_id="w1")
        assert [row["task_id"] for row in claimed] == ["t1", "t2"]
        assert pool.transactions == 1
        select_query, select_args = pool.statements[0]
        update_query, update_args = pool.statements[1]
        claimed_query, claimed_args = pool.statements[2]
        assert "FOR UPDATE SKIP LOCKED" in select_query
        assert "RETURNING" not in select_query.upper()
        assert "ON CONFLICT" not in update_query.upper()
        assert select_args == (2,)
        # The write re-checks the status and binds the worker id first.
        assert "WHERE status IN ('pending', 'retrying')" in update_query
        assert update_args == ("w1", "t1", "t2")
        assert claimed_args == ("w1", "t1", "t2")
        assert "worker_id = %s" in claimed_query
        assert update_query.count("%s") == len(update_args)

    async def test_mysql_claim_without_candidates_skips_the_write(self) -> None:
        builder, pool = self._builder(MYSQL, rows=[])
        assert await builder.claim_pending_tasks(limit=2, worker_id="w1") == []
        assert len(pool.statements) == 1

    async def test_sqlite_claims_in_a_single_statement_without_locking(self) -> None:
        builder, pool = self._builder(SQLITE, rows=[{"task_id": "t1"}])
        await builder.claim_pending_tasks(limit=2, worker_id="w1")
        assert len(pool.statements) == 1
        query, args = pool.statements[0]
        assert "WITH claimed AS MATERIALIZED" in query
        assert "SET status = 'processing'" in query
        assert "RETURNING *" in query
        # SQLite has no row locking; writers are serialised by BEGIN IMMEDIATE,
        # so the single statement above is already atomic.
        assert "FOR UPDATE" not in query
        assert args == (2, "w1")
        assert query.count("?") == len(args)

    async def test_claim_keeps_retrying_and_dependency_gate(self) -> None:
        for dialect in (PostgresDialect(), MYSQL, SQLITE):
            builder, pool = self._builder(dialect)
            await builder.claim_pending_tasks(limit=1, worker_id="w1")
            rendered = " ".join(query for query, _ in pool.statements)
            assert "status IN ('pending', 'retrying')" in rendered
            assert "d.status NOT IN ('completed', 'cancelled')" in rendered
            assert "scheduled_for" in rendered

    async def test_claim_rejects_an_empty_worker_id(self) -> None:
        builder, _ = self._builder(MYSQL)
        with pytest.raises(ValueError, match="worker_id must not be empty"):
            await builder.claim_pending_tasks(limit=1, worker_id="  ")

    async def test_release_claim_clears_the_claim_markers(self) -> None:
        builder, pool = self._builder(MYSQL)
        assert await builder.release_claim("t1") is True
        query, args = pool.statements[0]
        assert "SET status = 'pending', worker_id = NULL, started_at = NULL" in query
        assert "AND status = 'processing'" in query
        assert args == ("t1",)

    async def test_release_claim_reports_an_unclaimed_task(self) -> None:
        builder, _ = self._builder(MYSQL, rowcount=0)
        assert await builder.release_claim("t1") is False

    async def test_reclaim_stale_tasks_excludes_alive_workers(self) -> None:
        builder, pool = self._builder(MYSQL, rows=[{"worker_id": "w-alive"}], rowcount=2)
        assert await builder.reclaim_stale_tasks(60.0) == 2
        select_query, select_args = pool.statements[0]
        update_query, update_args = pool.statements[1]
        assert "FROM conductor_workers" in select_query
        assert "last_heartbeat >= %s" in select_query
        assert "SET status = 'pending', worker_id = NULL, started_at = NULL" in update_query
        assert "started_at < %s" in update_query
        # An owner with no heartbeat row at all counts as dead too.
        assert "worker_id IS NULL OR worker_id NOT IN (%s)" in update_query
        # cutoff, then the alive worker id — in textual order.
        assert update_args == (select_args[0], "w-alive")
        assert update_query.count("%s") == len(update_args)

    async def test_reclaim_stale_tasks_without_alive_workers(self) -> None:
        builder, pool = self._builder(MYSQL, rows=[], rowcount=0)
        assert await builder.reclaim_stale_tasks(60.0) == 0
        update_query, update_args = pool.statements[1]
        # No owner condition is needed when nothing is alive.
        assert "NOT IN" not in update_query
        assert "worker_id IS NULL OR" not in update_query
        assert len(update_args) == 1
        assert update_query.count("%s") == 1

    async def test_reclaim_rejects_a_non_positive_window(self) -> None:
        builder, _ = self._builder(MYSQL)
        with pytest.raises(ValueError, match="must be > 0"):
            await builder.reclaim_stale_tasks(0)


# ===================================================================
# MySQL connection wrapper (fake asyncmy driver)
# ===================================================================


class _FakeCursor:
    """Stand-in for an asyncmy dict cursor."""

    def __init__(self, rows: list[dict[str, Any]], rowcount: Any) -> None:
        self._rows = rows
        self.rowcount = rowcount
        self.calls: list[tuple[str, Any]] = []
        self.closed = False

    async def execute(self, query: str, args: Any = None) -> None:
        self.calls.append((query, args))

    async def fetchall(self) -> list[dict[str, Any]]:
        return self._rows

    async def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    async def close(self) -> None:
        self.closed = True


class _FakeAsyncmyConnection:
    """Stand-in for ``asyncmy.Connection`` (``begin``/``commit`` are coroutines)."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, rowcount: Any = 1) -> None:
        self._rows = rows or []
        self._rowcount = rowcount
        self.events: list[str] = []
        self.cursors: list[_FakeCursor] = []

    async def cursor(self) -> _FakeCursor:
        cursor = _FakeCursor(self._rows, self._rowcount)
        self.cursors.append(cursor)
        return cursor

    async def begin(self) -> None:
        self.events.append("begin")

    async def commit(self) -> None:
        self.events.append("commit")

    async def rollback(self) -> None:
        self.events.append("rollback")


class TestMySqlConnection:

    async def test_fetch_normalises_rows(self) -> None:
        fake = _FakeAsyncmyConnection(
            rows=[
                {
                    "payload": b'{"a": 1}',
                    "created_at": datetime(2026, 9, 16, 12, 0),
                    "discarded": 0,
                    "status": "pending",
                }
            ]
        )
        rows = await MySqlConnection(fake, MYSQL).fetch("SELECT 1", "arg")
        assert rows == [
            {
                "payload": {"a": 1},
                "created_at": datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc),
                "discarded": False,
                "status": "pending",
            }
        ]
        # Cursors are always closed and parameters are encoded for the driver.
        assert fake.cursors[0].closed is True
        assert fake.cursors[0].calls == [("SELECT 1", ("arg",))]

    async def test_fetchval_returns_first_column(self) -> None:
        fake = _FakeAsyncmyConnection(rows=[{"v": 1, "other": 2}])
        assert await MySqlConnection(fake, MYSQL).fetchval("SELECT 1") == 1

    async def test_fetchval_and_fetchrow_return_none_when_empty(self) -> None:
        connection = MySqlConnection(_FakeAsyncmyConnection(), MYSQL)
        assert await connection.fetchval("SELECT 1") is None
        assert await connection.fetchrow("SELECT 1") is None

    async def test_execute_returns_an_integer_rowcount(self) -> None:
        fake = _FakeAsyncmyConnection(rowcount=3)
        assert await MySqlConnection(fake, MYSQL).execute("UPDATE t SET a = 1") == 3

    async def test_execute_reports_zero_when_the_driver_has_no_rowcount(self) -> None:
        """DDL/SELECT leave ``rowcount`` unset (``None``)."""
        fake = _FakeAsyncmyConnection(rowcount=None)
        assert await MySqlConnection(fake, MYSQL).execute("CREATE TABLE t (a INT)") == 0

    async def test_binds_are_encoded(self) -> None:
        fake = _FakeAsyncmyConnection()
        await MySqlConnection(fake, MYSQL).execute(
            "INSERT INTO t (a, b, c) VALUES (%s, %s, %s)",
            datetime(2026, 9, 16, 12, 0, tzinfo=timezone.utc),
            ["x"],
            True,
        )
        assert fake.cursors[0].calls[0][1] == ("2026-09-16 12:00:00.000000", '["x"]', 1)

    async def test_transaction_commits_on_success(self) -> None:
        fake = _FakeAsyncmyConnection()
        async with MySqlConnection(fake, MYSQL).transaction():
            pass
        assert fake.events == ["begin", "commit"]

    async def test_transaction_rolls_back_on_error(self) -> None:
        fake = _FakeAsyncmyConnection()
        with pytest.raises(RuntimeError):
            async with MySqlConnection(fake, MYSQL).transaction():
                raise RuntimeError("boom")
        assert fake.events == ["begin", "rollback"]


class TestMySqlServerVersion:
    """The connect-time guard that rejects servers without ``SKIP LOCKED``.

    Verified against live servers: MySQL 8.0.46/8.4 and MariaDB 11.8.9 connect,
    MariaDB 10.5.29 is rejected (``SKIP LOCKED`` arrived in MariaDB 10.6).
    """

    def test_minimums_are_documented_values(self) -> None:
        assert MIN_MYSQL_VERSION == (8, 0, 16)
        assert MIN_MARIADB_VERSION == (10, 6, 0)

    @pytest.mark.parametrize(
        ("version", "flavour", "parts"),
        [
            ("8.0.46", "mysql", (8, 0, 46)),
            ("8.4.6-log", "mysql", (8, 4, 6)),
            ("9", "mysql", (9,)),
            ("8.0", "mysql", (8, 0)),
            ("11.8.9-MariaDB-ubu2404", "mariadb", (11, 8, 9)),
            ("10.6.0-MariaDB", "mariadb", (10, 6, 0)),
            # MariaDB's ``5.5.5-`` compatibility prefix must be stripped.
            ("5.5.5-10.11.6-MariaDB-1:10.11.6+maria~ubu2204", "mariadb", (10, 11, 6)),
            ("not a version", "mysql", ()),
        ],
    )
    def test_parse_server_version(self, version: str, flavour: str, parts: tuple[int, ...]) -> None:
        assert parse_server_version(version) == (flavour, parts)

    @pytest.mark.parametrize(
        "version",
        ["8.0.16", "8.0.46", "8.4.6-log", "10.6.0-MariaDB", "11.8.9-MariaDB-ubu2404", "weird"],
    )
    def test_supported_versions_are_accepted(self, version: str) -> None:
        validate_server_version(version)

    @pytest.mark.parametrize(
        "version",
        ["5.7.44", "8.0.15", "8", "10.5.29-MariaDB-ubu2004", "5.5.5-10.4.34-MariaDB"],
    )
    def test_old_versions_are_rejected_with_the_requirement(self, version: str) -> None:
        with pytest.raises(ConductorConnectionError) as excinfo:
            validate_server_version(version)
        message = str(excinfo.value)
        assert version in message
        assert "requires MySQL 8.0.16+ or MariaDB 10.6.0+" in message


class TestMySqlConnectErrors:
    """The pool's retry loop must wrap driver failures in ``ConductorConnectionError``."""

    async def test_unreachable_server_reports_a_conductor_error(self) -> None:
        from conductor.db.connection import DatabasePool

        pool = DatabasePool(
            dsn="mysql://conductor:conductor@127.0.0.1:3307/conductor_unreachable",
            min_size=1,
            max_size=1,
            timeout=1.0,
            max_retries=1,
        )
        assert pool.backend_name == "mysql"
        assert pool.dialect.name == "mysql"
        try:
            await pool.connect()
        except ConductorConnectionError as exc:
            # A missing extra, a refused connection and a bad DSN must all end
            # up here rather than leaking an asyncmy exception.
            assert "MySQL" in str(exc)
            assert pool.is_connected is False
            return
        await pool.disconnect()
        pytest.skip("a MySQL server is listening on port 3307")


# ===================================================================
# DSN detection and pool construction
# ===================================================================


class TestDetectBackend:

    @pytest.mark.parametrize(
        ("dsn", "expected"),
        [
            ("postgresql://user:pass@localhost:5432/db", "postgresql"),
            ("postgres://user:pass@localhost:5432/db", "postgresql"),
            ("sqlite:///conductor.db", "sqlite"),
            ("sqlite:////var/lib/conductor.db", "sqlite"),
            ("sqlite://:memory:", "sqlite"),
            ("mysql://user:pass@localhost/db", "mysql"),
            ("mariadb://user:pass@localhost/db", "mysql"),
        ],
    )
    def test_supported_schemes(self, dsn: str, expected: str) -> None:
        assert detect_backend(dsn) == expected

    def test_empty_dsn_rejected(self) -> None:
        with pytest.raises(ConductorConnectionError, match="must not be empty"):
            detect_backend("")

    def test_unsupported_scheme_rejected(self) -> None:
        with pytest.raises(ConductorConnectionError, match="Unsupported database scheme"):
            detect_backend("oracle://host/db")

    def test_missing_driver_reports_the_extra(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A missing optional driver must name the extra, not leak an ImportError."""
        # ``None`` in ``sys.modules`` makes the ``from … import`` raise ImportError.
        monkeypatch.setitem(sys.modules, "conductor.db.backends.mysql", None)
        with pytest.raises(ConductorConnectionError, match=r"\[mysql\]"):
            create_pool("mysql://user:pass@localhost/db")

    def test_missing_sqlite_driver_reports_the_extra(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "conductor.db.backends.sqlite", None)
        with pytest.raises(ConductorConnectionError, match=r"\[sqlite\]"):
            create_pool("sqlite:///conductor.db")


class TestCreatePool:

    def test_postgres_pool(self) -> None:
        pool = create_pool("postgresql://user:pass@localhost/db")
        assert isinstance(pool, PostgresPool)
        assert pool.dialect.name == "postgresql"

    def test_sqlite_pool(self) -> None:
        pool = create_pool("sqlite:///conductor.db")
        assert isinstance(pool, SqlitePool)
        assert pool.dialect.name == "sqlite"

    def test_sqlite_dsn_parsing(self) -> None:
        assert parse_dsn("sqlite:///relative.db") == "relative.db"
        assert parse_dsn("sqlite:////absolute/path.db") == "/absolute/path.db"
        assert parse_dsn("sqlite://:memory:") == MEMORY_PATH
        assert parse_dsn("sqlite://") == MEMORY_PATH
        assert parse_dsn("sqlite:///conductor.db?cache=shared") == "conductor.db"

    def test_mysql_pool(self) -> None:
        pool = create_pool("mysql://user:pass@localhost:3306/db")
        assert isinstance(pool, MySqlPool)
        assert pool.dialect.name == "mysql"
        assert pool.is_connected is False

    def test_mysql_dsn_parsing(self) -> None:
        params = mysql_parse_dsn("mysql://user:p%40ss@db.internal:3307/conductor?charset=utf8mb4")
        assert params["host"] == "db.internal"
        assert params["port"] == 3307
        assert params["user"] == "user"
        assert params["password"] == "p@ss"
        assert params["db"] == "conductor"
        assert params["charset"] == "utf8mb4"
        assert params["autocommit"] is True

    def test_mysql_dsn_defaults_and_passthrough(self) -> None:
        params = mysql_parse_dsn("mariadb://user:pass@localhost/db?connect_timeout=3&echo=true")
        assert params["port"] == 3306
        assert params["charset"] == "utf8mb4"
        assert params["connect_timeout"] == 3
        assert params["echo"] == "true"

    def test_mysql_dsn_rejects_other_schemes(self) -> None:
        with pytest.raises(ConductorConnectionError, match="Not a MySQL DSN"):
            mysql_parse_dsn("postgresql://user@localhost/db")


class TestDatabasePoolFacade:

    def test_config_records_backend_independently(self) -> None:
        config = PoolConfig(dsn="sqlite:///x.db")
        assert config.backend == "sqlite"
        assert config.busy_timeout == 5.0

    def test_invalid_busy_timeout(self) -> None:
        with pytest.raises(ValueError, match="busy_timeout"):
            PoolConfig(dsn="sqlite:///x.db", busy_timeout=0).validate()

    def test_backend_created_lazily(self) -> None:
        """Constructing a pool must not fail on a bad DSN (connect() reports it)."""
        pool = DatabasePool(dsn="pg://legacy-typo")
        assert pool.is_connected is False
        with pytest.raises(ConductorConnectionError):
            _ = pool.dialect

    def test_dialect_from_dsn(self) -> None:
        assert DatabasePool(dsn="sqlite:///x.db").dialect.name == "sqlite"
        assert DatabasePool(dsn="postgresql://u@h/db").dialect.name == "postgresql"
        assert DatabasePool(dsn="mysql://u@h/db").dialect.name == "mysql"


class TestQueryBuilderDialectSelection:

    def test_uses_pool_dialect(self) -> None:
        builder = QueryBuilder(DatabasePool(dsn="sqlite:///x.db"))
        assert builder._dialect.name == "sqlite"

    def test_falls_back_to_postgres_for_duck_typed_pools(self) -> None:
        class _FakePool:
            """Minimal double without a dialect attribute (unit-test style)."""

        builder = QueryBuilder(_FakePool())  # type: ignore[arg-type]
        assert builder._dialect.name == "postgresql"

    def test_explicit_dialect_override(self) -> None:
        builder = QueryBuilder(DatabasePool(dsn="postgresql://u@h/db"), SQLITE)
        assert builder._dialect.name == "sqlite"


def test_sql_dialect_is_abstract() -> None:
    """The dialect is an ABC: subclasses must render every fragment."""
    with pytest.raises(TypeError):
        SqlDialect()  # type: ignore[abstract]
