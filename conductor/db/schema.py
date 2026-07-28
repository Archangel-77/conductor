"""
Schema management and auto-migration for Conductor.

Handles creation and versioning of all database tables, indexes,
constraints, and checks.  Migrations are idempotent – running them
multiple times is safe.
"""

from __future__ import annotations

import logging

from conductor.db.connection import DatabasePool

logger = logging.getLogger("conductor.db.schema")

# ---------------------------------------------------------------------------
# Version tracking
# ---------------------------------------------------------------------------

SCHEMA_VERSION = 1
"""The current schema version expected by this code."""

CREATE_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_version (
    version     INTEGER NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (version)
);
"""

# ---------------------------------------------------------------------------
# v0 → v1 migration
# ---------------------------------------------------------------------------

CREATE_TASKS_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_tasks (
    task_id         TEXT        NOT NULL,
    task_type       TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'pending',
    priority        INTEGER     NOT NULL DEFAULT 0,
    route           TEXT        NOT NULL DEFAULT 'default',
    attempt         INTEGER     NOT NULL DEFAULT 0,
    max_retries     INTEGER     NOT NULL DEFAULT 3,
    retry_policy    JSONB       NOT NULL DEFAULT '{}',
    scheduled_for   TIMESTAMPTZ,
    worker_id       TEXT,
    result          JSONB,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,

    CONSTRAINT pk_tasks PRIMARY KEY (task_id),
    CONSTRAINT chk_task_status CHECK (
        status IN ('pending', 'processing', 'completed', 'failed', 'retrying')
    ),
    CONSTRAINT chk_task_priority CHECK (priority >= -100 AND priority <= 100),
    CONSTRAINT chk_task_attempt CHECK (attempt >= 0),
    CONSTRAINT chk_task_max_retries CHECK (max_retries >= 0)
);
"""

CREATE_WORKERS_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_workers (
    worker_id           TEXT        NOT NULL,
    status              TEXT        NOT NULL DEFAULT 'idle',
    current_task_id     TEXT,
    hostname            TEXT        NOT NULL DEFAULT '',
    pid                 INTEGER     NOT NULL DEFAULT 0,
    uptime_seconds      REAL        NOT NULL DEFAULT 0.0,
    tasks_processed_total INTEGER   NOT NULL DEFAULT 0,
    tasks_failed_total  INTEGER     NOT NULL DEFAULT 0,
    last_heartbeat      TIMESTAMPTZ,
    started_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_workers PRIMARY KEY (worker_id),
    CONSTRAINT chk_worker_status CHECK (
        status IN ('idle', 'processing', 'unhealthy')
    )
);
"""

CREATE_RETRIES_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_retries (
    id              TEXT        NOT NULL,
    task_id         TEXT        NOT NULL,
    attempt         INTEGER     NOT NULL,
    error_message   TEXT,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_retries PRIMARY KEY (id),
    CONSTRAINT fk_retries_task
        FOREIGN KEY (task_id)
        REFERENCES conductor_tasks (task_id)
        ON DELETE CASCADE
);
"""

CREATE_DEAD_LETTER_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_dead_letter (
    task_id         TEXT        NOT NULL,
    task_type       TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    error_message   TEXT,
    attempts        INTEGER     NOT NULL DEFAULT 0,
    retry_policy    JSONB       NOT NULL DEFAULT '{}',
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    discarded       BOOLEAN     NOT NULL DEFAULT FALSE,
    discard_reason  TEXT,
    discarded_at    TIMESTAMPTZ,

    CONSTRAINT pk_dead_letter PRIMARY KEY (task_id)
);
"""

CREATE_RECURRING_TASKS_TABLE = """
CREATE TABLE IF NOT EXISTS conductor_recurring_tasks (
    id              TEXT        NOT NULL,
    task_type       TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    cron_expression TEXT        NOT NULL,
    route           TEXT        NOT NULL DEFAULT 'default',
    priority        INTEGER     NOT NULL DEFAULT 0,
    retry_policy    JSONB       NOT NULL DEFAULT '{}',
    enabled         BOOLEAN     NOT NULL DEFAULT TRUE,
    next_run_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_run_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_recurring_tasks PRIMARY KEY (id)
);
"""

# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------

TASK_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_tasks_status"
    " ON conductor_tasks (status);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_task_type"
    " ON conductor_tasks (task_type);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_route"
    " ON conductor_tasks (route);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_priority"
    " ON conductor_tasks (priority DESC);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_created_at"
    " ON conductor_tasks (created_at);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_scheduled_for"
    " ON conductor_tasks (scheduled_for);",
    "CREATE INDEX IF NOT EXISTS idx_tasks_worker_id"
    " ON conductor_tasks (worker_id);",
    # Composite index used by the polling query
    (
        "CREATE INDEX IF NOT EXISTS idx_tasks_polling"
        " ON conductor_tasks"
        " (status, scheduled_for, priority DESC, created_at);"
    ),
]

WORKER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_workers_status"
    " ON conductor_workers (status);",
    "CREATE INDEX IF NOT EXISTS idx_workers_last_heartbeat"
    " ON conductor_workers (last_heartbeat);",
]

RETRIES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_retries_task_id"
    " ON conductor_retries (task_id);",
    "CREATE INDEX IF NOT EXISTS idx_retries_scheduled_at"
    " ON conductor_retries (scheduled_at);",
]

DEAD_LETTER_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_dead_letter_discarded"
    " ON conductor_dead_letter (discarded);",
    "CREATE INDEX IF NOT EXISTS idx_dead_letter_moved_at"
    " ON conductor_dead_letter (moved_at);",
]

RECURRING_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_recurring_next_run"
    " ON conductor_recurring_tasks (next_run_at);",
    "CREATE INDEX IF NOT EXISTS idx_recurring_enabled"
    " ON conductor_recurring_tasks (enabled);",
]

# ---------------------------------------------------------------------------
# Rollback (v1 → v0)
# ---------------------------------------------------------------------------

ROLLBACK_SQL = [
    "DROP TABLE IF EXISTS conductor_recurring_tasks CASCADE;",
    "DROP TABLE IF EXISTS conductor_dead_letter CASCADE;",
    "DROP TABLE IF EXISTS conductor_retries CASCADE;",
    "DROP TABLE IF EXISTS conductor_workers CASCADE;",
    "DROP TABLE IF EXISTS conductor_tasks CASCADE;",
    "DELETE FROM conductor_version WHERE version = 1;",
]

# ---------------------------------------------------------------------------
# SchemaManager
# ---------------------------------------------------------------------------


class SchemaManager:
    """Manages database schema creation, migration, and version tracking.

    Typical usage::

        pool = DatabasePool(dsn=...)
        await pool.connect()
        mgr = SchemaManager(pool)
        await mgr.ensure_schema()   # auto-migrate on startup
    """

    def __init__(self, pool: DatabasePool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def ensure_schema(self) -> None:
        """Ensure the database schema is up-to-date.

        Creates the version table if needed, then runs any pending
        migrations.  Safe to call multiple times (idempotent).
        """
        await self._create_version_table()
        current_version = await self._get_current_version()

        if current_version < SCHEMA_VERSION:
            logger.info(
                "Migrating schema from v%s to v%s ...",
                current_version,
                SCHEMA_VERSION,
            )
            await self._migrate_v0_to_v1()
        else:
            logger.info("Schema is already at v%s.", SCHEMA_VERSION)

    async def get_current_version(self) -> int:
        """Return the current schema version stored in the database."""
        return await self._get_current_version()

    async def rollback(self, target_version: int = 0) -> None:
        """Rollback the schema to *target_version* (default 0 = no tables).

        .. warning::
           This **drops** tables and all their data.  Use with care.
        """
        current = await self._get_current_version()
        if current <= target_version:
            logger.info(
                "Nothing to rollback (current=%s <= target=%s).",
                current,
                target_version,
            )
            return

        logger.warning(
            "Rolling back schema from v%s to v%s ...",
            current,
            target_version,
        )

        for stmt in ROLLBACK_SQL:
            await self._pool.execute(stmt)

        logger.info("Schema rollback complete.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _create_version_table(self) -> None:
        await self._pool.execute(CREATE_VERSION_TABLE)

    async def _get_current_version(self) -> int:
        row = await self._pool.fetchrow(
            "SELECT COALESCE(MAX(version), 0) AS v FROM conductor_version"
        )
        return row["v"] if row else 0

    async def _migrate_v0_to_v1(self) -> None:
        """Run the full v0 → v1 migration (tables + indexes)."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Create tables
                await conn.execute(CREATE_TASKS_TABLE)
                await conn.execute(CREATE_WORKERS_TABLE)
                await conn.execute(CREATE_RETRIES_TABLE)
                await conn.execute(CREATE_DEAD_LETTER_TABLE)
                await conn.execute(CREATE_RECURRING_TASKS_TABLE)

                # Create indexes
                for idx_sql in TASK_INDEXES:
                    await conn.execute(idx_sql)
                for idx_sql in WORKER_INDEXES:
                    await conn.execute(idx_sql)
                for idx_sql in RETRIES_INDEXES:
                    await conn.execute(idx_sql)
                for idx_sql in DEAD_LETTER_INDEXES:
                    await conn.execute(idx_sql)
                for idx_sql in RECURRING_INDEXES:
                    await conn.execute(idx_sql)

                # Record version
                await conn.execute(
                    "INSERT INTO conductor_version (version) VALUES ($1)",
                    SCHEMA_VERSION,
                )

        logger.info("Migration v0 → v1 completed successfully.")
