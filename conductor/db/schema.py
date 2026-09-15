"""
Schema management and auto-migration for Conductor.

Handles creation and versioning of all database tables, indexes,
constraints, and checks.  Migrations are idempotent – running them
multiple times is safe.

The statements come from the backend's DDL plan (``conductor/db/ddl/``), so
this module contains no SQL of its own and the same migration ledger is
maintained on every backend.
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from conductor.db.backends.base import SqlDialect
from conductor.db.backends.postgres import PostgresDialect
from conductor.db.connection import DatabasePool
from conductor.db.ddl import SCHEMA_VERSION, SchemaDDL, get_ddl
from conductor.exceptions import ConductorException

# Re-exported for backwards compatibility: the PostgreSQL statements used to
# live in this module and are imported by tests and by the DDL plan itself.
from conductor.db.ddl.postgres import (  # noqa: F401
    CREATE_DEAD_LETTER_TABLE,
    CREATE_RECURRING_TASKS_TABLE,
    CREATE_RETRIES_TABLE,
    CREATE_TASKS_TABLE,
    CREATE_VERSION_TABLE,
    CREATE_WORKERS_TABLE,
    DEAD_LETTER_INDEXES,
    MIGRATE_V1_TO_V2_SQL,
    MIGRATE_V2_TO_V3_SQL,
    MIGRATE_V3_TO_V4_SQL,
    MIGRATE_V4_TO_V5_SQL,
    RECURRING_INDEXES,
    RETRIES_INDEXES,
    ROLLBACK_SQL,
    TASK_INDEXES,
    WORKER_INDEXES,
)

logger = logging.getLogger("conductor.db.schema")

__all__: list[str] = [
    "CREATE_DEAD_LETTER_TABLE",
    "CREATE_RECURRING_TASKS_TABLE",
    "CREATE_RETRIES_TABLE",
    "CREATE_TASKS_TABLE",
    "CREATE_VERSION_TABLE",
    "CREATE_WORKERS_TABLE",
    "MIGRATE_V1_TO_V2_SQL",
    "MIGRATE_V2_TO_V3_SQL",
    "MIGRATE_V3_TO_V4_SQL",
    "MIGRATE_V4_TO_V5_SQL",
    "SCHEMA_VERSION",
    "SchemaManager",
]


def _pool_dialect(pool: Any) -> SqlDialect:
    """Return the dialect of *pool*, defaulting to PostgreSQL.

    Duck-typed pools (test doubles without a ``dialect`` attribute) fall back to
    the PostgreSQL dialect.
    """
    dialect = getattr(pool, "dialect", None)
    if isinstance(dialect, SqlDialect):
        return dialect
    return PostgresDialect()


class SchemaManager:
    """Manages database schema creation, migration, and version tracking.

    Typical usage::

        pool = DatabasePool(dsn=...)
        await pool.connect()
        mgr = SchemaManager(pool)
        await mgr.ensure_schema()   # auto-migrate on startup

    Args:
        pool: The connection pool whose backend DDL plan is applied.
        ddl: Optional explicit DDL plan (defaults to the pool's backend).
    """

    def __init__(self, pool: DatabasePool, ddl: Optional[SchemaDDL] = None) -> None:
        self._pool: Any = pool
        self._dialect: SqlDialect = _pool_dialect(pool)
        self._ddl: SchemaDDL = ddl or get_ddl(self._dialect.name)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @property
    def backend(self) -> str:
        """The backend this manager maintains (``postgresql``, ``sqlite``, …)."""
        return self._ddl.backend

    async def ensure_schema(self) -> None:
        """Ensure the database schema is up-to-date.

        Creates the version table if needed, then runs any pending
        migrations step by step (v0→v1, v1→v2, …).  Safe to call multiple
        times (idempotent).
        """
        await self._create_version_table()
        current_version = await self._get_current_version()

        if current_version < SCHEMA_VERSION:
            logger.info(
                "Migrating schema from v%s to v%s ...",
                current_version,
                SCHEMA_VERSION,
            )
            for target_version in range(current_version + 1, SCHEMA_VERSION + 1):
                await self._run_migration(target_version)
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

        for stmt in self._ddl.rollback_statements:
            await self._pool.execute(stmt)

        logger.info("Schema rollback complete.")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _create_version_table(self) -> None:
        await self._pool.execute(self._ddl.version_table)

    async def _get_current_version(self) -> int:
        row = await self._pool.fetchrow(
            "SELECT COALESCE(MAX(version), 0) AS v FROM conductor_version"
        )
        if not row:
            return 0
        value = row["v"]
        return int(value) if value is not None else 0

    async def _record_version(self, conn: Any, version: int) -> None:
        """Record an applied migration step (idempotent)."""
        dialect = self._dialect
        conflict = dialect.insert_ignore(["version"])
        await conn.execute(
            "INSERT INTO conductor_version (version) VALUES ("
            f"{dialect.placeholder(1)}) {conflict}",
            version,
        )

    async def _migrate_v0_to_v1(self) -> None:
        """Run the full v0 → v1 migration (tables + indexes)."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for statement in self._ddl.create_statements:
                    await conn.execute(statement)
                for statement in self._ddl.index_statements:
                    await conn.execute(statement)
                await self._record_version(conn, 1)

        logger.info("Migration v0 → v1 completed successfully.")

    async def _run_migration(self, target_version: int) -> None:
        """Run the single migration step that lands on *target_version*.

        Args:
            target_version: Schema version to migrate to.  Step 1 creates the
                full base schema; later steps apply the backend's incremental
                statements (which may legitimately be empty for a backend that
                shipped with a later shape).

        Raises:
            ConductorException: If no migration is defined for the target.
        """
        if target_version == 1:
            await self._migrate_v0_to_v1()
            return

        if target_version not in self._ddl.migrations:
            raise ConductorException(
                f"No migration defined for schema v{target_version} "
                f"(backend '{self._ddl.backend}')."
            )

        statements = self._ddl.statements_for(target_version)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for statement in statements:
                    await conn.execute(statement)
                await self._record_version(conn, target_version)

        logger.info(
            "Migration v%s → v%s completed successfully (%d statement(s)).",
            target_version - 1,
            target_version,
            len(statements),
        )
