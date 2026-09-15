"""
Per-backend DDL and migration statements.

Each backend module exposes the same names (``CREATE_VERSION_TABLE``,
``CREATE_*_TABLE``, ``*_INDEXES``, ``MIGRATIONS``, ``ROLLBACK_SQL``) so
``SchemaManager`` can drive any backend generically.

Migration keying
----------------
``MIGRATIONS[n]`` holds the statements that take a database from ``n - 1`` to
``n``.  Backends that shipped after a schema version exist (SQLite ships at the
current version) declare the full latest shape in ``CREATE_*``/``*_INDEXES``
and leave the historic steps empty: applying a no-op step still records the
version row, so every backend ends up at the same logical schema.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SCHEMA_VERSION = 6
"""The current schema version expected by this code."""


@dataclass(frozen=True)
class SchemaDDL:
    """The DDL statements and migration plan for one backend."""

    backend: str
    """Backend name (``postgresql``, ``sqlite``, …)."""

    version_table: str = ""
    """Statement creating the ``conductor_version`` bookkeeping table."""

    create_statements: list[str] = field(default_factory=list)
    """Table-creation statements in dependency order (the v0 → v1 step)."""

    index_statements: list[str] = field(default_factory=list)
    """Index-creation statements (the v0 → v1 step)."""

    migrations: dict[int, list[str]] = field(default_factory=dict)
    """Incremental migration statements keyed by target schema version."""

    rollback_statements: list[str] = field(default_factory=list)
    """Statements that drop every table (``DROP TABLE``, newest first)."""

    def statements_for(self, target_version: int) -> list[str]:
        """Return the statements required to reach *target_version*.

        Args:
            target_version: The schema version to migrate to.

        Returns:
            The migration statements (empty for backend-specific no-op steps).
        """
        return list(self.migrations.get(target_version, []))


def get_ddl(backend: str) -> SchemaDDL:
    """Return the DDL plan for *backend*.

    Args:
        backend: A backend name (``postgresql``, ``sqlite``, ``mysql``).

    Returns:
        The matching :class:`SchemaDDL`.

    Raises:
        KeyError: If the backend has no DDL module.
    """
    if backend == "postgresql":
        from conductor.db.ddl import postgres

        return postgres.DDL
    if backend == "sqlite":
        from conductor.db.ddl import sqlite

        return sqlite.DDL
    if backend == "mysql":
        from conductor.db.ddl import mysql

        return mysql.DDL

    raise KeyError(f"No DDL defined for backend '{backend}'")


__all__: list[str] = ["SCHEMA_VERSION", "SchemaDDL", "get_ddl"]
