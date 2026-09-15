"""
Database backends.

Each backend provides a connection pool (``PoolProtocol``) and an SQL dialect
(``SqlDialect``) so that the rest of Conductor – ``QueryBuilder``, the schema
manager and every caller – stays backend-agnostic.

* ``postgres.py`` – asyncpg (default; requires the core install)
* ``sqlite.py`` – ``aiosqlite`` (optional extra ``sqlite``)
* ``mysql.py`` – ``asyncmy`` (optional extra ``mysql``)

The driver-backed backends are **not** re-exported here: their drivers are
optional, so ``registry.create_pool()`` imports them lazily and reports a
missing extra as ``ConductorConnectionError``.
"""

from conductor.db.backends.base import (
    ConnectionProtocol,
    PoolProtocol,
    SqlDialect,
)
from conductor.db.backends.postgres import (
    PostgresConnection,
    PostgresDialect,
    PostgresPool,
)
from conductor.db.backends.registry import (
    BACKEND_EXTRAS,
    create_pool,
    detect_backend,
)

__all__: list[str] = [
    "BACKEND_EXTRAS",
    "ConnectionProtocol",
    "PoolProtocol",
    "PostgresConnection",
    "PostgresDialect",
    "PostgresPool",
    "SqlDialect",
    "create_pool",
    "detect_backend",
]
