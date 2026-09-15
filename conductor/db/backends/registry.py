"""
Backend detection and pool construction.

``create_pool`` is the single entry point used by
:class:`~conductor.db.connection.DatabasePool`: it inspects the DSN scheme and
returns the matching backend implementation.  Backends whose driver lives in
an optional extra are imported lazily so that a missing extra produces a clear
``ConductorConnectionError`` instead of an ``ImportError`` at package import.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlsplit

from conductor.db.backends.base import PoolProtocol
from conductor.exceptions import ConductorConnectionError

logger = logging.getLogger("conductor.db.backends.registry")

POSTGRES_SCHEMES = frozenset({"postgres", "postgresql"})
"""DSN schemes handled by the PostgreSQL backend."""

SQLITE_SCHEMES = frozenset({"sqlite", "sqlite3"})
"""DSN schemes handled by the SQLite backend."""

MYSQL_SCHEMES = frozenset({"mysql", "mariadb"})
"""DSN schemes handled by the MySQL/MariaDB backend."""

BACKEND_EXTRAS: dict[str, str] = {
    "sqlite": "conductor-task-queue[sqlite]",
    "mysql": "conductor-task-queue[mysql]",
}
"""Extra to install when a backend's driver is missing."""


def detect_backend(dsn: str) -> str:
    """Return the backend name for *dsn*.

    Args:
        dsn: A database URL such as ``postgresql://user:pass@host/db``,
            ``sqlite:///conductor.db`` or ``mysql://user:pass@host/db``.

    Returns:
        One of ``"postgresql"``, ``"sqlite"`` or ``"mysql"``.

    Raises:
        ConductorConnectionError: If the DSN is empty or its scheme is not
            supported.
    """
    if not dsn or not str(dsn).strip():
        raise ConductorConnectionError("database_url must not be empty")

    scheme = urlsplit(str(dsn)).scheme.lower()
    if not scheme:
        raise ConductorConnectionError(
            f"Could not determine the database backend from '{dsn}'. "
            "Use a DSN such as 'postgresql://user:pass@host/db', "
            "'sqlite:///conductor.db' or 'mysql://user:pass@host/db'."
        )
    if scheme in POSTGRES_SCHEMES:
        return "postgresql"
    if scheme in SQLITE_SCHEMES:
        return "sqlite"
    if scheme in MYSQL_SCHEMES:
        return "mysql"

    raise ConductorConnectionError(
        f"Unsupported database scheme '{scheme}'. Supported schemes: "
        "postgresql://, sqlite:/// and mysql://"
    )


def create_pool(dsn: str, **kwargs: Any) -> PoolProtocol:
    """Create the backend pool matching *dsn*.

    Args:
        dsn: The database URL whose scheme selects the backend.
        **kwargs: Backend options (``min_size``, ``max_size``, ``timeout``,
            ``command_timeout``, ``busy_timeout``, retry settings).

    Returns:
        A backend pool implementing ``PoolProtocol``.

    Raises:
        ConductorConnectionError: If the scheme is unsupported or the driver
            for the selected backend is not installed.
    """
    backend = detect_backend(dsn)

    if backend == "postgresql":
        from conductor.db.backends.postgres import PostgresPool

        return PostgresPool(dsn, **kwargs)

    if backend == "sqlite":
        try:
            from conductor.db.backends.sqlite import SqlitePool
        except ImportError as exc:  # pragma: no cover - depends on the env
            raise _missing_driver_error("sqlite", exc) from exc

        pool: PoolProtocol = SqlitePool(dsn, **kwargs)
        return pool

    # MySQL/MariaDB (Track A2) is the only remaining supported scheme.
    try:
        from conductor.db.backends.mysql import MySqlPool
    except ImportError as exc:
        raise _missing_driver_error("mysql", exc) from exc

    mysql_pool: PoolProtocol = MySqlPool(dsn, **kwargs)
    return mysql_pool


def _missing_driver_error(backend: str, exc: ImportError) -> ConductorConnectionError:
    """Build a helpful error for a backend whose optional driver is missing."""
    extra = BACKEND_EXTRAS[backend]
    return ConductorConnectionError(
        f"The {backend} backend requires an optional dependency. "
        f"Install it with: pip install {extra}  (original error: {exc})"
    )
