"""
Database connection management.

Provides :class:`DatabasePool` – a backend-agnostic connection pool facade –
and :class:`PoolConfig`.

The backend is selected from the DSN scheme:

=========================  ==================================================
DSN                        Backend
=========================  ==================================================
``postgresql://…``         PostgreSQL (asyncpg; always installed)
``sqlite:///path.db``      SQLite (``conductor-task-queue[sqlite]``)
``mysql://…``              MySQL/MariaDB (``conductor-task-queue[mysql]``)
=========================  ==================================================

Every call site keeps working against any backend: the pool exposes the same
asyncpg-shaped surface (``fetch``/``fetchval``/``fetchrow``/``execute``/
``acquire``/``transaction``), and SQL is rendered by the backend's dialect.
"""

from __future__ import annotations

import logging
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Any, Optional

from conductor.db.backends.base import ConnectionProtocol, PoolProtocol, SqlDialect
from conductor.db.backends.registry import create_pool, detect_backend

logger = logging.getLogger("conductor.db.connection")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class PoolConfig:
    """Configuration for the database connection pool."""

    dsn: str
    """Database URL; its scheme selects the backend."""

    min_size: int = 2
    """Minimum number of connections to keep in the pool (ignored by SQLite)."""

    max_size: int = 10
    """Maximum number of connections allowed in the pool (ignored by SQLite)."""

    timeout: float = 30.0
    """Maximum time (seconds) to wait for a connection from the pool."""

    command_timeout: float = 60.0
    """Default timeout (seconds) for SQL commands (ignored by SQLite)."""

    max_retries: int = 3
    """Number of times to retry creating the pool on failure."""

    retry_initial_delay: float = 0.5
    """Initial delay (seconds) before the first connection retry."""

    retry_max_delay: float = 30.0
    """Maximum delay (seconds) between connection retries."""

    busy_timeout: float = 5.0
    """SQLite: seconds to wait for a locked database before failing."""

    @property
    def backend(self) -> str:
        """The backend name derived from the DSN scheme.

        Raises:
            ConductorConnectionError: If the DSN scheme is unsupported.
        """
        return detect_backend(self.dsn)

    def validate(self) -> None:
        """Raise ``ValueError`` if any configuration value is invalid."""
        if self.min_size < 0:
            raise ValueError("min_size must be >= 0")
        if self.max_size < 1:
            raise ValueError("max_size must be >= 1")
        if self.max_size < self.min_size:
            raise ValueError("max_size must be >= min_size")
        if self.timeout <= 0:
            raise ValueError("timeout must be > 0")
        if self.command_timeout <= 0:
            raise ValueError("command_timeout must be > 0")
        if self.busy_timeout <= 0:
            raise ValueError("busy_timeout must be > 0")


# ---------------------------------------------------------------------------
# DatabasePool
# ---------------------------------------------------------------------------


class DatabasePool:
    """Backend-agnostic connection pool.

    Typical usage::

        pool = DatabasePool(
            dsn="postgresql://user:pass@localhost:5432/conductor"
        )
        await pool.connect()
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
        finally:
            await pool.disconnect()

    Can also be used as an async context manager::

        async with DatabasePool(dsn=...) as pool:
            async with pool.acquire() as conn:
                ...

    The underlying backend is chosen from the DSN scheme – see the module
    docstring.  ``sqlite:///conductor.db`` yields an embedded, single-process
    pool; ``postgresql://…`` yields an asyncpg pool.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 2,
        max_size: int = 10,
        timeout: float = 30.0,
        command_timeout: float = 60.0,
        max_retries: int = 3,
        retry_initial_delay: float = 0.5,
        retry_max_delay: float = 30.0,
        busy_timeout: float = 5.0,
    ) -> None:
        self._config = PoolConfig(
            dsn=dsn,
            min_size=min_size,
            max_size=max_size,
            timeout=timeout,
            command_timeout=command_timeout,
            max_retries=max_retries,
            retry_initial_delay=retry_initial_delay,
            retry_max_delay=retry_max_delay,
            busy_timeout=busy_timeout,
        )
        self._config.validate()
        # The backend is created lazily: constructing a pool must not fail on a
        # bad DSN or a missing optional driver — ``connect()`` reports those.
        self._backend: Optional[PoolProtocol] = None

    def _get_backend(self) -> PoolProtocol:
        """Create (and cache) the backend pool selected by the DSN scheme.

        Raises:
            ConductorConnectionError: If the DSN scheme is unsupported or the
                backend's optional driver is not installed.
        """
        if self._backend is None:
            self._backend = create_pool(
                self._config.dsn,
                min_size=self._config.min_size,
                max_size=self._config.max_size,
                timeout=self._config.timeout,
                command_timeout=self._config.command_timeout,
                busy_timeout=self._config.busy_timeout,
                max_retries=self._config.max_retries,
                retry_initial_delay=self._config.retry_initial_delay,
                retry_max_delay=self._config.retry_max_delay,
            )
        return self._backend

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def config(self) -> PoolConfig:
        """The configuration this pool was built with."""
        return self._config

    @property
    def dialect(self) -> SqlDialect:
        """The SQL dialect of the selected backend."""
        return self._get_backend().dialect

    @property
    def backend_name(self) -> str:
        """Name of the selected backend (``postgresql``, ``sqlite``, …)."""
        return self._get_backend().dialect.name

    @property
    def is_connected(self) -> bool:
        """``True`` if the pool has been created and not yet closed."""
        return self._backend is not None and self._backend.is_connected

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the connection pool with retry-and-backoff.

        Raises:
            ConductorConnectionError: If all retry attempts are exhausted, the
                DSN scheme is unsupported, or the backend's driver is missing.
        """
        await self._get_backend().connect()

    async def disconnect(self) -> None:
        """Close the connection pool and release all resources."""
        if self._backend is not None:
            await self._backend.disconnect()

    async def health_check(self) -> bool:
        """Run a simple query to verify database connectivity.

        Returns:
            ``True`` if the database responds, ``False`` otherwise.
        """
        return await self._get_backend().health_check()

    # ------------------------------------------------------------------
    # Connection acquisition
    # ------------------------------------------------------------------

    def acquire(self) -> AbstractAsyncContextManager[ConnectionProtocol]:
        """Acquire a connection from the pool (async context manager).

        Raises:
            DatabaseError: If the pool is not available or acquisition times out.
        """
        return self._get_backend().acquire()

    def transaction(self) -> AbstractAsyncContextManager[ConnectionProtocol]:
        """Acquire a connection and open a transaction on it.

        Prefer this over ``acquire()`` + ``conn.transaction()``: SQLite opens
        write transactions with ``BEGIN IMMEDIATE``, which only the backend can
        do correctly.
        """
        return self._get_backend().transaction()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        return await self._get_backend().fetchval(query, *args, **kwargs)

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query and return all rows."""
        return await self._get_backend().fetch(query, *args, **kwargs)

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        return await self._get_backend().fetchrow(query, *args, **kwargs)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the backend's command result.

        PostgreSQL/SQLite return a command tag string; MySQL returns an integer
        row count.  ``SqlDialect.normalize_rowcount()`` handles both.
        """
        return await self._get_backend().execute(query, *args, **kwargs)

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> DatabasePool:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.disconnect()


__all__: list[str] = ["DatabasePool", "PoolConfig"]
