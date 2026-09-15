"""
PostgreSQL backend (asyncpg).

``PostgresPool`` is the reference :class:`~conductor.db.backends.base.PoolProtocol`
implementation: an asyncpg connection pool with health checks, configurable
timeouts, and exponential-backoff retry logic.  ``PostgresDialect`` renders the
PostgreSQL SQL fragments used by ``QueryBuilder``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Optional, cast

import asyncpg

from conductor.db.backends.base import ConnectionProtocol, SqlDialect
from conductor.exceptions import ConductorConnectionError, DatabaseError

logger = logging.getLogger("conductor.db.backends.postgres")


class PostgresDialect(SqlDialect):
    """PostgreSQL SQL rendering (positional ``$n`` parameters, ``jsonb``, arrays)."""

    name = "postgresql"

    def placeholder(self, index: int) -> str:
        """Render the *index*-th positional parameter (``$1``, ``$2``, …)."""
        return f"${index}"

    def json_param(self, index: int) -> str:
        """Render a placeholder cast to ``jsonb``."""
        return f"${index}::jsonb"

    def now(self) -> str:
        """Render ``NOW()``."""
        return "NOW()"

    def interval_ago(self, column: str, index: int) -> str:
        """Render ``column >= NOW() - MAKE_INTERVAL(secs => $n)``."""
        return f"{column} >= NOW() - MAKE_INTERVAL(secs => ${index})"

    def array_type(self) -> str:
        """Arrays are native PostgreSQL ``TEXT[]`` columns."""
        return "TEXT[]"

    def array_contains(self, column: str, index: int) -> str:
        """Render native array containment (``@> ARRAY[$n]``)."""
        return f"{column} @> ARRAY[${index}]"

    def array_element_in(self, element: str, array_column: str) -> str:
        """Render ``element = ANY(array_column)``."""
        return f"{element} = ANY({array_column})"

    def cardinality(self, column: str) -> str:
        """Render ``COALESCE(cardinality(column), 0)``."""
        return f"COALESCE(cardinality({column}), 0)"

    def for_update_skip_locked(self) -> str:
        """PostgreSQL claims rows with ``FOR UPDATE SKIP LOCKED``."""
        return "FOR UPDATE SKIP LOCKED"

    def ilike(self, column: str, index: int) -> str:
        """Render a native case-insensitive ``ILIKE``."""
        return f"{column} ILIKE ${index}"

    def encode_param(self, value: Any) -> Any:
        """asyncpg binds Python values natively (lists, datetimes, …)."""
        return value

    def normalize_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """Return the row as a plain dict with JSON text decoded.

        asyncpg decodes most types natively but can hand back ``JSONB`` columns
        as strings; decoding them keeps callers independent of the driver.
        """
        return {key: self.decode_json_value(value) for key, value in row.items()}


class PostgresConnection:
    """Exposes an asyncpg connection through ``ConnectionProtocol``."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    @property
    def raw(self) -> asyncpg.Connection:
        """The underlying asyncpg connection."""
        return self._conn

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query and return all rows."""
        return cast("list[Any]", await self._conn.fetch(query, *args, **kwargs))

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        return await self._conn.fetchval(query, *args, **kwargs)

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        return await self._conn.fetchrow(query, *args, **kwargs)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        """Execute a statement and return the asyncpg command tag."""
        return cast(str, await self._conn.execute(query, *args, **kwargs))

    def transaction(self) -> AbstractAsyncContextManager[None]:
        """Start an asyncpg transaction on this connection."""
        return cast(
            "AbstractAsyncContextManager[None]",
            self._conn.transaction(),
        )


class PostgresPool:
    """asyncpg connection pool with health checks and retry logic.

    Typical usage::

        pool = PostgresPool(dsn="postgresql://user:pass@localhost:5432/db")
        await pool.connect()
        try:
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT 1")
        finally:
            await pool.disconnect()
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
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._timeout = timeout
        self._command_timeout = command_timeout
        self._max_retries = max_retries
        self._retry_initial_delay = retry_initial_delay
        self._retry_max_delay = retry_max_delay
        self._busy_timeout = busy_timeout
        self._dialect = PostgresDialect()
        self._pool: Optional[asyncpg.Pool] = None
        self._closed = False

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def dialect(self) -> SqlDialect:
        """The PostgreSQL SQL dialect."""
        return self._dialect

    @property
    def is_connected(self) -> bool:
        """``True`` if the pool has been created and not yet closed."""
        return self._pool is not None and not self._closed

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the connection pool with retry-and-backoff.

        Raises:
            ConductorConnectionError: If every retry attempt fails.
        """
        last_exc: Optional[Exception] = None
        delay = self._retry_initial_delay

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(
                    "Connecting to PostgreSQL (attempt %d/%d) ...",
                    attempt,
                    self._max_retries,
                )
                self._pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    min_size=self._min_size,
                    max_size=self._max_size,
                    timeout=self._timeout,
                    command_timeout=self._command_timeout,
                )
                logger.info("Database pool created successfully.")
                return
            except (OSError, asyncpg.PostgresError) as exc:
                last_exc = exc
                logger.warning(
                    "Connection attempt %d failed: %s. Retrying in %.2fs ...",
                    attempt,
                    exc,
                    delay,
                )
                if attempt < self._max_retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._retry_max_delay)

        raise ConductorConnectionError(
            f"Could not connect to PostgreSQL after "
            f"{self._max_retries} attempts. Last error: {last_exc}"
        ) from last_exc

    async def disconnect(self) -> None:
        """Close the connection pool and release all resources."""
        if self._pool is not None and not self._closed:
            await self._pool.close()
            self._closed = True
            logger.info("Database pool closed.")

    async def health_check(self) -> bool:
        """Run ``SELECT 1`` to verify database connectivity."""
        if not self.is_connected:
            return False
        try:
            async with self.acquire() as conn:
                val = await conn.fetchval("SELECT 1 AS ok")
                return cast(bool, val == 1)
        except (OSError, asyncpg.PostgresError) as exc:
            logger.error("Health check failed: %s", exc, extra={"error": str(exc)})
            return False

    # ------------------------------------------------------------------
    # Connection acquisition
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[ConnectionProtocol, None]:
        """Acquire a connection from the pool (async context manager).

        Raises:
            DatabaseError: If the pool is unavailable or acquisition times out.
        """
        if self._pool is None:
            raise DatabaseError("Pool not initialised. Call connect() first.")
        if self._closed:
            raise DatabaseError("Pool has been closed.")

        try:
            async with self._pool.acquire(timeout=self._timeout) as conn:
                # asyncpg yields a ``PoolConnectionProxy``; cast it to the
                # public ``Connection`` type the adapter is annotated with.
                yield PostgresConnection(cast(asyncpg.Connection, conn))
        except asyncpg.PostgresError as exc:
            raise DatabaseError(f"Failed to acquire connection: {exc}") from exc
        except asyncio.TimeoutError as exc:
            raise DatabaseError(f"Timed out waiting for connection ({self._timeout}s)") from exc

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[ConnectionProtocol, None]:
        """Acquire a connection and yield it inside a transaction."""
        async with self.acquire() as conn:
            async with conn.transaction():
                yield conn

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query on a pooled connection and return all rows."""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args, **kwargs)

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args, **kwargs)

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args, **kwargs)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        """Execute a statement and return the command status tag."""
        async with self.acquire() as conn:
            return cast(str, await conn.execute(query, *args, **kwargs))
