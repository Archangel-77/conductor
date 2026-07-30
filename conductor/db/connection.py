"""
PostgreSQL connection pool management.

Provides ``DatabasePool`` – an asyncpg-based connection pool with health
checks, configurable timeouts, and exponential-backoff retry logic.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Optional, cast

import asyncpg

from conductor.exceptions import ConductorConnectionError, DatabaseError

logger = logging.getLogger("conductor.db.connection")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass
class PoolConfig:
    """Configuration for the database connection pool."""

    dsn: str
    """PostgreSQL connection URI."""

    min_size: int = 2
    """Minimum number of connections to keep in the pool."""

    max_size: int = 10
    """Maximum number of connections allowed in the pool."""

    timeout: float = 30.0
    """Maximum time (seconds) to wait for a connection from the pool."""

    command_timeout: float = 60.0
    """Default timeout (seconds) for SQL commands."""

    max_retries: int = 3
    """Number of times to retry creating the pool on failure."""

    retry_initial_delay: float = 0.5
    """Initial delay (seconds) before the first connection retry."""

    retry_max_delay: float = 30.0
    """Maximum delay (seconds) between connection retries."""

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


# ---------------------------------------------------------------------------
# DatabasePool
# ---------------------------------------------------------------------------


class DatabasePool:
    """Asyncpg connection pool with health checks and retry logic.

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
        )
        self._config.validate()
        self._pool: Optional[asyncpg.Pool] = None
        self._closed = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Create the connection pool with retry-and-backoff.

        Raises ``ConductorConnectionError``
        if all retry attempts are exhausted.
        """
        last_exc: Optional[Exception] = None
        delay = self._config.retry_initial_delay

        for attempt in range(1, self._config.max_retries + 1):
            try:
                logger.info(
                    "Connecting to PostgreSQL (attempt %d/%d) ...",
                    attempt,
                    self._config.max_retries,
                )
                self._pool = await asyncpg.create_pool(
                    dsn=self._config.dsn,
                    min_size=self._config.min_size,
                    max_size=self._config.max_size,
                    timeout=self._config.timeout,
                    command_timeout=self._config.command_timeout,
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
                if attempt < self._config.max_retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._config.retry_max_delay)

        raise ConductorConnectionError(
            f"Could not connect to PostgreSQL after "
            f"{self._config.max_retries} attempts. Last error: {last_exc}"
        ) from last_exc

    async def disconnect(self) -> None:
        """Close the connection pool and release all resources."""
        if self._pool is not None and not self._closed:
            await self._pool.close()
            self._closed = True
            logger.info("Database pool closed.")

    @property
    def is_connected(self) -> bool:
        """``True`` if the pool has been created and not yet closed."""
        return self._pool is not None and not self._closed

    # ------------------------------------------------------------------
    async def health_check(self) -> bool:
        """Run a simple query to verify database connectivity.

        Returns ``True`` if the database responds, ``False`` otherwise.
        """
        if not self.is_connected:
            return False
        try:
            async with self.acquire() as conn:
                val = await conn.fetchval("SELECT 1 AS ok")
                return cast(bool, val == 1)
        except (OSError, asyncpg.PostgresError) as exc:
            logger.error(
                "Health check failed: %s",
                exc,
                extra={"error": str(exc)},
            )
            return False

    # ------------------------------------------------------------------
    # Connection acquisition
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Acquire a connection from the pool (async context manager).

        Raises ``DatabaseError`` if the pool is not available or the
        acquisition times out.
        """
        if self._pool is None:
            raise DatabaseError("Pool not initialised. Call connect() first.")
        if self._closed:
            raise DatabaseError("Pool has been closed.")

        try:
            async with self._pool.acquire(timeout=self._config.timeout) as conn:
                yield conn
        except asyncpg.PostgresError as exc:
            raise DatabaseError(f"Failed to acquire connection: {exc}") from exc
        except asyncio.TimeoutError as exc:
            raise DatabaseError(
                f"Timed out waiting for connection " f"({self._config.timeout}s)"
            ) from exc

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        async with self.acquire() as conn:
            return cast(Any, await conn.fetchval(query, *args, **kwargs))

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[asyncpg.Record]:
        """Execute a query and return all rows as a list of ``Record``."""
        async with self.acquire() as conn:
            result = await conn.fetch(query, *args, **kwargs)
            return cast("list[asyncpg.Record]", result)

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[asyncpg.Record]:
        """Execute a query and return the first row (or ``None``)."""
        async with self.acquire() as conn:
            result = await conn.fetchrow(query, *args, **kwargs)
            return cast("Optional[asyncpg.Record]", result)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        """Execute a query and return the command status tag."""
        async with self.acquire() as conn:
            return cast(str, await conn.execute(query, *args, **kwargs))

    # ------------------------------------------------------------------
    # Async context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> DatabasePool:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: Any) -> None:
        await self.disconnect()
