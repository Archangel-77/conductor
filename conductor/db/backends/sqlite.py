"""
SQLite backend (``aiosqlite``).

SQLite is the embedded backend: no server, no separate process, a single
database file.

Concurrency contract
--------------------
**Exactly one worker process may target a given database file.**  SQLite has no
``SELECT … FOR UPDATE SKIP LOCKED``, so row-level claim semantics cannot be
reproduced across processes.  Within a single process this backend enforces
strict serialisation of database access (an :class:`asyncio.Lock` held for the
duration of every operation, re-entrant within the owning task) and opens write
transactions with ``BEGIN IMMEDIATE``, which gives the same exactly-once
guarantee Conductor provides on PostgreSQL — but only inside that contract.

The journal runs in WAL mode with a configurable ``busy_timeout`` so that a
concurrent reader never blocks a writer in the same process.

Threading note
--------------
``aiosqlite`` runs a dedicated worker thread per connection.  Conductor's
"asyncio-native, no threads" rule applies to *application-level* concurrency:
no blocking calls are made from the event loop, and no user code runs on that
thread.  There is no asyncio-native SQLite driver in the standard library, so
this is the accepted trade-off (see ``todo_p3.md`` decision 0.3 #1).
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from collections.abc import AsyncGenerator, AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, Optional, cast

import aiosqlite

from conductor.db.backends.base import ConnectionProtocol, SqlDialect
from conductor.exceptions import ConductorConnectionError, DatabaseError

logger = logging.getLogger("conductor.db.backends.sqlite")

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
"""SQLite stores UTC timestamps in this sortable, index-friendly format."""

MEMORY_PATH = ":memory:"
"""Sentinel path for an in-memory database."""


def parse_dsn(dsn: str) -> str:
    """Return the database path encoded in a SQLite DSN.

    Supported forms::

        sqlite:///relative/path.db     # relative to the working directory
        sqlite:////absolute/path.db    # absolute
        sqlite://:memory:              # in-memory (single connection)
        sqlite://                      # in-memory

    Args:
        dsn: The SQLite database URL.

    Returns:
        The filesystem path, or ``:memory:`` for an in-memory database.
    """
    _, _, rest = str(dsn).partition("://")
    rest = rest.split("?", 1)[0]

    if rest in ("", "/", "/:memory:", ":memory:"):
        return MEMORY_PATH
    if rest.startswith("//"):
        return rest[1:]
    return rest.lstrip("/") or MEMORY_PATH


def format_timestamp(value: datetime) -> str:
    """Format a datetime as a sortable UTC string."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


def parse_timestamp(value: Any) -> Any:
    """Parse a stored timestamp string back into an aware UTC datetime."""
    if not isinstance(value, str):
        return value
    for parser in (
        lambda v: datetime.strptime(v, TIMESTAMP_FORMAT),
        datetime.fromisoformat,
    ):
        try:
            parsed = parser(value)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return value


class SqliteDialect(SqlDialect):
    """SQLite SQL rendering (``?`` parameters, JSON1 arrays, no row locking)."""

    name = "sqlite"

    boolean_columns: frozenset[str] = frozenset({"discarded", "enabled"})
    """Columns stored as ``INTEGER`` 0/1 but exposed as ``bool``."""

    def placeholder(self, index: int) -> str:
        """Render an anonymous parameter (SQLite binds positionally)."""
        return "?"

    def json_param(self, index: int) -> str:
        """SQLite stores JSON as text, so no cast is required."""
        return "?"

    def now(self) -> str:
        """Render the current UTC timestamp in the shared storage format.

        SQLite's ``%f`` is ``SS.SSS`` (seconds with milliseconds), not Python's
        six-digit microseconds, so the trailing ``000`` pads it to match
        :data:`TIMESTAMP_FORMAT` exactly.
        """
        return "strftime('%Y-%m-%d %H:%M:%f000', 'now')"

    def interval_ago(self, column: str, index: int) -> str:
        """Render ``column >= <now minus n seconds>``.

        ``strftime`` is used instead of ``datetime('now')`` because the latter
        truncates to whole seconds, which would make sub-second liveness
        thresholds compare equal to a just-written heartbeat.
        """
        return f"{column} >= strftime('%Y-%m-%d %H:%M:%f000', 'now', '-' || ? || ' seconds')"

    def array_type(self) -> str:
        """Arrays are stored as JSON text."""
        return "TEXT"

    def array_contains(self, column: str, index: int) -> str:
        """Render array containment using the JSON1 extension."""
        return f"EXISTS (SELECT 1 FROM json_each({column}) WHERE json_each.value = ?)"

    def array_element_in(self, element: str, array_column: str) -> str:
        """Render ``element IN (SELECT value FROM json_each(array_column))``."""
        return f"{element} IN (SELECT value FROM json_each({array_column}))"

    def cardinality(self, column: str) -> str:
        """Render ``COALESCE(json_array_length(column), 0)``."""
        return f"COALESCE(json_array_length({column}), 0)"

    def for_update_skip_locked(self) -> str:
        """SQLite has no row-locking clause (single-process contract)."""
        return ""

    def ilike(self, column: str, index: int) -> str:
        """Render a case-insensitive comparison via ``LOWER()``."""
        return f"LOWER({column}) LIKE LOWER(?)"

    def encode_param(self, value: Any) -> Any:
        """Encode datetimes and arrays for SQLite storage."""
        if isinstance(value, datetime):
            return format_timestamp(value)
        if isinstance(value, (list, tuple)):
            return json.dumps(list(value), default=str, ensure_ascii=False)
        return value

    def normalize_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """Decode JSON text and timestamp columns into Python values."""
        result: dict[str, Any] = {}
        for key, value in row.items():
            if key in self.timestamp_columns:
                result[key] = parse_timestamp(value)
            elif key in self.boolean_columns:
                result[key] = None if value is None else bool(value)
            else:
                result[key] = self.decode_json_value(value)
        return result


class _SqliteGuard:
    """Serialises access to the single SQLite connection.

    The lock is re-entrant within the task that holds it: nested operations
    (for example an insert issued on a connection already acquired by the same
    task) must not deadlock.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._held: ContextVar[bool] = ContextVar("conductor_sqlite_lock", default=False)

    @asynccontextmanager
    async def __call__(self) -> AsyncIterator[None]:
        if self._held.get():
            yield
            return
        async with self._lock:
            token = self._held.set(True)
            try:
                yield
            finally:
                self._held.reset(token)


class SqliteConnection:
    """Exposes an ``aiosqlite`` connection through ``ConnectionProtocol``."""

    def __init__(self, conn: aiosqlite.Connection, dialect: SqliteDialect) -> None:
        self._conn = conn
        self._dialect = dialect

    @property
    def raw(self) -> aiosqlite.Connection:
        """The underlying aiosqlite connection."""
        return self._conn

    def _encode(self, args: tuple[Any, ...]) -> tuple[Any, ...]:
        """Encode bind parameters for SQLite."""
        return tuple(self._dialect.encode_param(arg) for arg in args)

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query and return all rows as normalised dicts."""
        cursor = await self._conn.execute(query, self._encode(args))
        try:
            rows = await cursor.fetchall()
        finally:
            await cursor.close()
        return [self._dialect.normalize_row(dict(row)) for row in rows]

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        cursor = await self._conn.execute(query, self._encode(args))
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        if row is None:
            return None
        return row[0]

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        cursor = await self._conn.execute(query, self._encode(args))
        try:
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        if row is None:
            return None
        return self._dialect.normalize_row(dict(row))

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> str:
        """Execute a statement and return an asyncpg-style command tag.

        Conductor normalises affected-row counts from command tags
        (``"UPDATE 1"``), so the SQLite backend synthesises the same shape.
        """
        cursor = await self._conn.execute(query, self._encode(args))
        try:
            rowcount = cursor.rowcount
        finally:
            await cursor.close()
        verb = query.strip().split(" ", 1)[0].upper() if query.strip() else ""
        return f"{verb} {rowcount}"

    def transaction(self) -> AbstractAsyncContextManager[None]:
        """Open a write transaction (``BEGIN IMMEDIATE``) on this connection."""
        return self._transaction()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[None]:
        await self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except BaseException:
            await self._conn.execute("ROLLBACK")
            raise
        await self._conn.execute("COMMIT")


class SqlitePool:
    """Single-connection SQLite pool with process-wide serialisation.

    The class mirrors :class:`~conductor.db.backends.postgres.PostgresPool` so
    the rest of Conductor (``QueryBuilder``, ``SchemaManager``, the CLI) can use
    either backend unchanged.

    Pool sizing options (``min_size``/``max_size``) are accepted for interface
    parity but ignored: SQLite uses exactly one connection per process.
    """

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 2,
        max_size: int = 10,
        timeout: float = 30.0,
        command_timeout: float = 60.0,
        busy_timeout: float = 5.0,
        max_retries: int = 3,
        retry_initial_delay: float = 0.5,
        retry_max_delay: float = 30.0,
    ) -> None:
        self._dsn = dsn
        self._path = parse_dsn(dsn)
        self._busy_timeout = busy_timeout
        self._max_retries = max_retries
        self._retry_initial_delay = retry_initial_delay
        self._retry_max_delay = retry_max_delay
        self._dialect = SqliteDialect()
        self._guard = _SqliteGuard()
        self._conn: Optional[aiosqlite.Connection] = None
        self._closed = False
        logger.debug(
            "SQLite ignores pool sizing and command timeout "
            "(min_size=%s, max_size=%s, timeout=%s, command_timeout=%s); "
            "exactly one connection is used per process.",
            min_size,
            max_size,
            timeout,
            command_timeout,
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def dialect(self) -> SqlDialect:
        """The SQLite SQL dialect."""
        return self._dialect

    @property
    def database_path(self) -> str:
        """The resolved database path (``:memory:`` for in-memory databases)."""
        return self._path

    @property
    def is_connected(self) -> bool:
        """``True`` if the connection is open."""
        return self._conn is not None and not self._closed

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the database file and apply the required pragmas.

        Raises:
            ConductorConnectionError: If the database cannot be opened.
        """
        last_exc: Optional[Exception] = None
        delay = self._retry_initial_delay

        for attempt in range(1, self._max_retries + 1):
            try:
                logger.info(
                    "Opening SQLite database '%s' (attempt %d/%d) ...",
                    self._path,
                    attempt,
                    self._max_retries,
                )
                conn = await aiosqlite.connect(
                    self._path,
                    isolation_level=None,
                    timeout=self._busy_timeout,
                )
                try:
                    conn.row_factory = sqlite3.Row
                    await conn.execute("PRAGMA journal_mode=WAL")
                    await conn.execute(f"PRAGMA busy_timeout={int(self._busy_timeout * 1000)}")
                    await conn.execute("PRAGMA foreign_keys=ON")
                except (OSError, sqlite3.Error):
                    await conn.close()
                    raise
                if self._path == MEMORY_PATH:
                    logger.warning(
                        "Using an in-memory SQLite database: all data is lost "
                        "when the process exits."
                    )
                self._conn = conn
                logger.info("SQLite database opened successfully.")
                return
            except (OSError, sqlite3.Error) as exc:
                last_exc = exc
                logger.warning(
                    "SQLite open attempt %d failed: %s. Retrying in %.2fs ...",
                    attempt,
                    exc,
                    delay,
                )
                if attempt < self._max_retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._retry_max_delay)

        raise ConductorConnectionError(
            f"Could not open SQLite database '{self._path}' after "
            f"{self._max_retries} attempts. Last error: {last_exc}"
        ) from last_exc

    async def disconnect(self) -> None:
        """Close the database connection."""
        if self._conn is not None and not self._closed:
            await self._conn.close()
            self._closed = True
            logger.info("SQLite database closed.")

    async def health_check(self) -> bool:
        """Run ``SELECT 1`` to verify the database responds."""
        if not self.is_connected:
            return False
        try:
            async with self.acquire() as conn:
                value = await conn.fetchval("SELECT 1 AS ok")
                return bool(value == 1)
        except (OSError, sqlite3.Error) as exc:
            logger.error("Health check failed: %s", exc, extra={"error": str(exc)})
            return False

    # ------------------------------------------------------------------
    # Connection acquisition
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[ConnectionProtocol, None]:
        """Yield the serialised connection (async context manager).

        Raises:
            DatabaseError: If the database is not open.
        """
        if self._conn is None:
            raise DatabaseError("Pool not initialised. Call connect() first.")
        if self._closed:
            raise DatabaseError("Pool has been closed.")

        async with self._guard():
            try:
                yield SqliteConnection(self._conn, self._dialect)
            except sqlite3.Error as exc:
                raise DatabaseError(f"SQLite error: {exc}") from exc

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[ConnectionProtocol, None]:
        """Yield a connection inside a ``BEGIN IMMEDIATE`` transaction."""
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
        """Execute a statement and return an asyncpg-style command tag."""
        async with self.acquire() as conn:
            return cast(str, await conn.execute(query, *args, **kwargs))
