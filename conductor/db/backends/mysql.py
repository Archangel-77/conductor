"""
MySQL/MariaDB backend (``asyncmy``).

``MySqlPool`` is an asyncmy connection pool; ``MySqlDialect`` renders the MySQL
SQL fragments used by ``QueryBuilder``.

Differences from PostgreSQL that shape this implementation:

* **Positional ``%s`` placeholders** (not ``$n``).
* **No ``RETURNING``** — inserts and updates report an affected-row count
  instead, so ``QueryBuilder`` falls back to ``rowcount`` for conflict detection
  and does a select-then-update where a returning clause would be used.
* **JSON arrays** instead of native arrays: ``depends_on`` is a ``JSON`` column
  queried with ``JSON_CONTAINS``/``JSON_LENGTH``.
* **``ON DUPLICATE KEY UPDATE``** instead of ``ON CONFLICT``.
* **``DATETIME(6)`` in UTC** instead of ``TIMESTAMPTZ``: timestamps are stored
  and compared as ``YYYY-MM-DD HH:MM:SS.ffffff`` strings/datetimes in UTC, and
  read back as timezone-aware datetimes.

Requires **MySQL 8.0+** (``CHECK`` constraints 8.0.16+, ``DROP CHECK`` 8.0.19+,
``FOR UPDATE SKIP LOCKED`` on InnoDB 8.0+).  MariaDB 10.6+ works for the
features used here (``SKIP LOCKED`` arrived in 10.6; 10.5 is rejected at
``connect()`` with a clear error).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import AsyncGenerator, AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Optional, cast
from urllib.parse import parse_qsl, unquote, urlsplit

import asyncmy
from asyncmy.cursors import DictCursor

from conductor.db.backends.base import ConnectionProtocol, SqlDialect
from conductor.exceptions import ConductorConnectionError, DatabaseError

logger = logging.getLogger("conductor.db.backends.mysql")

DEFAULT_PORT = 3306
"""Default MySQL server port."""

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
"""MySQL stores UTC timestamps as ``DATETIME(6)`` in this shape."""

MIN_MYSQL_VERSION: tuple[int, ...] = (8, 0, 16)
"""Minimum MySQL server version (named ``CHECK`` constraints 8.0.16+, ``SKIP LOCKED`` 8.0+)."""

MIN_MARIADB_VERSION: tuple[int, ...] = (10, 6, 0)
"""Minimum MariaDB server version (``FOR UPDATE SKIP LOCKED`` arrived in 10.6)."""

_INT_QUERY_PARAMS = frozenset({"connect_timeout"})

_VERSION_RE = re.compile(r"^(?P<major>\d+)(?:\.(?P<minor>\d+))?(?:\.(?P<patch>\d+))?")


def parse_server_version(version: str) -> tuple[str, tuple[int, ...]]:
    """Split a server version string into ``(flavour, version)``.

    The flavour is ``"mariadb"`` when the string names MariaDB, otherwise
    ``"mysql"``.  MariaDB reports a ``5.5.5-`` compatibility prefix for clients
    that predate its version scheme, so it is stripped first.

    Args:
        version: A value returned by ``SELECT VERSION()``, e.g. ``"8.0.46"``,
            ``"8.4.6-log"`` or ``"11.8.9-MariaDB-ubu2404"``.

    Returns:
        The flavour and the numeric version components (empty when the string
        could not be parsed).
    """
    text = str(version).strip()
    flavour = "mariadb" if "mariadb" in text.lower() else "mysql"
    if text.startswith("5.5.5-"):
        text = text[len("5.5.5-") :]

    match = _VERSION_RE.match(text)
    if match is None:
        return flavour, ()
    return flavour, tuple(int(part) for part in match.groups() if part is not None)


def validate_server_version(version: str) -> None:
    """Reject servers that predate the SQL features Conductor relies on.

    Pending rows are claimed with ``FOR UPDATE SKIP LOCKED`` and the schema uses
    named ``CHECK`` constraints; on an older server the polling query fails with
    an opaque driver syntax error, so the rejection happens at ``connect()``.

    Args:
        version: The server version string from ``SELECT VERSION()``.

    Raises:
        ConductorConnectionError: If the server is too old for its flavour.
    """
    flavour, parts = parse_server_version(version)
    minimum = MIN_MARIADB_VERSION if flavour == "mariadb" else MIN_MYSQL_VERSION
    if not parts:
        logger.warning(
            "Could not parse the server version %r; assuming it supports the "
            "features Conductor requires.",
            version,
        )
        return

    # Pad so that "8" and "8.0" compare meaningfully against "8.0.16".
    comparable = parts[:3] + (0,) * (3 - len(parts[:3]))
    if comparable < minimum:
        raise ConductorConnectionError(
            f"Unsupported server version '{version}'. Conductor requires MySQL "
            f"{'.'.join(str(part) for part in MIN_MYSQL_VERSION)}+ or MariaDB "
            f"{'.'.join(str(part) for part in MIN_MARIADB_VERSION)}+ "
            "(FOR UPDATE SKIP LOCKED, named CHECK constraints)."
        )


def parse_dsn(dsn: str) -> dict[str, Any]:
    """Turn a MySQL DSN into ``asyncmy`` connection keyword arguments.

    Supported form::

        mysql://user:pass@host:3306/database?charset=utf8mb4

    Unknown query-string parameters are passed through to the driver (which
    validates them), so driver-specific options remain available.

    Args:
        dsn: The MySQL/MariaDB database URL.

    Returns:
        Keyword arguments for :func:`asyncmy.create_pool`.

    Raises:
        ConductorConnectionError: If the DSN is malformed.
    """
    parts = urlsplit(str(dsn))
    if parts.scheme not in ("mysql", "mariadb"):
        raise ConductorConnectionError(
            f"Not a MySQL DSN: '{dsn}'. Use 'mysql://user:pass@host:3306/database'."
        )

    kwargs: dict[str, Any] = {
        "host": parts.hostname or "localhost",
        "port": parts.port or DEFAULT_PORT,
        "user": unquote(parts.username) if parts.username else None,
        "password": unquote(parts.password) if parts.password else "",
        "db": parts.path.lstrip("/") or None,
        # Rows are dictionaries, matching every other backend's row shape.
        "cursor_cls": DictCursor,
        # Transactions are opened explicitly (``Connection.begin()``), exactly
        # like the SQLite backend, so statements outside a transaction commit
        # immediately.
        "autocommit": True,
    }

    params = dict(parse_qsl(parts.query))
    kwargs["charset"] = params.pop("charset", "utf8mb4")
    for key in _INT_QUERY_PARAMS:
        if key in params:
            kwargs[key] = int(params.pop(key))
    kwargs.update(params)
    return kwargs


def format_timestamp(value: datetime) -> str:
    """Format a datetime as a MySQL UTC timestamp string."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


def parse_timestamp(value: Any) -> Any:
    """Normalise a stored timestamp into an aware UTC datetime."""
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
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


class MySqlDialect(SqlDialect):
    """MySQL/MariaDB SQL rendering (``%s`` parameters, JSON arrays, no RETURNING)."""

    name = "mysql"

    supports_returning = False
    """MySQL has no ``RETURNING`` clause."""

    boolean_columns: frozenset[str] = frozenset({"discarded", "enabled"})
    """Columns stored as ``TINYINT(1)`` but exposed as ``bool``."""

    def placeholder(self, index: int) -> str:
        """Render an anonymous parameter (``%s``)."""
        return "%s"

    def json_param(self, index: int) -> str:
        """MySQL validates JSON columns on insert, so no cast is required."""
        return "%s"

    def now(self) -> str:
        """Render ``NOW(6)`` (microsecond precision, matching ``DATETIME(6)``)."""
        return "NOW(6)"

    def interval_ago(self, column: str, index: int) -> str:
        """Render ``column >= DATE_SUB(NOW(6), INTERVAL n SECOND)``."""
        return f"{column} >= DATE_SUB(NOW(6), INTERVAL %s SECOND)"

    def array_type(self) -> str:
        """Arrays are stored as JSON documents."""
        return "JSON"

    def array_contains(self, column: str, index: int) -> str:
        """Render array containment with ``JSON_CONTAINS``.

        ``JSON_QUOTE`` turns the bound string into a JSON string value, which is
        what ``JSON_CONTAINS`` expects as its candidate document.
        """
        return f"JSON_CONTAINS({column}, JSON_QUOTE(%s))"

    def array_element_in(self, element: str, array_column: str) -> str:
        """Render "the array contains *element*" (the reverse containment)."""
        return f"JSON_CONTAINS({array_column}, JSON_QUOTE({element}))"

    def cardinality(self, column: str) -> str:
        """Render ``COALESCE(JSON_LENGTH(column), 0)``."""
        return f"COALESCE(JSON_LENGTH({column}), 0)"

    def for_update_skip_locked(self) -> str:
        """InnoDB supports ``FOR UPDATE SKIP LOCKED`` (MySQL 8.0+)."""
        return "FOR UPDATE SKIP LOCKED"

    def ilike(self, column: str, index: int) -> str:
        """Render a case-insensitive comparison via ``LOWER()``."""
        return f"LOWER({column}) LIKE LOWER(%s)"

    def nulls_last(self, expression: str) -> str:
        """Render ``col IS NULL, col DESC`` (MySQL has no ``NULLS LAST``)."""
        column = expression.split()[0]
        suffix = expression[len(column) :]
        return f"{column} IS NULL, {column}{suffix}"

    def upsert(
        self,
        conflict_columns: Sequence[str],
        assignments: Sequence[tuple[str, str]],
    ) -> str:
        """Render ``ON DUPLICATE KEY UPDATE``.

        ``EXCLUDED.x`` references become ``VALUES(x)``.

        ``VALUES()`` is deprecated in MySQL 8.0.20+ (warning 1287 at runtime, which
        the driver logs) in favour of a row alias — ``INSERT … VALUES (…) AS new
        ON DUPLICATE KEY UPDATE col = new.col``.  The alias form is kept out
        deliberately: **verified live, MariaDB 10.5 and 11.8 reject it with a
        syntax error** (``near 'AS new ON DUPLICATE KEY UPDATE'``), so the
        deprecated-but-portable form is the only one that works on every
        supported server.  Do not "modernise" it without dropping MariaDB.
        """
        sets: list[str] = []
        for column, expression in assignments:
            if expression.startswith("EXCLUDED."):
                sets.append(f"{column} = VALUES({expression[len('EXCLUDED.') :]})")
            else:
                sets.append(f"{column} = {expression}")
        return "ON DUPLICATE KEY UPDATE " + ", ".join(sets)

    def insert_ignore(self, conflict_columns: Sequence[str]) -> str:
        """Render a no-op upsert (MySQL has no ``ON CONFLICT DO NOTHING``).

        Updating a column to itself leaves the existing row unchanged, which
        also makes ``rowcount == 0`` a reliable "row already existed" signal.
        """
        first = conflict_columns[0]
        return f"ON DUPLICATE KEY UPDATE {first} = {first}"

    def drop_constraint(self, table: str, name: str) -> str:
        """Render ``ALTER TABLE … DROP CHECK`` (MySQL 8.0.19+)."""
        return f"ALTER TABLE {table} DROP CHECK {name};"

    def encode_param(self, value: Any) -> Any:
        """Encode datetimes, arrays and booleans for MySQL."""
        if isinstance(value, datetime):
            return format_timestamp(value)
        if isinstance(value, (list, tuple)):
            return json.dumps(list(value), default=str, ensure_ascii=False)
        if isinstance(value, bool):
            return int(value)
        return value

    def normalize_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """Decode timestamps, booleans and JSON text into Python values."""
        result: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, (bytes, bytearray)):
                value = value.decode("utf-8", errors="replace")
            if key in self.timestamp_columns:
                result[key] = parse_timestamp(value)
            elif key in self.boolean_columns:
                result[key] = None if value is None else bool(value)
            else:
                result[key] = self.decode_json_value(value)
        return result

    def is_retryable_transaction_error(self, exc: BaseException) -> bool:
        """Report InnoDB deadlocks and lock-wait timeouts as retryable.

        A task claim locks the rows it scans (``FOR UPDATE``), so two workers
        claiming at the same moment can deadlock (error 1213) or hit the lock
        wait timeout (1205).  InnoDB has already rolled the transaction back, so
        the claim can simply run again.
        """
        if not isinstance(exc, asyncmy.errors.OperationalError):
            return False
        code = exc.args[0] if exc.args else None
        return code in (1205, 1213)


class MySqlConnection:
    """Exposes an asyncmy connection through ``ConnectionProtocol``."""

    def __init__(self, conn: asyncmy.Connection, dialect: MySqlDialect) -> None:
        self._conn = conn
        self._dialect = dialect

    @property
    def raw(self) -> asyncmy.Connection:
        """The underlying asyncmy connection."""
        return self._conn

    def _encode(self, args: tuple[Any, ...]) -> tuple[Any, ...]:
        """Encode bind parameters for MySQL."""
        return tuple(self._dialect.encode_param(arg) for arg in args)

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query and return all rows as normalised dicts."""
        cursor = await self._conn.cursor()
        try:
            await cursor.execute(query, self._encode(args) or None)
            rows = await cursor.fetchall()
        finally:
            await cursor.close()
        return [self._dialect.normalize_row(row) for row in rows]

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        cursor = await self._conn.cursor()
        try:
            await cursor.execute(query, self._encode(args) or None)
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        if not row:
            return None
        return next(iter(row.values()))

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        cursor = await self._conn.cursor()
        try:
            await cursor.execute(query, self._encode(args) or None)
            row = await cursor.fetchone()
        finally:
            await cursor.close()
        if not row:
            return None
        return self._dialect.normalize_row(row)

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> int:
        """Execute a statement and return the affected-row count."""
        cursor = await self._conn.cursor()
        try:
            await cursor.execute(query, self._encode(args) or None)
            raw_rowcount = cursor.rowcount
        finally:
            await cursor.close()
        # asyncmy types ``rowcount`` loosely; DML always reports an int while
        # DDL/SELECT report ``None`` or ``-1`` (no rows involved).
        return raw_rowcount if isinstance(raw_rowcount, int) else 0

    def transaction(self) -> AbstractAsyncContextManager[None]:
        """Open a transaction on this connection."""
        return self._transaction()

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[None]:
        """Run the block inside ``BEGIN`` … ``COMMIT``/``ROLLBACK``.

        asyncmy exposes ``begin()``/``commit()``/``rollback()`` as coroutines
        rather than a transaction context manager, so the commit/rollback
        handling is explicit here.
        """
        await self._conn.begin()
        try:
            yield
        except BaseException:
            await self._conn.rollback()
            raise
        await self._conn.commit()


class MySqlPool:
    """asyncmy connection pool with health checks and retry logic.

    Pool sizing options map to asyncmy's ``minsize``/``maxsize``; ``timeout``
    bounds how long ``acquire()`` waits for a free connection and
    ``command_timeout`` becomes the driver's ``read_timeout``.
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
        self._min_size = min_size
        self._max_size = max_size
        self._timeout = timeout
        self._command_timeout = command_timeout
        self._max_retries = max_retries
        self._retry_initial_delay = retry_initial_delay
        self._retry_max_delay = retry_max_delay
        self._dialect = MySqlDialect()
        self._pool: Optional[asyncmy.Pool] = None
        self._closed = False
        logger.debug(
            "MySQL ignores busy_timeout (SQLite only); got %s. "
            "Use the driver's connect_timeout/read_timeout instead.",
            busy_timeout,
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def dialect(self) -> SqlDialect:
        """The MySQL SQL dialect."""
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
                    "Connecting to MySQL/MariaDB (attempt %d/%d) ...",
                    attempt,
                    self._max_retries,
                )
                self._pool = await asyncmy.create_pool(
                    minsize=self._min_size,
                    maxsize=self._max_size,
                    connect_timeout=self._timeout,
                    read_timeout=self._command_timeout,
                    **parse_dsn(self._dsn),
                )
                # A server that is too old cannot run the polling query at all
                # (``FOR UPDATE SKIP LOCKED``), so report that here — with the
                # detected version — instead of failing later with a driver
                # syntax error.
                try:
                    await self._check_server_version()
                except ConductorConnectionError:
                    await self._release_pool()
                    raise
                logger.info("MySQL connection pool created successfully.")
                return
            except (OSError, asyncmy.errors.Error, ValueError) as exc:
                last_exc = exc
                logger.warning(
                    "MySQL connection attempt %d failed: %s. Retrying in %.2fs ...",
                    attempt,
                    exc,
                    delay,
                )
                if attempt < self._max_retries:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, self._retry_max_delay)

        raise ConductorConnectionError(
            f"Could not connect to MySQL/MariaDB after "
            f"{self._max_retries} attempts. Last error: {last_exc}"
        ) from last_exc

    async def disconnect(self) -> None:
        """Close the pool and release all resources."""
        if self._pool is not None and not self._closed:
            self._pool.close()
            await self._pool.wait_closed()
            self._closed = True
            logger.info("MySQL connection pool closed.")

    async def _release_pool(self) -> None:
        """Close the driver pool without marking this object as closed.

        Used on a failed ``connect()`` (an unsupported server version) so the
        pool can be reused or garbage-collected; ``disconnect()`` would instead
        latch ``_closed`` and refuse a later ``connect()``.
        """
        pool = self._pool
        self._pool = None
        if pool is not None:
            pool.close()
            await pool.wait_closed()

    async def _check_server_version(self) -> None:
        """Verify the server is new enough for Conductor's polling query.

        Raises:
            ConductorConnectionError: If the server is too old.
        """
        version = await self.fetchval("SELECT VERSION() AS version")
        if not isinstance(version, str):
            return
        logger.info("Connected to server version %s.", version)
        validate_server_version(version)

    async def health_check(self) -> bool:
        """Run ``SELECT 1`` to verify the server responds."""
        if not self.is_connected:
            return False
        try:
            async with self.acquire() as conn:
                value = await conn.fetchval("SELECT 1 AS ok")
                return bool(value == 1)
        except (OSError, asyncmy.errors.Error) as exc:
            logger.error("Health check failed: %s", exc, extra={"error": str(exc)})
            return False

    # ------------------------------------------------------------------
    # Connection acquisition
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def acquire(self) -> AsyncGenerator[ConnectionProtocol, None]:
        """Acquire a pooled connection (async context manager).

        Raises:
            DatabaseError: If the pool is unavailable or acquisition times out.
        """
        if self._pool is None:
            raise DatabaseError("Pool not initialised. Call connect() first.")
        if self._closed:
            raise DatabaseError("Pool has been closed.")

        try:
            raw = await self._acquire_connection()
        except asyncio.TimeoutError as exc:
            raise DatabaseError(
                f"Timed out waiting for a MySQL connection ({self._timeout}s)"
            ) from exc
        except asyncmy.errors.Error as exc:
            raise DatabaseError(f"Failed to acquire connection: {exc}") from exc

        try:
            yield MySqlConnection(raw, self._dialect)
        finally:
            if self._pool is not None and not self._closed:
                self._pool.release(raw)

    async def _acquire_connection(self) -> asyncmy.Connection:
        """Acquire a raw connection, re-checking the pool until the deadline.

        ``asyncmy``'s pool can lose a wakeup when a waiter is cancelled: the
        released connection goes back to the free list, but the waiter that was
        notified about it may already have been cancelled by ``wait_for`` (seen
        on CPython 3.11, where the notification is consumed and no other waiter
        is woken).  A *fresh* acquire checks the free list before parking, so
        the acquisition is retried in short slices until the timeout budget is
        spent.

        Returns:
            A connection from the pool.

        Raises:
            asyncio.TimeoutError: If no connection becomes available in time.
        """
        pool = self._pool
        assert pool is not None

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout
        attempts = 0
        while True:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise asyncio.TimeoutError
            # Retry immediately when the pool reports a free connection (the
            # lost-wakeup case); otherwise poll the pool in one-second slices.
            slice_timeout = min(remaining, 0.05 if pool.freesize > 0 else 1.0)
            try:
                return await asyncio.wait_for(pool.acquire(), timeout=slice_timeout)
            except asyncio.TimeoutError:
                attempts += 1
                if attempts == 1 or attempts % 10 == 0:
                    logger.debug(
                        "Retrying MySQL pool acquire (attempt %d, %d free, %.2fs left).",
                        attempts,
                        pool.freesize,
                        remaining,
                    )

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

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> int:
        """Execute a statement and return the affected-row count."""
        async with self.acquire() as conn:
            return cast(int, await conn.execute(query, *args, **kwargs))
