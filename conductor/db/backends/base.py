"""
Backend protocols and SQL dialect rendering.

Conductor reaches the database through two abstractions:

* :class:`PoolProtocol` – a connection pool with an asyncpg-like surface
  (``fetch``/``fetchval``/``fetchrow``/``execute``/``acquire``/``transaction``).
* :class:`SqlDialect` – renders every backend-specific SQL fragment
  (placeholders, JSON casts, array operations, upserts, locking clauses) and
  normalises values on the way in and out.

PostgreSQL is the reference implementation (``backends/postgres.py``); the
embedded backend lives in ``backends/sqlite.py``.  ``QueryBuilder`` holds a
single :class:`SqlDialect` and never branches on the backend itself.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from typing import Any, Optional, Protocol, runtime_checkable


@runtime_checkable
class ConnectionProtocol(Protocol):
    """The connection surface used by ``QueryBuilder`` and the migrations."""

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query and return all rows."""
        ...

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        ...

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        ...

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a statement and return a backend-specific result.

        PostgreSQL and SQLite return a command tag (``"UPDATE 1"``) while MySQL
        returns an integer row count; callers must run the result through
        ``SqlDialect.normalize_rowcount`` instead of parsing it themselves.
        """
        ...

    def transaction(self) -> AbstractAsyncContextManager[None]:
        """Start a transaction on this connection."""
        ...


@runtime_checkable
class PoolProtocol(Protocol):
    """The connection-pool surface shared by every backend."""

    async def connect(self) -> None:
        """Create the pool (with retry-and-backoff)."""
        ...

    async def disconnect(self) -> None:
        """Close the pool and release all resources."""
        ...

    @property
    def is_connected(self) -> bool:
        """``True`` once connected and before ``disconnect()``."""
        ...

    @property
    def dialect(self) -> "SqlDialect":
        """The SQL dialect this backend renders."""
        ...

    async def health_check(self) -> bool:
        """Run a trivial query to verify connectivity."""
        ...

    def acquire(self) -> AbstractAsyncContextManager[ConnectionProtocol]:
        """Acquire a connection."""
        ...

    def transaction(self) -> AbstractAsyncContextManager[ConnectionProtocol]:
        """Acquire a connection and open a transaction on it."""
        ...

    async def fetch(self, query: str, *args: Any, **kwargs: Any) -> list[Any]:
        """Execute a query on a pooled connection and return all rows."""
        ...

    async def fetchval(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a query and return the first column of the first row."""
        ...

    async def fetchrow(self, query: str, *args: Any, **kwargs: Any) -> Optional[Any]:
        """Execute a query and return the first row (or ``None``)."""
        ...

    async def execute(self, query: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a statement and return a backend-specific result."""
        ...


class SqlDialect(ABC):
    """Renders backend-specific SQL and normalises bound/returned values.

    Concrete dialects declare which columns need value translation via
    :attr:`array_columns`, :attr:`json_columns` and :attr:`timestamp_columns`.
    The default implementations of :meth:`upsert`, :meth:`nulls_last` and
    :meth:`normalize_rowcount` are shared by PostgreSQL and SQLite; only
    MySQL overrides them (``ON DUPLICATE KEY UPDATE`` and integer rowcounts).
    """

    name: str = "base"
    """Human-readable backend name (``postgresql``, ``sqlite``, …)."""

    supports_returning: bool = True
    """Whether ``INSERT … RETURNING`` is supported."""

    array_columns: frozenset[str] = frozenset({"depends_on"})
    """Columns holding an array of strings."""

    json_columns: frozenset[str] = frozenset({"payload", "result", "retry_policy"})
    """Columns holding a JSON document."""

    timestamp_columns: frozenset[str] = frozenset(
        {
            "applied_at",
            "completed_at",
            "created_at",
            "discarded_at",
            "last_heartbeat",
            "last_run_at",
            "moved_at",
            "next_run_at",
            "scheduled_at",
            "scheduled_for",
            "started_at",
        }
    )
    """Columns holding a UTC timestamp."""

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    @abstractmethod
    def placeholder(self, index: int) -> str:
        """Render the *index*-th positional parameter (1-based)."""

    def placeholder_list(self, start: int, count: int) -> str:
        """Render a comma-separated list of *count* placeholders."""
        return ", ".join(self.placeholder(i) for i in range(start, start + count))

    @abstractmethod
    def json_param(self, index: int) -> str:
        """Render a placeholder for a JSON-valued parameter."""

    @abstractmethod
    def now(self) -> str:
        """Render the current UTC timestamp expression."""

    @abstractmethod
    def interval_ago(self, column: str, index: int) -> str:
        """Render ``column >= now - interval`` for a seconds parameter."""

    @abstractmethod
    def array_type(self) -> str:
        """The DDL type used for array columns."""

    @abstractmethod
    def array_contains(self, column: str, index: int) -> str:
        """Render "array *column* contains the element in parameter *index*"."""

    @abstractmethod
    def array_element_in(self, element: str, array_column: str) -> str:
        """Render "the value in *element* is one of *array_column*"."""

    @abstractmethod
    def cardinality(self, column: str) -> str:
        """Render the number of elements in an array column."""

    @abstractmethod
    def for_update_skip_locked(self) -> str:
        """Render the row-locking clause (may be empty)."""

    @abstractmethod
    def ilike(self, column: str, index: int) -> str:
        """Render a case-insensitive ``LIKE`` comparison."""

    @abstractmethod
    def encode_param(self, value: Any) -> Any:
        """Encode a Python value for binding (timestamps, arrays, …)."""

    @abstractmethod
    def normalize_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """Normalise a returned row into dialect-independent Python values."""

    def upsert(
        self,
        conflict_columns: Sequence[str],
        assignments: Sequence[tuple[str, str]],
    ) -> str:
        """Render an upsert clause.

        Args:
            conflict_columns: Columns forming the conflict target.
            assignments: ``(column, expression)`` pairs, where the expression
                is written in terms of ``EXCLUDED`` (e.g.
                ``("priority", "EXCLUDED.priority")`` or
                ``("discarded", "FALSE")``).

        Returns:
            The ``ON CONFLICT …`` clause (without a trailing semicolon).
        """
        targets = ", ".join(conflict_columns)
        sets = ", ".join(f"{column} = {expr}" for column, expr in assignments)
        return f"ON CONFLICT ({targets}) DO UPDATE SET {sets}"

    def insert_ignore(self, conflict_columns: Sequence[str]) -> str:
        """Render an ``INSERT … ON CONFLICT DO NOTHING`` clause.

        Args:
            conflict_columns: Columns forming the conflict target.

        Returns:
            The conflict clause (without a trailing semicolon).
        """
        targets = ", ".join(conflict_columns)
        return f"ON CONFLICT ({targets}) DO NOTHING"

    def nulls_last(self, expression: str) -> str:
        """Render an ascending order expression that sorts ``NULL`` last."""
        return f"{expression} NULLS LAST"

    def drop_constraint(self, table: str, name: str) -> str:
        """Render ``DROP CONSTRAINT`` for a named table constraint."""
        return f"ALTER TABLE {table} DROP CONSTRAINT {name};"

    def add_check(self, table: str, name: str, expression: str) -> str:
        """Render ``ADD CONSTRAINT … CHECK`` for a named table constraint."""
        return f"ALTER TABLE {table} ADD CONSTRAINT {name} CHECK ({expression});"

    def decode_json_value(self, value: Any) -> Any:
        """Parse a JSON-looking string into Python objects.

        asyncpg returns ``JSON``/``JSONB`` columns as text on newer Pythons and
        both PostgreSQL's ``JSONB`` and SQLite's ``TEXT`` JSON columns are read
        back as strings, so every dialect decodes them the same way.

        Args:
            value: A column value that may hold a JSON document or array.

        Returns:
            The parsed value, or the original value when it is not JSON.
        """
        if isinstance(value, str) and value[:1] in ("{", "["):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        return value

    def normalize_rowcount(self, result: Any, *, verb: str = "") -> int:
        """Extract the affected-row count from a command tag or rowcount.

        PostgreSQL and SQLite both report command tags such as ``UPDATE 1``
        or ``DELETE 3``; MySQL drivers report a plain integer.
        """
        if isinstance(result, int):
            return result
        if not result:
            return 0
        parts = str(result).split()
        for token in reversed(parts):
            if token.isdigit():
                return int(token)
        return 0

    def is_retryable_transaction_error(self, exc: BaseException) -> bool:
        """Whether *exc* aborts a transaction that is safe to retry.

        Backends with row-level locking can abort a transaction to break a
        deadlock; the transaction is already rolled back, so the caller may
        simply run it again.  PostgreSQL and SQLite never need this (PostgreSQL
        serialises claims in a single statement), so the default is ``False``.

        Args:
            exc: The exception raised by the failed statement.

        Returns:
            ``True`` if the operation should be retried.
        """
        return False
