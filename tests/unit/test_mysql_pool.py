"""Tests for the MySQL/MariaDB connection pool wrapper.

``asyncmy``'s pool can lose a wakeup when a waiter is cancelled: the released
connection goes back to the free list, but the notification is consumed by the
waiter that ``asyncio.wait_for`` just cancelled (reproduced on CPython 3.11, a
supported version), leaving every other waiter parked.  ``MySqlPool.acquire``
therefore re-checks the pool until its timeout budget is spent.  These tests use
a fake driver pool — no server required.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from conductor.db.backends.mysql import MySqlConnection, MySqlPool
from conductor.exceptions import DatabaseError

pytestmark = pytest.mark.unit

DSN = "mysql://user:pass@localhost:3306/conductor_test"


class _FakeRawConnection:
    """Stand-in for an ``asyncmy.Connection``."""

    def __init__(self) -> None:
        self.connected = True


class _FakeAsyncmyPool:
    """A pool double whose ``acquire`` hangs while it reports free capacity.

    Args:
        freesize: Value reported by the ``freesize`` property.
        hang_times: Number of leading ``acquire`` calls that hang forever
            (simulating a notification that was lost).
    """

    def __init__(self, *, freesize: int = 0, hang_times: int = 0) -> None:
        self.freesize = freesize
        self.size = max(freesize, 1)
        self.hang_times = hang_times
        self.acquire_calls = 0
        self.released: list[Any] = []
        self.closed = False

    async def acquire(self) -> _FakeRawConnection:
        self.acquire_calls += 1
        if self.acquire_calls <= self.hang_times:
            await asyncio.sleep(3600)  # replaced by wait_for's cancellation
        if self.freesize > 0:
            self.freesize -= 1
        return _FakeRawConnection()

    def release(self, conn: Any) -> None:
        self.released.append(conn)
        self.freesize += 1

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


def _pool_with(fake: _FakeAsyncmyPool, *, timeout: float = 1.0) -> MySqlPool:
    """Build a ``MySqlPool`` wired to *fake* without connecting."""
    pool = MySqlPool(DSN, min_size=1, max_size=2, timeout=timeout)
    pool._pool = fake  # type: ignore[assignment]  # pylint: disable=protected-access
    return pool


class TestAcquireRetry:

    async def test_retries_when_a_free_connection_is_reported(self) -> None:
        fake = _FakeAsyncmyPool(freesize=1, hang_times=1)
        pool = _pool_with(fake)

        async with pool.acquire() as connection:
            assert isinstance(connection, MySqlConnection)

        # The first attempt was cancelled by the wait_for timeout; the retry saw
        # the free connection the lost wakeup had hidden.
        assert fake.acquire_calls >= 2
        assert len(fake.released) == 1

    async def test_reports_a_timeout_when_the_pool_is_exhausted(self) -> None:
        fake = _FakeAsyncmyPool(freesize=0, hang_times=99)
        pool = _pool_with(fake, timeout=0.2)

        with pytest.raises(
            DatabaseError, match=r"Timed out waiting for a MySQL connection \(0\.2s\)"
        ):
            async with pool.acquire():
                pytest.fail("no connection should have been acquired")

        assert fake.released == []

    async def test_requires_a_connected_pool(self) -> None:
        pool = MySqlPool(DSN)
        with pytest.raises(DatabaseError, match="Pool not initialised"):
            async with pool.acquire():
                pytest.fail("no connection should have been acquired")

    async def test_rejects_a_closed_pool(self) -> None:
        fake = _FakeAsyncmyPool(freesize=1)
        pool = _pool_with(fake)
        pool._closed = True  # type: ignore[assignment]  # pylint: disable=protected-access

        with pytest.raises(DatabaseError, match="Pool has been closed"):
            async with pool.acquire():
                pytest.fail("no connection should have been acquired")
