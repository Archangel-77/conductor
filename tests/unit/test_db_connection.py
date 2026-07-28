"""
Unit tests for DatabasePool (connection management).

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from conductor.db.connection import DatabasePool, PoolConfig
from conductor.exceptions import ConductorConnectionError


pytestmark = pytest.mark.integration


# ===================================================================
# PoolConfig validation
# ===================================================================

class TestPoolConfig:

    def test_defaults(self) -> None:
        cfg = PoolConfig(dsn="postgresql://u:p@localhost/db")
        cfg.validate()
        assert cfg.min_size == 2
        assert cfg.max_size == 10

    def test_invalid_negative_min_size(self) -> None:
        with pytest.raises(ValueError, match="min_size"):
            PoolConfig(dsn="pg://", min_size=-1).validate()

    def test_invalid_zero_max_size(self) -> None:
        with pytest.raises(ValueError, match="max_size"):
            PoolConfig(dsn="pg://", max_size=0).validate()

    def test_invalid_max_less_than_min(self) -> None:
        with pytest.raises(ValueError, match="max_size"):
            PoolConfig(dsn="pg://", min_size=5, max_size=3).validate()

    def test_invalid_zero_timeout(self) -> None:
        with pytest.raises(ValueError, match="timeout"):
            PoolConfig(dsn="pg://", timeout=0).validate()


# ===================================================================
# DatabasePool – connection lifecycle
# ===================================================================

class TestDatabasePoolConnect:

    async def test_connect_and_disconnect(self, db_pool: Any) -> None:
        """Verify a healthy pool can connect and disconnect."""
        assert db_pool.is_connected
        assert await db_pool.health_check() is True

    async def test_health_check_on_disconnected_pool(self) -> None:
        """A pool that hasn't been connected should report unhealthy."""
        pool = DatabasePool(dsn="postgresql://u:p@localhost/db")
        assert pool.is_connected is False
        assert await pool.health_check() is False

    async def test_double_disconnect_safe(self, db_pool: Any) -> None:
        """Disconnecting twice should not raise."""
        await db_pool.disconnect()
        await db_pool.disconnect()  # second call is a no-op
        assert db_pool.is_connected is False

    async def test_acquire_after_disconnect_raises(self, db_pool: Any) -> None:
        """Acquiring a connection after disconnect should raise."""
        await db_pool.disconnect()
        with pytest.raises(Exception):
            async with db_pool.acquire():
                pass  # pragma: no cover

    async def test_connect_fails_with_bad_uri(self) -> None:
        """Connecting with an invalid URI should raise error."""
        pool = DatabasePool(
            dsn="postgresql://invalid:invalid@localhost:9999/nonexistent",
            timeout=1.0,
            max_retries=1,
        )
        with pytest.raises(ConductorConnectionError):
            await pool.connect()


# ===================================================================
# DatabasePool – query helpers
# ===================================================================

class TestDatabasePoolQueries:

    async def test_fetchval(self, db_pool: Any) -> None:
        val = await db_pool.fetchval("SELECT 42 AS answer")
        assert val == 42

    async def test_fetchrow(self, db_pool: Any) -> None:
        row = await db_pool.fetchrow("SELECT 1 AS a, 2 AS b")
        assert row is not None
        assert row["a"] == 1
        assert row["b"] == 2

    async def test_execute(self, db_pool: Any) -> None:
        tag = await db_pool.execute("SELECT 1")
        assert isinstance(tag, str)

    async def test_acquire_context_manager(self, db_pool: Any) -> None:
        async with db_pool.acquire() as conn:
            val = await conn.fetchval("SELECT 1")
            assert val == 1


# ===================================================================
# Concurrent connections
# ===================================================================

class TestConcurrentConnections:

    async def test_ten_concurrent_queries(self) -> None:
        """The pool should handle 10+ concurrent connections."""
        from conductor.exceptions import ConductorConnectionError
        from tests.conftest import TEST_DATABASE_URL

        pool = DatabasePool(
            dsn=TEST_DATABASE_URL,
            min_size=2,
            max_size=12,
            timeout=5.0,
            max_retries=1,
        )
        try:
            await pool.connect()
        except ConductorConnectionError as exc:
            pytest.skip(f"Could not connect: {exc}")

        try:
            results = await asyncio.gather(
                *[pool.fetchval("SELECT 1 AS x") for _ in range(10)],
            )
            assert all(r == 1 for r in results)
            assert len(results) == 10
        finally:
            await pool.disconnect()
