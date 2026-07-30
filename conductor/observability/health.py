"""
Health checks for Conductor.

Provides ``HealthChecker`` that reports database connectivity, pending
task counts, DLQ size, and active worker counts.

Typical usage::

    from conductor.db.connection import DatabasePool
    from conductor.observability.health import HealthChecker

    pool = DatabasePool(dsn="postgresql://...")
    await pool.connect()
    checker = HealthChecker(pool)
    result = await checker.check()
    print(result.to_dict())
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder

logger = logging.getLogger("conductor.observability.health")


# ---------------------------------------------------------------------------
# Health status
# ---------------------------------------------------------------------------


class HealthStatus(str, Enum):
    """Possible health check status values."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"

    def __str__(self) -> str:
        return self.value


# ---------------------------------------------------------------------------
# Health result model
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class HealthResult:
    """Result of a health check invocation.

    Attributes:
        status: Overall system health.
        database: ``"connected"`` or ``"disconnected"``.
        pending_tasks: Number of tasks with status ``pending``.
        dead_letter_queue: Number of non-discarded DLQ tasks.
        workers_active: Number of workers with recent heartbeats.
        uptime_seconds: Seconds since the checker was created.
        last_check: Timestamp of when the check was performed.
    """

    status: HealthStatus
    database: str
    pending_tasks: int
    dead_letter_queue: int
    workers_active: int
    uptime_seconds: float
    last_check: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc),
    )

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-compatible dictionary."""
        return {
            "status": self.status.value,
            "database": self.database,
            "pending_tasks": self.pending_tasks,
            "dead_letter_queue": self.dead_letter_queue,
            "workers_active": self.workers_active,
            "uptime_seconds": self.uptime_seconds,
            "last_check": self.last_check.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> HealthResult:
        """Deserialize from a dictionary produced by ``to_dict()``."""
        return cls(
            status=HealthStatus(data["status"]),
            database=data["database"],
            pending_tasks=data["pending_tasks"],
            dead_letter_queue=data["dead_letter_queue"],
            workers_active=data["workers_active"],
            uptime_seconds=data["uptime_seconds"],
            last_check=datetime.fromisoformat(data["last_check"]),
        )


# ---------------------------------------------------------------------------
# Health checker
# ---------------------------------------------------------------------------


class HealthChecker:
    """Performs health checks against the database and task state.

    Args:
        pool: The database pool to check connectivity against.
        dlq_size_threshold: Number of DLQ tasks above which the system
            is considered ``DEGRADED``.
    """

    def __init__(
        self,
        pool: DatabasePool,
        dlq_size_threshold: int = 100,
    ) -> None:
        self._pool = pool
        self._dlq_size_threshold = dlq_size_threshold
        self._started_at = datetime.now(timezone.utc)

    async def check(self) -> HealthResult:
        """Run a health check, gathering all data in parallel.

        Returns:
            A ``HealthResult`` with the aggregated health status.
        """
        last_check = datetime.now(timezone.utc)
        uptime = (last_check - self._started_at).total_seconds()

        # Run all queries in parallel, catching exceptions individually so
        # one failure doesn't prevent the others from completing.
        gather_results: Any = await asyncio.gather(
            self._pool.health_check(),
            self._count_pending(),
            self._count_dlq(),
            self._count_active_workers(),
            return_exceptions=True,
        )
        db_ok: Any = gather_results[0]
        pending_count: Any = gather_results[1]
        dlq_count: Any = gather_results[2]
        active_workers: Any = gather_results[3]

        # Interpret results, treating exceptions as connectivity failures
        if isinstance(db_ok, Exception) or not db_ok:
            database = "disconnected"
        else:
            database = "connected"

        pending_val = -1 if isinstance(pending_count, Exception) else (pending_count or 0)
        dlq_val = -1 if isinstance(dlq_count, Exception) else (dlq_count or 0)
        workers_val = -1 if isinstance(active_workers, Exception) else (active_workers or 0)

        # Determine overall status
        if database == "disconnected":
            status = HealthStatus.UNHEALTHY
        elif dlq_val > self._dlq_size_threshold:
            status = HealthStatus.DEGRADED
        else:
            status = HealthStatus.HEALTHY

        logger.debug(
            "Health check complete: status=%s, pending=%d, dlq=%d, workers=%d",
            status.value,
            pending_count,
            dlq_count,
            active_workers,
        )

        return HealthResult(
            status=status,
            database=database,
            pending_tasks=pending_val,
            dead_letter_queue=dlq_val,
            workers_active=workers_val,
            uptime_seconds=uptime,
            last_check=last_check,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _count_pending(self) -> int:
        """Return the number of pending tasks."""
        queries = QueryBuilder(self._pool)
        return await queries.count_tasks_by_status("pending")

    async def _count_dlq(self) -> int:
        """Return the number of non-discarded DLQ tasks."""
        queries = QueryBuilder(self._pool)
        return await queries.count_dlq_tasks(include_discarded=False)

    async def _count_active_workers(self) -> int:
        """Return the number of workers with heartbeats within 30s."""
        queries = QueryBuilder(self._pool)
        workers = await queries.select_active_workers(heartbeat_timeout=30.0)
        return len(workers)
