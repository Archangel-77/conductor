"""
Conductor — Lightweight async task queue for Python
(PostgreSQL-backed by default; SQLite and MySQL/MariaDB supported via
backend extras.  No Redis, no message broker).

Exposes the public API of the Conductor library.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _package_version

try:
    __version__: str = _package_version("conductor-task-queue")
except PackageNotFoundError:  # pragma: no cover - uninstalled source checkout
    __version__ = "0.0.0.dev0"

from conductor.core.models import (
    BackoffStrategyType,
    DLQTask,
    ExponentialBackoff,
    FixedBackoff,
    LinearBackoff,
    RecurringTask,
    RetryPolicy,
    RetryRecord,
    Task,
    TaskStatus,
    WorkerInfo,
    WorkerStatus,
)
from conductor.db.connection import DatabasePool, PoolConfig
from conductor.db.queries import QueryBuilder
from conductor.db.schema import SchemaManager
from conductor.exceptions import (
    CircuitBreakerError,
    ConductorConnectionError,
    ConductorException,
    DatabaseError,
    RetryPolicyError,
    TaskError,
    WorkerError,
)

from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker

from conductor.dlq.dead_letter_queue import DeadLetterQueue
from conductor.recurring.scheduler import RecurringScheduler
from conductor.grpc.server import GrpcWorkerServer

from conductor.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerRegistry,
    CircuitState,
)

from conductor.api.server import DashboardServer
from conductor.api.app import create_app

from conductor.observability.health import HealthChecker, HealthResult, HealthStatus

__all__: list[str] = [
    "BackoffStrategyType",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerError",
    "CircuitBreakerRegistry",
    "CircuitState",
    "ConductorConnectionError",
    "ConductorException",
    "create_app",
    "DatabaseError",
    "DatabasePool",
    "DashboardServer",
    "DLQTask",
    "ExponentialBackoff",
    "FixedBackoff",
    "GrpcWorkerServer",
    "HealthChecker",
    "HealthResult",
    "HealthStatus",
    "LinearBackoff",
    "PoolConfig",
    "QueryBuilder",
    "RecurringScheduler",
    "RecurringTask",
    "RetryPolicy",
    "RetryPolicyError",
    "RetryRecord",
    "SchemaManager",
    "Task",
    "TaskError",
    "TaskQueue",
    "TaskStatus",
    "Worker",
    "WorkerError",
    "WorkerInfo",
    "WorkerStatus",
    "DeadLetterQueue",
]
