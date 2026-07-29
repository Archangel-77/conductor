"""
Conductor — Lightweight async task queue for Python
(PostgreSQL-backed, no Redis).

Exposes the public API of the Conductor library.
"""

from conductor.core.models import (
    BackoffStrategyType,
    DLQTask,
    ExponentialBackoff,
    FixedBackoff,
    LinearBackoff,
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
    ConductorConnectionError,
    ConductorException,
    DatabaseError,
    RetryPolicyError,
    TaskError,
    WorkerError,
)

from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker

# The following will be uncommented as those modules are implemented:
# from conductor.dlq.dead_letter_queue import DeadLetterQueue

__all__: list[str] = [
    "BackoffStrategyType",
    "ConductorConnectionError",
    "ConductorException",
    "DatabaseError",
    "DatabasePool",
    "DLQTask",
    "ExponentialBackoff",
    "FixedBackoff",
    "LinearBackoff",
    "PoolConfig",
    "QueryBuilder",
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
    # To be added:
    # "DeadLetterQueue",
]
