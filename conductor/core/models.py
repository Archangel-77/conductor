"""
Data models for Conductor task queue.

All core domain objects are defined here as immutable dataclasses with
Pydantic-like validation helpers.  Every public model has full type hints.
"""

from __future__ import annotations

import json
import os
import socket
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, cast

from conductor.exceptions import RetryPolicyError, TaskError

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class TaskStatus(str, Enum):
    """Possible states of a task through its lifecycle."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"

    def __str__(self) -> str:
        return self.value


class WorkerStatus(str, Enum):
    """Possible states of a registered worker."""

    IDLE = "idle"
    PROCESSING = "processing"
    UNHEALTHY = "unhealthy"

    def __str__(self) -> str:
        return self.value


class BackoffStrategyType(str, Enum):
    """Supported backoff strategies for retry policies."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"

    def __str__(self) -> str:
        return self.value


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryPolicy:
    """Defines how a task should be retried on failure."""

    max_retries: int = 3
    """Maximum number of retry attempts (default 3)."""

    backoff_strategy: BackoffStrategyType = BackoffStrategyType.EXPONENTIAL
    """Backoff algorithm to use between retries."""

    initial_delay: float = 1.0
    """Delay before the first retry, in seconds."""

    max_delay: float = 3600.0
    """Maximum delay between retries, in seconds (capped)."""

    def validate(self) -> None:
        """Validate policy values.
        Raises ``RetryPolicyError`` on failure.
        """
        if self.max_retries < 0:
            raise RetryPolicyError("max_retries must be >= 0")
        if self.initial_delay <= 0:
            raise RetryPolicyError("initial_delay must be > 0")
        if self.max_delay <= 0:
            raise RetryPolicyError("max_delay must be > 0")
        if self.max_delay < self.initial_delay:
            raise RetryPolicyError("max_delay must be >= initial_delay")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dictionary."""
        return {
            "max_retries": self.max_retries,
            "backoff_strategy": self.backoff_strategy.value,
            "initial_delay": self.initial_delay,
            "max_delay": self.max_delay,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetryPolicy:
        """Deserialize from a dictionary."""
        return cls(
            max_retries=data.get("max_retries", 3),
            backoff_strategy=BackoffStrategyType(data.get("backoff_strategy", "exponential")),
            initial_delay=float(data.get("initial_delay", 1.0)),
            max_delay=float(data.get("max_delay", 3600.0)),
        )


@dataclass(frozen=True)
class Task:
    """A unit of work to be processed by a worker."""

    task_id: str
    """Unique identifier (UUID v4)."""

    task_type: str
    """Logical type used to route the task to a handler."""

    payload: dict[str, Any]
    """Arbitrary JSON-serialisable data passed to the handler."""

    status: TaskStatus = TaskStatus.PENDING
    """Current lifecycle status."""

    priority: int = 0
    """Task priority (higher = more urgent).  Used in v0.2+."""

    route: str = "default"
    """Route name for selective worker polling.  Used in v0.2+."""

    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    """Retry configuration for this task."""

    attempt: int = 0
    """Current retry attempt number."""

    max_retries: int = 3
    """Alias for quick access (matches retry_policy.max_retries)."""

    scheduled_for: Optional[datetime] = None
    """If set, the task should not be picked up before this time."""

    worker_id: Optional[str] = None
    """ID of the worker currently processing this task."""

    result: Optional[dict[str, Any]] = None
    """Output produced by the handler on success."""

    error_message: Optional[str] = None
    """Error message if the task failed."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """Timestamp when the task was created."""

    started_at: Optional[datetime] = None
    """Timestamp when a worker started processing this task."""

    completed_at: Optional[datetime] = None
    """Timestamp when the task completed (success or failure)."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dictionary."""
        result: dict[str, Any] = asdict(self)
        result["status"] = self.status.value
        result["retry_policy"] = self.retry_policy.to_dict()
        # Convert datetimes to ISO strings
        datetime_fields = ("scheduled_for", "created_at", "started_at", "completed_at")
        for field_name in datetime_fields:
            val = getattr(self, field_name)
            result[field_name] = val.isoformat() if val else None
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Task:
        """Deserialize from a dictionary."""
        payload = data.copy()
        payload["status"] = TaskStatus(payload.get("status", "pending"))
        payload["retry_policy"] = RetryPolicy.from_dict(payload.get("retry_policy", {}))
        # Parse datetime fields
        datetime_fields = ("scheduled_for", "created_at", "started_at", "completed_at")
        for field_name in datetime_fields:
            val = payload.get(field_name)
            if isinstance(val, str):
                payload[field_name] = datetime.fromisoformat(val)
        return cls(**payload)


@dataclass(frozen=True)
class WorkerInfo:
    """Information about a registered worker process."""

    worker_id: str
    """Unique worker identifier."""

    status: WorkerStatus = WorkerStatus.IDLE
    """Current worker status."""

    current_task_id: Optional[str] = None
    """Task ID the worker is currently processing, if any."""

    hostname: str = ""
    """Hostname of the machine running the worker."""

    pid: int = 0
    """Process ID of the worker."""

    uptime_seconds: float = 0.0
    """Seconds since the worker started."""

    tasks_processed_total: int = 0
    """Total tasks successfully processed."""

    tasks_failed_total: int = 0
    """Total tasks that failed."""

    last_heartbeat: Optional[datetime] = None
    """Timestamp of the last heartbeat received."""

    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """Timestamp when the worker started."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dictionary."""
        result: dict[str, Any] = asdict(self)
        result["status"] = self.status.value
        for field_name in ("last_heartbeat", "started_at"):
            val = getattr(self, field_name)
            result[field_name] = val.isoformat() if val else None
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerInfo:
        """Deserialize from a dictionary."""
        payload = data.copy()
        payload["status"] = WorkerStatus(payload.get("status", "idle"))
        for field_name in ("last_heartbeat", "started_at"):
            val = payload.get(field_name)
            if isinstance(val, str):
                payload[field_name] = datetime.fromisoformat(val)
        return cls(**payload)


@dataclass(frozen=True)
class RetryRecord:
    """A record of a single retry attempt for a task."""

    task_id: str
    """The task that was retried."""

    attempt: int
    """Which attempt number this record corresponds to."""

    scheduled_at: datetime
    """When the retry is (or was) scheduled to execute."""

    error_message: Optional[str] = None
    """The error that triggered the retry."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    """Unique record identifier."""

    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """When this record was created."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dictionary."""
        result: dict[str, Any] = asdict(self)
        for field_name in ("scheduled_at", "created_at"):
            val = getattr(self, field_name)
            result[field_name] = val.isoformat() if val else None
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetryRecord:
        """Deserialize from a dictionary."""
        payload = data.copy()
        for field_name in ("scheduled_at", "created_at"):
            val = payload.get(field_name)
            if isinstance(val, str):
                payload[field_name] = datetime.fromisoformat(val)
        return cls(**payload)


@dataclass(frozen=True)
class DLQTask:
    """A task that has exhausted its retry attempts and been moved to the
    dead-letter queue."""

    task_id: str
    """Original task ID."""

    task_type: str
    """Original task type."""

    payload: dict[str, Any]
    """Original task payload."""

    error_message: Optional[str] = None
    """Final error message that caused the DLQ move."""

    attempts: int = 0
    """Total number of attempts before exhaustion."""

    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    """The retry policy that was applied."""

    moved_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    """When the task was moved to the DLQ."""

    discarded: bool = False
    """Whether the task has been manually discarded."""

    discard_reason: Optional[str] = None
    """Reason provided when the task was discarded."""

    discarded_at: Optional[datetime] = None
    """When the task was discarded."""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dictionary."""
        result: dict[str, Any] = asdict(self)
        result["retry_policy"] = self.retry_policy.to_dict()
        for field_name in ("moved_at", "discarded_at"):
            val = getattr(self, field_name)
            result[field_name] = val.isoformat() if val else None
        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DLQTask:
        """Deserialize from a dictionary."""
        payload = data.copy()
        payload["retry_policy"] = RetryPolicy.from_dict(payload.get("retry_policy", {}))
        for field_name in ("moved_at", "discarded_at"):
            val = payload.get(field_name)
            if isinstance(val, str):
                payload[field_name] = datetime.fromisoformat(val)
        return cls(**payload)


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def generate_task_id() -> str:
    """Generate a unique task identifier (UUID v4)."""
    return str(uuid.uuid4())


def utc_now() -> datetime:
    """Return the current UTC datetime (timezone-aware)."""
    return datetime.now(timezone.utc)


def serialize_payload(payload: dict[str, Any]) -> str:
    """Serialize a payload dictionary to a JSON string."""
    return json.dumps(payload, default=str, ensure_ascii=False)


def deserialize_payload(data: str) -> dict[str, Any]:
    """Deserialize a JSON string back to a dictionary."""
    try:
        return cast("dict[str, Any]", json.loads(data))
    except (json.JSONDecodeError, TypeError) as exc:
        raise TaskError(f"Failed to deserialize payload: {exc}") from exc


def generate_correlation_id() -> str:
    """Generate a unique correlation ID for tracing requests."""
    return f"corr_{uuid.uuid4().hex}"


def get_hostname() -> str:
    """Return the machine hostname."""
    return socket.gethostname()


def get_pid() -> int:
    """Return the current process ID."""
    return os.getpid()


def get_worker_label() -> str:
    """Return a human-readable label combining hostname and PID.

    Useful as a default ``worker_id``.
    """
    return f"{get_hostname()}-{get_pid()}"


# ---------------------------------------------------------------------------
# Backoff strategy stubs (will be fleshed out in Sprint 4)
# ---------------------------------------------------------------------------


class ExponentialBackoff:
    """Exponential backoff: delay = initial_delay * (2 ^ attempt),
    capped at max_delay."""

    def __init__(self, initial_delay: float = 1.0, max_delay: float = 3600.0) -> None:
        self.initial_delay = initial_delay
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int) -> float:
        """Return the delay in seconds for the given *attempt* number."""
        delay = self.initial_delay * float(2**attempt)
        return min(delay, self.max_delay)


class LinearBackoff:
    """Linear backoff: delay = initial_delay + (initial_delay * attempt),
    capped."""

    def __init__(self, initial_delay: float = 1.0, max_delay: float = 3600.0) -> None:
        self.initial_delay = initial_delay
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int) -> float:
        """Return the delay in seconds for the given *attempt* number."""
        delay = self.initial_delay + (self.initial_delay * float(attempt))
        return min(delay, self.max_delay)


class FixedBackoff:
    """Fixed backoff: always returns the same delay."""

    def __init__(self, initial_delay: float = 1.0, max_delay: float = 3600.0) -> None:
        self.initial_delay = initial_delay
        self.max_delay = max_delay

    def calculate_delay(self, attempt: int) -> float:
        """Return the delay in seconds for the given *attempt* number."""
        _ = attempt  # unused – always return initial_delay
        return min(self.initial_delay, self.max_delay)
