"""
Unit tests for Conductor data models.

Tests cover construction, validation, serialisation, and edge cases
for all dataclass models and enums.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from conductor.core.models import (
    BackoffStrategyType,
    DLQTask,
    RetryPolicy,
    RetryRecord,
    Task,
    TaskStatus,
    WorkerInfo,
    WorkerStatus,
    generate_task_id,
    utc_now,
)
from conductor.exceptions import RetryPolicyError


# ===================================================================
# Enums
# ===================================================================

class TestTaskStatus:
    """Verify the TaskStatus enum values and conversions."""

    def test_values(self):
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.PROCESSING.value == "processing"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.RETRYING.value == "retrying"

    def test_str(self):
        assert str(TaskStatus.PENDING) == "pending"

    def test_membership(self):
        valid = set(TaskStatus)
        assert TaskStatus("pending") in valid
        with pytest.raises(ValueError):
            TaskStatus("nonexistent")


class TestWorkerStatus:
    """Verify the WorkerStatus enum."""

    def test_values(self):
        assert WorkerStatus.IDLE.value == "idle"
        assert WorkerStatus.PROCESSING.value == "processing"
        assert WorkerStatus.UNHEALTHY.value == "unhealthy"


class TestBackoffStrategyType:
    """Verify the BackoffStrategyType enum."""

    def test_values(self):
        assert BackoffStrategyType.EXPONENTIAL.value == "exponential"
        assert BackoffStrategyType.LINEAR.value == "linear"
        assert BackoffStrategyType.FIXED.value == "fixed"


# ===================================================================
# RetryPolicy
# ===================================================================

class TestRetryPolicy:

    def test_defaults(self):
        p = RetryPolicy()
        assert p.max_retries == 3
        assert p.backoff_strategy == BackoffStrategyType.EXPONENTIAL
        assert p.initial_delay == 1.0
        assert p.max_delay == 3600.0

    def test_custom_values(self):
        p = RetryPolicy(max_retries=5, initial_delay=2.0, max_delay=60.0)
        assert p.max_retries == 5
        assert p.initial_delay == 2.0
        assert p.max_delay == 60.0

    def test_validate_valid(self):
        RetryPolicy().validate()  # should not raise
        RetryPolicy(max_retries=0).validate()
        RetryPolicy(max_retries=10, initial_delay=0.5).validate()

    def test_validate_negative_max_retries(self):
        with pytest.raises(RetryPolicyError, match="max_retries"):
            RetryPolicy(max_retries=-1).validate()

    def test_validate_zero_initial_delay(self):
        with pytest.raises(RetryPolicyError, match="initial_delay"):
            RetryPolicy(initial_delay=0).validate()

    def test_validate_negative_max_delay(self):
        with pytest.raises(RetryPolicyError, match="max_delay"):
            RetryPolicy(max_delay=-1).validate()

    def test_validate_max_delay_less_than_initial(self):
        with pytest.raises(RetryPolicyError):
            RetryPolicy(initial_delay=10, max_delay=5).validate()

    def test_to_dict(self):
        p = RetryPolicy(max_retries=5)
        d = p.to_dict()
        assert d["max_retries"] == 5
        assert d["backoff_strategy"] == "exponential"

    def test_from_dict(self):
        d = {"max_retries": 5, "backoff_strategy": "linear",
             "initial_delay": 2.0, "max_delay": 30.0}
        p = RetryPolicy.from_dict(d)
        assert p.max_retries == 5
        assert p.backoff_strategy == BackoffStrategyType.LINEAR

    def test_round_trip(self):
        p = RetryPolicy(
            max_retries=7, backoff_strategy=BackoffStrategyType.FIXED
        )
        p2 = RetryPolicy.from_dict(p.to_dict())
        assert p == p2


# ===================================================================
# Task
# ===================================================================

class TestTask:

    def test_minimal_construction(self):
        t = Task(task_id="t1", task_type="email", payload={"to": "a@b.com"})
        assert t.task_id == "t1"
        assert t.task_type == "email"
        assert t.payload == {"to": "a@b.com"}
        assert t.status == TaskStatus.PENDING
        assert t.priority == 0
        assert t.route == "default"
        assert t.attempt == 0
        assert t.max_retries == 3
        assert isinstance(t.created_at, datetime)

    def test_with_all_fields(self, sample_task_dict):
        t = Task.from_dict(sample_task_dict)
        assert t.task_id == sample_task_dict["task_id"]
        assert t.task_type == sample_task_dict["task_type"]
        assert t.status == TaskStatus.PENDING

    def test_frozen(self):
        t = Task(task_id="t1", task_type="t", payload={})
        with pytest.raises(AttributeError):
            t.status = TaskStatus.COMPLETED  # type: ignore[misc]

    def test_to_dict_serialisation(self):
        t = Task(task_id="t1", task_type="email", payload={"x": 1})
        d = t.to_dict()
        assert d["task_id"] == "t1"
        assert d["status"] == "pending"
        assert isinstance(d["created_at"], str)  # ISO format

    def test_from_dict_deserialisation(self):
        d = {
            "task_id": "t1",
            "task_type": "email",
            "payload": {"x": 1},
            "status": "completed",
            "priority": 0,
            "route": "default",
            "attempt": 0,
            "max_retries": 3,
            "retry_policy": {},
            "scheduled_for": None,
            "worker_id": None,
            "result": {"output": "ok"},
            "error_message": None,
            "created_at": "2025-01-15T10:00:00+00:00",
            "started_at": None,
            "completed_at":
                "2025-01-15T10:00:05+00:00",
        }
        t = Task.from_dict(d)
        assert t.task_id == "t1"
        assert t.status == TaskStatus.COMPLETED
        assert t.result == {"output": "ok"}
        assert t.completed_at is not None

    def test_round_trip(self, sample_task_dict):
        t1 = Task.from_dict(sample_task_dict)
        t2 = Task.from_dict(t1.to_dict())
        assert t1.task_id == t2.task_id
        assert t1.task_type == t2.task_type
        assert t1.status == t2.status

    def test_multiple_task_ids_unique(self):
        ids = {generate_task_id() for _ in range(100)}
        assert len(ids) == 100


# ===================================================================
# WorkerInfo
# ===================================================================

class TestWorkerInfo:

    def test_minimal_construction(self):
        w = WorkerInfo(worker_id="w1")
        assert w.worker_id == "w1"
        assert w.status == WorkerStatus.IDLE

    def test_frozen(self):
        w = WorkerInfo(worker_id="w1")
        with pytest.raises(AttributeError):
            w.worker_id = "w2"  # type: ignore[misc]

    def test_to_dict(self):
        w = WorkerInfo(worker_id="w1", hostname="h1", pid=99)
        d = w.to_dict()
        assert d["worker_id"] == "w1"
        assert d["hostname"] == "h1"

    def test_round_trip(self):
        w = WorkerInfo(
            worker_id="w1", hostname="h1", pid=99,
            tasks_processed_total=42,
        )
        w2 = WorkerInfo.from_dict(w.to_dict())
        assert w.worker_id == w2.worker_id
        assert w.tasks_processed_total == w2.tasks_processed_total


# ===================================================================
# RetryRecord
# ===================================================================

class TestRetryRecord:

    def test_construction(self):
        now = utc_now()
        r = RetryRecord(task_id="t1", attempt=2, scheduled_at=now)
        assert r.task_id == "t1"
        assert r.attempt == 2
        assert r.scheduled_at == now
        assert r.id is not None  # auto-generated

    def test_to_dict(self):
        now = utc_now()
        r = RetryRecord(task_id="t1", attempt=1, scheduled_at=now)
        d = r.to_dict()
        assert d["task_id"] == "t1"
        assert d["attempt"] == 1
        assert isinstance(d["scheduled_at"], str)

    def test_round_trip(self, sample_retry_record_dict):
        r1 = RetryRecord.from_dict(sample_retry_record_dict)
        r2 = RetryRecord.from_dict(r1.to_dict())
        assert r1.id == r2.id
        assert r1.attempt == r2.attempt


# ===================================================================
# DLQTask
# ===================================================================

class TestDLQTask:

    def test_minimal_construction(self):
        d = DLQTask(task_id="t1", task_type="email", payload={})
        assert d.task_id == "t1"
        assert d.discarded is False

    def test_discard(self):
        d = DLQTask(
            task_id="t1", task_type="email", payload={},
            discarded=True, discard_reason="manual",
        )
        assert d.discarded is True
        assert d.discard_reason == "manual"

    def test_round_trip(self):
        d1 = DLQTask(
            task_id="t1", task_type="email", payload={"a": 1},
            attempts=3, error_message="fail",
        )
        d2 = DLQTask.from_dict(d1.to_dict())
        assert d1.task_id == d2.task_id
        assert d1.attempts == d2.attempts
