"""
Unit tests for Conductor utility functions.

Covers ID generation, timestamps, JSON serialisation,
correlation IDs, and hostname/PID utilities.
"""

from __future__ import annotations

import pytest

from conductor.core.models import (
    deserialize_payload,
    generate_correlation_id,
    generate_task_id,
    get_hostname,
    get_pid,
    get_worker_label,
    serialize_payload,
    utc_now,
)
from conductor.exceptions import TaskError


class TestGenerateTaskId:

    def test_type(self):
        tid = generate_task_id()
        assert isinstance(tid, str)

    def test_format(self):
        tid = generate_task_id()
        # UUID v4: 8-4-4-4-12 hex digits = 36 chars
        assert len(tid) == 36
        assert tid.count("-") == 4

    def test_uniqueness(self):
        ids = {generate_task_id() for _ in range(1000)}
        assert len(ids) == 1000


class TestGenerateCorrelationId:

    def test_type(self):
        cid = generate_correlation_id()
        assert isinstance(cid, str)

    def test_prefix(self):
        cid = generate_correlation_id()
        assert cid.startswith("corr_")

    def test_length(self):
        cid = generate_correlation_id()
        # "corr_" (5) + 32 hex chars = 37
        assert len(cid) == 37

    def test_uniqueness(self):
        ids = {generate_correlation_id() for _ in range(1000)}
        assert len(ids) == 1000


class TestUtcNow:

    def test_type(self):
        now = utc_now()
        assert hasattr(now, "tzinfo")

    def test_timezone(self):
        now = utc_now()
        assert now.tzinfo is not None
        assert str(now.tzinfo) == "UTC"


class TestSerializePayload:

    def test_simple_dict(self):
        result = serialize_payload({"a": 1, "b": "hello"})
        assert result == '{"a": 1, "b": "hello"}'

    def test_nested(self):
        result = serialize_payload({"x": {"y": [1, 2, 3]}})
        assert '"y"' in result

    def test_empty(self):
        assert serialize_payload({}) == "{}"


class TestDeserializePayload:

    def test_simple(self):
        result = deserialize_payload('{"a": 1}')
        assert result == {"a": 1}

    def test_invalid_json(self):
        with pytest.raises(TaskError):
            deserialize_payload("not json")

    def test_round_trip(self):
        original = {"key": "value", "nested": {"a": [1, 2]}}
        serialized = serialize_payload(original)
        deserialized = deserialize_payload(serialized)
        assert deserialized == original


class TestHostnameAndPid:

    def test_get_hostname(self):
        host = get_hostname()
        assert isinstance(host, str)
        assert len(host) > 0

    def test_get_pid(self):
        pid = get_pid()
        assert isinstance(pid, int)
        assert pid > 0

    def test_get_worker_label(self):
        label = get_worker_label()
        assert isinstance(label, str)
        assert "-" in label
        # Should match the pattern <hostname>-<pid>
        parts = label.rsplit("-", 1)
        assert len(parts) == 2
        assert parts[0] == get_hostname()
        assert parts[1] == str(get_pid())
