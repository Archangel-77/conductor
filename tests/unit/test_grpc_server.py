"""
Unit tests for the gRPC servicer (``conductor.grpc.server``).

No database required — the servicer methods are called directly with a fake
context, and the ``Worker`` is constructed without connecting.
"""

# pylint: disable=missing-class-docstring,protected-access

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from conductor.core.worker import Worker
from conductor.grpc import conductor_pb2
from conductor.grpc.server import ConductorWorkerServicer, GrpcWorkerServer


class _AbortError(Exception):
    """Raised by the fake context to simulate ``context.abort(...)``."""

    def __init__(self, code: Any, details: str) -> None:
        super().__init__(details)
        self.code = code
        self.details = details


class _FakeContext:
    """Minimal stand-in for ``grpc.aio.ServicerContext``."""

    def __init__(self) -> None:
        self.aborted: Any = None

    async def abort(self, code: Any, details: str) -> Any:
        self.aborted = (code, details)
        raise _AbortError(code, details)


@pytest.fixture(name="worker")
def _worker_factory() -> Worker:
    """A Worker that is not connected (handlers only)."""
    return Worker(database_url="postgresql://mock@localhost/db")


def _servicer(worker: Worker) -> ConductorWorkerServicer:
    return ConductorWorkerServicer(worker)


# ===================================================================
# ProcessTask
# ===================================================================


class TestProcessTask:

    async def test_success(self, worker: Worker) -> None:
        """A registered handler executes and returns a JSON result."""

        @worker.task("echo")
        async def echo(payload: dict[str, Any]) -> dict[str, Any]:
            return {"echo": payload["message"]}

        resp = await _servicer(worker).ProcessTask(
            conductor_pb2.TaskRequest(
                task_type="echo",
                payload=json.dumps({"message": "hi"}).encode("utf-8"),
            ),
            _FakeContext(),
        )

        assert resp.success is True
        assert json.loads(resp.result.decode("utf-8")) == {"echo": "hi"}

    async def test_handler_not_found(self, worker: Worker) -> None:
        """An unknown task_type returns success=False with an error message."""
        resp = await _servicer(worker).ProcessTask(
            conductor_pb2.TaskRequest(task_type="nope"),
            _FakeContext(),
        )

        assert resp.success is False
        assert "No handler registered" in resp.error

    async def test_empty_task_type_aborts(self, worker: Worker) -> None:
        """An empty task_type aborts with INVALID_ARGUMENT."""
        ctx = _FakeContext()
        with pytest.raises(_AbortError):
            await _servicer(worker).ProcessTask(
                conductor_pb2.TaskRequest(task_type=""),
                ctx,
            )
        assert ctx.aborted is not None

    async def test_bad_payload_json_aborts(self, worker: Worker) -> None:
        """Non-JSON payload bytes abort with INVALID_ARGUMENT."""

        @worker.task("echo")
        async def echo(_payload: dict[str, Any]) -> dict[str, Any]:
            return {}

        ctx = _FakeContext()
        with pytest.raises(_AbortError):
            await _servicer(worker).ProcessTask(
                conductor_pb2.TaskRequest(task_type="echo", payload=b"not-json"),
                ctx,
            )
        assert ctx.aborted is not None

    async def test_handler_raises(self, worker: Worker) -> None:
        """A handler exception is returned as success=False with the message."""

        @worker.task("boom")
        async def boom(_payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("boom!")

        resp = await _servicer(worker).ProcessTask(
            conductor_pb2.TaskRequest(task_type="boom"),
            _FakeContext(),
        )

        assert resp.success is False
        assert "boom!" in resp.error

    async def test_persist_records_task(self, worker: Worker) -> None:
        """With persist=True the outcome is recorded via the worker's queries."""
        from datetime import datetime, timezone

        completed_row = {
            "task_id": "tid-1",
            "task_type": "echo",
            "payload": {},
            "status": "completed",
            "priority": 0,
            "route": "default",
            "attempt": 0,
            "max_retries": 3,
            "retry_policy": {},
            "scheduled_for": None,
            "worker_id": "grpc-persist",
            "result": {"ok": True},
            "error_message": None,
            "created_at": datetime.now(timezone.utc),
            "started_at": datetime.now(timezone.utc),
            "completed_at": datetime.now(timezone.utc),
        }
        queries = MagicMock()
        queries.select_task = AsyncMock(side_effect=[None, completed_row])
        queries.insert_task = AsyncMock(return_value="tid-1")
        queries.update_task_status = AsyncMock(return_value=True)
        worker._queries = queries  # noqa: SLF001

        @worker.task("echo")
        async def echo(_payload: dict[str, Any]) -> dict[str, Any]:
            return {"ok": True}

        resp = await _servicer(worker).ProcessTask(
            conductor_pb2.TaskRequest(task_type="echo", persist=True),
            _FakeContext(),
        )

        assert resp.success is True
        assert json.loads(resp.result.decode("utf-8")) == {"ok": True}
        # The task row was inserted, then status advanced processing -> completed.
        queries.insert_task.assert_awaited_once()
        assert queries.update_task_status.await_count >= 2


# ===================================================================
# RegisterHandler
# ===================================================================


class TestRegisterHandler:

    async def test_register_new(self, worker: Worker) -> None:
        """A new task_type is registered (idempotent capability)."""
        resp = await _servicer(worker).RegisterHandler(
            conductor_pb2.RegisterRequest(task_type="remote_a"),
            _FakeContext(),
        )

        assert resp.registered is True
        assert "remote_a" in worker._handlers  # noqa: SLF001

    async def test_register_duplicate(self, worker: Worker) -> None:
        """Registering the same task_type twice returns registered=False."""
        svc = _servicer(worker)
        await svc.RegisterHandler(
            conductor_pb2.RegisterRequest(task_type="remote_a"),
            _FakeContext(),
        )
        resp = await svc.RegisterHandler(
            conductor_pb2.RegisterRequest(task_type="remote_a"),
            _FakeContext(),
        )

        assert resp.registered is False

    async def test_register_empty_aborts(self, worker: Worker) -> None:
        """An empty task_type aborts with INVALID_ARGUMENT."""
        ctx = _FakeContext()
        with pytest.raises(_AbortError):
            await _servicer(worker).RegisterHandler(
                conductor_pb2.RegisterRequest(task_type=""),
                ctx,
            )
        assert ctx.aborted is not None


# ===================================================================
# GetWorkerStatus
# ===================================================================


class TestGetWorkerStatus:

    async def test_maps_status(self, worker: Worker) -> None:
        """Worker status fields map onto the proto message."""
        resp = await _servicer(worker).GetWorkerStatus(
            conductor_pb2.StatusRequest(),
            _FakeContext(),
        )

        assert resp.worker_id == worker.worker_id
        assert resp.grpc_enabled is False
        assert resp.grpc_port == worker._grpc_port  # noqa: SLF001
        assert resp.registered_handlers == []


# ===================================================================
# GrpcWorkerServer
# ===================================================================


class TestGrpcWorkerServer:

    def test_get_status(self, worker: Worker) -> None:
        """get_status() reports the server configuration."""
        server = GrpcWorkerServer(worker, port=0)
        status = server.get_status()

        assert status["enabled"] is True
        assert status["serving"] is False
        # ``add_insecure_port`` resolves an ephemeral port immediately.
        assert status["port"] > 0
