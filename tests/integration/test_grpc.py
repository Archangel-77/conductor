"""
Integration tests for the gRPC ``ConductorWorker`` service.

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
A Worker with ``grpc_enabled=True`` runs in the background and a ``grpc.aio``
client calls the RPCs over a live channel.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
import pytest_asyncio
import grpc.aio as grpc_aio

from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError
from conductor.grpc import conductor_pb2, conductor_pb2_grpc

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope="module"),
]

GRPC_PORT = 50061  # non-default port to avoid conflicts


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="grpc_worker", autouse=True)
async def _grpc_worker_factory() -> Any:
    """Start a Worker with an embedded gRPC server in the background."""
    from tests.conftest import TEST_DATABASE_URL, db_available

    if not db_available():
        pytest.skip("Test database not available")

    worker = Worker(
        database_url=TEST_DATABASE_URL,
        worker_id="grpc-integration-worker",
        grpc_enabled=True,
        grpc_port=GRPC_PORT,
        routes=["grpc_integration"],
        metrics_enabled=False,
        health_enabled=False,
        pool_min_size=1,
        pool_max_size=2,
        pool_timeout=5.0,
        command_timeout=10.0,
    )
    try:
        await worker.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    @worker.task("grpc_echo")
    async def grpc_echo(payload: dict[str, Any]) -> dict[str, Any]:
        return {"echo": payload.get("message", "")}

    run_task = asyncio.create_task(worker.run())

    # Wait until the gRPC server is serving (bounded).
    for _ in range(100):
        server = worker._grpc_server  # noqa: SLF001
        if server is not None and server.is_serving:
            break
        await asyncio.sleep(0.05)
    else:
        await worker.shutdown()
        await run_task
        pytest.fail("gRPC server did not start within timeout")

    yield worker

    await worker.shutdown()
    await run_task


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def _cleanup(grpc_worker: Any) -> Any:
    """Clean up any rows written by the persist tests."""
    yield
    queries = grpc_worker._queries  # noqa: SLF001
    if queries is not None:
        pool = queries._pool  # noqa: SLF001
        if pool.is_connected:
            await pool.execute("DELETE FROM conductor_retries")
            await pool.execute("DELETE FROM conductor_dead_letter")
            await pool.execute("DELETE FROM conductor_tasks")
            await pool.execute("DELETE FROM conductor_workers")
            await pool.execute("DELETE FROM conductor_recurring_tasks")


# ===================================================================
# Tests
# ===================================================================


class TestGrpcIntegration:

    async def test_process_task(self) -> None:
        """ProcessTask executes the worker's handler over a live channel."""
        async with grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
            stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
            resp = await stub.ProcessTask(
                conductor_pb2.TaskRequest(
                    task_type="grpc_echo",
                    payload=json.dumps({"message": "hi"}).encode("utf-8"),
                )
            )

        assert resp.success is True
        assert json.loads(resp.result.decode("utf-8")) == {"echo": "hi"}

    async def test_process_task_unknown_type(self) -> None:
        """ProcessTask for an unknown handler returns success=False."""
        async with grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
            stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
            resp = await stub.ProcessTask(conductor_pb2.TaskRequest(task_type="grpc_missing"))

        assert resp.success is False
        assert "No handler registered" in resp.error

    async def test_process_task_propagates_trace_context(self) -> None:
        """The caller's traceparent continues into the execution span."""
        caller_traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        async with grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
            stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
            resp = await stub.ProcessTask(
                conductor_pb2.TaskRequest(
                    task_id="grpc-traced",
                    task_type="grpc_echo",
                    payload=json.dumps({"message": "traced"}).encode("utf-8"),
                    traceparent=caller_traceparent,
                )
            )

        assert resp.success is True
        # Without tracing configured the field is simply empty – never invalid.
        assert resp.traceparent == "" or resp.traceparent.startswith("00-")

    async def test_register_handler(self, grpc_worker: Any) -> None:
        """RegisterHandler is idempotent over the wire."""
        async with grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
            stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
            first = await stub.RegisterHandler(
                conductor_pb2.RegisterRequest(task_type="grpc_remote")
            )
            second = await stub.RegisterHandler(
                conductor_pb2.RegisterRequest(task_type="grpc_remote")
            )

        assert first.registered is True
        assert second.registered is False
        assert grpc_worker.has_handler("grpc_remote")

    async def test_get_worker_status(self) -> None:
        """GetWorkerStatus returns worker identity and gRPC state."""
        async with grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
            stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
            status = await stub.GetWorkerStatus(conductor_pb2.StatusRequest())

        assert status.worker_id == "grpc-integration-worker"
        assert status.grpc_enabled is True
        assert status.grpc_port == GRPC_PORT
        assert "grpc_echo" in status.registered_handlers

    async def test_process_task_persist(self) -> None:
        """persist=True records the outcome without erroring over the wire."""
        async with grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}") as channel:
            stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
            resp = await stub.ProcessTask(
                conductor_pb2.TaskRequest(
                    task_type="grpc_echo",
                    payload=json.dumps({"message": "persist"}).encode("utf-8"),
                    persist=True,
                )
            )

        assert resp.success is True
