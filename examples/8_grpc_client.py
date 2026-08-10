"""
Example 8 — gRPC API (polyglot workers).

Demonstrates the ``ConductorWorker`` gRPC service: a Worker embeds a gRPC
server (``grpc_enabled=True``), and a client (here the Python ``grpc.aio``
client) connects to execute a task via ``ProcessTask``, register a handler
via ``RegisterHandler``, and read worker status via ``GetWorkerStatus``.

Expected output (paraphrased)::

    ProcessTask: success=True result={'echo': 'hello'}
    RegisterHandler('echo2'): registered=True
    GetWorkerStatus: worker_id=grpc-example-worker, handlers=['echo', 'echo2']

Run (PostgreSQL must be reachable)::

    python examples/8_grpc_client.py
"""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any

from grpc import aio as grpc_aio

from conductor import Worker
from conductor.grpc import conductor_pb2, conductor_pb2_grpc

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")
GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))


async def main() -> None:
    # The Worker embeds the gRPC server (it is started inside ``run()``).
    async with Worker(
        database_url=DB_URL,
        worker_id="grpc-example-worker",
        grpc_enabled=True,
        grpc_port=GRPC_PORT,
        metrics_enabled=False,
        health_enabled=False,
    ) as worker:

        @worker.task("echo")
        async def echo(payload: dict[str, Any]) -> dict[str, Any]:
            return {"echo": payload.get("message", "")}

        run_task = asyncio.create_task(worker.run())
        await asyncio.sleep(1.0)  # let the worker + gRPC server start

        channel = grpc_aio.insecure_channel(f"localhost:{GRPC_PORT}")
        stub = conductor_pb2_grpc.ConductorWorkerStub(channel)
        try:
            # ProcessTask: execute a task through the worker's handler.
            resp = await stub.ProcessTask(
                conductor_pb2.TaskRequest(
                    task_type="echo",
                    payload=json.dumps({"message": "hello"}).encode("utf-8"),
                )
            )
            result = resp.result.decode("utf-8") if resp.result else ""
            print(f"ProcessTask: success={resp.success} result={result}")

            # RegisterHandler: declare a handler for a new task_type.
            reg = await stub.RegisterHandler(conductor_pb2.RegisterRequest(task_type="echo2"))
            print(f"RegisterHandler('echo2'): registered={reg.registered}")

            # GetWorkerStatus: read worker health and statistics.
            status = await stub.GetWorkerStatus(conductor_pb2.StatusRequest())
            print(
                f"GetWorkerStatus: worker_id={status.worker_id}, "
                f"handlers={list(status.registered_handlers)}"
            )
        finally:
            await channel.close()
            await worker.shutdown()
            await run_task


if __name__ == "__main__":
    asyncio.run(main())
