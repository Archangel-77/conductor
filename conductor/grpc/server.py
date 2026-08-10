"""
gRPC worker server for Conductor.

Provides ``GrpcWorkerServer`` — an async (``grpc.aio``) server exposing the
``ConductorWorker`` service on a :class:`~conductor.core.worker.Worker`, so
polyglot clients can execute tasks, register handlers, and inspect worker
status over gRPC.

Typical usage::

    server = GrpcWorkerServer(worker, port=50051)
    await server.start()
    # ...
    await server.stop()

A Worker embeds this automatically when constructed with
``Worker(grpc_enabled=True)``.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

import grpc
from grpc import aio as grpc_aio

from conductor.core.models import TaskStatus, generate_task_id
from conductor.core.worker import Worker, _call_handler
from conductor.grpc import conductor_pb2, conductor_pb2_grpc

logger = logging.getLogger("conductor.grpc.server")


class ConductorWorkerServicer:
    """gRPC servicer implementing the ``ConductorWorker`` service.

    Wraps a :class:`~conductor.core.worker.Worker` to execute handlers,
    register task types, and report worker status.
    """

    def __init__(self, worker: Worker) -> None:
        self._worker = worker

    # ------------------------------------------------------------------
    # RPCs
    # ------------------------------------------------------------------

    async def ProcessTask(
        self,
        request: conductor_pb2.TaskRequest,
        context: Any,
    ) -> conductor_pb2.TaskResponse:
        """Execute a task through the worker's registered handler.

        By default this is pure execution (no queue writes).  When
        ``request.persist`` is set, the outcome is recorded in
        ``conductor_tasks`` (processing → completed/failed, metrics and
        worker statistics updated, retry/DLQ handling applied).
        """
        task_type = request.task_type
        if not task_type or not task_type.strip():
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "task_type must not be empty",
            )

        handler = self._worker.get_handler(task_type)
        if handler is None:
            return conductor_pb2.TaskResponse(
                task_id=request.task_id,
                success=False,
                error=f"No handler registered for task_type '{task_type}'.",
            )

        try:
            payload: dict[str, Any] = (
                json.loads(request.payload.decode("utf-8")) if request.payload else {}
            )
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                f"payload must be JSON-encoded bytes: {exc}",
            )
        if not isinstance(payload, dict):
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "payload must decode to a JSON object",
            )

        if request.persist:
            return await self._process_persisted(request, task_type, payload)

        result, handler_error = await _call_handler(handler, payload)
        if handler_error is not None:
            return conductor_pb2.TaskResponse(
                task_id=request.task_id,
                success=False,
                error=str(handler_error),
            )
        return conductor_pb2.TaskResponse(
            task_id=request.task_id,
            success=True,
            result=json.dumps(result or {}, default=str).encode("utf-8"),
        )

    async def _process_persisted(
        self,
        request: conductor_pb2.TaskRequest,
        task_type: str,
        payload: dict[str, Any],
    ) -> conductor_pb2.TaskResponse:
        """Execute a task and record its lifecycle in ``conductor_tasks``.

        Reuses the worker's persisted-execution path so status transitions,
        metrics, retries, and DLQ handling behave exactly like a polled task.
        """
        worker = self._worker
        task = await worker.persist_and_execute(
            task_id=request.task_id or generate_task_id(),
            task_type=task_type,
            payload=payload,
        )

        if task is None:
            return conductor_pb2.TaskResponse(
                task_id=request.task_id,
                success=False,
                error="Task row not found after execution",
            )

        success = task.status == TaskStatus.COMPLETED
        return conductor_pb2.TaskResponse(
            task_id=task.task_id,
            success=success,
            result=(json.dumps(task.result or {}, default=str).encode("utf-8") if success else b""),
            error=task.error_message or ("" if success else "Task failed"),
        )

    async def RegisterHandler(
        self,
        request: conductor_pb2.RegisterRequest,
        context: Any,
    ) -> conductor_pb2.RegisterResponse:
        """Register a ``task_type`` handler on the worker (idempotent).

        A remotely registered type is tracked so ``ProcessTask`` reports a
        clear error if it is executed in-process, and it appears in
        ``GetWorkerStatus``.  It cannot be re-registered with the
        ``@worker.task()`` decorator.
        """
        task_type = request.task_type
        if not task_type or not task_type.strip():
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "task_type must not be empty",
            )

        worker = self._worker
        is_new = worker.register_remote_handler(task_type)

        logger.info(
            "Handler for task_type '%s' %s via gRPC.",
            task_type,
            "registered" if is_new else "already registered",
            extra={"task_type": task_type},
        )
        return conductor_pb2.RegisterResponse(
            registered=is_new,
            task_type=task_type,
        )

    async def GetWorkerStatus(
        self,
        _request: conductor_pb2.StatusRequest,
        _context: Any,
    ) -> conductor_pb2.WorkerStatus:
        """Return the worker's health and statistics."""
        status = self._worker.get_status()
        return conductor_pb2.WorkerStatus(
            worker_id=status["worker_id"],
            status=status["status"],
            uptime_seconds=status["uptime_seconds"],
            tasks_processed_total=status["tasks_processed_total"],
            tasks_failed_total=status["tasks_failed_total"],
            registered_handlers=status["registered_handlers"],
            grpc_enabled=status.get("grpc_enabled", False),
            grpc_port=status.get("grpc_port", 0),
        )


class GrpcWorkerServer:
    """Async gRPC server exposing the ``ConductorWorker`` service on a Worker.

    Args:
        worker: The :class:`~conductor.core.worker.Worker` to serve.
        host: Bind host (default ``0.0.0.0``).
        port: Bind port (default ``50051``).  ``0`` asks gRPC for an
            ephemeral port (read via :attr:`port`).
        max_message_size: Optional max send/receive message size in bytes.
    """

    def __init__(
        self,
        worker: Worker,
        *,
        host: str = "0.0.0.0",
        port: int = 50051,
        max_message_size: Optional[int] = None,
    ) -> None:
        self._worker = worker
        self._host = host
        self._port = port
        self._started = False

        options: list[tuple[str, int]] = []
        if max_message_size is not None:
            options = [
                ("grpc.max_send_message_length", max_message_size),
                ("grpc.max_receive_message_length", max_message_size),
            ]
        self._server = grpc_aio.server(options=options)
        servicer = ConductorWorkerServicer(worker)
        conductor_pb2_grpc.add_ConductorWorkerServicer_to_server(servicer, self._server)
        self._bound_port = self._server.add_insecure_port(f"{host}:{port}")

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def port(self) -> int:
        """The actual bound port (resolves ``port=0`` ephemeral binding)."""
        return self._bound_port or self._port

    @property
    def is_serving(self) -> bool:
        """``True`` if the server has been started and not yet stopped."""
        return self._started

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start serving on the configured address."""
        await self._server.start()
        self._started = True
        logger.info(
            "gRPC server listening on %s:%s.",
            self._host,
            self.port,
            extra={"grpc_port": self.port},
        )

    async def stop(self, grace: float = 5.0) -> None:
        """Gracefully stop the server, allowing up to *grace* seconds."""
        await self._server.stop(grace)
        self._started = False
        logger.info("gRPC server stopped.", extra={"grpc_port": self.port})

    def get_status(self) -> dict[str, Any]:
        """Return a snapshot of the gRPC server's state."""
        return {
            "enabled": True,
            "host": self._host,
            "port": self.port,
            "serving": self.is_serving,
        }
