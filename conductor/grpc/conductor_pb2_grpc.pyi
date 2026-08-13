"""Type stubs for the generated ``conductor_pb2_grpc`` gRPC module.

The generated stub classes are declared here so that gRPC clients and
servicers type-check.  The servicer methods are declared as ``async`` so
``grpc.aio`` implementations (as used by ``conductor.grpc.server``) override
them cleanly.  Keep in sync with ``proto/conductor.proto``.
"""

from typing import Any, Awaitable, Optional

from . import conductor_pb2


class ConductorWorkerServicer:
    async def ProcessTask(
        self,
        _request: conductor_pb2.TaskRequest,
        _context: Any,
    ) -> conductor_pb2.TaskResponse: ...

    async def RegisterHandler(
        self,
        _request: conductor_pb2.RegisterRequest,
        _context: Any,
    ) -> conductor_pb2.RegisterResponse: ...

    async def GetWorkerStatus(
        self,
        _request: conductor_pb2.StatusRequest,
        _context: Any,
    ) -> conductor_pb2.WorkerStatus: ...


class ConductorWorkerStub:
    """Client stub. Over a ``grpc.aio`` channel the RPC methods return
    awaitables (as used by Conductor's Python clients).
    """

    def __init__(self, _channel: Any) -> None: ...

    def ProcessTask(
        self,
        _request: conductor_pb2.TaskRequest,
        *,
        _timeout: Optional[float] = None,
        _metadata: Any = None,
        _credentials: Any = None,
        _wait_for_ready: Optional[bool] = None,
        _compression: Any = None,
    ) -> Awaitable[conductor_pb2.TaskResponse]: ...

    def RegisterHandler(
        self,
        _request: conductor_pb2.RegisterRequest,
        *,
        _timeout: Optional[float] = None,
        _metadata: Any = None,
        _credentials: Any = None,
        _wait_for_ready: Optional[bool] = None,
        _compression: Any = None,
    ) -> Awaitable[conductor_pb2.RegisterResponse]: ...

    def GetWorkerStatus(
        self,
        _request: conductor_pb2.StatusRequest,
        *,
        _timeout: Optional[float] = None,
        _metadata: Any = None,
        _credentials: Any = None,
        _wait_for_ready: Optional[bool] = None,
        _compression: Any = None,
    ) -> Awaitable[conductor_pb2.WorkerStatus]: ...


def add_ConductorWorkerServicer_to_server(
    _servicer: Any,
    _server: Any,
) -> None: ...
