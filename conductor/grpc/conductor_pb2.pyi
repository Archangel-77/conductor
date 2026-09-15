"""Type stubs for the generated ``conductor_pb2`` protobuf module.

The generated ``conductor_pb2.py`` defines its message classes dynamically via
the descriptor builder, so type checkers cannot infer them.  These stubs
declare the real message types and field types.  Construction accepts keyword
arguments (protobuf messages).  Keep in sync with ``proto/conductor.proto``
and regenerate alongside ``scripts/generate_grpc.py``.
"""

from typing import Any


class TaskRequest:
    task_id: str
    task_type: str
    payload: bytes
    persist: bool
    traceparent: str

    def __init__(self, **_kwargs: Any) -> None: ...


class TaskResponse:
    task_id: str
    success: bool
    result: bytes
    error: str
    traceparent: str

    def __init__(self, **_kwargs: Any) -> None: ...


class RegisterRequest:
    task_type: str

    def __init__(self, **_kwargs: Any) -> None: ...


class RegisterResponse:
    registered: bool
    task_type: str
    error: str

    def __init__(self, **_kwargs: Any) -> None: ...


class StatusRequest:
    def __init__(self, **_kwargs: Any) -> None: ...


class WorkerStatus:
    worker_id: str
    status: str
    uptime_seconds: float
    tasks_processed_total: int
    tasks_failed_total: int
    registered_handlers: list[str]
    grpc_enabled: bool
    grpc_port: int

    def __init__(self, **_kwargs: Any) -> None: ...
