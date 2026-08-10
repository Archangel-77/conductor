"""gRPC API for Conductor (polyglot workers).

Exposes the async ``ConductorWorker`` gRPC service on a
:class:`~conductor.core.worker.Worker` via :class:`GrpcWorkerServer`.
Generated protobuf stubs live in this package as ``conductor_pb2`` /
``conductor_pb2_grpc`` (regenerate with ``scripts/generate_grpc.py``).
"""

from __future__ import annotations

from conductor.grpc.server import ConductorWorkerServicer, GrpcWorkerServer

__all__: list[str] = ["ConductorWorkerServicer", "GrpcWorkerServer"]
