# gRPC Polyglot Client Stubs

Reference gRPC clients for Conductor's `ConductorWorker` service, demonstrating
how workers written in **Go**, **Rust**, and **Node.js** can connect to a
Conductor Worker and execute tasks. A fully working **Python** client is in
`examples/8_grpc_client.py`.

> These are **reference stubs** — they show the wire contract but are not
> built or run in CI. Generate client stubs from `proto/conductor.proto`
> before using them.

## Prerequisites

1. Run a worker with the gRPC server enabled:

   ```bash
   GRPC_ENABLED=true GRPC_PORT=50051 conductor worker --handlers myapp.handlers
   ```

   or programmatically: `Worker(database_url="...", grpc_enabled=True)`.

2. `proto/conductor.proto` defines the service:

   ```proto
   service ConductorWorker {
     rpc ProcessTask(TaskRequest) returns (TaskResponse);
     rpc RegisterHandler(RegisterRequest) returns (RegisterResponse);
     rpc GetWorkerStatus(StatusRequest) returns (WorkerStatus);
   }
   ```

## Generating stubs

| Language | Tools | Command |
|---|---|---|
| Go | `protoc-gen-go`, `protoc-gen-go-grpc` | `protoc -I proto --go_out=. --go-grpc_out=. proto/conductor.proto` |
| Rust | `tonic-build` | `tonic_build::configure().compile(&["proto/conductor.proto"], &["proto"])` |
| Node.js | `grpc_tools_node_protoc` | `grpc_tools_node_protoc -I proto --grpc_out=grpc_js:<out> --js_out=import_style=commonjs,binary:<out> proto/conductor.proto` |

Each stub file includes the exact command in its header.

## Wire notes

- `TaskRequest.payload` and `TaskResponse.result` are **JSON-encoded bytes** —
  no language-specific serialization needed.
- `ProcessTask` executes through the worker's registered handler and returns
  `success` + `result` (or `error`). Set `persist=true` to also record the
  outcome in `conductor_tasks`.
- `RegisterHandler` declares a handler for a `task_type` (idempotent).
- `GetWorkerStatus` returns worker health and statistics.
