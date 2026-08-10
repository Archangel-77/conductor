// Reference gRPC client for Conductor's ConductorWorker service (Rust).
//
// Reference-only stub using tonic. Generate client stubs from
// proto/conductor.proto (see examples/grpc/README.md), then dial a worker
// started with GRPC_ENABLED=true:
//
//   tonic_build::configure().compile(&["proto/conductor.proto"], &["proto"]);
//
// Adjust the generated module path below.
use tonic::transport::Channel;

pub mod conductor {
    tonic::include_proto!("conductor");
}

use conductor::conductor_worker_client::ConductorWorkerClient;
use conductor::{StatusRequest, TaskRequest};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static("http://localhost:50051").connect().await?;
    let mut client = ConductorWorkerClient::new(channel);

    // ProcessTask — execute a task through the worker's registered handler.
    let resp = client
        .process_task(TaskRequest {
            task_id: String::new(),
            task_type: "echo".to_string(),
            payload: br#"{"message":"hello"}"#.to_vec(),
            persist: false,
        })
        .await?
        .into_inner();
    println!(
        "ProcessTask: success={} result={:?}",
        resp.success,
        String::from_utf8_lossy(&resp.result)
    );

    // GetWorkerStatus — read worker health and statistics.
    let status = client
        .get_worker_status(StatusRequest {})
        .await?
        .into_inner();
    println!(
        "GetWorkerStatus: worker_id={} status={}",
        status.worker_id, status.status
    );
    Ok(())
}
