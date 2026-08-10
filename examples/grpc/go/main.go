// Reference gRPC client for Conductor's ConductorWorker service (Go).
//
// Reference-only stub. Generate client stubs from proto/conductor.proto
// (requires protoc-gen-go and protoc-gen-go-grpc), then dial a worker that
// was started with GRPC_ENABLED=true:
//
//   protoc -I proto --go_out=. --go-grpc_out=. proto/conductor.proto
//
// Adjust the import path below to wherever you generated the stubs.
package main

import (
	"context"
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "example.com/conductor/grpc"
)

func main() {
	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewConductorWorkerClient(conn)

	// ProcessTask — execute a task through the worker's registered handler.
	resp, err := client.ProcessTask(context.Background(), &pb.TaskRequest{
		TaskType: "echo",
		Payload:  []byte(`{"message":"hello"}`),
	})
	if err != nil {
		log.Fatalf("ProcessTask failed: %v", err)
	}
	fmt.Printf("ProcessTask: success=%v result=%s\n", resp.Success, resp.Result)

	// GetWorkerStatus — read worker health and statistics.
	status, err := client.GetWorkerStatus(context.Background(), &pb.StatusRequest{})
	if err != nil {
		log.Fatalf("GetWorkerStatus failed: %v", err)
	}
	fmt.Printf("GetWorkerStatus: worker_id=%s status=%s\n", status.WorkerId, status.Status)
}
