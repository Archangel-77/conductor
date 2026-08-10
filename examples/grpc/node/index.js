// Reference gRPC client for Conductor's ConductorWorker service (Node.js).
//
// Reference-only stub using @grpc/grpc-js. Generate client stubs from
// proto/conductor.proto (see examples/grpc/README.md), then dial a worker
// started with GRPC_ENABLED=true:
//
//   grpc_tools_node_protoc -I proto \
//     --grpc_out=grpc_js:./examples/grpc/node/generated \
//     --js_out=import_style=commonjs,binary:./examples/grpc/node/generated \
//     proto/conductor.proto

const grpc = require('@grpc/grpc-js');
const {
  ConductorWorkerClient,
} = require('./generated/conductor_grpc_pb');
const { TaskRequest, StatusRequest } = require('./generated/conductor_pb');

async function main() {
  const client = new ConductorWorkerClient(
    'localhost:50051',
    grpc.credentials.createInsecure()
  );

  // ProcessTask — execute a task through the worker's registered handler.
  const req = new TaskRequest();
  req.setTaskType('echo');
  req.setPayload(Buffer.from(JSON.stringify({ message: 'hello' })));
  client.processTask(req, (err, resp) => {
    if (err) {
      console.error('ProcessTask failed:', err.message);
      return;
    }
    console.log('ProcessTask:', resp.getSuccess(), resp.getResult().toString());
  });

  // GetWorkerStatus — read worker health and statistics.
  client.getWorkerStatus(new StatusRequest(), (err, status) => {
    if (err) {
      console.error('GetWorkerStatus failed:', err.message);
      return;
    }
    console.log('GetWorkerStatus:', status.getWorkerId(), status.getStatus());
  });
}

main();
