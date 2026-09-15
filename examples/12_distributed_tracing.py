"""
Example 12 — Distributed tracing with OpenTelemetry.

Demonstrates the tracing added in v0.3:

1. Install a tracer provider (console exporter here, so no collector is needed).
2. Submit a task and watch the ``conductor.task.submit`` span.
3. Run a worker and watch ``conductor.task.execute`` continue the *same trace* —
   the trace context travels through the database, so this works across
   processes too.
4. See a failing task produce an ERROR span with a ``retry.scheduled`` event,
   and a blocked dependent produce its own span.

Run it with::

    pip install "conductor-task-queue[otel]"
    python examples/12_distributed_tracing.py

Expected output: console span JSON for submit/execute spans (the execute span
carries the submit span's trace id and parent span id), followed by a summary
printed by the script itself.
"""

import asyncio
import os

from conductor import RetryPolicy, TaskQueue, Worker
from conductor.observability import tracing

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor"
)

TASK_QUEUE_ROUTE = "tracing-demo"


async def cleanup(queue: TaskQueue) -> None:
    """Remove rows from previous runs of this example."""
    for table in (
        "conductor_retries",
        "conductor_dead_letter",
        "conductor_tasks",
        "conductor_workers",
        "conductor_recurring_tasks",
    ):
        await queue._pool.execute(f"DELETE FROM {table}")


async def main() -> None:
    # 1. Enable tracing. "console" prints spans as they finish; use
    #    exporter="otlp" + endpoint=... to ship them to a collector instead.
    tracing.setup_tracing(
        tracing.TracingConfig(
            enabled=True,
            exporter="console",
            service_name="conductor-tracing-demo",
            sample_ratio=1.0,
        )
    )
    print("tracing backend:", tracing.get_backend_name())

    async with TaskQueue(DATABASE_URL) as queue:
        await cleanup(queue)

        # 2. Submit: one producer span, and the trace context is persisted on
        #    the task row (schema v6).
        task_id = await queue.submit("send_email", {"to": "user@example.com"})
        task = await queue.get_task(task_id)
        assert task is not None
        print("\nsubmitted task:", task_id)
        print("stored traceparent:", task.traceparent)

        # 3. Execute: the worker's span continues that trace.
        async with Worker(DATABASE_URL, worker_id="tracing-demo") as worker:

            @worker.task("send_email")
            async def send_email(payload: dict) -> dict:
                return {"sent_to": payload["to"]}

            @worker.task("flaky")
            async def flaky(payload: dict) -> dict:
                raise RuntimeError("simulated upstream failure")

            await worker.run_once()
            executed = await queue.get_task(task_id)
            assert executed is not None
            print("after execution:", executed.status.value)

            # 4. A failure schedules a retry (span event) and a dependent task
            #    is blocked with its own span when retries are exhausted.
            failing = await queue.submit(
                "flaky",
                {},
                route=TASK_QUEUE_ROUTE,
                retry_policy=RetryPolicy(max_retries=1, initial_delay=0.01),
            )
            blocked = await queue.submit("send_email", {}, depends_on=[failing])

            await worker.run_once()
            failed_task = await queue.get_task(failing)
            assert failed_task is not None
            print("after first failure:", failed_task.status.value)

            await asyncio.sleep(0.05)  # wait out the backoff delay
            await worker.run_once()
            failed_task = await queue.get_task(failing)
            assert failed_task is not None
            print("after retry exhausted:", failed_task.status.value)

            blocked_task = await queue.get_task(blocked)
            assert blocked_task is not None
            print("dependent task:", blocked_task.status.value)

        # The worker is gone; make sure buffered spans are flushed.
        tracing.shutdown_tracing()

    print("\nDone. Each execute span above shares the submit span's trace id.")


if __name__ == "__main__":
    asyncio.run(main())
