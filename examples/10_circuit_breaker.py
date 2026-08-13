"""
Example 10 — Circuit Breaker.

Demonstrates the per-task-type circuit breaker: a Worker with
``circuit_breaker_enabled=True`` stops executing a failing task type after
``threshold`` consecutive failures, skips it while the circuit is open (tasks
stay pending), and recovers via a half-open probe once the handler starts
succeeding again.

Run (PostgreSQL must be reachable)::

    python examples/10_circuit_breaker.py

Expected output (paraphrased)::

    circuit_breaker_enabled=True
    circuit_breaker_open=['external.call']
    after recovery: circuit_breaker_open=[]
    abc12345 -> retrying
    ...       -> retrying
    ...       -> completed
    ...       -> completed
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from conductor import CircuitBreakerConfig, TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")

# Toggle whether the downstream service is healthy.
state: dict[str, bool] = {"healthy": False}


async def main() -> None:
    queue = TaskQueue(database_url=DB_URL, pool_min_size=1, pool_max_size=2)
    await queue.connect()

    async with Worker(
        database_url=DB_URL,
        worker_id="circuit-breaker-example",
        pool_min_size=1,
        pool_max_size=2,
        circuit_breaker_enabled=True,
        circuit_breaker_config=CircuitBreakerConfig(
            threshold=3,
            timeout=1.0,
            half_open_attempts=1,
        ),
    ) as worker:

        @worker.task("external.call")
        async def call_external(payload: dict[str, Any]) -> dict[str, Any]:
            if not state["healthy"]:
                raise RuntimeError("downstream service is down")
            return {"ok": True, **payload}

        # Submit tasks while the downstream is down.  After 3 consecutive
        # failures the circuit opens and later tasks are skipped (pending).
        task_ids: list[str] = []
        for attempt in range(5):
            task_id = await queue.submit("external.call", {"attempt": attempt})
            task_ids.append(task_id)
            await worker.run_once()

        status = worker.get_status()
        print(f"circuit_breaker_enabled={status['circuit_breaker_enabled']}")
        print(f"circuit_breaker_open={status['circuit_breaker_open']}")

        # Fix the service and wait for the timeout; the next run is a
        # half-open probe which succeeds and closes the circuit.
        state["healthy"] = True
        await asyncio.sleep(1.2)
        await worker.run_once()

        status = worker.get_status()
        print(f"after recovery: circuit_breaker_open={status['circuit_breaker_open']}")

        # Drain any remaining pending tasks.
        for _ in range(10):
            await worker.run_once()

    for task_id in task_ids:
        task = await queue.get_task(task_id)
        if task is not None:
            print(f"{task_id[:8]} -> {task.status}")

    await queue.disconnect()
    print(
        "Breaker state is also reported by get_status() and via the "
        "conductor_circuit_breaker_open metric."
    )


if __name__ == "__main__":
    asyncio.run(main())
