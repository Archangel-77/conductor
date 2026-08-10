"""
Example 6 — Task routing & priority queues.

Demonstrates two v0.2 features:

1. **Routing** — submit tasks to named routes and run route-specific
   workers.  A worker only polls tasks on the routes it subscribes to
   (``routes=[...]``); pass ``routes=None`` to poll *all* routes.
2. **Priority** — tasks with a higher ``priority`` (range -100..100) are
   polled and executed before lower-priority ones.

Expected output (paraphrased)::

    Submitted 'send_sms' to route 'critical': <id>
    Submitted 'process_batch' to route 'batch': <id>
    Submitted 'send_sms' to route 'batch': <id>
    Critical worker processed: ['send_sms']
    Batch worker processed: ['process_batch', 'send_sms']
    Priority execution order (high -> low): [1, 2, 3]

Run (PostgreSQL must be reachable)::

    python examples/6_routing_priority.py
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from conductor import TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")


async def main() -> None:
    async with TaskQueue(database_url=DB_URL) as queue:
        # ------------------------------------------------------------------
        # Part 1 — Routing: workers subscribe to specific routes.
        # ------------------------------------------------------------------
        critical_id = await queue.submit("send_sms", {"to": "+1-555-0100"}, route="critical")
        batch_id_1 = await queue.submit("process_batch", {"rows": 10_000}, route="batch")
        batch_id_2 = await queue.submit("send_sms", {"to": "+1-555-0199"}, route="batch")
        print(f"Submitted 'send_sms' to route 'critical': {critical_id}")
        print(f"Submitted 'process_batch' to route 'batch': {batch_id_1}")
        print(f"Submitted 'send_sms' to route 'batch': {batch_id_2}")

        # A worker with routes=["critical"] only picks up critical-route tasks.
        async with Worker(
            database_url=DB_URL,
            worker_id="worker-critical",
            routes=["critical"],
        ) as worker:
            critical_handled: list[str] = []

            @worker.task("send_sms")
            async def send_sms(_payload: dict[str, Any]) -> dict[str, Any]:
                critical_handled.append("send_sms")
                return {"sent": True}

            await worker.run_once()

        print(f"Critical worker processed: {critical_handled}")

        # A worker with routes=["batch"] only picks up batch-route tasks.
        async with Worker(
            database_url=DB_URL,
            worker_id="worker-batch",
            routes=["batch"],
        ) as worker:
            batch_handled: list[str] = []

            @worker.task("send_sms")
            async def batch_sms(_payload: dict[str, Any]) -> dict[str, Any]:
                batch_handled.append("send_sms")
                return {"sent": True}

            @worker.task("process_batch")
            async def process_batch(_payload: dict[str, Any]) -> dict[str, Any]:
                batch_handled.append("process_batch")
                return {"processed": True}

            await worker.run_once()

        # Order within the batch is by priority DESC, created_at ASC.
        print(f"Batch worker processed: {sorted(batch_handled)}")

        # ------------------------------------------------------------------
        # Part 2 — Priority: higher priority executes first.
        # ------------------------------------------------------------------
        execution_order: list[int] = []

        async with Worker(
            database_url=DB_URL,
            worker_id="worker-priority",
            routes=["priority"],
        ) as worker:

            @worker.task("priority_task")
            async def priority_task(payload: dict[str, Any]) -> dict[str, Any]:
                execution_order.append(payload["rank"])
                return {"ok": True}

            # Submit out of order with distinct priorities: the worker should
            # still execute rank 1 (priority 50) first and rank 3 (-10) last.
            await queue.submit("priority_task", {"rank": 3}, route="priority", priority=-10)
            await queue.submit("priority_task", {"rank": 1}, route="priority", priority=50)
            await queue.submit("priority_task", {"rank": 2}, route="priority", priority=0)

            await worker.run_once()

        print(f"Priority execution order (high -> low): {execution_order}")


if __name__ == "__main__":
    asyncio.run(main())
