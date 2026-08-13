"""
Example 11 — Task Dependencies & Chaining.

Demonstrates native task chaining: ``submit(..., depends_on=[...])`` runs a
dependent task only after its dependencies complete, and marks dependents
``blocked`` (transitively) when a dependency fails.

Run (PostgreSQL must be reachable)::

    python examples/11_task_chaining.py

Expected output (paraphrased)::

    a -> completed
    b -> completed   (ran only after a completed)
    bad -> failed
    blocked_by_bad -> blocked   (dependency 'bad' failed)
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from conductor import RetryPolicy, TaskQueue, Worker
from conductor.core.models import TaskStatus

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")


async def main() -> None:
    queue = TaskQueue(database_url=DB_URL, pool_min_size=1, pool_max_size=2)
    await queue.connect()

    state: dict[str, bool] = {"fail": False}

    async with Worker(
        database_url=DB_URL,
        worker_id="chaining-example",
        pool_min_size=1,
        pool_max_size=2,
    ) as worker:

        @worker.task("pipeline.step")
        async def step(payload: dict[str, Any]) -> dict[str, Any]:
            if state["fail"]:
                raise RuntimeError("upstream failed")
            return {"processed": payload["n"]}

        # Happy chain: b runs only after a completes.
        a = await queue.submit("pipeline.step", {"n": 1})
        b = await queue.submit("pipeline.step", {"n": 2}, depends_on=[a])
        await worker.run_once()
        await worker.run_once()

        # Failure chain: f fails -> g (its dependent) is blocked.
        state["fail"] = True
        f = await queue.submit(
            "pipeline.step",
            {"n": -1},
            retry_policy=RetryPolicy(max_retries=0),
        )
        g = await queue.submit(
            "pipeline.step",
            {"n": -2},
            depends_on=[f],
            retry_policy=RetryPolicy(max_retries=0),
        )
        await worker.run_once()

    for task_id in (a, b, f, g):
        task = await queue.get_task(task_id)
        if task is not None:
            print(f"{task_id[:8]} -> {task.status}")
            if task.status is TaskStatus.BLOCKED:
                print(f"    error: {task.error_message}")

    await queue.disconnect()
    print("Done. Chaining gates execution; failed dependencies mark dependents blocked.")


if __name__ == "__main__":
    asyncio.run(main())
