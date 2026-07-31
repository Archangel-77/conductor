"""
Example 1 — Basic queue: submit and execute.

The simplest Conductor workflow: submit a task, run a worker once to
process it, and inspect the stored result.

Expected output (paraphrased)::

    Submitted task: <task_id>
    Task status: completed
    Result: {'greeting': 'Hello, Conductor!'}
    Processed by worker: <hostname>-<pid>
    started_at: <ts>  completed_at: <ts>

Run (PostgreSQL must be reachable)::

    python examples/1_basic_queue.py
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from conductor import TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")


async def main() -> None:
    # 1. Submit a task.
    async with TaskQueue(database_url=DB_URL) as queue:
        task_id = await queue.submit("greet", {"name": "Conductor"})
        print(f"Submitted task: {task_id}")

        # 2. Run a worker once to poll and execute it. run_once() performs a
        #    single poll-and-execute cycle, so the script exits cleanly.
        async with Worker(database_url=DB_URL, worker_id="example-basic-worker") as worker:

            @worker.task("greet")
            async def greet(payload: dict[str, Any]) -> dict[str, Any]:
                return {"greeting": f"Hello, {payload['name']}!"}

            await worker.run_once()

        # 3. Inspect the stored result.
        task = await queue.get_task(task_id)
        assert task is not None
        print(f"Task status: {task.status.value}")
        print(f"Result: {task.result}")
        print(f"Processed by worker: {task.worker_id}")
        print(f"started_at: {task.started_at}  completed_at: {task.completed_at}")


if __name__ == "__main__":
    asyncio.run(main())
