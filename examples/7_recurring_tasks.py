"""
Example 7 — Recurring (cron) tasks.

Demonstrates cron-driven recurring tasks:

1. ``queue.schedule_recurring(task_type, payload, cron_expression)`` registers
   a definition in ``conductor_recurring_tasks``.
2. The ``RecurringScheduler`` polls for due definitions and creates one task
   instance per fire, advancing ``next_run_at`` to the next cron time (UTC,
   skipping missed runs).
3. A ``Worker`` executes the fired instances.

To keep the example deterministic, it force-advances ``next_run_at`` into the
past so the first sweep fires immediately.  In production the scheduler runs
continuously (``scheduler.run()``) and fires automatically when due.

Expected output (paraphrased)::

    Scheduled recurring 'cleanup' (cron='* * * * *'): <rid>
    Executed instance: status=completed, result={'deleted_old_records': 30}
    Definition advanced to: <next_run_at>

Run (PostgreSQL must be reachable)::

    python examples/7_recurring_tasks.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from typing import Any

from conductor import RecurringScheduler, TaskQueue, Worker
from conductor.core.models import utc_now

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")


async def main() -> None:
    async with TaskQueue(database_url=DB_URL) as queue:
        # 1. Register a recurring definition (fires every minute at :00).
        rid = await queue.schedule_recurring(
            "cleanup",
            {"keep_days": 30},
            "* * * * *",
            route="default",
        )
        print(f"Scheduled recurring 'cleanup' (cron='* * * * *'): {rid}")

        # Force it due immediately so this example runs without waiting.
        now = utc_now()
        await queue.query_builder.update_recurring_run(
            rid,
            last_run_at=now,
            next_run_at=now - timedelta(seconds=5),
        )

        # 2. Run the scheduler once: it fires a task instance for the due
        #    definition and advances next_run_at to the next cron fire.
        scheduler = RecurringScheduler(database_url=DB_URL)
        await scheduler.connect()
        try:
            await scheduler.run_once()
        finally:
            await scheduler.disconnect()

        # 3. A worker polls and executes the fired instance.
        async with Worker(
            database_url=DB_URL,
            worker_id="example-recurring-worker",
        ) as worker:

            @worker.task("cleanup")
            async def cleanup(payload: dict[str, Any]) -> dict[str, Any]:
                return {"deleted_old_records": payload["keep_days"]}

            await worker.run_once()

        # 4. Inspect the executed instance and the advanced definition.
        completed = await queue.list_completed_tasks()
        fired = [t for t in completed if t.task_type == "cleanup"]
        if fired:
            task = fired[0]
            print(f"Executed instance: status={task.status.value}, result={task.result}")
        else:
            print("No fired instance found yet.")

        rt = await queue.get_recurring_task(rid)
        if rt is not None:
            print(f"Definition advanced to: {rt.next_run_at.isoformat()}")


if __name__ == "__main__":
    asyncio.run(main())
