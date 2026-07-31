"""
Example 4 — Scheduled cleanup (manual scheduling).

Shows the scheduled-task pattern: a task with a future ``scheduled_for``
is not polled until its time arrives. Native cron/scheduling is a v0.2
feature; for v0.1, schedule with ``scheduled_for`` and trigger
submissions from an external cron job / systemd timer.

Expected output (paraphrased)::

    Submitted cleanup task scheduled for <ts>
    Before due time: status=pending (not polled)
    ...waiting for the scheduled time...
      deleting expired sessions (batch_size=100)
    After due time: status=completed, result={'deleted': 3}

Run::

    python examples/4_scheduled_cleanup.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from conductor import TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")


async def main() -> None:
    scheduled = datetime.now(timezone.utc) + timedelta(seconds=3)

    async with TaskQueue(database_url=DB_URL) as queue:
        task_id = await queue.submit(
            "cleanup_expired_sessions",
            {"batch_size": 100},
            scheduled_for=scheduled,
        )
        print(f"Submitted cleanup task scheduled for {scheduled.isoformat()}")

        async with Worker(database_url=DB_URL, worker_id="example-schedule-worker") as worker:

            @worker.task("cleanup_expired_sessions")
            async def cleanup_expired_sessions(payload: dict[str, Any]) -> dict[str, Any]:
                # In a real app this would run SQL, e.g.:
                #   DELETE FROM sessions WHERE expires_at < NOW()
                print(f"  deleting expired sessions (batch_size={payload['batch_size']})")
                return {"deleted": 3}

            # Before due: the task must NOT be picked up.
            await worker.run_once()
            before = await queue.get_task(task_id)
            assert before is not None
            print(f"Before due time: status={before.status.value} (not polled)")

            print("...waiting for the scheduled time...")
            await asyncio.sleep(3.2)

            # After due: the task is picked up and executed.
            await worker.run_once()
            after = await queue.get_task(task_id)
            assert after is not None
            print(f"After due time: status={after.status.value}, result={after.result}")

        # For recurring runs, submit on a schedule from cron/systemd:
        #   0 2 * * *  conductor-demo-submit  (see examples/README.md)
        print(
            "Tip: for recurring runs, trigger submissions from cron or a "
            "systemd timer (native cron is planned for v0.2)."
        )


if __name__ == "__main__":
    asyncio.run(main())
