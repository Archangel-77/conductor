"""Verify a task executes end-to-end through the Docker Compose worker.

Submits a ``qa_echo`` task to the compose stack's PostgreSQL (mapped to
localhost:5432) and waits for the worker container to complete it.

Exit code is ``0`` on success, ``1`` on failure or timeout.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys

from conductor import TaskQueue

DATABASE_URL = os.environ.get(
    "COMPOSE_TEST_DATABASE_URL",
    "postgresql://conductor:conductor@localhost:5432/conductor",
)
POLL_SECONDS = 2
TIMEOUT_SECONDS = 120


async def main() -> int:
    """Submit a task and wait for it to complete."""
    async with TaskQueue(database_url=DATABASE_URL) as queue:
        task_id = await queue.submit("qa_echo", {"n": 42})
        print(f"submitted {task_id}")

        for _ in range(TIMEOUT_SECONDS // POLL_SECONDS):
            task = await queue.get_task(task_id)
            if task is not None and task.status == "completed":
                print("completed:", json.dumps(task.result))
                if task.result == {"echo": {"n": 42}}:
                    return 0
                print("unexpected result", file=sys.stderr)
                return 1
            await asyncio.sleep(POLL_SECONDS)

    print("task did not complete in time", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
