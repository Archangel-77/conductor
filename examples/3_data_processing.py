"""
Example 3 — Multi-step data processing pipeline.

Processes an uploaded file through a pipeline (download -> process ->
store) and then chains a follow-up notification task. Native task
chaining is a v0.2 feature; this example shows the manual pattern — a
handler submits the next task before returning.

Expected output (paraphrased)::

    Submitted process_upload task: <id1>
      downloading file_123 ...
      processing chunks ...
      storing result for file_123 ...
      chained send_notification task: <id2>
    process_upload status: completed, result: {'file_id': 'file_123', 'status': 'processed', ...}
    send_notification status: completed

Run::

    python examples/3_data_processing.py
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

from conductor import TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")


async def download_file(file_id: str) -> list[bytes]:
    """Fetch the file (mocked here — no real I/O)."""
    print(f"  downloading {file_id} ...")
    return [b"chunk-1", b"chunk-2"]


async def process_data(chunks: list[bytes]) -> dict[str, Any]:
    """Process the file (mocked)."""
    print("  processing chunks ...")
    return {"summary": f"processed {len(chunks)} chunks"}


async def store_result(file_id: str, result: dict[str, Any]) -> None:
    """Persist the result (mocked)."""
    summary = result.get("summary", "n/a")
    print(f"  storing result for {file_id}: {summary}")


async def main() -> None:
    async with TaskQueue(database_url=DB_URL) as queue:
        first_id = await queue.submit(
            "process_upload",
            {"file_id": "file_123", "user_id": "u42"},
        )
        print(f"Submitted process_upload task: {first_id}")

        async with Worker(database_url=DB_URL, worker_id="example-data-worker") as worker:

            @worker.task("process_upload")
            async def process_upload(payload: dict[str, Any]) -> dict[str, Any]:
                file_id = payload["file_id"]
                chunks = await download_file(file_id)
                result = await process_data(chunks)
                await store_result(file_id, result)

                # Manual chaining: submit the next task before returning.
                async with TaskQueue(database_url=DB_URL) as queue_inner:
                    chained_id = await queue_inner.submit(
                        "send_notification",
                        {
                            "user_id": payload["user_id"],
                            "message": "Your file is ready",
                        },
                    )
                print(f"  chained send_notification task: {chained_id}")
                return {
                    "file_id": file_id,
                    "status": "processed",
                    "summary": result["summary"],
                }

            @worker.task("send_notification")
            async def send_notification(payload: dict[str, Any]) -> dict[str, Any]:
                print(f"  notifying user {payload['user_id']}: {payload['message']}")
                return {"notified": payload["user_id"]}

            # First pass processes process_upload (and chains the notification).
            await worker.run_once()
            # Second pass processes the chained notification task.
            await worker.run_once()

        first = await queue.get_task(first_id)
        assert first is not None
        print(f"process_upload status: {first.status.value}, result: {first.result}")

        for task in await queue.list_completed_tasks(limit=10):
            if task.task_type == "send_notification":
                print(f"send_notification status: {task.status.value}")


if __name__ == "__main__":
    asyncio.run(main())
