"""
Example 2 — Email notifications with retry.

Sends an email with exponential-backoff retry. A transient network
failure on the first attempt demonstrates the retry workflow: the task
is scheduled again and succeeds on the second attempt.

If ``SENDGRID_API_KEY`` is set, the handler posts to the SendGrid v3 API
with aiohttp; otherwise a local mock transport is used, so the example
runs with no external credentials.

Expected output (paraphrased)::

    Submitted email task: <task_id>
    Attempt 1 failed (transient network error) -> task status: retrying
    Attempt 2 succeeded -> task status: completed
    Result: {'status': 'sent', 'to': 'user@example.com'}

Run::

    python examples/2_email_notifications.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any

from conductor import RetryPolicy, TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")


async def _send_via_sendgrid(to: str, subject: str, body: str) -> None:
    """POST the email to the SendGrid API using aiohttp."""
    import aiohttp

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.sendgrid.com/v3/mail/send",
            json={
                "personalizations": [{"to": [{"email": to}]}],
                "from": {"email": "noreply@example.com"},
                "subject": subject,
                "content": [{"type": "text/html", "value": body}],
            },
            headers={"Authorization": f"Bearer {SENDGRID_API_KEY}"},
        ) as resp:
            resp.raise_for_status()


async def main() -> None:
    attempt = 0

    async with TaskQueue(database_url=DB_URL) as queue:
        task_id = await queue.submit(
            "send_email",
            {
                "to": "user@example.com",
                "subject": "Welcome!",
                "body": "<h1>Thanks for signing up</h1>",
            },
            retry_policy=RetryPolicy(max_retries=3, backoff_strategy="exponential"),
        )
        print(f"Submitted email task: {task_id}")

        async with Worker(database_url=DB_URL, worker_id="example-email-worker") as worker:

            @worker.task("send_email")
            async def send_email(payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal attempt
                attempt += 1
                # Simulate a transient network failure on the first attempt.
                if attempt == 1:
                    raise ConnectionError("Simulated transient network failure")

                to = payload["to"]
                subject = payload["subject"]
                body = payload["body"]
                if SENDGRID_API_KEY:
                    await _send_via_sendgrid(to, subject, body)
                else:
                    print(f"  [mock transport] email to {to}: {subject}")
                return {"status": "sent", "to": to}

            # First pass: fails, task is scheduled for retry.
            await worker.run_once()

            task = await queue.get_task(task_id)
            assert task is not None
            print(f"Attempt 1 failed -> task status: {task.status.value}, attempt={task.attempt}")

            # Advance the retry so the next poll picks it up immediately.
            await queue.query_builder.update_task_status(
                task_id,
                "pending",
                attempt=1,
                scheduled_for=datetime.now(timezone.utc),
            )

            # Second pass: succeeds.
            await worker.run_once()

        task = await queue.get_task(task_id)
        assert task is not None
        print(f"Attempt 2 succeeded -> task status: {task.status.value}")
        print(f"Result: {task.result}")


if __name__ == "__main__":
    asyncio.run(main())
