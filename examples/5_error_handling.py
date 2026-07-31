"""
Example 5 — Error handling, custom exceptions, and idempotency.

Shows robust handler patterns:
- custom exception types to distinguish retryable vs. permanent failures
- a retry policy with backoff
- an idempotency guard (skip work if already processed)
- dead-letter queue recovery (exhausted retries -> DLQ -> manual retry)

Expected output (paraphrased)::

    Submitted order task: <id1>
    After attempt 1: status=retrying, error=Simulated transient DB hiccup
      charging payment for ord-1 ...
    After attempt 2: status=completed, result={'order_id': 'ord-1', 'status': 'success', ...}
    Submitted duplicate order: <id2>
    Idempotency: ord-1 already processed -> no-op
    Duplicate result: {'order_id': 'ord-1', 'status': 'already_processed'}
    Submitted failing task: <id3>
    Exhausted retries -> DLQ: error=Permanent payment failure
    Retried via DeadLetterQueue: <id3> -> pending again

Run::

    python examples/5_error_handling.py
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from typing import Any

from conductor import DeadLetterQueue, RetryPolicy, TaskQueue, Worker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")

# In-memory record of processed order IDs, used by the idempotency guard.
processed_orders: set[str] = set()


class TransientError(Exception):
    """A retryable, temporary failure."""


class PaymentError(Exception):
    """A permanent payment failure."""


async def charge_payment(order_id: str) -> dict[str, Any]:
    """Charge a payment (mocked)."""
    print(f"  charging payment for {order_id} ...")
    return {"payment_id": f"pay-{order_id}"}


async def main() -> None:
    async with TaskQueue(database_url=DB_URL) as queue:
        # --- Part 1: transient failure -> retry -> success ---
        order_id = await queue.submit(
            "process_order",
            {"order_id": "ord-1"},
            retry_policy=RetryPolicy(max_retries=3, backoff_strategy="exponential"),
        )
        print(f"Submitted order task: {order_id}")

        attempt = 0

        async with Worker(database_url=DB_URL, worker_id="example-err-worker") as worker:

            @worker.task("process_order")
            async def process_order(payload: dict[str, Any]) -> dict[str, Any]:
                nonlocal attempt
                oid = payload["order_id"]
                attempt += 1

                # Idempotency guard: skip if we already processed this order.
                if oid in processed_orders:
                    print(f"  idempotency: {oid} already processed -> no-op")
                    return {"order_id": oid, "status": "already_processed"}

                if attempt == 1:
                    raise TransientError("Simulated transient DB hiccup")

                payment = await charge_payment(oid)
                processed_orders.add(oid)
                return {"order_id": oid, "status": "success", **payment}

            # First pass: raises TransientError -> scheduled for retry.
            await worker.run_once()

            task = await queue.get_task(order_id)
            assert task is not None
            print(f"After attempt 1: status={task.status.value}, " f"error={task.error_message}")

            # Advance the retry so the next poll picks it up immediately.
            await queue.query_builder.update_task_status(
                order_id,
                "pending",
                attempt=1,
                scheduled_for=datetime.now(timezone.utc),
            )

            # Second pass: succeeds.
            await worker.run_once()

            task = await queue.get_task(order_id)
            assert task is not None
            print(f"After attempt 2: status={task.status.value}, result={task.result}")

            # Idempotency: submit the same order again — it must NOT charge twice.
            dup_id = await queue.submit("process_order", {"order_id": "ord-1"})
            print(f"Submitted duplicate order: {dup_id}")
            await worker.run_once()
            dup = await queue.get_task(dup_id)
            assert dup is not None
            print(f"Duplicate result: {dup.result}")

        # --- Part 2: exhausted retries -> DLQ -> recover via DeadLetterQueue ---
        failing_id = await queue.submit(
            "always_fail",
            {"n": 1},
            retry_policy=RetryPolicy(max_retries=0),  # fail immediately -> DLQ
        )
        async with Worker(database_url=DB_URL, worker_id="example-err-worker-2") as worker2:

            @worker2.task("always_fail")
            async def always_fail(_payload: dict[str, Any]) -> dict[str, Any]:
                raise PaymentError("Permanent payment failure")

            await worker2.run_once()

        async with DeadLetterQueue(database_url=DB_URL) as dlq:
            dlq_task = await dlq.get_task(failing_id)
            assert dlq_task is not None
            print(f"Exhausted retries -> DLQ: error={dlq_task.error_message}")
            recovered = await dlq.retry_task(failing_id)
            print(f"Retried via DeadLetterQueue: {recovered} -> pending again")


if __name__ == "__main__":
    asyncio.run(main())
