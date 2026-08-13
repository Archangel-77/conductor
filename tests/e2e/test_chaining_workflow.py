"""
End-to-end tests for task chaining / dependencies.

Covers the happy-path chain (A completes → B runs) and dependency-failure
propagation (A fails → B and C become blocked), through a live worker.

These tests require a running PostgreSQL instance (see ``docker-compose.yml``).
They are skipped automatically if the database is unreachable.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import RetryPolicy, TaskStatus
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.asyncio(loop_scope="module"),
]


# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="task_queue")
async def _task_queue_factory() -> Any:
    """Create a TaskQueue connected to the test database."""
    from tests.conftest import TEST_DATABASE_URL, db_available

    if not db_available():
        pytest.skip("Test database not available")

    q = TaskQueue(
        database_url=TEST_DATABASE_URL,
        pool_min_size=1,
        pool_max_size=2,
        pool_timeout=5.0,
        command_timeout=10.0,
    )
    try:
        await q.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    yield q

    await q.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def _cleanup(task_queue: Any) -> Any:
    """Clean all conductor tables after each test."""
    yield
    if task_queue.is_connected:
        await task_queue.execute_raw("DELETE FROM conductor_retries")
        await task_queue.execute_raw("DELETE FROM conductor_dead_letter")
        await task_queue.execute_raw("DELETE FROM conductor_tasks")
        await task_queue.execute_raw("DELETE FROM conductor_workers")
        await task_queue.execute_raw("DELETE FROM conductor_recurring_tasks")


# ===================================================================
# Chaining workflows
# ===================================================================


class TestChainingWorkflow:

    async def test_chain_executes_in_order(self, task_queue: Any) -> None:
        """A dependent task runs only after its dependency completes."""
        from tests.conftest import TEST_DATABASE_URL

        async with Worker(
            database_url=TEST_DATABASE_URL,
            worker_id="e2e-chain-happy",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("chain.step")
            async def step(payload: dict[str, Any]) -> dict[str, Any]:
                return {"processed": payload["n"]}

            a = await task_queue.submit("chain.step", {"n": 1})
            b = await task_queue.submit("chain.step", {"n": 2}, depends_on=[a])

            await worker.run_once()
            await worker.run_once()

            task_b = await task_queue.get_task(b)
            assert task_b is not None
            assert task_b.status == TaskStatus.COMPLETED
            assert task_b.result == {"processed": 2}

    async def test_dependency_failure_blocks_chain(self, task_queue: Any) -> None:
        """A failing dependency blocks the rest of the chain."""
        from tests.conftest import TEST_DATABASE_URL

        state: dict[str, bool] = {"fail": True}

        async with Worker(
            database_url=TEST_DATABASE_URL,
            worker_id="e2e-chain-block",
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("chain.step")
            async def step(_payload: dict[str, Any]) -> dict[str, Any]:
                if state["fail"]:
                    raise ValueError("down")
                return {"ok": True}

            a = await task_queue.submit(
                "chain.step",
                {},
                retry_policy=RetryPolicy(max_retries=0),
            )
            b = await task_queue.submit(
                "chain.step",
                {},
                depends_on=[a],
                retry_policy=RetryPolicy(max_retries=0),
            )

            await worker.run_once()

            task_b = await task_queue.get_task(b)
            assert task_b is not None
            assert task_b.status == TaskStatus.BLOCKED
            assert task_b.error_message is not None and "dependency" in task_b.error_message
