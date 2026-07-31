"""
Performance benchmarks for Conductor's core operations.

These benchmarks measure the v0.1 performance targets against a running
PostgreSQL instance (see ``docker-compose.yml``). They are skipped
automatically if the database is unreachable.

Targets (from TODO.md "Performance Targets (v0.1)"):
- Task submission: <2ms per task
- Polling latency: <500ms (dictated by poll_interval)
- Task execution (empty): <10ms
- Throughput: 400+ tasks/sec per worker
- Memory per worker: ~50MB base

Run them explicitly (coverage is disabled to avoid skewing timings)::

    pytest -m perf --no-cov -v

Thresholds can be relaxed via ``PERF_*`` environment variables for CI
(e.g. ``PERF_MAX_SUBMIT_MS=5``, ``PERF_MIN_THROUGHPUT=200``).
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

import asyncio
import os
import resource
import statistics
import sys
import time
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import Task
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.exceptions import ConductorConnectionError

pytestmark = [
    pytest.mark.perf,
    pytest.mark.asyncio(loop_scope="module"),
]

# ===================================================================
# Perf thresholds (env-overridable)
# ===================================================================

PERF_MAX_SUBMIT_MS = float(os.environ.get("PERF_MAX_SUBMIT_MS", "2.0"))
PERF_MAX_EXEC_MS = float(os.environ.get("PERF_MAX_EXEC_MS", "10.0"))
PERF_MAX_POLL_MS = float(os.environ.get("PERF_MAX_POLL_MS", "500.0"))
PERF_MIN_THROUGHPUT = float(os.environ.get("PERF_MIN_THROUGHPUT", "400"))
PERF_MAX_MEMORY_MB = float(os.environ.get("PERF_MAX_MEMORY_MB", "50.0"))

PERF_SAMPLES_SUBMIT = int(os.environ.get("PERF_SAMPLES_SUBMIT", "1000"))
PERF_SAMPLES_EXEC = int(os.environ.get("PERF_SAMPLES_EXEC", "200"))
PERF_SAMPLES_POLL = int(os.environ.get("PERF_SAMPLES_POLL", "20"))
PERF_SAMPLES_THROUGHPUT = int(os.environ.get("PERF_SAMPLES_THROUGHPUT", "300"))
PERF_SAMPLES_DB = int(os.environ.get("PERF_SAMPLES_DB", "50"))

# ===================================================================
# Fixtures
# ===================================================================


@pytest_asyncio.fixture(scope="module", loop_scope="module", name="task_queue")
async def _task_queue_factory() -> Any:
    """Create a TaskQueue connected to the test database (larger pool)."""
    from tests.conftest import TEST_DATABASE_URL, db_available

    if not db_available():
        pytest.skip("Test database not available")

    q = TaskQueue(
        database_url=TEST_DATABASE_URL,
        pool_min_size=2,
        pool_max_size=10,
        pool_timeout=5.0,
        command_timeout=30.0,
    )
    try:
        await q.connect()
    except ConductorConnectionError as exc:
        pytest.skip(f"Could not connect: {exc}")

    yield q

    await q.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="module")
async def _cleanup_test_data(task_queue: Any) -> Any:
    """Clean up all data after each test."""
    yield
    if task_queue.is_connected:
        await task_queue.execute_raw("DELETE FROM conductor_retries")
        await task_queue.execute_raw("DELETE FROM conductor_dead_letter")
        await task_queue.execute_raw("DELETE FROM conductor_tasks")
        await task_queue.execute_raw("DELETE FROM conductor_workers")


# ===================================================================
# Helpers
# ===================================================================


def _db_url() -> str:
    """Return the test database URL."""
    from tests.conftest import TEST_DATABASE_URL

    return TEST_DATABASE_URL


def _mean(values: list[float]) -> float:
    """Return the arithmetic mean of *values* (0.0 when empty)."""
    return statistics.mean(values) if values else 0.0


def _assert_avg_below(values: list[float], max_ms: float) -> float:
    """Assert the mean of *values* (milliseconds) is below *max_ms*.

    Returns the computed mean so callers can report it.
    """
    avg = _mean(values)
    assert avg < max_ms, f"avg {avg:.2f}ms >= limit {max_ms:.2f}ms"
    return avg


def _rss_mb() -> float:
    """Return the current process peak RSS in megabytes."""
    ru = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        return ru.ru_maxrss / (1024.0 * 1024.0)  # bytes -> MB
    return ru.ru_maxrss / 1024.0  # KB -> MB


async def _wait_for_completed_count(
    task_queue: Any,
    expected: int,
    timeout: float = 60.0,
) -> None:
    """Poll until *expected* tasks are completed in the queue."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        count = await task_queue.count_tasks_by_status("completed")
        if count >= expected:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Timed out waiting for {expected} completed tasks")


# ===================================================================
# Submission latency
# ===================================================================


class TestSubmissionLatency:
    """Benchmark TaskQueue.submit and submit_many."""

    async def test_single_submit_latency(self, task_queue: Any) -> None:
        """Mean single-submit latency should be below the target."""
        samples = PERF_SAMPLES_SUBMIT

        # Warm-up (pool + prepared statements).
        await task_queue.submit("perf_warmup", {"n": -1})

        timings_ms: list[float] = []
        for i in range(samples):
            start = time.perf_counter()
            await task_queue.submit("perf_submit", {"n": i})
            timings_ms.append((time.perf_counter() - start) * 1000.0)

        avg = _assert_avg_below(timings_ms, PERF_MAX_SUBMIT_MS)
        print(f"  single submit avg={avg:.3f}ms over {samples} samples")

    async def test_batch_submit_throughput(self, task_queue: Any) -> None:
        """Batch submit_many should stay below the per-task target."""
        total = PERF_SAMPLES_SUBMIT
        chunk_size = 250
        tasks: list[tuple[str, dict[str, Any]]] = [("perf_batch", {"n": i}) for i in range(total)]

        start = time.perf_counter()
        for i in range(0, total, chunk_size):
            await task_queue.submit_many(tasks[i : i + chunk_size])
        elapsed = time.perf_counter() - start

        per_task_ms = (elapsed / total) * 1000.0
        assert (
            per_task_ms < PERF_MAX_SUBMIT_MS
        ), f"batch per-task avg {per_task_ms:.2f}ms >= limit {PERF_MAX_SUBMIT_MS:.2f}ms"
        print(f"  batch submit: {total / elapsed:.0f} tasks/sec ({per_task_ms:.3f}ms/task)")


# ===================================================================
# Polling latency
# ===================================================================


class TestPollingLatency:
    """Benchmark the worker's poll round-trip."""

    async def test_poll_roundtrip_latency(self, task_queue: Any) -> None:
        """Mean _poll_tasks latency should be below the target."""
        # Seed pending tasks so each poll has work to find.
        await task_queue.submit_many([("perf_poll", {"n": i}) for i in range(10)])

        async with Worker(
            database_url=_db_url(),
            worker_id="perf-poll-1",
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            timings_ms: list[float] = []
            for _ in range(PERF_SAMPLES_POLL):
                start = time.perf_counter()
                batch = await worker._poll_tasks()
                timings_ms.append((time.perf_counter() - start) * 1000.0)
                assert len(batch) == 10

        avg = _assert_avg_below(timings_ms, PERF_MAX_POLL_MS)
        print(f"  poll roundtrip avg={avg:.3f}ms over {PERF_SAMPLES_POLL} samples")


# ===================================================================
# Execution latency
# ===================================================================


class TestExecutionLatency:
    """Benchmark empty-task execution + status updates."""

    async def test_empty_task_execution_latency(self, task_queue: Any) -> None:
        """Mean _execute_task latency (empty handler) should be below target."""
        async with Worker(
            database_url=_db_url(),
            worker_id="perf-exec-1",
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("perf_exec")
            async def empty_handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {}

            task_id = await task_queue.submit("perf_exec", {"n": 0})
            task: Task = await task_queue.get_task(task_id)
            assert task is not None

            timings_ms: list[float] = []
            for _ in range(PERF_SAMPLES_EXEC):
                start = time.perf_counter()
                await worker._execute_task(task)
                timings_ms.append((time.perf_counter() - start) * 1000.0)

        avg = _assert_avg_below(timings_ms, PERF_MAX_EXEC_MS)
        print(f"  empty exec avg={avg:.3f}ms over {PERF_SAMPLES_EXEC} samples")


# ===================================================================
# Overall throughput
# ===================================================================


class TestThroughput:
    """Benchmark end-to-end worker throughput."""

    async def test_worker_throughput(self, task_queue: Any) -> None:
        """A running worker should sustain the throughput target."""
        total = PERF_SAMPLES_THROUGHPUT

        async with Worker(
            database_url=_db_url(),
            worker_id="perf-throughput-1",
            concurrency=10,
            poll_interval=0.01,
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=2,
            pool_max_size=10,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("perf_throughput")
            async def noop(_payload: dict[str, Any]) -> dict[str, Any]:
                return {}

            run_task = asyncio.create_task(worker.run())
            start = time.perf_counter()
            try:
                await task_queue.submit_many([("perf_throughput", {"n": i}) for i in range(total)])
                await _wait_for_completed_count(task_queue, total)
            finally:
                await worker.shutdown()
                await run_task
            elapsed = time.perf_counter() - start

        throughput = total / elapsed
        assert (
            throughput >= PERF_MIN_THROUGHPUT
        ), f"throughput {throughput:.0f} tasks/sec < target {PERF_MIN_THROUGHPUT:.0f}"
        print(f"  throughput: {throughput:.0f} tasks/sec ({elapsed:.2f}s for {total} tasks)")


# ===================================================================
# Memory usage
# ===================================================================


class TestMemoryUsage:
    """Benchmark base worker memory footprint."""

    async def test_worker_base_memory(self, task_queue: Any) -> None:
        """Connecting and running a worker should stay within the memory target."""
        baseline = _rss_mb()

        async with Worker(
            database_url=_db_url(),
            worker_id="perf-mem-1",
            metrics_enabled=False,
            health_enabled=False,
            pool_min_size=1,
            pool_max_size=2,
            pool_timeout=5.0,
        ) as worker:

            @worker.task("perf_mem")
            async def noop(_payload: dict[str, Any]) -> dict[str, Any]:
                return {}

            await task_queue.submit("perf_mem", {"n": 0})
            await worker.run_once()

        after = _rss_mb()
        delta = after - baseline
        assert (
            delta < PERF_MAX_MEMORY_MB
        ), f"worker RSS grew by {delta:.1f}MB >= limit {PERF_MAX_MEMORY_MB:.1f}MB"
        print(f"  worker RSS delta={delta:.1f}MB " f"(baseline {baseline:.1f}MB -> {after:.1f}MB)")


# ===================================================================
# Database baseline (report-only)
# ===================================================================


class TestDatabaseBaseline:
    """Report raw database round-trip latency (no hard assertion)."""

    async def test_select_one_roundtrip(self, task_queue: Any) -> None:
        """Report mean SELECT 1 round-trip time as baseline context."""
        timings_ms: list[float] = []
        for _ in range(PERF_SAMPLES_DB):
            start = time.perf_counter()
            await task_queue._pool.fetchval("SELECT 1")
            timings_ms.append((time.perf_counter() - start) * 1000.0)

        avg = _mean(timings_ms)
        print(f"  SELECT 1 avg={avg:.3f}ms over {PERF_SAMPLES_DB} samples")
