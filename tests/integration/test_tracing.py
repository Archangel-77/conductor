"""
Integration tests for distributed tracing.

Verifies the parts that only matter against a real database: the ``traceparent``
persisted with a task, the worker continuing that trace (including retries and
DLQ round-trips), and the shape of the resulting spans.

A real ``TracerProvider`` with an in-memory exporter is installed, so no
collector is required.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest
import pytest_asyncio

from conductor.core.models import RetryPolicy
from conductor.core.queue import TaskQueue
from conductor.core.worker import Worker
from conductor.observability import tracing
from tests.conftest import TEST_DATABASE_URL, db_available, truncate_all

pytestmark = pytest.mark.integration


def _install_memory_tracer() -> Any:
    """Install tracing with an in-memory exporter and return that exporter."""
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    exporter = InMemorySpanExporter()
    tracing.setup_tracing(
        tracing.TracingConfig(enabled=True, exporter="console", service_name="conductor-test")
    )
    provider = tracing.get_backend()._provider  # noqa: SLF001 - SDK test hook
    provider._active_span_processor._span_processors = ()  # noqa: SLF001
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def exporter() -> AsyncIterator[Any]:
    """A module-wide tracing backend writing spans to memory."""
    if not db_available():
        pytest.skip("Test database not available")

    pytest.importorskip("opentelemetry.sdk")
    in_memory = _install_memory_tracer()
    yield in_memory
    tracing.shutdown_tracing()


@pytest_asyncio.fixture(scope="module", loop_scope="session")
async def queue(exporter: Any) -> AsyncIterator[TaskQueue]:  # noqa: N802
    """A connected ``TaskQueue`` against the test database."""
    task_queue = TaskQueue(TEST_DATABASE_URL)
    await task_queue.connect()
    yield task_queue
    await task_queue.disconnect()


@pytest_asyncio.fixture(autouse=True, loop_scope="session")
async def _clean_state(queue: TaskQueue, exporter: Any) -> AsyncIterator[None]:
    """Clear the tables and the recorded spans around every test."""
    await truncate_all(queue._pool)
    exporter.clear()
    yield
    await truncate_all(queue._pool)
    exporter.clear()


async def run_worker_once(
    handlers: dict[str, Any],
    *,
    worker_id: str = "tracing-worker",
    polls: int = 1,
) -> None:
    """Run a worker for *polls* iterations with *handlers* registered."""
    worker = Worker(TEST_DATABASE_URL, worker_id=worker_id, poll_interval=0.01)
    await worker.connect()
    try:
        for task_type, handler in handlers.items():
            worker.task(task_type)(handler)
        for _ in range(polls):
            await worker.run_once()
    finally:
        await worker.disconnect()


def _spans_named(exporter: Any, name: str) -> list[Any]:
    """Return the recorded spans with the given name, oldest first."""
    return [span for span in exporter.get_finished_spans() if span.name == name]


# ===================================================================
# Producer side
# ===================================================================


class TestSubmitSpans:

    async def test_submit_persists_traceparent(self, queue: TaskQueue, exporter: Any) -> None:
        task_id = await queue.submit("traced", {"n": 1}, route="traced-route", priority=7)

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.traceparent is not None
        assert task.traceparent.startswith("00-")
        assert len(task.traceparent.split("-")) == 4

        spans = _spans_named(exporter, tracing.SPAN_SUBMIT)
        assert len(spans) == 1
        span = spans[0]
        assert span.attributes[tracing.ATTR_TASK_ID] == task_id
        assert span.attributes[tracing.ATTR_TASK_TYPE] == "traced"
        assert span.attributes[tracing.ATTR_TASK_ROUTE] == "traced-route"
        assert span.attributes[tracing.ATTR_TASK_PRIORITY] == 7

        # The persisted traceparent identifies the submit span.
        assert task.traceparent.split("-")[2] == format(span.context.span_id, "016x")

    async def test_submit_many_shares_one_trace(self, queue: TaskQueue, exporter: Any) -> None:
        task_ids = await queue.submit_many([("batch", {"i": 0}), ("batch", {"i": 1})])

        batch_spans = _spans_named(exporter, tracing.SPAN_SUBMIT_MANY)
        assert len(batch_spans) == 1
        assert batch_spans[0].attributes["task.count"] == 2

        trace_ids = set()
        for task_id in task_ids:
            task = await queue.get_task(task_id)
            assert task is not None and task.traceparent is not None
            trace_ids.add(task.traceparent.split("-")[1])

        # Every task in the batch belongs to the batch's trace.
        assert trace_ids == {format(batch_spans[0].context.trace_id, "032x")}

    async def test_cancel_and_dlq_spans(self, queue: TaskQueue, exporter: Any) -> None:
        task_id = await queue.submit("doomed", {})
        await queue.cancel_task(task_id)

        cancel_spans = _spans_named(exporter, tracing.SPAN_CANCEL)
        assert len(cancel_spans) == 1
        assert cancel_spans[0].attributes[tracing.ATTR_TASK_ID] == task_id

    async def test_tracing_disabled_writes_no_traceparent(
        self, queue: TaskQueue, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Simulate "no active trace" for this call only: reinstalling tracing
        # with a different exporter here would invalidate the module fixture.
        monkeypatch.setattr(tracing, "current_traceparent", lambda: None)
        task_id = await queue.submit("untraced", {})

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.traceparent is None


# ===================================================================
# Consumer side
# ===================================================================


class TestExecuteSpans:

    async def test_execute_continues_the_submit_trace(
        self, queue: TaskQueue, exporter: Any
    ) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {"ok": True}

        task_id = await queue.submit("work", {"x": 1})
        await run_worker_once({"work": handler})

        submit_spans = _spans_named(exporter, tracing.SPAN_SUBMIT)
        execute_spans = _spans_named(exporter, tracing.SPAN_EXECUTE)
        assert len(submit_spans) == 1
        assert len(execute_spans) == 1

        submitted, executed = submit_spans[0], execute_spans[0]
        assert executed.context.trace_id == submitted.context.trace_id
        assert executed.parent is not None
        assert executed.parent.span_id == submitted.context.span_id
        assert executed.attributes[tracing.ATTR_TASK_ID] == task_id
        assert executed.attributes[tracing.ATTR_WORKER_ID] == "tracing-worker"
        assert executed.attributes[tracing.ATTR_TASK_ATTEMPT] == 0

    async def test_failure_records_error_status(self, queue: TaskQueue, exporter: Any) -> None:
        from opentelemetry.trace import StatusCode

        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("handler exploded")

        await queue.submit("explode", {}, retry_policy=RetryPolicy(max_retries=0))
        await run_worker_once({"explode": handler})

        executed = _spans_named(exporter, tracing.SPAN_EXECUTE)[0]
        assert executed.status.status_code is StatusCode.ERROR
        assert "handler exploded" in (executed.status.description or "")
        assert [event.name for event in executed.events] == ["exception", "task.dlq"]

    async def test_success_marks_ok(self, queue: TaskQueue, exporter: Any) -> None:
        from opentelemetry.trace import StatusCode

        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            return {}

        await queue.submit("fine", {})
        await run_worker_once({"fine": handler})
        assert _spans_named(exporter, tracing.SPAN_EXECUTE)[0].status.status_code is (StatusCode.OK)

    async def test_retries_are_siblings_under_the_submission(
        self, queue: TaskQueue, exporter: Any
    ) -> None:
        attempts: list[int] = []

        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            attempts.append(1)
            raise RuntimeError("always fails")

        task_id = await queue.submit(
            "flaky",
            {},
            retry_policy=RetryPolicy(max_retries=1, initial_delay=0.01, max_delay=0.02),
        )
        await run_worker_once({"flaky": handler})
        await asyncio.sleep(0.05)
        await run_worker_once({"flaky": handler})

        assert len(attempts) == 2
        execute_spans = _spans_named(exporter, tracing.SPAN_EXECUTE)
        assert [span.attributes[tracing.ATTR_TASK_ATTEMPT] for span in execute_spans] == [0, 1]

        submitted = _spans_named(exporter, tracing.SPAN_SUBMIT)[0]
        for span in execute_spans:
            assert span.context.trace_id == submitted.context.trace_id
            assert span.parent is not None
            assert span.parent.span_id == submitted.context.span_id

        # The first attempt recorded the retry scheduling.
        events = [event.name for span in execute_spans for event in span.events]
        assert "retry.scheduled" in events

        task = await queue.get_task(task_id)
        assert task is not None
        assert task.status.value == "failed"

    async def test_blocked_dependents_get_a_child_span(
        self, queue: TaskQueue, exporter: Any
    ) -> None:
        async def handler(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("parent failed")

        parent = await queue.submit("parent", {}, retry_policy=RetryPolicy(max_retries=0))
        child = await queue.submit("child", {}, depends_on=[parent])

        await run_worker_once({"parent": handler})

        blocked_spans = _spans_named(exporter, tracing.SPAN_BLOCK_DEPENDENTS)
        assert len(blocked_spans) == 1
        assert blocked_spans[0].attributes["blocked.count"] == 1
        # OTel normalises a list attribute into a tuple.
        assert tuple(blocked_spans[0].attributes["blocked.task_ids"]) == (child,)

        child_task = await queue.get_task(child)
        assert child_task is not None
        assert child_task.status.value == "blocked"


# ===================================================================
# DLQ round-trip
# ===================================================================


class TestDlqTracing:

    async def test_dlq_preserves_traceparent_and_retry_links(
        self, queue: TaskQueue, exporter: Any
    ) -> None:
        async def failing(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("dead")

        task_id = await queue.submit("dead", {}, retry_policy=RetryPolicy(max_retries=0))
        await run_worker_once({"dead": failing})

        task = await queue.get_task(task_id)
        assert task is not None
        dlq_task = await queue.get_dlq_task(task_id)
        assert dlq_task is not None
        assert dlq_task.traceparent == task.traceparent

        await queue.retry_dlq_task(task_id)

        retry_spans = _spans_named(exporter, tracing.SPAN_DLQ_RETRY)
        assert len(retry_spans) == 1
        assert retry_spans[0].attributes[tracing.ATTR_TASK_ID] == task_id
        # The retry operation links back to the original execution.
        assert len(retry_spans[0].links) == 1
        assert format(retry_spans[0].links[0].context.trace_id, "032x") == (
            task.traceparent.split("-")[1]  # type: ignore[union-attr]
        )

        requeued = await queue.get_task(task_id)
        assert requeued is not None
        assert requeued.traceparent == task.traceparent
        assert requeued.status.value == "pending"

    async def test_discard_span(self, queue: TaskQueue, exporter: Any) -> None:
        async def failing(payload: dict[str, Any]) -> dict[str, Any]:
            raise RuntimeError("dead")

        task_id = await queue.submit("dead", {}, retry_policy=RetryPolicy(max_retries=0))
        await run_worker_once({"dead": failing})
        await queue.discard_dlq_task(task_id, reason="give up")

        discard_spans = _spans_named(exporter, tracing.SPAN_DLQ_DISCARD)
        assert len(discard_spans) == 1
        assert discard_spans[0].attributes[tracing.ATTR_TASK_ID] == task_id


# ===================================================================
# Recurring
# ===================================================================


class TestRecurringTracing:

    async def test_recurring_fire_gets_its_own_trace(self, queue: TaskQueue, exporter: Any) -> None:
        recurring_id = await queue.schedule_recurring("cleanup", {}, "*/5 * * * *")

        definition = await queue.get_recurring_task(recurring_id)
        assert definition is not None
        now = definition.next_run_at + timedelta(seconds=1)
        claimed = await queue._queries.select_due_recurring_tasks(now)
        assert claimed

        # Fire the definition the way the scheduler does.
        from conductor.recurring.scheduler import RecurringScheduler

        scheduler = RecurringScheduler(TEST_DATABASE_URL)
        await scheduler.connect()
        try:
            assert scheduler._queries is not None
            async with scheduler._pool.transaction() as conn:
                await scheduler._fire(definition, now=now, conn=conn)
        finally:
            await scheduler.disconnect()

        fire_spans = _spans_named(exporter, tracing.SPAN_RECURRING_FIRE)
        assert len(fire_spans) == 1
        assert fire_spans[0].attributes["recurring.id"] == recurring_id

        # The generated task continues the fire span's trace.
        instances = await queue._queries.select_tasks(task_type="cleanup")
        assert len(instances) == 1
        assert instances[0]["traceparent"] is not None
        assert instances[0]["traceparent"].split("-")[1] == format(
            fire_spans[0].context.trace_id, "032x"
        )
