"""FastAPI application factory for the Conductor web dashboard.

Builds the HTTP API consumed by the React frontend in ``conductor/web``.
:func:`create_app` wires up the read-only dashboard endpoints (tasks,
workers, DLQ, metrics, health) against a :class:`~conductor.db.queries.
QueryBuilder` and serves the built single-page application from
``conductor/web/dist`` when present.

Typical usage::

    from conductor.api.app import create_app
    from conductor.db.connection import DatabasePool
    from conductor.db.queries import QueryBuilder
    from conductor.dlq.dead_letter_queue import DeadLetterQueue
    from conductor.observability.health import HealthChecker

    pool = DatabasePool(dsn="postgresql://...")
    await pool.connect()
    app = create_app(
        queries=QueryBuilder(pool),
        health_checker=HealthChecker(pool),
        dlq=DeadLetterQueue(pool=pool),
        api_key=None,
    )
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.staticfiles import StaticFiles
from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from conductor import __version__
from conductor.api.schemas import (
    CancelResponse,
    DiscardResponse,
    DlqListResponse,
    RetryResponse,
    TaskListResponse,
    WorkerListResponse,
)
from conductor.db.queries import QueryBuilder
from conductor.dlq.dead_letter_queue import DeadLetterQueue
from conductor.exceptions import TaskError
from conductor.observability.health import HealthChecker

# Register Conductor's metric families (counters/gauges/histograms) so the
# dashboard can report them even when no worker runs in this process.
import conductor.observability.metrics as _conductor_metrics  # noqa: F401

logger = logging.getLogger("conductor.api.app")

#: Directory of the committed frontend build (``conductor/web/dist``).
WEB_DIST_DIR = Path(__file__).resolve().parent.parent / "web" / "dist"


def _metrics_to_json() -> list[dict[str, Any]]:
    """Convert the Prometheus exposition format into a JSON-friendly list."""
    metrics: list[dict[str, Any]] = []
    families = text_string_to_metric_families(generate_latest().decode("utf-8"))
    for family in families:
        metrics.append(
            {
                "name": family.name,
                "type": family.type,
                "help": family.documentation,
                "samples": [
                    {
                        "name": sample.name,
                        "labels": dict(sample.labels),
                        "value": sample.value,
                    }
                    for sample in family.samples
                ],
            }
        )
    return metrics


def _require_api_key(api_key: Optional[str]) -> Any:
    """Build a FastAPI dependency enforcing the optional API key."""

    async def dependency(request: Request) -> None:
        if api_key is not None and request.headers.get("X-API-Key") != api_key:
            raise HTTPException(status_code=403, detail="Invalid or missing API key")

    return dependency


def create_app(
    *,
    queries: QueryBuilder,
    health_checker: HealthChecker,
    dlq: DeadLetterQueue,
    api_key: Optional[str] = None,
) -> FastAPI:
    """Create the Conductor dashboard FastAPI application.

    Args:
        queries: The ``QueryBuilder`` used for all read endpoints.
        health_checker: The ``HealthChecker`` backing ``/api/health``.
        dlq: The ``DeadLetterQueue`` used for retry/discard operations.
        api_key: Optional API key; when set, every ``/api/*`` endpoint
            requires an ``X-API-Key`` header matching it.

    Returns:
        A configured ``FastAPI`` application.
    """
    app = FastAPI(title="Conductor Dashboard", version=__version__)
    auth = _require_api_key(api_key)

    # ------------------------------------------------------------------
    # Tasks
    # ------------------------------------------------------------------

    @app.get(
        "/api/tasks",
        response_model=TaskListResponse,
        dependencies=[Depends(auth)],
    )
    async def list_tasks(
        status: Optional[str] = Query(default=None),
        route: Optional[str] = Query(default=None),
        task_type: Optional[str] = Query(default=None),
        search: Optional[str] = Query(default=None),
        limit: int = Query(default=10, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        """List tasks across all statuses with optional filters."""
        try:
            items = await queries.select_tasks(
                limit,
                offset,
                status=status,
                route=route,
                task_type=task_type,
                search=search,
            )
            total = await queries.count_tasks(
                status=status,
                route=route,
                task_type=task_type,
                search=search,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"items": items, "total": total, "limit": limit, "offset": offset}

    @app.get("/api/tasks/{task_id}", dependencies=[Depends(auth)])
    async def task_detail(task_id: str) -> dict[str, Any]:
        """Fetch a single task together with its retry history."""
        task = await queries.select_task(task_id)
        if task is None:
            raise HTTPException(
                status_code=404,
                detail=f"Task '{task_id}' not found.",
            )
        task["retries"] = await queries.select_retries_for_task(task_id)
        return task

    @app.post(
        "/api/tasks/{task_id}/cancel",
        response_model=CancelResponse,
        dependencies=[Depends(auth)],
    )
    async def cancel_task(task_id: str) -> dict[str, str]:
        """Cancel a pending or retrying task."""
        task = await queries.select_task(task_id)
        if task is None:
            raise HTTPException(
                status_code=404,
                detail=f"Task '{task_id}' not found.",
            )
        if task["status"] not in ("pending", "retrying"):
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Task '{task_id}' cannot be cancelled from status " f"'{task['status']}'."
                ),
            )
        await queries.cancel_task(task_id)
        logger.info("Task %s cancelled via dashboard API.", task_id)
        return {"status": "cancelled"}

    # ------------------------------------------------------------------
    # Workers
    # ------------------------------------------------------------------

    @app.get(
        "/api/workers",
        response_model=WorkerListResponse,
        dependencies=[Depends(auth)],
    )
    async def list_workers(
        limit: int = Query(default=10, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        """List all registered workers, newest heartbeat first."""
        items = await queries.select_all_workers(limit, offset)
        return {"items": items, "limit": limit, "offset": offset}

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    @app.get("/api/metrics", dependencies=[Depends(auth)])
    async def metrics() -> dict[str, Any]:
        """Return the Prometheus metrics as JSON."""
        return {"metrics": _metrics_to_json()}

    # ------------------------------------------------------------------
    # Dead-letter queue
    # ------------------------------------------------------------------

    @app.get(
        "/api/dlq",
        response_model=DlqListResponse,
        dependencies=[Depends(auth)],
    )
    async def list_dlq(
        limit: int = Query(default=10, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
        include_discarded: bool = Query(default=False),
    ) -> dict[str, Any]:
        """List tasks in the dead-letter queue."""
        items = await queries.select_dlq_tasks(
            limit,
            offset,
            include_discarded=include_discarded,
        )
        return {"items": items, "limit": limit, "offset": offset}

    @app.post(
        "/api/dlq/{task_id}/retry",
        response_model=RetryResponse,
        dependencies=[Depends(auth)],
    )
    async def retry_dlq(task_id: str) -> dict[str, str]:
        """Requeue a dead-lettered task as pending."""
        try:
            retried_id = await dlq.retry_task(task_id)
        except TaskError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"task_id": retried_id}

    @app.post(
        "/api/dlq/{task_id}/discard",
        response_model=DiscardResponse,
        dependencies=[Depends(auth)],
    )
    async def discard_dlq(task_id: str) -> dict[str, str]:
        """Soft-delete a dead-lettered task."""
        try:
            await dlq.discard_task(task_id)
        except TaskError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"status": "discarded", "task_id": task_id}

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    @app.get("/api/health", dependencies=[Depends(auth)])
    async def health() -> dict[str, Any]:
        """Return the current system health."""
        return (await health_checker.check()).to_dict()

    # ------------------------------------------------------------------
    # Frontend (built SPA)
    # ------------------------------------------------------------------

    if WEB_DIST_DIR.is_dir():
        app.mount(
            "/",
            StaticFiles(directory=str(WEB_DIST_DIR), html=True),
            name="web",
        )
    else:

        @app.get("/", include_in_schema=False)
        async def root() -> dict[str, str]:
            """Fallback when the frontend has not been built."""
            return {
                "message": "Conductor dashboard API is running, but the "
                "frontend is not built. Run `npm run build` in "
                "conductor/web to build it.",
            }

    return app
