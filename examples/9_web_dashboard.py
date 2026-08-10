"""
Example 9 — Web Dashboard (FastAPI + built React frontend).

Demonstrates the Conductor web dashboard: a ``DashboardServer`` is started
standalone (the same server that ``conductor api`` runs), tasks are submitted
via ``TaskQueue``, and a small HTTP client hits the dashboard API to list
tasks, cancel one, and read health/metrics.

Expected output (paraphrased)::

    GET  /api/tasks      -> 200, total=2
    POST /api/tasks/{id}/cancel -> 200 {'status': 'cancelled'}
    GET  /api/tasks/{id} -> 200, status=cancelled
    GET  /api/health     -> 200, status=healthy
    GET  /api/metrics    -> 200, 19 metric families

Run (PostgreSQL must be reachable)::

    python examples/9_web_dashboard.py

Then open http://127.0.0.1:8769/ in a browser to see the SPA (the built
frontend ships inside the wheel at ``conductor/web/dist``).
"""

from __future__ import annotations

import asyncio
import os

import httpx

from conductor import DashboardServer, TaskQueue
from conductor.db.connection import DatabasePool
from conductor.db.schema import SchemaManager
from conductor.observability.health import HealthChecker

DB_URL = os.environ.get("DATABASE_URL", "postgresql://conductor:conductor@localhost:5432/conductor")
PORT = int(os.environ.get("CONDUCTOR_API_PORT", "8769"))


async def main() -> None:
    # Standalone server: build its own pool + health checker.
    pool = DatabasePool(dsn=DB_URL, min_size=1, max_size=2)
    await pool.connect()
    await SchemaManager(pool).ensure_schema()

    server = DashboardServer(
        pool,
        health_checker=HealthChecker(pool),
        host="127.0.0.1",
        port=PORT,
        api_key=os.environ.get("CONDUCTOR_API_KEY") or None,
    )
    await server.start()
    await asyncio.sleep(1.0)  # let uvicorn finish startup

    # Seed a couple of tasks through the public TaskQueue API.
    queue = TaskQueue(database_url=DB_URL, pool_min_size=1, pool_max_size=2)
    await queue.connect()
    first = await queue.submit("email.send", {"to": "a@example.com"})
    second = await queue.submit("report.generate", {"days": 7})
    await queue.disconnect()

    base = f"http://127.0.0.1:{PORT}"
    async with httpx.AsyncClient(base_url=base) as client:
        resp = await client.get("/api/tasks")
        print(f"GET  /api/tasks         -> {resp.status_code}, total={resp.json()['total']}")

        resp = await client.post(f"/api/tasks/{first}/cancel")
        print(f"POST /api/tasks/{first[:8]}/cancel -> {resp.status_code} {resp.json()}")

        resp = await client.get(f"/api/tasks/{first}")
        print(f"GET  /api/tasks/{first[:8]} -> {resp.status_code}, status={resp.json()['status']}")

        resp = await client.get(f"/api/tasks/{second}")
        print(f"GET  /api/tasks/{second[:8]} -> {resp.status_code}, status={resp.json()['status']}")

        resp = await client.get("/api/health")
        print(f"GET  /api/health       -> {resp.status_code}, status={resp.json()['status']}")

        resp = await client.get("/api/metrics")
        print(
            f"GET  /api/metrics      -> {resp.status_code}, "
            f"{len(resp.json()['metrics'])} metric families"
        )

        resp = await client.get("/")
        print(f"GET  / (SPA)           -> {resp.status_code}, {resp.headers.get('content-type')}")

    await server.stop()
    await pool.disconnect()
    print("Open the dashboard in your browser: http://127.0.0.1:8769/")


if __name__ == "__main__":
    asyncio.run(main())
