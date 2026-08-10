"""Web dashboard API for Conductor.

Provides the FastAPI application (:func:`conductor.api.app.create_app`) and
the embeddable :class:`~conductor.api.server.DashboardServer` that serves
the built React frontend together with the JSON API.

Typical usage (embedded in a worker or standalone)::

    from conductor.api.server import DashboardServer
    from conductor.db.connection import DatabasePool

    pool = DatabasePool(dsn="postgresql://...")
    await pool.connect()
    server = DashboardServer(pool, port=8080)
    await server.start()
    ...
    await server.stop()
    await pool.disconnect()
"""

from __future__ import annotations

from conductor.api.app import create_app
from conductor.api.server import DashboardServer

__all__: list[str] = ["create_app", "DashboardServer"]
