"""Embeddable dashboard server for Conductor.

Serves the FastAPI dashboard application (:func:`conductor.api.app.
create_app`) together with the built single-page frontend.
:class:`DashboardServer` wraps a ``uvicorn.Server`` running as a
background asyncio task, mirroring the embed pattern of the metrics
exporter and the gRPC server.

Typical usage (standalone)::

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

import asyncio
import logging
import signal
import socket
from typing import Any, Optional

import uvicorn

from conductor.api.app import create_app
from conductor.db.connection import DatabasePool
from conductor.db.queries import QueryBuilder
from conductor.dlq.dead_letter_queue import DeadLetterQueue
from conductor.observability.health import HealthChecker

logger = logging.getLogger("conductor.api.server")


class DashboardServer:
    """Serve the Conductor web dashboard (API + built frontend).

    Args:
        pool: The database pool backing all queries.
        health_checker: Health checker for ``/api/health`` (created from
            ``pool`` if not provided).
        api_key: Optional API key; when set, ``/api/*`` endpoints require
            an ``X-API-Key`` header.
        host: Bind address.
        port: Bind port.
        log_level: Uvicorn log level (``"INFO"``, ``"WARNING"``, ...).
    """

    def __init__(
        self,
        pool: DatabasePool,
        *,
        health_checker: Optional[HealthChecker] = None,
        api_key: Optional[str] = None,
        host: str = "0.0.0.0",
        port: int = 8080,
        log_level: str = "INFO",
    ) -> None:
        self._pool = pool
        self._health_checker = health_checker or HealthChecker(pool)
        self._api_key = api_key
        self._host = host
        self._port = port
        self._log_level = log_level
        self._dlq = DeadLetterQueue(pool=pool)
        self._queries = QueryBuilder(pool)

        self._server: Optional[uvicorn.Server] = None
        self._task: Optional[asyncio.Task[None]] = None

    def create_app(self) -> Any:
        """Build the FastAPI application for this server."""
        return create_app(
            queries=self._queries,
            health_checker=self._health_checker,
            dlq=self._dlq,
            api_key=self._api_key,
        )

    async def start(self) -> None:
        """Start the dashboard server in a background asyncio task.

        If the port cannot be bound (``OSError``), the failure is logged
        and the server is skipped — the caller may continue without it.
        """
        if self._task is not None and not self._task.done():
            return

        # The DLQ shares our pool; ``connect()`` only initialises its query
        # builder (the caller owns the pool lifecycle).
        if not self._dlq.is_connected:
            await self._dlq.connect()

        # Pre-bind the socket so a busy port surfaces as a catchable
        # OSError instead of uvicorn calling ``sys.exit`` internally.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((self._host, self._port))
        except OSError as exc:
            sock.close()
            logger.warning(
                "Dashboard server could not bind to %s:%d: %s. " "Skipping dashboard server.",
                self._host,
                self._port,
                exc,
            )
            return
        sock.setblocking(False)

        config = uvicorn.Config(
            self.create_app(),
            host=self._host,
            port=self._port,
            log_level=self._log_level.lower(),
            access_log=False,
        )
        self._server = uvicorn.Server(config)
        self._task = asyncio.create_task(
            self._serve(self._server, sock),
            name="conductor-dashboard",
        )
        logger.info("Dashboard server starting on %s:%d.", self._host, self._port)

    async def _serve(self, server: uvicorn.Server, sock: socket.socket) -> None:
        """Run the uvicorn server over the pre-bound socket."""
        await server.serve(sockets=[sock])

    async def run(self) -> None:
        """Start the dashboard server and block until a shutdown signal.

        Installs ``SIGTERM``/``SIGINT`` handlers that stop the server,
        then waits for one to fire (or for ``stop()`` to be called).
        """
        await self.start()

        if self._task is None or self._task.done():
            # Server did not start (e.g. port already bound); nothing to do.
            return

        loop = asyncio.get_running_loop()
        shutdown_event = asyncio.Event()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, shutdown_event.set)
            except NotImplementedError:
                logger.warning(
                    "Signal handler not supported for %s on this platform.",
                    sig,
                )

        await shutdown_event.wait()
        await self.stop()

    async def stop(self) -> None:
        """Stop the dashboard server and wait for it to exit."""
        if self._server is not None:
            self._server.should_exit = True
        if self._task is not None:
            try:
                await asyncio.wait_for(self._task, timeout=10.0)
            except asyncio.TimeoutError:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            except Exception:
                logger.debug(
                    "Dashboard server task ended unexpectedly.",
                    exc_info=True,
                )
            self._task = None
        self._server = None
        logger.info("Dashboard server stopped.")

    def get_status(self) -> dict[str, Any]:
        """Return a snapshot of the dashboard server state."""
        return {
            "host": self._host,
            "port": self._port,
            "api_key_required": self._api_key is not None,
            "serving": self._task is not None and not self._task.done(),
        }
