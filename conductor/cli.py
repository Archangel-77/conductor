"""
Command-line interface for Conductor.

Provides the ``conductor`` console script for running workers from
environment-based configuration::

    conductor worker
    conductor worker --handlers myapp.handlers
    python -m conductor worker

The worker reads its configuration from environment variables (see
``.env.example``) via :class:`~conductor.config.WorkerSettings`.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
from typing import Optional

from dotenv import load_dotenv

from conductor import __version__
from conductor.config import WorkerSettings
from conductor.core.worker import Worker
from conductor.exceptions import ConductorException

logger = logging.getLogger("conductor.cli")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="conductor",
        description="Lightweight async task queue for Python (PostgreSQL/SQLite backed).",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
        help="Show the Conductor version and exit.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    worker_parser = sub.add_parser("worker", help="Run a worker process.")
    worker_parser.add_argument(
        "--handlers",
        default=None,
        help="Dotted path to a module exposing register(worker) that attaches "
        "task handlers (overrides CONDUCTOR_HANDLERS_MODULE).",
    )
    worker_parser.add_argument(
        "--env-file",
        default=None,
        help="Path to a .env file to load (default: ./.env if present).",
    )

    api_parser = sub.add_parser("api", help="Run the web dashboard server.")
    api_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Bind address (default: 0.0.0.0).",
    )
    api_parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Bind port (default: CONDUCTOR_API_PORT or 8080).",
    )
    api_parser.add_argument(
        "--api-key",
        default=None,
        help="Require this X-API-Key header on /api/* endpoints "
        "(default: CONDUCTOR_API_KEY env var, or open).",
    )
    api_parser.add_argument(
        "--env-file",
        default=None,
        help="Path to a .env file to load (default: ./.env if present).",
    )
    return parser


def register_handlers(worker: Worker, module_path: str) -> None:
    """Import *module_path* and call its ``register`` function.

    The module is expected to expose a callable ``register(worker)``
    (sync, or async which is awaited by the caller) that attaches task
    handlers to the worker.

    Args:
        worker: The worker to register handlers on.
        module_path: Dotted import path of the handlers module.

    Raises:
        ConductorException: If the module or its ``register`` function
            cannot be found.
    """
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ConductorException(
            f"Could not import handlers module '{module_path}': {exc}"
        ) from exc

    register = getattr(module, "register", None)
    if register is None:
        raise ConductorException(
            f"Handlers module '{module_path}' must expose a 'register(worker)' " "function."
        )

    result = register(worker)
    if asyncio.iscoroutine(result):
        # Not awaited here; log a clear hint so users keep register() sync.
        logger.warning(
            "register() in '%s' returned an unawaited coroutine. "
            "Prefer a sync register(worker) function.",
            module_path,
        )
        result.close()


async def _run_worker(handlers: Optional[str], env_file: Optional[str]) -> int:
    """Build a worker from the environment and run it until shutdown.

    Returns:
        ``0`` on clean shutdown, ``2`` on configuration errors.
    """
    if env_file:
        load_dotenv(env_file, override=False)
    else:
        load_dotenv()  # Loads ./.env if present; no-op otherwise.

    try:
        settings = WorkerSettings.from_env()
    except ConductorException as exc:
        logger.error("%s", exc)
        return 2

    worker = settings.build_worker()

    handlers_path = handlers or settings.handlers_module
    if handlers_path:
        try:
            register_handlers(worker, handlers_path)
        except ConductorException as exc:
            logger.error("%s", exc)
            return 2

    logger.info(
        "Starting conductor worker (id=%s, routes=%s, concurrency=%d).",
        worker.worker_id,
        settings.routes,
        settings.concurrency,
    )
    await worker.run()
    return 0


async def _run_api(
    host: str,
    port: Optional[int],
    api_key: Optional[str],
    env_file: Optional[str],
) -> int:
    """Run the standalone dashboard server until shutdown.

    Returns:
        ``0`` on clean shutdown, ``2`` on configuration errors.
    """
    if env_file:
        load_dotenv(env_file, override=False)
    else:
        load_dotenv()  # Loads ./.env if present; no-op otherwise.

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        logger.error("DATABASE_URL environment variable is required. See .env.example.")
        return 2

    if port is None:
        port = int(os.getenv("CONDUCTOR_API_PORT", "8080"))
    if api_key is None:
        api_key = os.getenv("CONDUCTOR_API_KEY") or None

    from conductor.api.server import DashboardServer
    from conductor.db.connection import DatabasePool
    from conductor.db.schema import SchemaManager

    pool = DatabasePool(dsn=database_url)
    await pool.connect()
    await SchemaManager(pool).ensure_schema()

    server = DashboardServer(
        pool,
        api_key=api_key,
        host=host,
        port=port,
    )
    logger.info("Starting conductor dashboard on %s:%d.", host, port)
    try:
        await server.run()
    finally:
        await pool.disconnect()
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point. Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "worker":
        return asyncio.run(_run_worker(args.handlers, args.env_file))
    if args.command == "api":
        return asyncio.run(_run_api(args.host, args.port, args.api_key, args.env_file))
    parser.error(f"Unknown command '{args.command}'")
    return 2
