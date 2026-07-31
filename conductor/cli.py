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
from typing import Optional

from dotenv import load_dotenv

from conductor.config import WorkerSettings
from conductor.core.worker import Worker
from conductor.exceptions import ConductorException

logger = logging.getLogger("conductor.cli")


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line argument parser."""
    parser = argparse.ArgumentParser(
        prog="conductor",
        description="Lightweight async task queue for Python (PostgreSQL-backed).",
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


def main(argv: Optional[list[str]] = None) -> int:
    """CLI entry point. Returns a process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_run_worker(args.handlers, args.env_file))
