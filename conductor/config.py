"""
Environment-based configuration for Conductor workers.

Provides ``WorkerSettings`` — a frozen dataclass that maps the documented
environment variables (see ``.env.example``) onto typed ``Worker``
constructor arguments, closing the gap between the ``.env`` contract and
the library's keyword-only constructor.

Typical usage::

    from conductor.config import WorkerSettings

    settings = WorkerSettings.from_env()
    worker = settings.build_worker()
    await worker.run()
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional

from conductor.core.worker import Worker
from conductor.exceptions import ConductorException

# ---------------------------------------------------------------------------
# Environment parsing helpers
# ---------------------------------------------------------------------------


def _env_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable (``true/1/yes/on``)."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    """Parse an integer environment variable."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_float(name: str, default: float) -> float:
    """Parse a float environment variable."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


def _env_list(name: str, default: list[str]) -> list[str]:
    """Parse a comma-separated environment variable into a list."""
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return list(default)
    return [item.strip() for item in raw.split(",") if item.strip()]


# ---------------------------------------------------------------------------
# Worker settings
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WorkerSettings:
    """Typed worker configuration sourced from environment variables.

    Attributes:
        database_url: PostgreSQL connection URI (required).
        worker_id: Worker identifier (defaults to ``hostname-pid``).
        concurrency: Maximum concurrent tasks per worker.
        poll_interval: Seconds between task polls.
        routes: Route names the worker will poll.
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR).
        pool_min_size: Minimum connection pool size.
        pool_max_size: Maximum connection pool size.
        pool_timeout: Connection acquisition timeout in seconds.
        command_timeout: SQL command timeout in seconds.
        heartbeat_interval: Heartbeat frequency in seconds.
        graceful_shutdown_timeout: Seconds to wait for in-flight tasks.
        metrics_port: Metrics/health HTTP server port.
        metrics_enabled: Toggle the Prometheus metrics endpoint.
        health_enabled: Toggle the health check endpoint.
        handlers_module: Optional dotted path to a module exposing a
            ``register(worker)`` function that attaches task handlers.
    """

    database_url: str
    worker_id: Optional[str] = None
    concurrency: int = 10
    poll_interval: float = 0.5
    routes: list[str] = field(default_factory=lambda: ["default"])
    log_level: str = "INFO"
    pool_min_size: int = 2
    pool_max_size: int = 10
    pool_timeout: float = 30.0
    command_timeout: float = 60.0
    heartbeat_interval: float = 10.0
    graceful_shutdown_timeout: float = 30.0
    metrics_port: int = 8000
    metrics_enabled: bool = True
    health_enabled: bool = True
    handlers_module: Optional[str] = None

    @classmethod
    def from_env(cls) -> WorkerSettings:
        """Build settings from the process environment.

        Raises:
            ConductorException: If ``DATABASE_URL`` is not set.
        """
        database_url = os.getenv("DATABASE_URL", "").strip()
        if not database_url:
            raise ConductorException(
                "DATABASE_URL environment variable is required. "
                "See .env.example for configuration."
            )
        return cls(
            database_url=database_url,
            worker_id=os.getenv("WORKER_ID") or None,
            concurrency=_env_int("CONCURRENCY", 10),
            poll_interval=_env_float("POLL_INTERVAL", 0.5),
            routes=_env_list("ROUTES", ["default"]),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            pool_min_size=_env_int("DB_MIN_SIZE", 2),
            pool_max_size=_env_int("DB_MAX_SIZE", 10),
            pool_timeout=_env_float("DB_TIMEOUT", 30.0),
            command_timeout=_env_float("DB_COMMAND_TIMEOUT", 60.0),
            heartbeat_interval=_env_float("HEARTBEAT_INTERVAL", 10.0),
            graceful_shutdown_timeout=_env_float("GRACEFUL_SHUTDOWN_TIMEOUT", 30.0),
            metrics_port=_env_int("METRICS_PORT", 8000),
            metrics_enabled=_env_bool("METRICS_ENABLED", True),
            health_enabled=_env_bool("HEALTH_ENABLED", True),
            handlers_module=os.getenv("CONDUCTOR_HANDLERS_MODULE") or None,
        )

    def build_worker(self) -> Worker:
        """Construct a :class:`~conductor.core.worker.Worker` from settings."""
        return Worker(
            database_url=self.database_url,
            worker_id=self.worker_id,
            concurrency=self.concurrency,
            poll_interval=self.poll_interval,
            routes=self.routes,
            log_level=self.log_level,
            pool_min_size=self.pool_min_size,
            pool_max_size=self.pool_max_size,
            pool_timeout=self.pool_timeout,
            command_timeout=self.command_timeout,
            heartbeat_interval=self.heartbeat_interval,
            graceful_shutdown_timeout=self.graceful_shutdown_timeout,
            metrics_port=self.metrics_port,
            metrics_enabled=self.metrics_enabled,
            health_enabled=self.health_enabled,
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary of the settings (for diagnostics)."""
        return {
            "database_url": self.database_url,
            "worker_id": self.worker_id,
            "concurrency": self.concurrency,
            "poll_interval": self.poll_interval,
            "routes": list(self.routes),
            "log_level": self.log_level,
            "pool_min_size": self.pool_min_size,
            "pool_max_size": self.pool_max_size,
            "pool_timeout": self.pool_timeout,
            "command_timeout": self.command_timeout,
            "heartbeat_interval": self.heartbeat_interval,
            "graceful_shutdown_timeout": self.graceful_shutdown_timeout,
            "metrics_port": self.metrics_port,
            "metrics_enabled": self.metrics_enabled,
            "health_enabled": self.health_enabled,
            "handlers_module": self.handlers_module,
        }
