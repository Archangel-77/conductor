"""
Unit tests for environment-based worker configuration.

These tests do **not** require a database — they verify env-var parsing
and worker construction from :class:`conductor.config.WorkerSettings`.
"""

# pylint: disable=missing-class-docstring,protected-access

from __future__ import annotations

import pytest

from conductor.config import WorkerSettings
from conductor.exceptions import ConductorException

_ENV_VARS = [
    "DATABASE_URL",
    "WORKER_ID",
    "CONCURRENCY",
    "POLL_INTERVAL",
    "ROUTES",
    "LOG_LEVEL",
    "DB_MIN_SIZE",
    "DB_MAX_SIZE",
    "DB_TIMEOUT",
    "DB_COMMAND_TIMEOUT",
    "HEARTBEAT_INTERVAL",
    "GRACEFUL_SHUTDOWN_TIMEOUT",
    "METRICS_PORT",
    "METRICS_ENABLED",
    "HEALTH_ENABLED",
    "CONDUCTOR_HANDLERS_MODULE",
]


def _unset_all(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear every environment variable that WorkerSettings reads."""
    for name in _ENV_VARS:
        monkeypatch.delenv(name, raising=False)


class TestFromEnv:
    """Verify env-var parsing and defaults."""

    def test_requires_database_url(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        with pytest.raises(ConductorException):
            WorkerSettings.from_env()

    def test_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        s = WorkerSettings.from_env()
        assert s.database_url == "postgresql://u:p@h/db"
        assert s.worker_id is None
        assert s.concurrency == 10
        assert s.poll_interval == 0.5
        assert s.routes == ["default"]
        assert s.log_level == "INFO"
        assert s.pool_min_size == 2
        assert s.pool_max_size == 10
        assert s.metrics_port == 8000
        assert s.metrics_enabled is True
        assert s.health_enabled is True
        assert s.handlers_module is None

    def test_type_coercion(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("CONCURRENCY", "5")
        monkeypatch.setenv("POLL_INTERVAL", "0.1")
        monkeypatch.setenv("METRICS_PORT", "9000")
        monkeypatch.setenv("DB_MAX_SIZE", "20")
        s = WorkerSettings.from_env()
        assert s.concurrency == 5
        assert s.poll_interval == 0.1
        assert s.metrics_port == 9000
        assert s.pool_max_size == 20

    def test_bool_parsing_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("METRICS_ENABLED", "false")
        monkeypatch.setenv("HEALTH_ENABLED", "0")
        s = WorkerSettings.from_env()
        assert s.metrics_enabled is False
        assert s.health_enabled is False

    def test_bool_parsing_true(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("METRICS_ENABLED", "yes")
        monkeypatch.setenv("HEALTH_ENABLED", "1")
        s = WorkerSettings.from_env()
        assert s.metrics_enabled is True
        assert s.health_enabled is True

    def test_routes_list(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("ROUTES", "default,email, priority ")
        s = WorkerSettings.from_env()
        assert s.routes == ["default", "email", "priority"]

    def test_worker_id_and_handlers_module(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("WORKER_ID", "custom-worker")
        monkeypatch.setenv("CONDUCTOR_HANDLERS_MODULE", "myapp.handlers")
        s = WorkerSettings.from_env()
        assert s.worker_id == "custom-worker"
        assert s.handlers_module == "myapp.handlers"

    def test_log_level_uppercased(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("LOG_LEVEL", "debug")
        s = WorkerSettings.from_env()
        assert s.log_level == "DEBUG"


class TestBuildWorker:
    """Verify build_worker produces a configured Worker."""

    def test_build_worker_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        monkeypatch.setenv("WORKER_ID", "build-test")
        monkeypatch.setenv("CONCURRENCY", "4")
        monkeypatch.setenv("ROUTES", "a,b")
        s = WorkerSettings.from_env()
        w = s.build_worker()
        assert w.worker_id == "build-test"
        assert w._concurrency == 4
        assert w._routes == ["a", "b"]

    def test_to_dict(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _unset_all(monkeypatch)
        monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@h/db")
        s = WorkerSettings.from_env()
        d = s.to_dict()
        assert d["database_url"] == "postgresql://u:p@h/db"
        assert d["concurrency"] == 10
        assert "handlers_module" in d
