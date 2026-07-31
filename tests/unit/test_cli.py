"""
Unit tests for the Conductor CLI.

These tests do **not** require a database — they verify argument parsing,
env-based worker construction, and handlers-module registration.
"""

# pylint: disable=missing-class-docstring,import-outside-toplevel,protected-access

from __future__ import annotations

import sys
from types import SimpleNamespace
from typing import Any

import pytest

from conductor.cli import build_parser, main, register_handlers
from conductor.core.worker import Worker
from conductor.exceptions import ConductorException


class TestParser:
    """Verify CLI argument parsing."""

    def test_worker_subcommand_parses(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["worker"])
        assert args.command == "worker"
        assert args.handlers is None

    def test_worker_handlers_flag(self) -> None:
        parser = build_parser()
        args = parser.parse_args(["worker", "--handlers", "myapp.handlers"])
        assert args.handlers == "myapp.handlers"

    def test_missing_subcommand_raises(self) -> None:
        parser = build_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])


class TestRegisterHandlers:
    """Verify the handlers-module plugin contract."""

    def test_register_handlers_attaches_handler(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        registered: list[str] = []

        def fake_register(worker: Worker) -> None:
            @worker.task("fake_type")
            async def handler(_payload: dict[str, Any]) -> dict[str, Any]:
                return {"ok": True}

            registered.append(worker.worker_id)

        monkeypatch.setitem(sys.modules, "fake_handlers", SimpleNamespace(register=fake_register))

        worker = Worker(database_url="postgresql://u:p@h/db")
        register_handlers(worker, "fake_handlers")

        assert registered == [worker.worker_id]
        assert "fake_type" in worker._handlers

    def test_missing_register_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(sys.modules, "no_register_mod", SimpleNamespace())

        worker = Worker(database_url="postgresql://u:p@h/db")
        with pytest.raises(ConductorException):
            register_handlers(worker, "no_register_mod")

    def test_missing_module_raises(self) -> None:
        worker = Worker(database_url="postgresql://u:p@h/db")
        with pytest.raises(ConductorException):
            register_handlers(worker, "definitely.not.a.module")


class TestMain:
    """Verify the entry point's error handling (no DB needed)."""

    def test_main_missing_database_url_returns_2(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        for name in [
            "DATABASE_URL",
            "WORKER_ID",
            "CONCURRENCY",
            "ROUTES",
            "CONDUCTOR_HANDLERS_MODULE",
        ]:
            monkeypatch.delenv(name, raising=False)
        # Use a nonexistent env-file so no stray ./.env is picked up.
        assert main(["worker", "--env-file", "/nonexistent/does-not-exist.env"]) == 2
