"""
Packaging guard rails.

These tests exist because a **packaging** defect cannot be caught by the rest of
the suite: every development and CI environment installs the ``dev`` extra, which
pulls in ``grpcio-tools`` → ``protobuf``, so the missing ``protobuf`` runtime
dependency in v0.2.0 was invisible until a clean ``pip install
conductor-task-queue`` was tried (``import conductor`` → ``ModuleNotFoundError:
No module named 'google'``).

The checks are static — they read the sources and the two packaging files, and
need no network and no installed extras:

* every third-party module imported anywhere under ``conductor/`` is declared as
  a core dependency or in an extra;
* ``pyproject.toml`` and ``setup.py`` declare the same requirements (the project
  ships both);
* the declared ``protobuf`` floor is at least the version the generated gRPC
  stubs validate against at runtime.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT / "conductor"
PYPROJECT = REPO_ROOT / "pyproject.toml"
SETUP_PY = REPO_ROOT / "setup.py"
PB2 = PACKAGE_ROOT / "grpc" / "conductor_pb2.py"

IMPORT_TO_DISTRIBUTION: dict[str, str] = {
    "aiohttp": "aiohttp",
    "aiosqlite": "aiosqlite",
    "asyncmy": "asyncmy",
    "asyncpg": "asyncpg",
    "croniter": "croniter",
    "dotenv": "python-dotenv",
    "fastapi": "fastapi",
    "google.protobuf": "protobuf",
    "grpc": "grpcio",
    "opentelemetry": "opentelemetry-api",
    "prometheus_client": "prometheus-client",
    "pydantic": "pydantic",
    "uvicorn": "uvicorn",
}
"""Third-party import root → the distribution that provides it."""


def _imported_roots() -> set[str]:
    """Return every third-party top-level module imported under ``conductor/``."""
    roots: set[str] = set()
    for path in PACKAGE_ROOT.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [node.module] if node.module and node.level == 0 else []
            else:
                continue
            for name in names:
                root = name.split(".")[0]
                if root == "conductor" or root in sys.stdlib_module_names:
                    continue
                roots.add(str(name if name.startswith("google") else root))
    return roots


def _distribution_for(module: str) -> str:
    """Map an imported module to its distribution name."""
    for prefix, distribution in IMPORT_TO_DISTRIBUTION.items():
        if module == prefix or module.startswith(f"{prefix}."):
            return distribution
    raise AssertionError(
        f"'{module}' is imported by conductor/ but has no entry in "
        "IMPORT_TO_DISTRIBUTION — add it (and declare the dependency)."
    )


def _requirement_name(requirement: str) -> str:
    """Return the distribution name from a PEP 508 requirement string."""
    match = re.match(r"^[A-Za-z0-9._-]+", requirement)
    assert match is not None, f"Unparseable requirement: {requirement}"
    return match.group(0).lower().replace("_", "-")


def _pyproject_requirements() -> tuple[set[str], dict[str, set[str]]]:
    """Return ``(core, extras)`` requirement names from ``pyproject.toml``."""
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    project = data["project"]
    core = {_requirement_name(item) for item in project["dependencies"]}
    extras = {
        name: {_requirement_name(item) for item in items}
        for name, items in project.get("optional-dependencies", {}).items()
    }
    return core, extras


def _setup_py_requirements() -> tuple[set[str], dict[str, set[str]]]:
    """Return ``(core, extras)`` requirement names from ``setup.py``.

    ``setup.py`` is parsed with :mod:`ast` rather than imported so the check
    works without executing the file.
    """
    tree = ast.parse(SETUP_PY.read_text(encoding="utf-8"))
    core: set[str] = set()
    extras: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        for keyword in node.keywords:
            if keyword.arg == "install_requires":
                core = {_requirement_name(str(item)) for item in ast.literal_eval(keyword.value)}
            elif keyword.arg == "extras_require":
                raw = ast.literal_eval(keyword.value)
                extras = {
                    str(name): {_requirement_name(str(item)) for item in items}
                    for name, items in raw.items()
                }
    assert core, "Could not find install_requires in setup.py"
    return core, extras


def _protobuf_floor() -> tuple[int, int, int]:
    """Return the protobuf version the generated stubs validate against."""
    text = PB2.read_text(encoding="utf-8")
    match = re.search(
        r"ValidateProtobufRuntimeVersion\(\s*"
        r"_runtime_version\.Domain\.PUBLIC,\s*"
        r"(\d+),\s*(\d+),\s*(\d+),",
        text,
    )
    assert match is not None, (
        "Could not find ValidateProtobufRuntimeVersion() in conductor_pb2.py — "
        "regenerate the stubs with scripts/generate_grpc.py and update the "
        "protobuf floor in pyproject.toml/setup.py."
    )
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


class TestDeclaredDependencies:
    """Every third-party import under ``conductor/`` must be installable."""

    def test_every_import_is_declared(self) -> None:
        core, extras = _pyproject_requirements()
        declared = set(core)
        for items in extras.values():
            declared |= items

        missing: list[str] = []
        for module in sorted(_imported_roots()):
            distribution = _distribution_for(module)
            if distribution not in declared:
                # opentelemetry-api/sdk/exporter all share the import root.
                if distribution == "opentelemetry-api" and any(
                    name.startswith("opentelemetry-") for name in declared
                ):
                    continue
                missing.append(f"{module} (needs {distribution})")

        assert not missing, (
            "conductor/ imports modules whose distribution is not declared in "
            "pyproject.toml, so a clean `pip install conductor-task-queue` would "
            "fail at import time: " + ", ".join(missing)
        )

    def test_pyproject_and_setup_declare_the_same_requirements(self) -> None:
        pyproject_core, pyproject_extras = _pyproject_requirements()
        setup_core, setup_extras = _setup_py_requirements()

        assert pyproject_core == setup_core, (
            "install_requires differs between pyproject.toml and setup.py — "
            "the project ships both, so they must stay in sync."
        )
        assert set(pyproject_extras) == set(setup_extras), (
            "Extras differ between pyproject.toml and setup.py: "
            f"{sorted(set(pyproject_extras) ^ set(setup_extras))}"
        )
        for name in sorted(pyproject_extras):
            assert (
                pyproject_extras[name] == setup_extras[name]
            ), f"Extra '{name}' differs between pyproject.toml and setup.py."

    def test_protobuf_floor_matches_the_generated_stubs(self) -> None:
        core, _ = _pyproject_requirements()
        assert "protobuf" in core, "protobuf must be a core dependency (grpc stubs)"

        declared = next(
            item
            for item in tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"][
                "dependencies"
            ]
            if _requirement_name(item) == "protobuf"
        )
        floor_match = re.search(r">=\s*(\d+)\.(\d+)\.(\d+)", declared)
        assert floor_match is not None, f"No '>=' floor in {declared!r}"
        floor = tuple(int(part) for part in floor_match.groups())

        required = _protobuf_floor()
        assert floor >= required, (
            f"Declared protobuf floor {floor} is older than the gencode version "
            f"{required} that conductor_pb2.py validates against — the runtime "
            "check would fail on install."
        )


class TestPackageData:
    """The wheel must ship the assets the runtime needs."""

    def test_declared_package_data_covers_frontend_and_typing(self) -> None:
        data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
        package_data: dict[str, Any] = data["tool"]["setuptools"]["package-data"]
        entries = package_data["conductor"]

        assert "py.typed" in entries
        assert any(entry.startswith("web/dist/") for entry in entries), (
            "The built dashboard (conductor/web/dist) is served by the API, so it "
            "must be declared in [tool.setuptools.package-data]."
        )

    def test_dashboard_bundle_is_committed(self) -> None:
        """CI verifies freshness with ``git diff --exit-code``; it must exist."""
        index = PACKAGE_ROOT / "web" / "dist" / "index.html"
        assert index.is_file(), (
            "conductor/web/dist/index.html is missing — rebuild the frontend with "
            "bash scripts/build_frontend.sh and commit the result."
        )
