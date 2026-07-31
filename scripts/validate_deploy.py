#!/usr/bin/env python3
"""
Validate Conductor's deployment artifacts.

Checks (no Docker required):
- ``Dockerfile`` — base image, exposed port, entrypoint, healthcheck
- ``docker-compose.yml`` / ``docker-compose.prod.yml`` — services,
  worker healthcheck, volumes, backup sidecar
- ``examples/kubernetes.yaml`` — ConfigMap/Secret/Deployment/Service,
  probes, envFrom, replicas

If a container CLI (``docker``/``podman``) is present, it also runs
``docker compose config --quiet`` for both compose files.

Usage:
    pip install -e ".[dev]"   # provides pyyaml + type stubs
    python scripts/validate_deploy.py
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError as exc:  # pragma: no cover - environment dependent
    print("pyyaml is required: pip install pyyaml", file=sys.stderr)
    raise SystemExit(1) from exc

ROOT = Path(__file__).resolve().parents[1]

EXPECTED_K8S_KINDS = ["ConfigMap", "Secret", "Deployment", "Service"]


def _check(ok: bool, label: str, detail: str = "") -> bool:
    """Print a check result and return its success."""
    status = "OK  " if ok else "FAIL"
    suffix = f" - {detail}" if detail else ""
    print(f"  [{status}] {label}{suffix}")
    return ok


def validate_dockerfile() -> bool:
    """Verify the Dockerfile has the expected directives."""
    text = (ROOT / "Dockerfile").read_text()
    checks = [
        _check("FROM python:3.11-slim" in text, "Dockerfile base image"),
        _check("EXPOSE 8000" in text, "Dockerfile exposes 8000"),
        _check('ENTRYPOINT ["conductor"]' in text, "Dockerfile ENTRYPOINT"),
        _check('CMD ["worker"]' in text, "Dockerfile CMD"),
        _check("HEALTHCHECK" in text, "Dockerfile HEALTHCHECK"),
        _check("USER conductor" in text, "Dockerfile non-root user"),
    ]
    return all(checks)


def validate_compose(path: Path, expected_services: list[str]) -> bool:
    """Verify a compose file parses and contains the expected services."""
    data: dict[str, Any] = yaml.safe_load(path.read_text())
    services = data.get("services", {})
    results = [
        _check(
            set(expected_services) <= set(services),
            f"{path.name} services",
            f"expected {expected_services}, got {sorted(services)}",
        )
    ]

    worker = services.get("worker", {})
    results.append(_check(bool(worker.get("healthcheck")), f"{path.name} worker healthcheck"))

    if path.name == "docker-compose.prod.yml":
        volumes = data.get("volumes", {})
        results.append(_check("pgdata" in volumes, "prod compose pgdata volume"))
        results.append(_check("pgbackups" in volumes, "prod compose pgbackups volume"))
        results.append(_check("pg_backup" in services, "prod compose pg_backup sidecar"))
        results.append(
            _check(
                bool(worker.get("deploy", {}).get("replicas")),
                "prod compose replicas",
            )
        )

    return all(results)


def validate_kubernetes() -> bool:
    """Verify the Kubernetes manifest has the expected resources."""
    docs = [d for d in yaml.safe_load_all((ROOT / "examples" / "kubernetes.yaml").read_text()) if d]
    kinds = [d.get("kind") for d in docs]
    results = [_check(kinds == EXPECTED_K8S_KINDS, "kubernetes.yaml kinds", str(kinds))]

    deployment: dict[str, Any] = next((d for d in docs if d.get("kind") == "Deployment"), {})
    spec = deployment.get("spec", {})
    containers = spec.get("template", {}).get("spec", {}).get("containers", [{}])
    container = containers[0] if containers else {}

    results.extend(
        [
            _check(bool(container.get("livenessProbe")), "k8s liveness probe"),
            _check(bool(container.get("readinessProbe")), "k8s readiness probe"),
            _check(bool(container.get("envFrom")), "k8s envFrom (configMap + secret)"),
            _check(spec.get("replicas") == 3, "k8s replicas", "3"),
            _check(bool(container.get("resources")), "k8s resource limits"),
        ]
    )
    return all(results)


def run_compose_config() -> bool:
    """Run ``docker compose config`` on both files if a CLI is available."""
    cli = shutil.which("docker") or shutil.which("podman")
    if cli is None:
        print("  [SKIP] no container CLI found - run this on a Docker host.")
        return True

    ok = True
    for path in [ROOT / "docker-compose.yml", ROOT / "docker-compose.prod.yml"]:
        env = dict(os.environ)
        if path.name == "docker-compose.prod.yml":
            env.setdefault("POSTGRES_PASSWORD", "test")
        proc = subprocess.run(
            [cli, "compose", "-f", str(path), "config", "--quiet"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        ok &= _check(
            proc.returncode == 0,
            f"compose config {path.name}",
            proc.stderr.strip(),
        )
    return ok


def main() -> int:
    """Run every validation and return the process exit code."""
    print("Validating Conductor deployment artifacts...")
    results = [
        validate_dockerfile(),
        validate_compose(ROOT / "docker-compose.yml", ["postgres", "worker"]),
        validate_compose(ROOT / "docker-compose.prod.yml", ["postgres", "worker", "pg_backup"]),
        validate_kubernetes(),
        run_compose_config(),
    ]
    all_ok = all(results)
    print("Result:", "PASS" if all_ok else "FAIL")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
