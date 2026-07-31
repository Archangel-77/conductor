"""
Unit tests for the example Grafana dashboard JSON.

These tests do **not** require a database or a Grafana instance — they
validate the structure and metric references of the dashboard artifact
under ``docs/grafana/conductor-dashboard.json`` so it stays in sync with
the metrics actually exported by ``conductor.observability.metrics``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

# Path to the dashboard artifact relative to the project root.
DASHBOARD_PATH = (
    Path(__file__).resolve().parents[2] / "docs" / "grafana" / "conductor-dashboard.json"
)

# Metric names exported by conductor.observability.metrics (plus the
# auto-generated histogram series, which are not written directly in code).
ALLOWED_METRIC_NAMES = {
    "conductor_tasks_submitted_total",
    "conductor_tasks_completed_total",
    "conductor_tasks_failed_total",
    "conductor_tasks_retried_total",
    "conductor_task_duration_seconds",
    "conductor_task_duration_seconds_bucket",
    "conductor_task_duration_seconds_sum",
    "conductor_task_duration_seconds_count",
    "conductor_workers_active",
    "conductor_dlq_size",
    "conductor_pending_tasks",
}

# Panels required by the Sprint 5 documentation checklist.
REQUIRED_PANEL_TITLES = [
    "Task Throughput",
    "Task Latency",
    "Error Rate",
    "Active Workers",
]

# PromQL metric-name token pattern (also matches the _bucket/_sum/_count suffix).
METRIC_TOKEN_RE = re.compile(r"\bconductor_[a-z0-9_]+")


def _load_dashboard() -> dict[str, Any]:
    """Load and parse the dashboard JSON artifact."""
    assert DASHBOARD_PATH.exists(), f"Dashboard file not found: {DASHBOARD_PATH}"
    with DASHBOARD_PATH.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    assert isinstance(data, dict), "Dashboard root must be a JSON object"
    return data


def _collect_promql_targets(data: Any, acc: list[str]) -> None:
    """Recursively collect every PromQL ``expr`` string from the dashboard."""
    if isinstance(data, dict):
        if "expr" in data and isinstance(data["expr"], str):
            acc.append(data["expr"])
        for value in data.values():
            _collect_promql_targets(value, acc)
    elif isinstance(data, list):
        for item in data:
            _collect_promql_targets(item, acc)


class TestDashboardStructure:
    """Verify the dashboard JSON parses and has the expected shape."""

    def test_file_is_valid_json_object(self) -> None:
        data = _load_dashboard()
        assert "title" in data
        assert "panels" in data
        assert isinstance(data["panels"], list)

    def test_dashboard_title(self) -> None:
        data = _load_dashboard()
        assert data["title"] == "Conductor — Task Queue Overview"

    def test_has_all_required_panels(self) -> None:
        data = _load_dashboard()
        titles = {p.get("title") for p in data["panels"]}
        for required in REQUIRED_PANEL_TITLES:
            assert required in titles, f"Missing required panel: {required}"


class TestDashboardPromQl:
    """Verify every PromQL expression references only real exported metrics."""

    def test_all_metric_names_are_known(self) -> None:
        data = _load_dashboard()
        exprs: list[str] = []
        _collect_promql_targets(data, exprs)
        assert exprs, "Dashboard must contain at least one PromQL target"

        for expr in exprs:
            tokens = METRIC_TOKEN_RE.findall(expr)
            assert tokens, f"No metric tokens found in expression: {expr}"
            for token in tokens:
                assert (
                    token in ALLOWED_METRIC_NAMES
                ), f"Unknown metric '{token}' in expression: {expr}"

    def test_uses_templated_datasource(self) -> None:
        """Datasource must be templated, not a hardcoded UID."""
        data = _load_dashboard()
        raw = json.dumps(data)
        assert "${DS_PROMETHEUS}" in raw

    def test_histogram_quantile_uses_bucket_series(self) -> None:
        """Latency queries must operate on the histogram bucket series."""
        data = _load_dashboard()
        exprs: list[str] = []
        _collect_promql_targets(data, exprs)
        quantile_exprs = [e for e in exprs if "histogram_quantile" in e]
        assert quantile_exprs
        for expr in quantile_exprs:
            assert (
                "conductor_task_duration_seconds_bucket" in expr
            ), f"histogram_quantile missing _bucket series: {expr}"


class TestDashboardRendering:
    """Verify dashboard-level defaults that affect rendering."""

    def test_has_refresh_interval(self) -> None:
        data = _load_dashboard()
        assert data.get("refresh") == "30s"

    def test_has_default_time_range(self) -> None:
        data = _load_dashboard()
        time_range = data.get("time", {})
        assert time_range.get("from") == "now-1h"
        assert time_range.get("to") == "now"

    def test_templating_defines_prometheus_datasource(self) -> None:
        data = _load_dashboard()
        templating = data.get("templating", {})
        variables = templating.get("list", [])
        names = {v.get("name") for v in variables}
        assert "DS_PROMETHEUS" in names
