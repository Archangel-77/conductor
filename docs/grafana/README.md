# Conductor — Grafana Dashboard

Example Grafana dashboard for visualizing the Prometheus metrics exported by
Conductor's `MetricsExporter` at `GET /metrics` (default port `8000`).

## Requirements

- A running Conductor worker with the metrics/health HTTP server enabled
  (`METRICS_ENABLED=true`, `METRICS_PORT=8000` — the defaults).
- A Prometheus instance configured to **scrape** that endpoint:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: "conductor"
    metrics_path: /metrics
    static_configs:
      - targets: ["<worker-host>:8000"]
```

- Grafana (any recent version) with a Prometheus datasource configured.

## Importing the dashboard

1. Open Grafana → **Dashboards → Import**.
2. Click **Upload dashboard JSON file** and select
   [`conductor-dashboard.json`](./conductor-dashboard.json) — or paste the
   file contents.
3. Choose your Prometheus datasource for the `DS_PROMETHEUS` variable.
4. Click **Import**.

## Panels

| Panel | Visualization | Query (5m rate window) |
|---|---|---|
| Active Workers | Stat / gauge | `conductor_workers_active` |
| Pending Tasks | Stat / gauge | `conductor_pending_tasks` |
| Dead Letter Queue Size | Stat / gauge | `conductor_dlq_size` |
| Task Throughput | Time series | `sum(rate(conductor_tasks_submitted_total[5m]))` and `sum(rate(conductor_tasks_completed_total[5m]))` |
| Task Throughput by Type | Bar chart | `sum(rate(conductor_tasks_completed_total[5m])) by (task_type)` |
| Task Latency | Time series | `histogram_quantile(0.50 / 0.95 / 0.99, sum(rate(conductor_task_duration_seconds_bucket[5m])) by (le))` |
| Error Rate | Time series | `sum(rate(conductor_tasks_failed_total[5m]))`, `sum(rate(conductor_tasks_retried_total[5m]))`, and error ratio `failed / max(completed, 0.0001)` |

## Metric reference

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `conductor_tasks_submitted_total` | counter | `task_type` | Tasks submitted to the queue |
| `conductor_tasks_completed_total` | counter | `task_type` | Tasks completed successfully |
| `conductor_tasks_failed_total` | counter | `task_type` | Tasks that failed execution |
| `conductor_tasks_retried_total` | counter | `task_type` | Tasks scheduled for retry |
| `conductor_task_duration_seconds` | histogram | `task_type` | Task execution duration (buckets `0.005` → `60` s) |
| `conductor_workers_active` | gauge | — | Workers with a heartbeat within 30 s |
| `conductor_dlq_size` | gauge | — | Non-discarded tasks in the dead-letter queue |
| `conductor_pending_tasks` | gauge | — | Tasks with status `pending` |

## Adjusting the rate window

All rate queries use a fixed `[5m]` window. To change it, replace `5m`
with your preferred Prometheus range (e.g. `[1m]`, `[15m]`, `[1h]`) in the
dashboard's query editor, or add a `$rate_interval` dashboard variable.

## Notes

- The dashboard is a **template**: the datasource is bound through the
  `DS_PROMETHEUS` variable, so it works in any Grafana instance without edits.
- The two stat panels for pending tasks and DLQ size are bonus panels that
  surface additional exported gauges beyond the four core graphs.
- Prometheus server configuration, Grafana provisioning YAML, and Docker
  deployment are covered in Sprint 6 (deployment documentation).
