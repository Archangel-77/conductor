import { api } from "../api.js";
import { usePolling } from "../hooks.js";
import BarChart from "../components/BarChart.jsx";

function series(metrics, name) {
  const fam = metrics.find((m) => m.name === name);
  if (!fam) return [];
  return fam.samples.map((s) => ({
    label: s.labels?.task_type || s.labels?.route || "total",
    value: s.value,
  }));
}

function gauge(metrics, name) {
  const fam = metrics.find((m) => m.name === name);
  if (!fam || fam.samples.length === 0) return 0;
  return fam.samples[fam.samples.length - 1].value;
}

export default function MetricsPage() {
  const { data, error, loading } = usePolling(() => api.getMetrics(), 5000, []);

  if (error) return <div className="error">Failed to load metrics: {error}</div>;
  if (loading && !data) return <div className="muted">Loading…</div>;

  const metrics = data?.metrics ?? [];
  const submitted = series(metrics, "conductor_tasks_submitted");
  const completed = series(metrics, "conductor_tasks_completed");
  const failed = series(metrics, "conductor_tasks_failed");
  const retried = series(metrics, "conductor_tasks_retried");

  const pending = gauge(metrics, "conductor_pending_tasks");
  const workers = gauge(metrics, "conductor_workers_active");
  const dlq = gauge(metrics, "conductor_dlq_size");

  return (
    <section>
      <div className="page-head">
        <h2>Metrics</h2>
        <span className="muted">Prometheus counters &amp; gauges</span>
      </div>

      <div className="cards">
        <div className="metric-card">
          <span className="metric-value">{pending}</span>
          <span className="metric-label">Pending tasks</span>
        </div>
        <div className="metric-card">
          <span className="metric-value">{workers}</span>
          <span className="metric-label">Active workers</span>
        </div>
        <div className="metric-card">
          <span className="metric-value">{dlq}</span>
          <span className="metric-label">Dead-letter size</span>
        </div>
      </div>

      <div className="grid">
        <div className="card">
          <h3>Submitted by type</h3>
          <BarChart data={submitted} color="#3b82f6" />
        </div>
        <div className="card">
          <h3>Completed by type</h3>
          <BarChart data={completed} color="#10b981" />
        </div>
        <div className="card">
          <h3>Failed by type</h3>
          <BarChart data={failed} color="#ef4444" />
        </div>
        <div className="card">
          <h3>Retried by type</h3>
          <BarChart data={retried} color="#8b5cf6" />
        </div>
      </div>

      <details className="card">
        <summary>Raw metrics ({metrics.length} families)</summary>
        <pre className="code">{JSON.stringify(metrics, null, 2)}</pre>
      </details>
    </section>
  );
}
