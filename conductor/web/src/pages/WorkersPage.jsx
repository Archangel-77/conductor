import { api } from "../api.js";
import { usePolling, formatTime } from "../hooks.js";

export default function WorkersPage() {
  const { data, error, loading } = usePolling(
    () => api.listWorkers({ limit: 100 }),
    5000,
    []
  );

  return (
    <section>
      <div className="page-head">
        <h2>Workers</h2>
        <span className="muted">{data?.items?.length ?? 0} registered</span>
      </div>

      {error && <div className="error">Failed to load workers: {error}</div>}
      {loading && !data && <div className="muted">Loading…</div>}

      {data && (
        <table className="table">
          <thead>
            <tr>
              <th>Worker ID</th>
              <th>Status</th>
              <th>Current task</th>
              <th>Processed</th>
              <th>Failed</th>
              <th>Uptime</th>
              <th>Last heartbeat</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((w) => (
              <tr key={w.worker_id}>
                <td className="mono">{w.worker_id}</td>
                <td>{w.status}</td>
                <td className="mono">{w.current_task_id || "—"}</td>
                <td>{w.tasks_processed_total}</td>
                <td>{w.tasks_failed_total}</td>
                <td>{Math.round(w.uptime_seconds)}s</td>
                <td className="muted">{formatTime(w.last_heartbeat)}</td>
              </tr>
            ))}
            {data.items.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  No workers registered yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </section>
  );
}
