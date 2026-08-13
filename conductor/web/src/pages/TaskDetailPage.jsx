import { useState } from "react";
import { Link, useParams } from "react-router-dom";
import { api } from "../api.js";
import { usePolling, formatTime } from "../hooks.js";
import StatusBadge from "../components/StatusBadge.jsx";

export default function TaskDetailPage() {
  const { id } = useParams();
  const [notice, setNotice] = useState(null);

  const { data, error, loading, reload } = usePolling(() => api.getTask(id), 3000, [id]);

  async function onCancel() {
    try {
      await api.cancelTask(id);
      setNotice("Task cancelled.");
      reload();
    } catch (err) {
      setNotice(err.message || String(err));
    }
  }

  if (error) return <div className="error">Failed to load task: {error}</div>;
  if (loading && !data) return <div className="muted">Loading…</div>;
  if (!data) return <div className="muted">Task not found.</div>;

  return (
    <section>
      <div className="page-head">
        <h2>
          <Link to="/" className="back">
            ‹ Tasks
          </Link>
          <span className="mono">{data.task_id}</span>
        </h2>
        <StatusBadge status={data.status} />
      </div>

      {notice && <div className="notice">{notice}</div>}

      <div className="grid">
        <div className="card">
          <h3>Overview</h3>
          <dl className="kv">
            <dt>Type</dt>
            <dd>{data.task_type}</dd>
            <dt>Route</dt>
            <dd>{data.route}</dd>
            <dt>Depends on</dt>
            <dd>
              {data.depends_on && data.depends_on.length > 0 ? (
                data.depends_on.map((dep) => (
                  <div key={dep}>
                    <Link to={`/tasks/${dep}`} className="mono">
                      {dep.slice(0, 8)}
                    </Link>
                  </div>
                ))
              ) : (
                "—"
              )}
            </dd>
            <dt>Priority</dt>
            <dd>{data.priority}</dd>
            <dt>Attempt</dt>
            <dd>
              {data.attempt}/{data.max_retries}
            </dd>
            <dt>Created</dt>
            <dd>{formatTime(data.created_at)}</dd>
            <dt>Started</dt>
            <dd>{formatTime(data.started_at)}</dd>
            <dt>Completed</dt>
            <dd>{formatTime(data.completed_at)}</dd>
            <dt>Worker</dt>
            <dd>{data.worker_id || "—"}</dd>
          </dl>
          {(data.status === "pending" || data.status === "retrying") && (
            <button className="danger" onClick={onCancel}>
              Cancel task
            </button>
          )}
        </div>

        <div className="card">
          <h3>Payload</h3>
          <pre className="code">{JSON.stringify(data.payload, null, 2)}</pre>
        </div>
      </div>

      {data.error_message && (
        <div className="card">
          <h3>Last error</h3>
          <pre className="code error-text">{data.error_message}</pre>
        </div>
      )}

      {data.result && (
        <div className="card">
          <h3>Result</h3>
          <pre className="code">{JSON.stringify(data.result, null, 2)}</pre>
        </div>
      )}

      <div className="card">
        <h3>Retry history ({data.retries?.length ?? 0})</h3>
        {data.retries && data.retries.length > 0 ? (
          <table className="table">
            <thead>
              <tr>
                <th>Attempt</th>
                <th>Scheduled at</th>
                <th>Error</th>
              </tr>
            </thead>
            <tbody>
              {data.retries.map((r) => (
                <tr key={r.id}>
                  <td>{r.attempt}</td>
                  <td className="muted">{formatTime(r.scheduled_for)}</td>
                  <td className="mono error-text">{r.error_message || "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <p className="muted">No retries recorded.</p>
        )}
      </div>
    </section>
  );
}
