import { useState } from "react";
import { api } from "../api.js";
import { usePolling, formatTime } from "../hooks.js";

export default function DlqPage() {
  const [includeDiscarded, setIncludeDiscarded] = useState(false);
  const [notice, setNotice] = useState(null);

  const { data, error, loading, reload } = usePolling(
    () => api.listDlq({ limit: 100, include_discarded: includeDiscarded || undefined }),
    5000,
    [includeDiscarded]
  );

  async function act(action, id) {
    try {
      if (action === "retry") await api.retryDlq(id);
      else await api.discardDlq(id);
      setNotice(`${action}ed ${id.slice(0, 8)}…`);
      reload();
    } catch (err) {
      setNotice(err.message || String(err));
    }
  }

  return (
    <section>
      <div className="page-head">
        <h2>Dead Letter Queue</h2>
        <label className="checkbox">
          <input
            type="checkbox"
            checked={includeDiscarded}
            onChange={(e) => setIncludeDiscarded(e.target.checked)}
          />
          Include discarded
        </label>
      </div>

      {notice && <div className="notice">{notice}</div>}
      {error && <div className="error">Failed to load DLQ: {error}</div>}
      {loading && !data && <div className="muted">Loading…</div>}

      {data && (
        <table className="table">
          <thead>
            <tr>
              <th>Task ID</th>
              <th>Type</th>
              <th>Attempts</th>
              <th>Route</th>
              <th>Moved at</th>
              <th>Error</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((t) => (
              <tr key={t.task_id} className={t.discarded ? "dimmed" : ""}>
                <td className="mono">{t.task_id.slice(0, 12)}</td>
                <td>{t.task_type}</td>
                <td>{t.attempts}</td>
                <td>{t.route}</td>
                <td className="muted">{formatTime(t.moved_at)}</td>
                <td className="mono error-text">
                  {(t.error_message || "—").slice(0, 60)}
                </td>
                <td>
                  {t.discarded ? (
                    <span className="muted">discarded</span>
                  ) : (
                    <span className="actions">
                      <button onClick={() => act("retry", t.task_id)}>Retry</button>
                      <button className="danger" onClick={() => act("discard", t.task_id)}>
                        Discard
                      </button>
                    </span>
                  )}
                </td>
              </tr>
            ))}
            {data.items.length === 0 && (
              <tr>
                <td colSpan={7} className="muted">
                  Dead-letter queue is empty. 🎉
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </section>
  );
}
