import { useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api.js";
import { usePolling, formatTime } from "../hooks.js";
import StatusBadge from "../components/StatusBadge.jsx";

const STATUSES = [
  "pending",
  "processing",
  "completed",
  "failed",
  "retrying",
  "cancelled",
  "blocked",
];

export default function TasksPage() {
  const [status, setStatus] = useState("");
  const [search, setSearch] = useState("");
  const [route, setRoute] = useState("");
  const [page, setPage] = useState(0);
  const limit = 20;

  const { data, error, loading } = usePolling(
    () =>
      api.listTasks({
        status: status || undefined,
        search: search || undefined,
        route: route || undefined,
        limit,
        offset: page * limit,
      }),
    5000,
    [status, search, route, page]
  );

  const total = data?.total ?? 0;
  const pages = Math.max(1, Math.ceil(total / limit));

  return (
    <section>
      <div className="page-head">
        <h2>Tasks</h2>
        <span className="muted">
          {total} total · {data?.items?.length ?? 0} shown
        </span>
      </div>

      <div className="filters">
        <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(0); }}>
          <option value="">All statuses</option>
          {STATUSES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        <input
          type="text"
          placeholder="Search task id / type…"
          value={search}
          onChange={(e) => { setSearch(e.target.value); setPage(0); }}
        />
        <input
          type="text"
          placeholder="Route"
          value={route}
          onChange={(e) => { setRoute(e.target.value); setPage(0); }}
        />
      </div>

      {error && <div className="error">Failed to load tasks: {error}</div>}
      {loading && !data && <div className="muted">Loading…</div>}

      {data && (
        <>
          <table className="table">
            <thead>
              <tr>
                <th>Task ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>Route</th>
                <th>Priority</th>
                <th>Attempts</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {data.items.map((t) => (
                <tr key={t.task_id}>
                  <td>
                    <Link to={`/tasks/${t.task_id}`} className="mono">
                      {t.task_id.slice(0, 8)}
                    </Link>
                  </td>
                  <td>{t.task_type}</td>
                  <td>
                    <StatusBadge status={t.status} />
                  </td>
                  <td>{t.route}</td>
                  <td>{t.priority}</td>
                  <td>
                    {t.attempt}/{t.max_retries}
                  </td>
                  <td className="muted">{formatTime(t.created_at)}</td>
                </tr>
              ))}
              {data.items.length === 0 && (
                <tr>
                  <td colSpan={7} className="muted">
                    No tasks match the current filters.
                  </td>
                </tr>
              )}
            </tbody>
          </table>

          <div className="pager">
            <button disabled={page === 0} onClick={() => setPage(page - 1)}>
              ‹ Prev
            </button>
            <span className="muted">
              Page {page + 1} / {pages}
            </span>
            <button disabled={page + 1 >= pages} onClick={() => setPage(page + 1)}>
              Next ›
            </button>
          </div>
        </>
      )}
    </section>
  );
}
