// Small fetch helper for the Conductor dashboard API.
//
// Set VITE_API_BASE at build time to point the SPA at a non-default API
// location (defaults to "/api", which the dashboard server serves itself).

const BASE = import.meta.env.VITE_API_BASE || "/api";

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      if (body && body.detail) detail = body.detail;
    } catch {
      /* response had no JSON body */
    }
    throw new Error(detail || `Request failed (${res.status})`);
  }
  return res.json();
}

function qs(params) {
  const url = new URLSearchParams();
  Object.entries(params || {}).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") url.set(k, v);
  });
  const s = url.toString();
  return s ? `?${s}` : "";
}

export const api = {
  listTasks: (params) => request(`/tasks${qs(params)}`),
  getTask: (id) => request(`/tasks/${encodeURIComponent(id)}`),
  cancelTask: (id) => request(`/tasks/${encodeURIComponent(id)}/cancel`, { method: "POST" }),
  listWorkers: (params) => request(`/workers${qs(params)}`),
  getMetrics: () => request("/metrics"),
  listDlq: (params) => request(`/dlq${qs(params)}`),
  retryDlq: (id) => request(`/dlq/${encodeURIComponent(id)}/retry`, { method: "POST" }),
  discardDlq: (id) => request(`/dlq/${encodeURIComponent(id)}/discard`, { method: "POST" }),
  health: () => request("/health"),
};
