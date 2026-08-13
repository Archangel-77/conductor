const COLORS = {
  pending: "#f59e0b",
  processing: "#3b82f6",
  completed: "#10b981",
  failed: "#ef4444",
  retrying: "#8b5cf6",
  cancelled: "#6b7280",
  blocked: "#64748b",
};

export default function StatusBadge({ status }) {
  return (
    <span
      className="badge"
      style={{ backgroundColor: COLORS[status] || "#6b7280" }}
    >
      {status}
    </span>
  );
}
