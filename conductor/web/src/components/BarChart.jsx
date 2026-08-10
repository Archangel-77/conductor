// Hand-rolled SVG bar chart — keeps the dashboard dependency-free.
// `data` is an array of { label, value } objects.

export default function BarChart({ data, height = 180, color = "#10b981" }) {
  const width = 640;
  const pad = 30;
  const max = Math.max(1, ...data.map((d) => d.value));
  const bw = (width - pad * 2) / Math.max(1, data.length);
  const chartH = height - pad - 20;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="chart" role="img">
      {data.map((d, i) => {
        const h = (d.value / max) * chartH;
        const x = pad + i * bw + bw * 0.2;
        const y = height - pad - h;
        return (
          <g key={d.label}>
            <rect
              x={x}
              y={y}
              width={bw * 0.6}
              height={Math.max(h, 1)}
              rx={3}
              fill={color}
            >
              <title>{`${d.label}: ${d.value}`}</title>
            </rect>
            <text
              x={x + bw * 0.3}
              y={height - pad + 14}
              textAnchor="middle"
              className="chart-label"
            >
              {d.label}
            </text>
            <text
              x={x + bw * 0.3}
              y={y - 6}
              textAnchor="middle"
              className="chart-value"
            >
              {d.value}
            </text>
          </g>
        );
      })}
    </svg>
  );
}
