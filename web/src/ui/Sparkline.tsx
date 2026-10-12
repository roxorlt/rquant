/** 241 trading minutes: 09:30-11:30 (121) + 13:01-15:00 (120). */
export function minuteIndex(hhmm: string): number | null {
  const [h, m] = hhmm.split(":").map(Number);
  if (h === undefined || m === undefined || Number.isNaN(h) || Number.isNaN(m)) return null;
  const t = h * 60 + m;
  if (t < 570) return 0;
  if (t <= 690) return t - 570;
  if (t < 781) return 120;
  return Math.min(240, t - 780 + 120);
}

interface SparklineProps {
  values: readonly number[];
  times: readonly string[];
  base: number | null;
  width?: number;
  height?: number;
  label?: string;
}

/**
 * Intraday mini chart on a fixed full-session x axis (so a morning-only line stops
 * mid-cell), dashed prior-close baseline, red above / green below (A-share colours).
 */
export function Sparkline({
  values,
  times,
  base,
  width = 132,
  height = 30,
  label,
}: SparklineProps) {
  const pts = values
    .map((v, i) => [minuteIndex(times[i] ?? ""), v] as const)
    .filter((p): p is readonly [number, number] => p[0] !== null && Number.isFinite(p[1]));
  if (pts.length < 2) return <span className="faint">—</span>;
  const ys = pts.map((p) => p[1]);
  const ref = base ?? ys[0] ?? 0;
  const span = Math.max(...ys.map((y) => Math.abs(y - ref)), ref * 0.01, 1e-6);
  const pad = 2;
  const x = (i: number) => pad + (i / 240) * (width - 2 * pad);
  const y = (v: number) => height / 2 - ((v - ref) / span) * (height / 2 - pad);
  const line = pts
    .map((p, i) => `${i ? "L" : "M"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`)
    .join("");
  const last = pts[pts.length - 1] as readonly [number, number];
  const first = pts[0] as readonly [number, number];
  const tone = last[1] > ref ? "up" : last[1] < ref ? "down" : "flat";
  const area = `${line}L${x(last[0]).toFixed(1)},${y(ref).toFixed(1)}L${x(first[0]).toFixed(1)},${y(ref).toFixed(1)}Z`;
  return (
    <svg
      className={`sparkline ${tone}`}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      role="img"
      aria-label={label ?? "分时走势"}
    >
      <line className="sparkline-noon" x1={x(120)} x2={x(120)} y1={0} y2={height} />
      <line className="sparkline-base" x1={pad} x2={width - pad} y1={y(ref)} y2={y(ref)} />
      <path className="sparkline-area" d={area} />
      <path className="sparkline-line" d={line} />
      <circle className="sparkline-dot" cx={x(last[0])} cy={y(last[1])} r={2} />
    </svg>
  );
}
