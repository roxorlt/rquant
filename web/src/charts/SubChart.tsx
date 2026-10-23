import { useEffect, useRef, useState } from "react";
import { formatFixed } from "@/format/number";
import { PAD } from "./Candles";

export interface SubSeries {
  name: string;
  values: readonly (number | null)[];
  color?: string;
  /** histogram bars coloured by sign (red up / green down) */
  bars?: boolean;
}

/** Indicator pane aligned with ``Candles`` (same padding, window and hover index). */
export function SubChart({
  label,
  series,
  view,
  hover,
  height = 90,
  format = (v) => formatFixed(v, 2),
  levels = [],
}: {
  label: string;
  series: SubSeries[];
  view: [number, number];
  hover: number | null;
  height?: number;
  format?: (v: number) => string;
  levels?: number[];
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(800);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => {
      if (e) setW(Math.max(320, Math.floor(e.contentRect.width)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const [s, e] = view;
  const n = e - s;
  const vis = series.map((x) => x.values.slice(s, e));
  const nums = vis.flat().filter((v): v is number => v != null && Number.isFinite(v));
  const has = nums.length > 0;
  let lo = has ? Math.min(...nums, ...levels) : 0;
  let hi = has ? Math.max(...nums, ...levels) : 1;
  if (series.some((x) => x.bars)) {
    lo = Math.min(lo, 0);
    hi = Math.max(hi, 0);
  }
  if (hi === lo) hi = lo + 1;
  const top = 4;
  const h = height - top - 4;
  const plotR = w - PAD.right;
  const step = (plotR - PAD.left) / Math.max(n, 1);
  const x = (i: number) => PAD.left + step * (i + 0.5);
  const y = (v: number) => top + ((hi - v) / (hi - lo)) * h;
  const at = hover != null && hover >= s && hover < e ? hover - s : n - 1;
  return (
    <div className="subchart" ref={ref}>
      <div className="chart-readout">
        <span className="when">{label}</span>
        {series.map((x, k) => {
          const v = vis[k]?.[at];
          return (
            <span key={x.name} style={x.color ? { color: x.color } : undefined}>
              {x.name} {v == null ? "—" : format(v)}
            </span>
          );
        })}
      </div>
      <svg width={w} height={height} viewBox={`0 0 ${w} ${height}`} role="img" aria-label={label}>
        <line className="chart-grid" x1={PAD.left} x2={plotR} y1={y(hi)} y2={y(hi)} />
        <line className="chart-grid" x1={PAD.left} x2={plotR} y1={y(lo)} y2={y(lo)} />
        {[hi, lo].map((v) => (
          <text key={v} className="chart-axis" x={plotR + 6} y={y(v) + (v === hi ? 8 : 0)}>
            {format(v)}
          </text>
        ))}
        {levels.map((l) => (
          <line
            key={l}
            className="chart-grid"
            x1={PAD.left}
            x2={plotR}
            y1={y(l)}
            y2={y(l)}
            strokeDasharray="3 3"
          />
        ))}
        {series.map((sr, k) => {
          const vals = vis[k] ?? [];
          if (sr.bars) {
            const bw = Math.max(1, step * 0.7);
            return (
              <g key={sr.name}>
                {vals.map((v, i) =>
                  v == null ? null : (
                    <rect
                      // biome-ignore lint/suspicious/noArrayIndexKey: bars are positional.
                      key={i}
                      x={x(i) - bw / 2}
                      width={bw}
                      y={Math.min(y(v), y(0))}
                      height={Math.max(0.5, Math.abs(y(v) - y(0)))}
                      fill={sr.color ?? (v >= 0 ? "var(--up)" : "var(--down)")}
                      opacity={0.75}
                    />
                  ),
                )}
              </g>
            );
          }
          const d = vals
            .map((v, i) =>
              v == null
                ? ""
                : `${vals[i - 1] == null ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`,
            )
            .join("");
          return <path key={sr.name} d={d} fill="none" stroke={sr.color} strokeWidth={1.2} />;
        })}
        {hover != null && hover >= s && hover < e ? (
          <line className="chart-cross" x1={x(at)} x2={x(at)} y1={0} y2={height} />
        ) : null}
      </svg>
    </div>
  );
}
