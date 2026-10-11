import { type MouseEvent, type ReactNode, useEffect, useRef, useState } from "react";

/**
 * Lean SVG charts shared by backtest, factor and paper pages: one axis style,
 * one tooltip, theme colours from tokens.css. No chart library.
 */

export interface LineSeries {
  name: string;
  values: readonly (number | null)[];
  color: string;
  dashed?: boolean;
  /** Fill between the line and the baseline (drawdown style). */
  area?: boolean;
}

interface Frame {
  w: number;
  h: number;
  left: number;
  right: number;
  top: number;
  bottom: number;
}

const FRAME: Frame = { w: 760, h: 200, left: 52, right: 12, top: 10, bottom: 22 };

function niceTicks(lo: number, hi: number, count = 4): number[] {
  const span = hi - lo || Math.abs(hi) || 1;
  const raw = span / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => span / s <= count) ?? raw;
  const out: number[] = [];
  for (let v = Math.ceil(lo / step) * step; v <= hi + step * 1e-9; v += step) {
    out.push(Number(v.toFixed(10)));
  }
  return out;
}

/** Track the container width so axis text stays at its real pixel size. */
function useWidth(fallback: number) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(fallback);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(([entry]) => {
      if (entry) setW(Math.max(240, Math.round(entry.contentRect.width)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  return { ref, w };
}

function useHover(n: number, f: Frame) {
  const [idx, setIdx] = useState<number | null>(null);
  const onMove = (e: MouseEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect();
    const px = ((e.clientX - box.left) / box.width) * f.w;
    const t = (px - f.left) / (f.w - f.left - f.right);
    setIdx(Math.max(0, Math.min(n - 1, Math.round(t * (n - 1)))));
  };
  return { idx, onMove, onLeave: () => setIdx(null) };
}

export interface TimeChartProps {
  label: string;
  dates: readonly string[];
  series: readonly LineSeries[];
  /** Formats axis ticks and tooltip values. */
  format: (v: number) => string;
  height?: number;
  /** Draw a dashed reference line (e.g. NAV 1.0 or IC 0). */
  baseline?: number;
  /** Bars instead of lines (first series only), coloured red-up / green-down around baseline. */
  bars?: boolean;
}

export function TimeChart({
  label,
  dates,
  series,
  format,
  height = 200,
  baseline,
  bars = false,
}: TimeChartProps) {
  const { ref, w } = useWidth(FRAME.w);
  const f = { ...FRAME, w, h: height };
  const n = dates.length;
  const hover = useHover(n, f);
  if (n < 2) return null;
  const all = series.flatMap((s) => s.values.filter((v): v is number => v !== null));
  if (baseline !== undefined) all.push(baseline);
  let lo = Math.min(...all);
  let hi = Math.max(...all);
  if (lo === hi) {
    lo -= 1;
    hi += 1;
  }
  const pad = (hi - lo) * 0.06;
  lo -= pad;
  hi += pad;
  const iw = f.w - f.left - f.right;
  const ih = f.h - f.top - f.bottom;
  const x = (i: number) => f.left + (i / (n - 1)) * iw;
  const y = (v: number) => f.top + (1 - (v - lo) / (hi - lo)) * ih;
  const ticks = niceTicks(lo, hi);
  const xTicks = [0, Math.floor((n - 1) / 2), n - 1];
  const base = baseline ?? lo;

  const path = (values: readonly (number | null)[]) => {
    let d = "";
    let pen = false;
    values.forEach((v, i) => {
      if (v === null) {
        pen = false;
        return;
      }
      d += `${pen ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
      pen = true;
    });
    return d;
  };

  const hi_ = hover.idx;
  const tooltip: ReactNode =
    hi_ === null ? null : (
      <div
        className="chart-tip"
        style={{
          left: `${(x(hi_) / f.w) * 100}%`,
          transform: x(hi_) > f.w * 0.7 ? "translateX(calc(-100% - 10px))" : "translateX(10px)",
        }}
      >
        <div className="chart-tip-date">{dates[hi_]}</div>
        {series.map((s) => {
          const v = s.values[hi_];
          return (
            <div key={s.name} className="chart-tip-row">
              <span className="chart-swatch" style={{ background: s.color }} />
              {s.name}
              <span className="num">{v == null ? "—" : format(v)}</span>
            </div>
          );
        })}
      </div>
    );

  return (
    <figure className="chart">
      {series.length > 1 ? (
        <figcaption className="chart-legend">
          {series.map((s) => (
            <span key={s.name}>
              <span className="chart-swatch" style={{ background: s.color }} />
              {s.name}
            </span>
          ))}
        </figcaption>
      ) : null}
      <div className="chart-body" ref={ref}>
        <svg
          viewBox={`0 0 ${f.w} ${f.h}`}
          width={f.w}
          height={f.h}
          role="img"
          aria-label={label}
          onMouseMove={hover.onMove}
          onMouseLeave={hover.onLeave}
        >
          {ticks.map((t) => (
            <g key={t}>
              <line x1={f.left} x2={f.w - f.right} y1={y(t)} y2={y(t)} className="chart-grid" />
              <text x={f.left - 6} y={y(t) + 3} textAnchor="end" className="chart-axis">
                {format(t)}
              </text>
            </g>
          ))}
          {xTicks.map((i, k) => (
            <text
              key={i}
              x={x(i)}
              y={f.h - 6}
              textAnchor={k === 0 ? "start" : k === 2 ? "end" : "middle"}
              className="chart-axis"
            >
              {dates[i]}
            </text>
          ))}
          {baseline !== undefined ? (
            <line
              x1={f.left}
              x2={f.w - f.right}
              y1={y(baseline)}
              y2={y(baseline)}
              className="chart-base"
            />
          ) : null}
          {bars && series[0]
            ? series[0].values.map((v, i) => {
                if (v === null) return null;
                const bw = Math.max(1, iw / n - 1);
                const top = Math.min(y(v), y(base));
                return (
                  <rect
                    // biome-ignore lint/suspicious/noArrayIndexKey: bars are positional.
                    key={i}
                    x={x(i) - bw / 2}
                    width={bw}
                    y={top}
                    height={Math.max(0.5, Math.abs(y(v) - y(base)))}
                    fill={v >= base ? "var(--up)" : "var(--down)"}
                    opacity={hi_ === null || hi_ === i ? 0.9 : 0.55}
                  />
                );
              })
            : series.map((s) => (
                <g key={s.name}>
                  {s.area ? (
                    <path
                      d={`${path(s.values)}L${x(n - 1)},${y(base)}L${x(0)},${y(base)}Z`}
                      fill={s.color}
                      opacity={0.18}
                    />
                  ) : null}
                  <path
                    d={path(s.values)}
                    fill="none"
                    stroke={s.color}
                    strokeWidth={s.dashed ? 1 : 1.6}
                    strokeDasharray={s.dashed ? "4 3" : undefined}
                  />
                </g>
              ))}
          {hi_ !== null ? (
            <line x1={x(hi_)} x2={x(hi_)} y1={f.top} y2={f.h - f.bottom} className="chart-cross" />
          ) : null}
        </svg>
        {tooltip}
      </div>
    </figure>
  );
}
