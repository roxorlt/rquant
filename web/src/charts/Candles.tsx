import { useEffect, useRef, useState } from "react";

export interface Candle {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  amount?: number | null;
}

const PAD = { left: 52, right: 10, top: 8, bottom: 20 };

function ma(values: number[], n: number): (number | null)[] {
  let sum = 0;
  return values.map((v, i) => {
    sum += v;
    if (i >= n) sum -= values[i - n] ?? 0;
    return i >= n - 1 ? sum / n : null;
  });
}

/** Daily K-line (red up / green down, A-share convention) with MA5/20/60 and amount bars. */
export function Candles({ bars, height = 300 }: { bars: readonly Candle[]; height?: number }) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(800);
  const [hover, setHover] = useState<number | null>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => {
      if (e) setW(Math.max(320, Math.floor(e.contentRect.width)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  if (bars.length === 0) return <p className="hint">没有日线数据。</p>;
  const volH = 50;
  const priceH = height - volH - PAD.top - PAD.bottom - 8;
  const lo = Math.min(...bars.map((b) => b.low));
  const hi = Math.max(...bars.map((b) => b.high));
  const step = (w - PAD.left - PAD.right) / bars.length;
  const x = (i: number) => PAD.left + step * (i + 0.5);
  const y = (v: number) => PAD.top + ((hi - v) / (hi - lo || 1)) * priceH;
  const maxAmt = Math.max(...bars.map((b) => b.amount ?? 0), 1);
  const volTop = PAD.top + priceH + 8;
  const closes = bars.map((b) => b.close);
  const lines: [string, (number | null)[]][] = [
    ["var(--warn)", ma(closes, 5)],
    ["var(--accent)", ma(closes, 20)],
    ["var(--muted)", ma(closes, 60)],
  ];
  const path = (vals: (number | null)[]) =>
    vals
      .map((v, i) =>
        v == null ? "" : `${vals[i - 1] == null ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`,
      )
      .join("");
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => lo + (hi - lo) * t);
  const h = hover == null ? bars[bars.length - 1] : bars[hover];
  const bw = Math.max(1, step * 0.7);
  return (
    <div className="candles" ref={ref}>
      {h ? (
        <div className="chart-readout">
          <span className="when">{h.date}</span>
          <span>开 {h.open.toFixed(2)}</span>
          <span>高 {h.high.toFixed(2)}</span>
          <span>低 {h.low.toFixed(2)}</span>
          <span className={h.close >= h.open ? "up" : "down"}>收 {h.close.toFixed(2)}</span>
          <span className="hint">MA5 / MA20 / MA60</span>
        </div>
      ) : null}
      <svg
        width={w}
        height={height}
        viewBox={`0 0 ${w} ${height}`}
        role="img"
        aria-label="日K线"
        onMouseLeave={() => setHover(null)}
        onMouseMove={(e) => {
          const r = e.currentTarget.getBoundingClientRect();
          const i = Math.floor((e.clientX - r.left - PAD.left) / step);
          setHover(i >= 0 && i < bars.length ? i : null);
        }}
      >
        {ticks.map((t) => (
          <g key={t}>
            <line className="chart-grid" x1={PAD.left} x2={w - PAD.right} y1={y(t)} y2={y(t)} />
            <text className="chart-axis" x={PAD.left - 6} y={y(t) + 3} textAnchor="end">
              {t.toFixed(2)}
            </text>
          </g>
        ))}
        {bars.map((b, i) => {
          const up = b.close >= b.open;
          const cls = up ? "candle up" : "candle down";
          return (
            <g key={b.date} className={cls}>
              <line x1={x(i)} x2={x(i)} y1={y(b.high)} y2={y(b.low)} />
              <rect
                x={x(i) - bw / 2}
                width={bw}
                y={y(Math.max(b.open, b.close))}
                height={Math.max(1, Math.abs(y(b.open) - y(b.close)))}
              />
              <rect
                x={x(i) - bw / 2}
                width={bw}
                y={volTop + volH - ((b.amount ?? 0) / maxAmt) * volH}
                height={((b.amount ?? 0) / maxAmt) * volH}
                opacity={0.5}
              />
            </g>
          );
        })}
        {lines.map(([color, vals]) => (
          <path key={color} d={path(vals)} fill="none" stroke={color} strokeWidth={1.2} />
        ))}
        {hover != null ? (
          <line
            className="chart-cross"
            x1={x(hover)}
            x2={x(hover)}
            y1={PAD.top}
            y2={volTop + volH}
          />
        ) : null}
        {[0, Math.floor(bars.length / 2), bars.length - 1].map((i) => (
          <text key={i} className="chart-axis" x={x(i)} y={height - 4} textAnchor="middle">
            {bars[i]?.date}
          </text>
        ))}
      </svg>
    </div>
  );
}
