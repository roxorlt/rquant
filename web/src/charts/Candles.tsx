import { useEffect, useRef, useState } from "react";

export interface Candle {
  date: string;
  vol?: number | null;
  open: number;
  high: number;
  low: number;
  close: number;
  amount?: number | null;
}

export const PAD = { left: 8, right: 58, top: 8, bottom: 20 };

/** Clamp a [start, end) window of ``n`` bars to at least ``min`` bars. */
export function clampView(s: number, e: number, n: number, min = 10): [number, number] {
  let len = Math.min(n, Math.max(min, Math.round(e - s)));
  let a = Math.round(s);
  if (a < 0) a = 0;
  if (a + len > n) a = n - len;
  len = Math.min(len, n - a);
  return [a, a + len];
}

function ma(values: number[], n: number): (number | null)[] {
  let sum = 0;
  return values.map((v, i) => {
    sum += v;
    if (i >= n) sum -= values[i - n] ?? 0;
    return i >= n - 1 ? sum / n : null;
  });
}

export interface SideDist {
  /** Bins from ``lo`` to ``hi``. */
  dist: readonly number[];
  lo: number;
  hi: number;
  close: number;
  avg: number;
}

export interface ProfileOverlay {
  dist: readonly number[];
  lo: number;
  hi: number;
  poc: number;
  val: number;
  vah: number;
}

/**
 * Daily K-line (red up / green down, A-share convention) with MA5/20/60 and amount bars.
 * Optional: a fixed price ``range`` (to share a grid with distributions), a chip
 * distribution column on the right (``side``), a volume-profile overlay, and ``onHover``.
 */
export function Candles({
  bars: all,
  height = 300,
  view,
  onView,
  side,
  profile,
  onHover,
  overlays,
  anchor,
  onPick,
}: {
  bars: readonly Candle[];
  height?: number;
  /** visible window [start, end) over ``bars``; hover/anchor indices are into ``bars`` */
  view?: [number, number];
  onView?: (v: [number, number]) => void;
  side?: SideDist | null;
  profile?: ProfileOverlay | null;
  onHover?: (i: number | null) => void;
  /** extra price lines (e.g. anchored VWAP and its bands) */
  overlays?: { key: string; values: (number | null)[]; color: string; dash?: boolean }[];
  /** bar index marked as an anchor; ``onPick`` makes bars clickable */
  anchor?: number | null;
  onPick?: (i: number) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(800);
  const [hover, setHover] = useState<number | null>(null);
  const [hy, setHy] = useState<number | null>(null);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => {
      if (e) setW(Math.max(320, Math.floor(e.contentRect.width)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const [s0, e0] = view ?? [0, all.length];
  const bars = all.slice(s0, e0);
  const svgRef = useRef<SVGSVGElement>(null);
  const drag = useRef<{ x: number; view: [number, number]; moved: boolean } | null>(null);
  const live = useRef({ s0, e0, n: all.length, w, onView });
  live.current = { s0, e0, n: all.length, w, onView };
  useEffect(() => {
    const el = svgRef.current;
    if (!el) return;
    // wheel / trackpad pinch (ctrlKey) zooms around the cursor; horizontal swipe pans
    const onWheel = (e: WheelEvent) => {
      const { s0: a, e0: b, n, w: width, onView: cb } = live.current;
      if (!cb) return;
      e.preventDefault();
      const r = el.getBoundingClientRect();
      const span = b - a;
      if (Math.abs(e.deltaX) > Math.abs(e.deltaY) && !e.ctrlKey) {
        const shift = (e.deltaX / (width - PAD.left - PAD.right)) * span;
        cb(clampView(a + shift, b + shift, n));
        return;
      }
      const frac = Math.min(
        1,
        Math.max(0, (e.clientX - r.left - PAD.left) / (width - PAD.left - PAD.right)),
      );
      const k = Math.exp(e.deltaY * (e.ctrlKey ? 0.01 : 0.0025));
      const len = span * k;
      const at = a + span * frac;
      cb(clampView(at - len * frac, at - len * frac + len, n));
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, []);
  if (bars.length === 0) return <p className="hint">没有日线数据。</p>;
  const volH = 50;
  const priceH = height - volH - PAD.top - PAD.bottom - 8;
  const rawLo = Math.min(...bars.map((b) => b.low));
  const rawHi = Math.max(...bars.map((b) => b.high));
  const padY = (rawHi - rawLo) * 0.04 || rawHi * 0.01 || 1;
  const lo = rawLo - padY;
  const hi = rawHi + padY;
  const plotR = w - PAD.right;
  const chipW = (plotR - PAD.left) * 0.28;
  const step = (plotR - PAD.left) / bars.length;
  const x = (i: number) => PAD.left + step * (i + 0.5);
  const y = (v: number) => PAD.top + ((hi - v) / (hi - lo || 1)) * priceH;
  const maxAmt = Math.max(...bars.map((b) => b.amount ?? 0), 1);
  const volTop = PAD.top + priceH + 8;
  const closes = all.map((b) => b.close);
  const lines: [string, (number | null)[]][] = [
    ["var(--warn)", ma(closes, 5).slice(s0, e0)],
    ["var(--accent)", ma(closes, 20).slice(s0, e0)],
    ["var(--muted)", ma(closes, 60).slice(s0, e0)],
  ];
  const path = (vals: (number | null)[]) =>
    vals
      .map((v, i) =>
        v == null ? "" : `${vals[i - 1] == null ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`,
      )
      .join("");
  const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => lo + (hi - lo) * t);
  const h = hover == null ? bars[bars.length - 1] : bars[hover];
  const anchorAt = anchor == null ? null : anchor - s0;
  const bw = Math.max(1, step * 0.7);
  const scrub = (e: {
    currentTarget: Element;
    clientX: number;
    clientY: number;
    pointerType?: string;
  }) => {
    const r = e.currentTarget.getBoundingClientRect();
    const touch = e.pointerType != null && e.pointerType !== "mouse";
    let i = Math.floor((e.clientX - r.left - PAD.left) / step);
    // a finger can overshoot the edges; clamp so the crosshair sticks to the first/last bar
    if (touch) i = Math.min(bars.length - 1, Math.max(0, i));
    const next = i >= 0 && i < bars.length ? i : null;
    const yy = e.clientY - r.top;
    setHy(yy >= PAD.top && yy <= PAD.top + priceH ? yy : null);
    setHover(next);
    onHover?.(next == null ? null : next + s0);
  };
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
      {/* biome-ignore lint/a11y/useKeyWithClickEvents: picking an anchor is a pointer shortcut; a default anchor is always set */}
      <svg
        ref={svgRef}
        className={onView ? "pannable touch-scrub" : "touch-scrub"}
        width={w}
        height={height}
        viewBox={`0 0 ${w} ${height}`}
        role="img"
        aria-label="日K线"
        onMouseLeave={() => {
          setHover(null);
          setHy(null);
          onHover?.(null);
        }}
        onClick={() => {
          if (drag.current?.moved) return;
          if (hover != null) onPick?.(hover + s0);
        }}
        onPointerDown={(e) => {
          if (e.pointerType !== "mouse") {
            // touch / pen: drag scrubs the crosshair (pan-y keeps vertical page scroll)
            drag.current = null;
            scrub(e);
            return;
          }
          drag.current = { x: e.clientX, view: [s0, e0], moved: false };
        }}
        onPointerMove={(e) => {
          if (e.pointerType !== "mouse") scrub(e);
        }}
        onPointerUp={() => {
          setTimeout(() => {
            drag.current = null;
          }, 0);
        }}
        onMouseMove={(e) => {
          const d = drag.current;
          if (d && e.buttons === 1 && onView) {
            const dx = e.clientX - d.x;
            if (Math.abs(dx) > 3) d.moved = true;
            const shift = -dx / step;
            onView(clampView(d.view[0] + shift, d.view[1] + shift, all.length));
            return;
          }
          scrub(e);
        }}
      >
        {ticks.map((t) => (
          <g key={t}>
            <line className="chart-grid" x1={PAD.left} x2={plotR} y1={y(t)} y2={y(t)} />
            <text className="chart-axis" x={plotR + 6} y={y(t) + 3} textAnchor="start">
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
        {profile ? (
          <g className="vp" aria-label="成交量分布">
            {profile.dist.map((v, k) => {
              const bh = (profile.hi - profile.lo) / profile.dist.length;
              const b0 = profile.lo + bh * k;
              if (b0 + bh < lo || b0 > hi) return null;
              const top = y(b0 + bh);
              const inVa = b0 + bh / 2 >= profile.val && b0 + bh / 2 <= profile.vah;
              return (
                <rect
                  // biome-ignore lint/suspicious/noArrayIndexKey: bins are positional.
                  key={k}
                  x={PAD.left}
                  y={top}
                  width={(v / 1000) * (plotR - PAD.left) * 0.3}
                  height={Math.max(0.5, y(b0) - top - 0.5)}
                  className={inVa ? "vp-bin va" : "vp-bin"}
                />
              );
            })}
            <line
              className="vp-poc"
              x1={PAD.left}
              x2={plotR}
              y1={y(profile.poc)}
              y2={y(profile.poc)}
            />
            <line
              className="vp-va"
              x1={PAD.left}
              x2={plotR}
              y1={y(profile.vah)}
              y2={y(profile.vah)}
            />
            <line
              className="vp-va"
              x1={PAD.left}
              x2={plotR}
              y1={y(profile.val)}
              y2={y(profile.val)}
            />
          </g>
        ) : null}
        {side ? (
          <g aria-label="筹码分布">
            {side.dist.map((v, k) => {
              const bh = (side.hi - side.lo) / side.dist.length;
              const b0 = side.lo + bh * k;
              if (b0 + bh < lo || b0 > hi) return null;
              const top = y(b0 + bh);
              const mid = b0 + bh / 2;
              const len = (v / 1000) * chipW;
              return (
                <rect
                  // biome-ignore lint/suspicious/noArrayIndexKey: bins are positional.
                  key={k}
                  x={plotR - len}
                  y={top}
                  width={len}
                  height={Math.max(0.5, y(b0) - top - 0.3)}
                  className={mid <= side.close ? "chip win" : "chip lose"}
                />
              );
            })}
            <line
              className="chip-avg"
              x1={plotR - chipW}
              x2={plotR}
              y1={y(side.avg)}
              y2={y(side.avg)}
            />
            <rect
              className="axis-tag avg"
              x={plotR + 1}
              y={y(side.avg) - 8}
              width={PAD.right - 2}
              height={16}
            />
            <text className="axis-tag-t" x={plotR + 5} y={y(side.avg) + 4}>
              {side.avg.toFixed(2)}
            </text>
          </g>
        ) : null}
        <defs>
          <clipPath id="candles-plot">
            <rect x={PAD.left} y={PAD.top} width={plotR - PAD.left} height={priceH} />
          </clipPath>
        </defs>
        <g clipPath="url(#candles-plot)">
          {(overlays ?? []).map((o) => (
            <path
              key={o.key}
              d={path(o.values.slice(s0, e0))}
              fill="none"
              stroke={o.color}
              strokeWidth={o.dash ? 1 : 1.6}
              strokeDasharray={o.dash ? "4 3" : undefined}
            />
          ))}
          {anchorAt != null && anchorAt >= 0 && anchorAt < bars.length ? (
            <line
              className="avwap-anchor"
              x1={x(anchorAt)}
              x2={x(anchorAt)}
              y1={PAD.top}
              y2={volTop + volH}
            />
          ) : null}
          {lines.map(([color, vals]) => (
            <path key={color} d={path(vals)} fill="none" stroke={color} strokeWidth={1.2} />
          ))}
        </g>
        {hover != null ? (
          <line
            className="chart-cross"
            x1={x(hover)}
            x2={x(hover)}
            y1={PAD.top}
            y2={volTop + volH}
          />
        ) : null}
        {hy != null ? (
          <g>
            <line className="chart-cross" x1={PAD.left} x2={plotR} y1={hy} y2={hy} />
            <rect className="axis-tag" x={plotR + 1} y={hy - 8} width={PAD.right - 2} height={16} />
            <text className="axis-tag-t" x={plotR + 5} y={hy + 4}>
              {(hi - ((hy - PAD.top) / priceH) * (hi - lo)).toFixed(2)}
            </text>
          </g>
        ) : null}
        {[0, Math.floor(bars.length / 2), bars.length - 1].map((i) => (
          <text
            key={i}
            className="chart-axis"
            x={x(i)}
            y={height - 4}
            textAnchor={i === 0 ? "start" : i === bars.length - 1 ? "end" : "middle"}
          >
            {bars[i]?.date}
          </text>
        ))}
      </svg>
    </div>
  );
}
