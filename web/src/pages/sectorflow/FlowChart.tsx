import { type PointerEvent, useEffect, useMemo, useRef, useState } from "react";
import { fmtValue, type Mode, placeEndLabels, type Sector, SLOTS, seriesOf } from "./model";

const AUCTION_W = 8; // the 09:25 auction gets its own band, ~8 minutes wide

function measureLabel(text: string, fontPx: number): number {
  if (typeof document === "undefined") return text.length * fontPx;
  try {
    const canvas = document.createElement("canvas");
    const ctx = canvas.getContext("2d");
    if (!ctx) return text.length * fontPx;
    ctx.font = `600 ${fontPx}px "IBM Plex Sans", "PingFang SC", sans-serif`;
    return Math.ceil(ctx.measureText(text).width) + 8;
  } catch {
    return text.length * fontPx;
  }
}

function slotX(slot: number): number {
  return slot === 0 ? 0 : AUCTION_W + (slot - 1);
}
const X_MAX = slotX(SLOTS - 1);

function niceStep(span: number, n: number): number {
  const raw = span / n;
  const mag = 10 ** Math.floor(Math.log10(raw || 1));
  return [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => span / s <= n) ?? raw;
}

export interface FlowChartProps {
  t: string[];
  shown: Sector[];
  colors: Map<string, string>;
  mode: Mode;
  /** Replay cursor (index into t); lines are drawn up to it. */
  upto: number;
  phone: boolean;
  focus: string | null;
  onFocus: (name: string | null) => void;
}

export function FlowChart({ t, shown, colors, mode, upto, phone, focus, onFocus }: FlowChartProps) {
  const box = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(() =>
    typeof window !== "undefined" ? Math.min(800, window.innerWidth - 40) : 800,
  );
  const [hover, setHover] = useState<number | null>(null);
  const press = useRef<{ timer: number; scrub: boolean; id: number } | null>(null);
  useEffect(() => {
    const el = box.current;
    if (!el) return;
    const ro = new ResizeObserver(() => setW(el.clientWidth || 800));
    ro.observe(el);
    setW(el.clientWidth || 800);
    return () => ro.disconnect();
  }, []);

  const h = phone ? 300 : 440;
  const font = phone ? 12 : 11;
  const labelGap = font + 10;
  const mTop = 22;
  const mBot = 24;
  const mLeft = phone ? 34 : 46;
  const ih = h - mTop - mBot;

  const vals = useMemo(
    () => shown.map((s) => seriesOf(s, mode).slice(0, upto + 1)),
    [shown, mode, upto],
  );
  const flat = vals.flat().filter((v): v is number => v != null);
  let lo = Math.min(0, ...flat);
  let hi = Math.max(0, ...flat);
  if (hi - lo < 1e-9) {
    hi += 1;
    lo -= 1;
  }
  const step = niceStep(hi - lo, phone ? 4 : 6);
  lo = Math.floor(lo / step) * step;
  hi = Math.ceil(hi / step) * step;
  const Y = (v: number) => mTop + ((hi - v) / (hi - lo)) * ih;
  const ticks: number[] = [];
  for (let v = lo; v <= hi + step / 2; v += step) ticks.push(Number(v.toFixed(6)));
  const unit = mode === "net" ? "亿元" : "%";

  const path = (vs: (number | null)[]) => {
    let d = "";
    let pen = false;
    vs.forEach((v, i) => {
      if (v == null) {
        pen = false;
        return;
      }
      d += `${pen ? "L" : "M"}${X(i).toFixed(1)} ${Y(v).toFixed(1)}`;
      pen = true;
    });
    return d;
  };
  const lastVal = (vs: (number | null)[]) => {
    for (let i = vs.length - 1; i >= 0; i--) {
      const v = vs[i];
      if (v != null) return v;
    }
    return 0;
  };

  // Labels live in a right gutter, spaced apart, with a leader from each line end.
  const allLabs = shown.map((s, k) => {
    const v = lastVal(vals[k] ?? []);
    const labelName = phone && s.name.length > 4 ? s.name.slice(0, 4) : s.name;
    return { s, v, y: Y(v), text: `${labelName} ${fmtValue(v, mode)}`, labelName };
  });
  const keep = new Set(
    phone && allLabs.length > 6
      ? [
          ...[...allLabs].sort((a, b) => b.v - a.v).slice(0, 3),
          ...[...allLabs].sort((a, b) => a.v - b.v).slice(0, 3),
        ].map((item) => item.s.name)
      : allLabs.map((item) => item.s.name),
  );
  const labs = allLabs.filter((item) => keep.has(item.s.name)).sort((a, b) => a.y - b.y);
  const gutter = Math.max(96, ...labs.map((item) => measureLabel(item.text, font)), 0);
  const m = { l: mLeft, r: gutter + 22, t: mTop, b: mBot };
  const iw = Math.max(50, w - m.l - m.r);
  const X = (slot: number) => m.l + (slotX(slot) / X_MAX) * iw;
  const plotRight = m.l + iw;
  const placed = placeEndLabels(labs, {
    gap: labelGap,
    top: m.t + 8,
    bottom: m.t + ih - 4,
    plotRight,
  });
  labs.forEach((item, i) => {
    item.y = placed[i]?.y ?? item.y;
  });

  const slotAt = (clientX: number): number => {
    const rect = box.current?.getBoundingClientRect();
    if (!rect) return upto;
    const x = ((clientX - rect.left - m.l) / iw) * X_MAX;
    const s = x < AUCTION_W / 2 ? 0 : Math.round(x - AUCTION_W + 1);
    return Math.max(0, Math.min(upto, s));
  };
  const nearest = (slot: number, clientY: number): string | null => {
    const rect = box.current?.getBoundingClientRect();
    if (!rect) return null;
    const y = clientY - rect.top;
    let best: string | null = null;
    let bd = phone ? 28 : 18;
    shown.forEach((s, k) => {
      const v = vals[k]?.[slot];
      if (v == null) return;
      const d = Math.abs(Y(v) - y);
      if (d < bd) {
        bd = d;
        best = s.name;
      }
    });
    return best;
  };

  const onMove = (e: PointerEvent<HTMLDivElement>) => {
    if (e.pointerType === "mouse") {
      const s = slotAt(e.clientX);
      setHover(s);
      onFocus(nearest(s, e.clientY));
    } else if (press.current?.scrub) {
      setHover(slotAt(e.clientX));
    }
  };
  const onDown = (e: PointerEvent<HTMLDivElement>) => {
    if (e.pointerType === "mouse") return;
    const x = e.clientX;
    const y = e.clientY;
    const id = e.pointerId;
    const target = e.currentTarget;
    const timer = window.setTimeout(() => {
      if (!press.current) return;
      press.current.scrub = true; // long-press: crosshair follows the finger
      target.setPointerCapture?.(id);
      setHover(slotAt(x));
      navigator.vibrate?.(8);
    }, 280);
    press.current = { timer, scrub: false, id };
    // a tap picks the nearest line and shows its value at that time
    const s = slotAt(x);
    const n = nearest(s, y);
    setHover(s);
    onFocus(n === focus ? null : n);
  };
  const onUp = () => {
    if (press.current) window.clearTimeout(press.current.timer);
    press.current = null;
  };

  const hv = hover ?? (phone ? null : null);
  const rows =
    hv == null
      ? []
      : shown
          .map((s, k) => ({ s, v: vals[k]?.[hv] ?? null }))
          .filter((r) => r.v != null)
          .sort((a, b) => (b.v ?? 0) - (a.v ?? 0));
  const tipW = 178;
  const tipX = hv == null ? 0 : X(hv) + 14 + tipW > w - 8 ? X(hv) - 14 - tipW : X(hv) + 14;
  const xt: [number, string][] = phone
    ? [
        [0, "09:25"],
        [61, "10:30"],
        [121, "11:30/13:00"],
        [182, "14:00"],
        [242, "15:00"],
      ]
    : [
        [0, "09:25"],
        [31, "10:00"],
        [61, "10:30"],
        [91, "11:00"],
        [121, "11:30/13:00"],
        [152, "13:30"],
        [182, "14:00"],
        [212, "14:30"],
        [242, "15:00"],
      ];

  return (
    <div
      ref={box}
      className="sf-chart"
      style={{ height: h }}
      onPointerMove={onMove}
      onPointerDown={onDown}
      onPointerUp={onUp}
      onPointerCancel={onUp}
      onPointerLeave={(e) => {
        if (e.pointerType === "mouse") {
          setHover(null);
          onFocus(null);
        }
      }}
    >
      <svg width={w} height={h} role="img" aria-label="板块累计净流入走势">
        <defs>
          <pattern
            id="sf-hatch"
            width="6"
            height="6"
            patternUnits="userSpaceOnUse"
            patternTransform="rotate(45)"
          >
            <line x1="0" y1="0" x2="0" y2="6" className="sf-hatch" strokeWidth="2" />
          </pattern>
        </defs>
        <rect x={X(0) - 2} y={m.t} width={X(1) - X(0) - 2} height={ih} className="sf-auction" />
        <text x={(X(0) + X(1)) / 2 - 2} y={m.t + 11} className="sf-axis-note" textAnchor="middle">
          竞价
        </text>
        {upto < SLOTS - 1 ? (
          <rect
            x={X(upto)}
            y={m.t}
            width={X(SLOTS - 1) - X(upto)}
            height={ih}
            fill="url(#sf-hatch)"
            opacity={0.5}
          />
        ) : null}
        {ticks.map((v) => (
          <g key={v}>
            <line
              x1={m.l}
              x2={m.l + iw}
              y1={Y(v)}
              y2={Y(v)}
              className={v === 0 ? "sf-zero" : "sf-grid"}
            />
            <text
              x={m.l - 6}
              y={Y(v) + 4}
              textAnchor="end"
              className={`sf-tick ${v > 0 ? "up" : v < 0 ? "down" : ""}`}
            >
              {v > 0 ? "+" : ""}
              {mode === "net" ? v : v.toFixed(step < 1 ? 1 : 0)}
            </text>
          </g>
        ))}
        <text x={m.l - 6} y={m.t - 9} textAnchor="end" className="sf-axis-note">
          {unit}
        </text>
        <line x1={X(121)} x2={X(121)} y1={m.t} y2={m.t + ih} className="sf-grid" />
        {xt.map(([s, l]) => (
          <text
            key={s}
            x={X(s)}
            y={m.t + ih + 17}
            className="sf-xtick"
            textAnchor={s === 0 ? "start" : s === 242 ? "end" : "middle"}
          >
            {l}
          </text>
        ))}
        {shown
          .map((s, k) => ({ s, k }))
          .sort((a, b) => Number(a.s.name === focus) - Number(b.s.name === focus))
          .map(({ s, k }) => {
            const c = colors.get(s.name) ?? "#888";
            const d = path(vals[k] ?? []);
            const on = focus === s.name;
            const dim = focus != null && !on;
            const v0 = vals[k]?.[0];
            return (
              <g key={s.name} opacity={dim ? 0.18 : 1}>
                {on ? <path d={d} fill="none" stroke={c} strokeWidth={7} opacity={0.2} /> : null}
                <path
                  d={d}
                  fill="none"
                  stroke={c}
                  strokeWidth={on ? 2.6 : 1.6}
                  strokeLinejoin="round"
                />
                {v0 != null ? <circle cx={X(0)} cy={Y(v0)} r={2.6} fill={c} /> : null}
                <circle
                  cx={X(Math.min(upto, (vals[k]?.length ?? 1) - 1))}
                  cy={Y(lastVal(vals[k] ?? []))}
                  r={on ? 4 : 3}
                  fill={c}
                />
              </g>
            );
          })}
        <line x1={X(upto)} x2={X(upto)} y1={m.t} y2={m.t + ih} className="sf-now" />
        <g>
          <rect
            x={X(upto) - 22}
            y={m.t - 18}
            width={44}
            height={15}
            rx={3}
            className="sf-now-tag"
          />
          <text x={X(upto)} y={m.t - 7} textAnchor="middle" className="sf-now-text">
            {t[upto] ?? ""}
          </text>
        </g>
        {labs.map(({ s, v, y, labelName }, index) => {
          const c = colors.get(s.name) ?? "#888";
          const xText = placed[index]?.x ?? plotRight + 12;
          const yLine = Y(v);
          const dim = focus != null && focus !== s.name;
          const rail = plotRight + 6;
          return (
            <g
              key={s.name}
              opacity={dim ? 0.3 : 1}
              className="sf-endlab"
              onPointerDown={(e) => {
                e.stopPropagation();
                onFocus(focus === s.name ? null : s.name);
              }}
            >
              <path
                d={`M${plotRight} ${yLine} H${rail} V${y} H${xText - 4}`}
                stroke={c}
                fill="none"
                opacity={0.55}
              />
              <text
                x={xText}
                y={y + 4}
                textAnchor="start"
                fill={c}
                fontSize={font}
                fontWeight={focus === s.name ? 800 : 600}
              >
                {labelName} <tspan className={v >= 0 ? "up" : "down"}>{fmtValue(v, mode)}</tspan>
              </text>
            </g>
          );
        })}
        {hv != null ? (
          <g pointerEvents="none">
            <line x1={X(hv)} x2={X(hv)} y1={m.t} y2={m.t + ih} className="sf-cross" />
            {shown.map((s, k) => {
              const v = vals[k]?.[hv];
              return v == null ? null : (
                <circle
                  key={s.name}
                  cx={X(hv)}
                  cy={Y(v)}
                  r={s.name === focus ? 4.5 : 2.6}
                  fill={colors.get(s.name)}
                  className="sf-dot"
                />
              );
            })}
            {!phone ? (
              <g>
                <rect
                  x={tipX}
                  y={m.t + 6}
                  width={tipW}
                  height={32 + rows.length * 18}
                  rx={8}
                  className="sf-tip"
                />
                <text x={tipX + 12} y={m.t + 26} className="sf-tip-h">
                  {hv === 0 ? "09:25 集合竞价" : `${t[hv]} 累计`}
                </text>
                {rows.map((r, i) => {
                  const yy = m.t + 46 + i * 18;
                  const on = r.s.name === focus;
                  return (
                    <g key={r.s.name}>
                      {on ? (
                        <rect
                          x={tipX + 4}
                          y={yy - 12}
                          width={tipW - 8}
                          height={17}
                          rx={4}
                          className="sf-tip-hl"
                        />
                      ) : null}
                      <rect
                        x={tipX + 12}
                        y={yy - 6}
                        width={9}
                        height={3}
                        rx={1.5}
                        fill={colors.get(r.s.name)}
                      />
                      <text x={tipX + 28} y={yy} className="sf-tip-n" fontWeight={on ? 700 : 400}>
                        {r.s.name}
                      </text>
                      <text
                        x={tipX + tipW - 12}
                        y={yy}
                        textAnchor="end"
                        className={`sf-tip-v ${(r.v ?? 0) >= 0 ? "up" : "down"}`}
                      >
                        {fmtValue(r.v, mode)}
                      </text>
                    </g>
                  );
                })}
              </g>
            ) : null}
          </g>
        ) : null}
      </svg>
      {phone && hv != null ? (
        <div className="sf-phone-tip" onPointerDown={(e) => e.stopPropagation()}>
          <div className="sf-phone-tip-h">
            <b>{hv === 0 ? "09:25 竞价" : t[hv]}</b>
            <button
              type="button"
              onClick={() => {
                setHover(null);
                onFocus(null);
              }}
              aria-label="关闭"
            >
              ✕
            </button>
          </div>
          <div className="sf-phone-tip-rows">
            {rows.map((r) => (
              <span key={r.s.name} className={r.s.name === focus ? "on" : ""}>
                <i style={{ background: colors.get(r.s.name) }} />
                {r.s.name}
                <b className={(r.v ?? 0) >= 0 ? "up" : "down"}>{fmtValue(r.v, mode)}</b>
              </span>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}
