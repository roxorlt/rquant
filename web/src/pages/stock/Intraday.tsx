import { useEffect, useMemo, useRef, useState } from "react";
import { useIntraday } from "@/api/endpoints";
import { deltas, profile, stitch } from "@/charts/orderflow";
import { useTouchScrub } from "@/charts/useTouchScrub";
import { DatePicker, Panel } from "@/ui";

const P = { left: 56, right: 56, top: 8 };
const PRICE_H = 260;
const VOL_H = 60;
const DELTA_H = 80;
const GAP = 10;

/** 分时: price & VWAP vs prior close, volume, estimated order flow (all estimates from bars). */
export function IntradayPanel({ code, span = 1 }: { code: string; span?: 1 | 5 }) {
  const [day, setDay] = useState<string | undefined>(undefined);
  const q = useIntraday(code, day, span);
  const [bands, setBands] = useState(true);
  const [showVp, setShowVp] = useState(true);
  const [marks, setMarks] = useState(true);
  const [mode, setMode] = useState<"tick" | "bvc">("bvc");
  const [hover, setHover] = useState<number | null>(null);
  const ref = useRef<HTMLDivElement>(null);
  const [w, setW] = useState(900);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => {
      if (e) setW(Math.max(360, Math.floor(e.contentRect.width)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const d = q.data;
  const sessions = d?.sessions ?? [];
  const bars = useMemo(() => stitch(sessions), [sessions]);
  const vw = bars;
  const pc = sessions[0]?.pre_close ?? bars[0]?.open ?? 1;
  const delta = useMemo(() => {
    let acc = 0;
    return sessions
      .flatMap((x) => deltas(x.bars, x.pre_close ?? x.bars[0]?.open ?? 1, mode))
      .map((v) => {
        acc += v;
        return acc;
      });
  }, [sessions, mode]);
  const multi = sessions.length > 1;
  const [svgEl, setSvgEl] = useState<SVGSVGElement | null>(null);
  const scrubRef = useRef<(clientX: number) => void>(() => {});
  useTouchScrub(svgEl, (clientX) => scrubRef.current(clientX));
  if (!d || bars.length === 0) {
    return (
      <Panel title={span === 5 ? "5日分时" : "分时"}>
        <div ref={ref} />
        <p className="hint">
          {q.isLoading
            ? "加载中…"
            : "这只股票没有分钟线（研究库只覆盖约 300 只大市值股票的 5 分钟线）。"}
        </p>
      </Panel>
    );
  }
  const step = d.freq === "1min" ? 1 : 5;
  const n = 240 * Math.max(1, sessions.length);
  const plotR = w - P.right;
  const X = (i: number) => P.left + ((bars[i]?.x ?? 0) / n) * (plotR - P.left);
  const XM = (m: number) => P.left + (m / n) * (plotR - P.left);
  const di = d.days.indexOf(d.date);
  const go = (k: number) => {
    const x = d.days[di + k];
    if (x) setDay(x);
  };
  const lastVw = vw[vw.length - 1];
  const extra = bands && lastVw ? Math.max(...vw.map((v) => Math.abs(v.vwap - pc) + 2 * v.sd)) : 0;
  const dev =
    Math.max(
      ...bars.map((b) => Math.max(Math.abs(b.high - pc), Math.abs(b.low - pc))),
      extra,
      pc * 0.005,
    ) * 1.08;
  const lo = pc - dev;
  const hi = pc + dev;
  const Y = (v: number) => P.top + ((hi - v) / (hi - lo)) * PRICE_H;
  const volTop = P.top + PRICE_H + GAP;
  const maxVol = Math.max(...bars.map((b) => b.vol), 1);
  const dTop = volTop + VOL_H + GAP + 28;
  const dMin = Math.min(0, ...delta);
  const dMax = Math.max(0, ...delta);
  const DY = (v: number) => dTop + ((dMax - v) / (dMax - dMin || 1)) * DELTA_H;
  const H = dTop + DELTA_H + 18;
  // brk: start a new subpath at each day (VWAP/bands reset daily in the 5日 view)
  const line = (vals: number[], brk = false) =>
    vals
      .map((v, i) => {
        const cont = i > 0 && !(brk && bars[i]?.day !== bars[i - 1]?.day);
        return `${cont ? "L" : "M"}${X(i).toFixed(1)},${Y(v).toFixed(1)}`;
      })
      .join("");
  const dline = delta
    .map((v, i) => `${i ? "L" : "M"}${X(i).toFixed(1)},${DY(v).toFixed(1)}`)
    .join("");
  const prof = profile(bars, lo, hi, 48);
  const profMax = Math.max(...prof.dist, 1);
  const profW = (plotR - P.left) * 0.16;
  // markers: volume > 3× the day's median; |bar return| > 3σ of the day's bar returns
  const vols = [...bars.map((b) => b.vol)].sort((a, b) => a - b);
  const medVol = vols[Math.floor(vols.length / 2)] ?? 0;
  const rets = bars.map((b, i) => b.close / (i ? (bars[i - 1]?.close ?? b.open) : pc) - 1);
  const rsd = Math.sqrt(rets.reduce((s, r) => s + r * r, 0) / (rets.length || 1));
  const at = hover ?? bars.length - 1;
  const scrub = (el: Element, clientX: number, touch: boolean) => {
    const r = el.getBoundingClientRect();
    let xm = clientX - r.left;
    // a finger may overshoot the plot edges; clamp so the crosshair sticks to the ends
    if (touch) xm = Math.min(plotR, Math.max(P.left, xm));
    let best = 0;
    bars.forEach((_, i) => {
      if (Math.abs(X(i) - xm) < Math.abs(X(best) - xm)) best = i;
    });
    setHover(xm >= P.left && xm <= plotR ? best : null);
  };
  scrubRef.current = (x) => scrub(svgEl as Element, x, true);
  const hb = bars[at];
  const hv = vw[at];
  const pct = (v: number) => `${v >= pc ? "+" : ""}${(((v - pc) / pc) * 100).toFixed(2)}%`;
  const tone = (v: number) => (v > pc ? "up" : v < pc ? "down" : "");
  const ticks = [-1, -0.5, 0, 0.5, 1].map((k) => pc + dev * k * 0.9);
  const times: [string, number][] = multi
    ? []
    : [
        ["09:30", 0],
        ["10:30", 60],
        ["11:30/13:00", 120],
        ["14:00", 180],
        ["15:00", 240],
      ];
  const totalVol = bars.reduce((s, b) => s + b.vol, 0);
  return (
    <Panel
      title={span === 5 ? "5日分时" : "分时"}
      sub={`${multi ? `${sessions[0]?.date} ~ ` : ""}${d.date} · ${d.freq === "1min" ? "1 分钟线" : "5 分钟线"} · ${d.source === "intraday" ? "盘中库" : "研究库"}；黄线 = 均价(VWAP)，资金/订单流均为由 K 线估算`}
      actions={
        <span className="qc-ctl">
          <span className="seg" title={`共 ${d.days.length} 个交易日有分钟线`}>
            <button type="button" disabled={di <= 0} onClick={() => go(-1)} aria-label="前一交易日">
              ‹
            </button>
            <DatePicker
              label="分时日期"
              value={d.date}
              allowed={d.days}
              onChange={(x) => setDay(x)}
            />
            <button
              type="button"
              disabled={di < 0 || di >= d.days.length - 1}
              onClick={() => go(1)}
              aria-label="后一交易日"
            >
              ›
            </button>
            {di < d.days.length - 1 ? (
              <button type="button" onClick={() => setDay(undefined)}>
                最新
              </button>
            ) : null}
          </span>
          <label>
            <input type="checkbox" checked={bands} onChange={(e) => setBands(e.target.checked)} />{" "}
            VWAP ±1σ/2σ
          </label>
          <label>
            <input type="checkbox" checked={showVp} onChange={(e) => setShowVp(e.target.checked)} />{" "}
            成交量分布
          </label>
          <label>
            <input type="checkbox" checked={marks} onChange={(e) => setMarks(e.target.checked)} />{" "}
            异动标记
          </label>
        </span>
      }
    >
      <div ref={ref} className="intraday">
        {hb && hv ? (
          <div className="chart-readout">
            <span className="when">{multi ? `${hb.date.slice(5)} ${hb.t}` : hb.t}</span>
            <span className={tone(hb.close)}>
              价 {hb.close.toFixed(2)} {pct(hb.close)}
            </span>
            <span style={{ color: "#e0a93a" }}>均价 {hv.vwap.toFixed(2)}</span>
            <span>σ {hv.sd.toFixed(2)}</span>
            <span>量 {(hb.vol / 100).toFixed(0)} 手</span>
            <span className={(delta[at] ?? 0) >= 0 ? "up" : "down"}>
              累计Δ(估) {(((delta[at] ?? 0) / Math.max(totalVol, 1)) * 100).toFixed(1)}% 成交量
            </span>
          </div>
        ) : null}
        <svg
          width={w}
          height={H}
          viewBox={`0 0 ${w} ${H}`}
          role="img"
          aria-label="分时图"
          onMouseLeave={() => setHover(null)}
          className="touch-scrub"
          ref={setSvgEl}
          onPointerDown={(e) => {
            if (e.pointerType === "pen") scrub(e.currentTarget, e.clientX, true);
          }}
          onPointerMove={(e) => {
            if (e.pointerType === "pen") scrub(e.currentTarget, e.clientX, true);
          }}
          onMouseMove={(e) => scrub(e.currentTarget, e.clientX, false)}
        >
          <defs>
            <clipPath id="id-above">
              <rect x={P.left} y={P.top} width={plotR - P.left} height={Y(pc) - P.top} />
            </clipPath>
            <clipPath id="id-below">
              <rect x={P.left} y={Y(pc)} width={plotR - P.left} height={P.top + PRICE_H - Y(pc)} />
            </clipPath>
          </defs>
          {ticks.map((t) => (
            <g key={t}>
              <line
                className="chart-grid"
                x1={P.left}
                x2={plotR}
                y1={Y(t)}
                y2={Y(t)}
                strokeDasharray={Math.abs(t - pc) < 1e-9 ? "4 3" : undefined}
              />
              <text
                className={`chart-axis ${tone(t)}`}
                x={P.left - 4}
                y={Y(t) + 3}
                textAnchor="end"
              >
                {t.toFixed(2)}
              </text>
              <text className={`chart-axis ${tone(t)}`} x={plotR + 4} y={Y(t) + 3}>
                {pct(t)}
              </text>
            </g>
          ))}
          {times.map(([l, m]) => (
            <g key={l}>
              <line
                className="chart-grid"
                x1={P.left + (m / n) * (plotR - P.left)}
                x2={P.left + (m / n) * (plotR - P.left)}
                y1={P.top}
                y2={volTop + VOL_H}
              />
              <text
                className="chart-axis"
                x={P.left + (m / n) * (plotR - P.left)}
                y={volTop + VOL_H + 12}
                textAnchor={m === 0 ? "start" : m === 240 ? "end" : "middle"}
              >
                {l}
              </text>
            </g>
          ))}
          {multi
            ? sessions.map((x, k) => (
                <g key={x.date}>
                  {k ? (
                    <line
                      className="chart-grid day-sep"
                      x1={XM(k * 240)}
                      x2={XM(k * 240)}
                      y1={P.top}
                      y2={dTop + DELTA_H}
                    />
                  ) : null}
                  <line
                    className="chart-grid"
                    strokeDasharray="2 4"
                    x1={XM(k * 240 + 120)}
                    x2={XM(k * 240 + 120)}
                    y1={P.top}
                    y2={volTop + VOL_H}
                  />
                  <text
                    className="chart-axis"
                    x={XM(k * 240 + 120)}
                    y={volTop + VOL_H + 12}
                    textAnchor="middle"
                  >
                    {x.date.slice(5)}
                  </text>
                </g>
              ))
            : null}
          {showVp ? (
            <g aria-label="分时成交量分布（估算）">
              {prof.dist.map((v, k) => {
                const y0 = lo + prof.w * k;
                const mid = y0 + prof.w / 2;
                const len = (v / profMax) * profW;
                return (
                  <rect
                    // biome-ignore lint/suspicious/noArrayIndexKey: bins are positional.
                    key={k}
                    x={plotR - len}
                    y={Y(y0 + prof.w)}
                    width={len}
                    height={Math.max(0.5, Y(y0) - Y(y0 + prof.w) - 0.5)}
                    className={mid >= prof.val && mid <= prof.vah ? "vp-bin va" : "vp-bin"}
                  />
                );
              })}
              <line
                className="vp-poc"
                x1={plotR - profW}
                x2={plotR}
                y1={Y(prof.poc)}
                y2={Y(prof.poc)}
              />
            </g>
          ) : null}
          {bands ? (
            <g fill="none" strokeDasharray="4 3">
              {[1, -1, 2, -2].map((k) => (
                <path
                  key={k}
                  d={line(
                    vw.map((v) => v.vwap + k * v.sd),
                    true,
                  )}
                  stroke={Math.abs(k) === 1 ? "#c792ea" : "#7f6a99"}
                  strokeWidth={1}
                />
              ))}
            </g>
          ) : null}
          <path
            d={line(bars.map((b) => b.close))}
            fill="none"
            stroke="var(--up)"
            strokeWidth={1.5}
            clipPath="url(#id-above)"
          />
          <path
            d={line(bars.map((b) => b.close))}
            fill="none"
            stroke="var(--down)"
            strokeWidth={1.5}
            clipPath="url(#id-below)"
          />
          <path
            d={line(
              vw.map((v) => v.vwap),
              true,
            )}
            fill="none"
            stroke="#e0a93a"
            strokeWidth={1.3}
          />
          {marks
            ? bars.map((b, i) => {
                const spike = medVol > 0 && b.vol > 3 * medVol;
                const big = rsd > 0 && Math.abs(rets[i] ?? 0) > 3 * rsd;
                if (!spike && !big) return null;
                const upBar = (rets[i] ?? 0) >= 0;
                return (
                  <g key={`${b.date}${b.t}`}>
                    {big ? (
                      <circle
                        cx={X(i)}
                        cy={Y(b.close)}
                        r={3.5}
                        className={upBar ? "mk up" : "mk down"}
                      >
                        <title>{`${b.date.slice(5)} ${b.t} 大幅波动 ${((rets[i] ?? 0) * 100).toFixed(2)}%`}</title>
                      </circle>
                    ) : null}
                    {spike ? (
                      <path d={`M${X(i)},${volTop - 2}l-4,-7h8z`} className="mk spike">
                        <title>{`${b.date.slice(5)} ${b.t} 放量 ${(b.vol / medVol).toFixed(1)}× 中位数`}</title>
                      </path>
                    ) : null}
                  </g>
                );
              })
            : null}
          {bars.map((b, i) => {
            const prev = i ? (bars[i - 1]?.close ?? pc) : pc;
            const h = (b.vol / maxVol) * VOL_H;
            const bw = Math.max(1, ((plotR - P.left) / n) * step * 0.7);
            return (
              <rect
                key={`v${b.date}${b.t}`}
                x={X(i) - bw / 2}
                y={volTop + VOL_H - h}
                width={bw}
                height={h}
                fill={b.close >= prev ? "var(--up)" : "var(--down)"}
                opacity={0.6}
              />
            );
          })}
          <text className="chart-axis" x={P.left} y={dTop - 4}>
            累计订单流 Δ（估算：{mode === "bvc" ? "BVC 批量成交分类" : "Tick 规则"}）
          </text>
          <line className="chart-grid" x1={P.left} x2={plotR} y1={DY(0)} y2={DY(0)} />
          <path d={dline} fill="none" stroke="var(--accent)" strokeWidth={1.3} />
          {hover != null ? (
            <g>
              <line
                className="chart-cross"
                x1={X(hover)}
                x2={X(hover)}
                y1={P.top}
                y2={dTop + DELTA_H}
              />
              {hb ? (
                <line
                  className="chart-cross"
                  x1={P.left}
                  x2={plotR}
                  y1={Y(hb.close)}
                  y2={Y(hb.close)}
                />
              ) : null}
            </g>
          ) : null}
        </svg>
        <div className="kline-tools">
          <span className="seg" title="Δ 估算方法">
            <button type="button" aria-pressed={mode === "bvc"} onClick={() => setMode("bvc")}>
              BVC
            </button>
            <button type="button" aria-pressed={mode === "tick"} onClick={() => setMode("tick")}>
              Tick 规则
            </button>
          </span>
          <span className="hint">
            POC {prof.poc.toFixed(2)} · 价值区 {prof.val.toFixed(2)}–{prof.vah.toFixed(2)} · 昨收{" "}
            {pc.toFixed(2)} · Δ/订单流、成交量分布均由 {d.freq} K 线估算，非逐笔数据
          </span>
        </div>
      </div>
    </Panel>
  );
}
