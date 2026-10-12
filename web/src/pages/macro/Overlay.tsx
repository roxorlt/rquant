import { type MouseEvent, useMemo, useState } from "react";
import type { MacroData } from "@/api/endpoints";
import { Panel, Segmented } from "@/ui";

/**
 * Main overlay: index (right axis) + chosen indicators as expanding, point-in-time
 * percentiles (left axis, 0–100%). Each indicator's percentile at date t uses only its own
 * history up to t. Lower-frequency series are step lines carried forward from the date they
 * became known (Buffett: month-end with GDP lagged by ~20d; holdings: end of the
 * announcement quarter).
 */

type Cols = NonNullable<MacroData["daily"]>;
type Range = "1y" | "3y" | "5y" | "all";

interface Ind {
  key: string;
  label: string;
  group: string;
  dates: string[];
  values: (number | null)[];
  fmt: (v: number) => string;
}

const GROUPS = ["估值", "资金", "情绪", "宽度", "期权期货", "港股"] as const;
const COLORS = [
  "#2f6fdb",
  "#d9480f",
  "#2b8a3e",
  "#ae3ec9",
  "#e8590c",
  "#1098ad",
  "#c2255c",
  "#5c940d",
];
const EVENTS: readonly [string, string][] = [
  ["20240920", "20241010"],
  ["20260501", "20260531"],
];
const PRESETS: { label: string; keys: string[] }[] = [
  { label: "估值", keys: ["buffett", "erp"] },
  { label: "杠杆+情绪", keys: ["fin_buy_share", "pcr_vol_300", "iv_300"] },
  { label: "宽度", keys: ["breadth_ma60"] },
  { label: "国家队代理", keys: ["etf_share", "huijin_flow", "net_IF"] },
];
const pctf = (v: number) => `${(v * 100).toFixed(1)}%`;
const num = (v: number) => (Math.abs(v) >= 1000 ? v.toFixed(0) : v.toFixed(2));

function fromCols(c: Cols | null | undefined, k: string): [string[], (number | null)[]] {
  return c ? [c.dates, c.cols[k] ?? []] : [[], []];
}

function buildIndicators(d: MacroData): Ind[] {
  const out: Ind[] = [];
  const add = (
    key: string,
    label: string,
    group: string,
    src: [string[], (number | null)[]],
    fmt = num,
  ) => {
    if (src[0].length > 1) out.push({ key, label, group, dates: src[0], values: src[1], fmt });
  };
  add("buffett", "巴菲特指标", "估值", fromCols(d.buffett, "ratio"), pctf);
  add("erp", "股债利差", "估值", fromCols(d.daily, "erp"), (v) => `${v.toFixed(2)}%`);
  add("pe_ttm", "沪深300 PE", "估值", fromCols(d.daily, "pe_ttm"));
  add(
    "rzrqye",
    "两融余额",
    "资金",
    fromCols(d.daily, "rzrqye"),
    (v) => `${(v / 1e8).toFixed(0)}亿`,
  );
  add("fin_buy_share", "融资买入占比", "资金", fromCols(d.daily, "fin_buy_share"), pctf);
  add(
    "north_cum",
    "北向累计净买入(至2024-08)",
    "资金",
    fromCols(d.daily, "north_net_cum"),
    (v) => `${(v / 100).toFixed(0)}亿`,
  );
  if (d.etf_share) {
    const e = d.etf_share;
    const tot = e.dates.map(
      (_, i) => Object.values(e.cols).reduce((s, v) => s + (v[i] ?? 0), 0) / 1e4,
    );
    add("etf_share", "宽基ETF合计份额", "资金", [e.dates, tot], (v) => `${v.toFixed(0)}亿份`);
  }
  const hj = d.holders.filter((h) => h.entity === "中央汇金" && h.basis === "announce");
  const qEnd = (q: string) =>
    `${q.slice(0, 4)}${["0331", "0630", "0930", "1231"][Number(q.slice(5)) - 1]}`;
  add(
    "huijin_flow",
    "汇金净增持(按公告季)",
    "资金",
    [hj.map((h) => qEnd(h.quarter)), hj.map((h) => h.flow ?? null)],
    (v) => `${(v / 1e8).toFixed(0)}亿`,
  );
  const o3 = d.options.沪300ETF;
  add("pcr_vol_300", "300ETF PCR(量)", "情绪", fromCols(o3, "pcr_vol"));
  add("pcr_oi_300", "300ETF PCR(持仓)", "情绪", fromCols(o3, "pcr_oi"));
  add("iv_300", "300ETF 平值IV", "情绪", fromCols(o3, "atm_iv"), pctf);
  for (const p of d.breadth_picks) {
    const key = p.group === "均线参与度" ? "breadth_ma60" : `breadth_${p.group}`;
    add(key, p.variant, "宽度", fromCols(d.breadth_sel, p.variant));
  }
  for (const s of ["IF", "IM"]) {
    add(`basis_${s}`, `${s}基差`, "期权期货", fromCols(d.futures[s], "basis_pct"), pctf);
    add(`net_${s}`, `${s}前20净多`, "期权期货", fromCols(d.futures[s], "net"), (v) => v.toFixed(0));
  }
  if (d.hk) {
    const sm = d.hk.cols.smart ?? [];
    const ma = d.hk.cols.smart_ma20 ?? [];
    add("hk_gap", "SMARTMONEY−MA20", "港股", [
      d.hk.dates,
      sm.map((v, i) => (v == null || ma[i] == null ? null : v - (ma[i] as number))),
    ]);
  }
  return out;
}

/** Expanding percentile: share of observations so far (inclusive) ≤ current value. */
function expandingPct(values: (number | null)[]): (number | null)[] {
  const sorted: number[] = [];
  return values.map((v) => {
    if (v == null) return null;
    let lo = 0;
    let hi = sorted.length;
    while (lo < hi) {
      const m = (lo + hi) >> 1;
      if ((sorted[m] as number) <= v) lo = m + 1;
      else hi = m;
    }
    sorted.splice(lo, 0, v);
    return sorted.length >= 20 ? lo / (sorted.length - 1 || 1) : null;
  });
}

/** Carry an irregular series onto the daily axis (step: last known value). */
function align(
  axis: readonly string[],
  dates: string[],
  vals: (number | null)[],
): (number | null)[] {
  const out: (number | null)[] = [];
  let j = -1;
  let last: number | null = null;
  for (const d of axis) {
    while (j + 1 < dates.length && (dates[j + 1] as string) <= d) {
      j++;
      if (vals[j] != null) last = vals[j] as number;
    }
    out.push(j >= 0 ? last : null);
  }
  return out;
}

export function MacroOverlay({ data }: { data: MacroData }) {
  const inds = useMemo(() => buildIndicators(data), [data]);
  const pcts = useMemo(() => new Map(inds.map((i) => [i.key, expandingPct(i.values)])), [inds]);
  const [sel, setSel] = useState<string[]>(["buffett", "erp"]);
  const [idx, setIdx] = useState<"all_a" | "csi300">("all_a");
  const [range, setRange] = useState<Range>("5y");
  const [hover, setHover] = useState<number | null>(null);
  const daily = data.daily;
  if (!daily) return null;

  const years = { "1y": 1, "3y": 3, "5y": 5, all: 99 }[range];
  const lastD = daily.dates[daily.dates.length - 1] ?? "";
  const from = `${Number(lastD.slice(0, 4)) - years}${lastD.slice(4)}`;
  const i0 = Math.max(
    0,
    daily.dates.findIndex((d) => d >= from),
  );
  const axis = daily.dates.slice(i0);
  const ixv = (daily.cols[idx] ?? []).slice(i0);
  const lines = inds
    .filter((i) => sel.includes(i.key))
    .map((i, n) => ({
      ind: i,
      color: COLORS[n % COLORS.length] as string,
      pct: align(axis, i.dates, pcts.get(i.key) ?? []),
      raw: align(axis, i.dates, i.values),
    }));

  const W = 1000;
  const H = 300;
  const L = 44;
  const R = 56;
  const T = 10;
  const B = 22;
  const n = axis.length;
  const x = (k: number) => L + (k / Math.max(1, n - 1)) * (W - L - R);
  const yP = (p: number) => T + (1 - p) * (H - T - B);
  const iv = ixv.filter((v): v is number => v != null);
  const lo = Math.min(...iv);
  const hi = Math.max(...iv);
  const yI = (v: number) => T + (1 - (v - lo) / (hi - lo || 1)) * (H - T - B);
  const path = (vals: (number | null)[], y: (v: number) => number) => {
    let s = "";
    let pen = false;
    vals.forEach((v, k) => {
      if (v == null) {
        pen = false;
        return;
      }
      s += `${pen ? "L" : "M"}${x(k).toFixed(1)},${y(v).toFixed(1)}`;
      pen = true;
    });
    return s;
  };
  const onMove = (e: MouseEvent<SVGSVGElement>) => {
    const r = e.currentTarget.getBoundingClientRect();
    const px = ((e.clientX - r.left) / r.width) * W;
    setHover(Math.max(0, Math.min(n - 1, Math.round(((px - L) / (W - L - R)) * (n - 1)))));
  };
  const toggle = (k: string) =>
    setSel((s) => (s.includes(k) ? s.filter((v) => v !== k) : [...s, k]));
  const xt = [0, Math.floor(n / 4), Math.floor(n / 2), Math.floor((3 * n) / 4), n - 1];

  return (
    <Panel
      title="主图：指数 × 指标历史分位"
      sub="左轴=各指标截至当日的历史分位（只用当日及以前数据，无前视）；右轴=指数。月/季度指标按可得日期阶梯延续；历史短的指标从有数据起画"
      actions={
        <>
          <Segmented
            label="指数"
            value={idx}
            onChange={setIdx}
            options={[
              { value: "all_a", label: "中证全指" },
              { value: "csi300", label: "沪深300" },
            ]}
          />
          <Segmented
            label="区间"
            value={range}
            onChange={setRange}
            options={[
              { value: "1y", label: "1年" },
              { value: "3y", label: "3年" },
              { value: "5y", label: "5年" },
              { value: "all", label: "全部" },
            ]}
          />
        </>
      }
    >
      <div
        className="macro-overlay-controls"
        style={{ display: "flex", flexWrap: "wrap", gap: "4px 14px", fontSize: 12 }}
      >
        <span>
          预设：
          {PRESETS.map((p) => (
            <button
              key={p.label}
              type="button"
              className="chip"
              onClick={() => setSel(p.keys.filter((k) => inds.some((i) => i.key === k)))}
              style={{ marginRight: 6 }}
            >
              {p.label}
            </button>
          ))}
        </span>
        {GROUPS.map((g) => (
          <fieldset key={g} style={{ border: 0, margin: 0, padding: 0 }}>
            <strong style={{ marginRight: 4 }}>{g}</strong>
            {inds
              .filter((i) => i.group === g)
              .map((i) => (
                <label key={i.key} style={{ marginRight: 8, whiteSpace: "nowrap" }}>
                  <input
                    type="checkbox"
                    checked={sel.includes(i.key)}
                    onChange={() => toggle(i.key)}
                  />{" "}
                  {i.label}
                </label>
              ))}
          </fieldset>
        ))}
      </div>
      <div style={{ position: "relative" }}>
        <svg
          viewBox={`0 0 ${W} ${H}`}
          width="100%"
          role="img"
          aria-label="宏观主图"
          onMouseMove={onMove}
          onMouseLeave={() => setHover(null)}
        >
          {EVENTS.map(([a, b]) => {
            const s = axis.findIndex((d) => d >= a);
            let e = axis.findIndex((d) => d > b);
            if (s < 0) return null;
            if (e < 0) e = n - 1;
            return (
              <rect
                key={a}
                x={x(s)}
                y={T}
                width={Math.max(2, x(e) - x(s))}
                height={H - T - B}
                fill="orange"
                opacity={0.18}
              />
            );
          })}
          {[0, 0.2, 0.5, 0.8, 1].map((p) => (
            <g key={p}>
              <line
                x1={L}
                x2={W - R}
                y1={yP(p)}
                y2={yP(p)}
                stroke="currentColor"
                opacity={p === 0.2 || p === 0.8 ? 0.25 : 0.1}
                strokeDasharray={p === 0.2 || p === 0.8 ? "4 3" : undefined}
              />
              <text
                x={L - 4}
                y={yP(p) + 4}
                textAnchor="end"
                fontSize={11}
                fill="currentColor"
                opacity={0.7}
              >
                {p * 100}%
              </text>
            </g>
          ))}
          {[lo, (lo + hi) / 2, hi].map((v) => (
            <text
              key={v}
              x={W - R + 4}
              y={yI(v) + 4}
              fontSize={11}
              fill="currentColor"
              opacity={0.7}
            >
              {v.toFixed(0)}
            </text>
          ))}
          {xt.map((k) => (
            <text
              key={k}
              x={x(k)}
              y={H - 6}
              textAnchor="middle"
              fontSize={11}
              fill="currentColor"
              opacity={0.7}
            >
              {axis[k]?.slice(0, 6)}
            </text>
          ))}
          <path
            d={path(ixv, yI)}
            fill="none"
            stroke="currentColor"
            strokeWidth={1.4}
            opacity={0.55}
          />
          {lines.map((l) => (
            <path
              key={l.ind.key}
              d={path(l.pct, yP)}
              fill="none"
              stroke={l.color}
              strokeWidth={1.4}
            />
          ))}
          {hover != null ? (
            <line
              x1={x(hover)}
              x2={x(hover)}
              y1={T}
              y2={H - B}
              stroke="currentColor"
              opacity={0.4}
            />
          ) : null}
        </svg>
        {hover != null ? (
          <div
            className="chart-tip"
            style={{
              left: `${(x(hover) / W) * 100}%`,
              top: 8,
              transform: x(hover) > W * 0.6 ? "translateX(calc(-100% - 10px))" : "translateX(10px)",
            }}
          >
            <div>
              <strong>{axis[hover]}</strong> {idx === "all_a" ? "中证全指" : "沪深300"}{" "}
              {ixv[hover]?.toFixed(0) ?? "—"}
            </div>
            {lines.map((l) => {
              const r = l.raw[hover];
              const p = l.pct[hover];
              return (
                <div key={l.ind.key} style={{ color: l.color }}>
                  {l.ind.label}：{r == null ? "—" : l.ind.fmt(r)} · 分位{" "}
                  {p == null ? "—" : `${(p * 100).toFixed(0)}%`}
                </div>
              );
            })}
          </div>
        ) : null}
      </div>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 12, fontSize: 12 }}>
        {lines.map((l) => (
          <span key={l.ind.key} style={{ color: l.color }}>
            ━ {l.ind.label}
          </span>
        ))}
        <span style={{ opacity: 0.6 }}>━ {idx === "all_a" ? "中证全指" : "沪深300"}（右轴）</span>
      </div>
    </Panel>
  );
}
