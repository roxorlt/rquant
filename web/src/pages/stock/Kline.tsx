import { useMemo, useState } from "react";
import type { Schemas } from "@/api/client";
import { useChips } from "@/api/endpoints";
import { Candles, clampView } from "@/charts/Candles";
import { boll, kdj, macd, obv, rsi, sma } from "@/charts/indicators";
import { SubChart, type SubSeries } from "@/charts/SubChart";
import { Panel } from "@/ui";

type Bar = Schemas["StockProfile"]["bars"][number];

const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
const band = (b: readonly number[]) => `${(b[0] ?? 0).toFixed(2)}–${(b[1] ?? 0).toFixed(2)}`;

const fmt = (v: number | null | undefined) => (v == null ? "—" : v.toFixed(2));

/**
 * Anchored VWAP from ``from`` on: volume-weighted typical price (H+L+C)/3 of the
 * forward-adjusted daily bars, with volume-weighted standard-deviation bands.
 */
export function anchoredVwap(bars: readonly Bar[], from: number) {
  const mid: (number | null)[] = [];
  const u1: (number | null)[] = [];
  const l1: (number | null)[] = [];
  const u2: (number | null)[] = [];
  const l2: (number | null)[] = [];
  let sv = 0;
  let spv = 0;
  let sp2v = 0;
  bars.forEach((b, i) => {
    if (i < from) {
      for (const s of [mid, u1, l1, u2, l2]) s.push(null);
      return;
    }
    const v = b.vol ?? (b.amount ?? 0) / b.close;
    const p = (b.high + b.low + b.close) / 3;
    sv += v;
    spv += p * v;
    sp2v += p * p * v;
    const m = sv > 0 ? spv / sv : p;
    const sd = sv > 0 ? Math.sqrt(Math.max(sp2v / sv - m * m, 0)) : 0;
    mid.push(m);
    u1.push(m + sd);
    l1.push(m - sd);
    u2.push(m + 2 * sd);
    l2.push(m - 2 * sd);
  });
  return { mid, u1, l1, u2, l2 };
}

/** K-line with a chip distribution column that follows the mouse, plus a volume-profile overlay. */
type Flow = Schemas["StockProfile"]["flow"][number];

const PRESETS: [string, number][] = [
  ["1月", 22],
  ["3月", 66],
  ["6月", 130],
  ["1年", 250],
  ["全部", 100000],
];

export const SUBS = {
  vol: "成交量",
  macd: "MACD",
  kdj: "KDJ",
  rsi: "RSI",
  obv: "OBV",
  flow: "主力资金",
  chip: "筹码获利",
} as const;
type Sub = keyof typeof SUBS;

function subSeries(
  kind: Sub,
  bars: readonly Bar[],
  flow: readonly Flow[],
  chipProfit: Map<string, number>,
): { series: SubSeries[]; levels?: number[]; format?: (v: number) => string } {
  const close = bars.map((b) => b.close);
  const vol = bars.map((b) => b.vol ?? 0);
  switch (kind) {
    case "vol": {
      return {
        series: [
          { name: "VOL(万手)", values: vol.map((v) => v / 1e4), bars: true, color: "var(--muted)" },
          {
            name: "MA5",
            values: sma(vol, 5).map((v) => (v == null ? null : v / 1e4)),
            color: "var(--warn)",
          },
          {
            name: "MA10",
            values: sma(vol, 10).map((v) => (v == null ? null : v / 1e4)),
            color: "var(--accent)",
          },
        ],
      };
    }
    case "macd": {
      const m = macd(close);
      return {
        series: [
          { name: "MACD", values: m.hist, bars: true },
          { name: "DIF", values: m.dif, color: "var(--warn)" },
          { name: "DEA", values: m.dea, color: "var(--accent)" },
        ],
      };
    }
    case "kdj": {
      const k = kdj(
        bars.map((b) => b.high),
        bars.map((b) => b.low),
        close,
      );
      return {
        series: [
          { name: "K", values: k.k, color: "var(--warn)" },
          { name: "D", values: k.d, color: "var(--accent)" },
          { name: "J", values: k.j, color: "#c792ea" },
        ],
        levels: [20, 80],
      };
    }
    case "rsi":
      return {
        series: [
          { name: "RSI6", values: rsi(close, 6), color: "var(--warn)" },
          { name: "RSI12", values: rsi(close, 12), color: "var(--accent)" },
          { name: "RSI24", values: rsi(close, 24), color: "#c792ea" },
        ],
        levels: [30, 70],
      };
    case "obv":
      return {
        series: [
          {
            name: "OBV(万手)",
            values: obv(close, vol).map((v) => v / 1e4),
            color: "var(--accent)",
          },
        ],
      };
    case "flow": {
      const m = new Map(flow.map((f) => [f.date, f.main_net / 1e4]));
      return {
        series: [
          { name: "主力净流入(亿)", values: bars.map((b) => m.get(b.date) ?? null), bars: true },
        ],
      };
    }
    case "chip":
      return {
        series: [
          {
            name: "获利比例%",
            values: bars.map((b) => {
              const v = chipProfit.get(b.date);
              return v == null ? null : v * 100;
            }),
            color: "#e0a93a",
          },
        ],
        levels: [50],
        format: (v) => v.toFixed(1),
      };
  }
}

export function KlinePanel({
  code,
  bars,
  flow = [],
}: {
  code: string;
  bars: readonly Bar[];
  flow?: readonly Flow[];
}) {
  const [view, setView] = useState<[number, number]>(() =>
    clampView(bars.length - 250, bars.length, bars.length),
  );
  const [subs, setSubs] = useState<Sub[]>(["macd", "flow"]);
  const [showBoll, setShowBoll] = useState(false);
  const zoom = (k: number) => {
    const [a, b] = view;
    const len = (b - a) * k;
    setView(clampView(b - len, b, bars.length));
  };
  const chips = useChips(code);
  const [hover, setHover] = useState<number | null>(null);
  const [showChips, setShowChips] = useState(true);
  const [showVp, setShowVp] = useState(false);
  const c = chips.data;
  const byDate = useMemo(() => new Map((c?.days ?? []).map((d) => [d.date, d])), [c]);
  const day = bars[hover ?? bars.length - 1];
  const chip = day ? byDate.get(day.date) : undefined;
  const chipProfit = useMemo(
    () => new Map((c?.days ?? []).map((d) => [d.date, d.profit_ratio])),
    [c],
  );
  const bb = useMemo(() => boll(bars.map((b) => b.close)), [bars]);
  const [showVwap, setShowVwap] = useState(true);
  const [anchor, setAnchor] = useState<number | null>(null);
  const a = anchor ?? Math.max(0, bars.length - 60);
  const vwap = useMemo(() => anchoredVwap(bars, a), [bars, a]);
  const overlays = [
    ...(showBoll
      ? [
          { key: "bm", values: bb.mid, color: "#8fb3ff" },
          { key: "bu", values: bb.up, color: "#8fb3ff", dash: true },
          { key: "bd", values: bb.dn, color: "#8fb3ff", dash: true },
        ]
      : []),
    ...(showVwap
      ? [
          { key: "vwap", values: vwap.mid, color: "#c792ea" },
          { key: "u1", values: vwap.u1, color: "#c792ea", dash: true },
          { key: "l1", values: vwap.l1, color: "#c792ea", dash: true },
          { key: "u2", values: vwap.u2, color: "#7f6a99", dash: true },
          { key: "l2", values: vwap.l2, color: "#7f6a99", dash: true },
        ]
      : []),
  ];
  return (
    <Panel
      title="日K线 · 筹码"
      sub="前复权；筹码 = 换手率衰减模型（近似）"
      actions={
        <span className="qc-ctl">
          <label>
            <input
              type="checkbox"
              checked={showChips}
              onChange={(e) => setShowChips(e.target.checked)}
            />{" "}
            筹码分布
          </label>
          <label>
            <input
              type="checkbox"
              checked={showBoll}
              onChange={(e) => setShowBoll(e.target.checked)}
            />{" "}
            BOLL
          </label>
          <label title="点击 K 线设置锚点">
            <input
              type="checkbox"
              checked={showVwap}
              onChange={(e) => setShowVwap(e.target.checked)}
            />{" "}
            锚定 VWAP ±1σ/2σ
          </label>
          <label>
            <input type="checkbox" checked={showVp} onChange={(e) => setShowVp(e.target.checked)} />{" "}
            成交量分布
            {c?.profile
              ? `（近 ${c.profile.days} 日 · ${c.profile.source === "5min" ? "5 分钟线" : "日线"}）`
              : ""}
          </label>
        </span>
      }
    >
      <Candles
        bars={bars}
        view={view}
        onView={setView}
        onHover={setHover}
        overlays={overlays}
        anchor={showVwap ? a : null}
        onPick={showVwap ? setAnchor : undefined}
        side={
          showChips && chip && c
            ? { dist: chip.dist, close: chip.close, avg: chip.avg_cost, lo: c.lo, hi: c.hi }
            : null
        }
        profile={showVp && c?.profile ? { ...c.profile, lo: c.lo, hi: c.hi } : null}
      />
      {subs.map((k, i) => {
        const sub = subSeries(k, bars, flow, chipProfit);
        return (
          <div key={`${k}-${String(i)}`} className="sub-wrap">
            <select
              aria-label={`副图${i + 1}`}
              className="sub-pick"
              value={k}
              onChange={(e) => setSubs(subs.map((x, j) => (j === i ? (e.target.value as Sub) : x)))}
            >
              {Object.entries(SUBS).map(([v, l]) => (
                <option key={v} value={v}>
                  {l}
                </option>
              ))}
            </select>
            <button
              type="button"
              className="sub-x"
              aria-label="移除副图"
              onClick={() => setSubs(subs.filter((_, j) => j !== i))}
            >
              ×
            </button>
            <SubChart label={SUBS[k]} view={view} hover={hover} {...sub} />
          </div>
        );
      })}
      <div className="kline-tools">
        <span className="seg" title="区间">
          {PRESETS.map(([l, n]) => {
            const on = view[1] === bars.length && view[1] - view[0] === Math.min(n, bars.length);
            return (
              <button
                key={l}
                type="button"
                aria-pressed={on}
                onClick={() => setView(clampView(bars.length - n, bars.length, bars.length))}
              >
                {l}
              </button>
            );
          })}
        </span>
        <span className="seg">
          <button type="button" aria-label="放大" onClick={() => zoom(0.7)}>
            ＋
          </button>
          <button type="button" aria-label="缩小" onClick={() => zoom(1.4)}>
            －
          </button>
        </span>
        <span className="hint">
          {bars[view[0]]?.date} ~ {bars[view[1] - 1]?.date} · {view[1] - view[0]} 根 ·
          滚轮/双指缩放，拖动平移
        </span>
        {subs.length < 4 ? (
          <button
            type="button"
            className="tbl-tool"
            onClick={() =>
              setSubs([
                ...subs,
                (Object.keys(SUBS) as Sub[]).find((k) => !subs.includes(k)) ?? "rsi",
              ])
            }
          >
            + 副图
          </button>
        ) : null}
      </div>
      {chip ? (
        <div className="chip-stats">
          <span>
            {chip.date} 平均成本 <b>{chip.avg_cost.toFixed(2)}</b>
          </span>
          <span>
            获利比例{" "}
            <b className={chip.profit_ratio >= 0.5 ? "up" : "down"}>{pct(chip.profit_ratio)}</b>
          </span>
          <span>
            90% 成本 <b>{band(chip.cost90)}</b> 集中度 <b>{pct(chip.conc90)}</b>
          </span>
          <span>
            70% 成本 <b>{band(chip.cost70)}</b> 集中度 <b>{pct(chip.conc70)}</b>
          </span>
          {showVwap ? (
            <span title="点击 K 线可移动锚点">
              VWAP 自 {bars[a]?.date} <b>{fmt(vwap.mid[vwap.mid.length - 1])}</b> ±1σ{" "}
              <b>
                {fmt(vwap.l1[vwap.l1.length - 1])}–{fmt(vwap.u1[vwap.u1.length - 1])}
              </b>
            </span>
          ) : null}
          {showVp && c?.profile ? (
            <span>
              POC <b>{c.profile.poc.toFixed(2)}</b> 价值区{" "}
              <b>
                {c.profile.val.toFixed(2)}–{c.profile.vah.toFixed(2)}
              </b>
            </span>
          ) : null}
        </div>
      ) : chips.isLoading ? (
        <p className="hint">筹码计算中…</p>
      ) : (
        <p className="hint">缺少换手率数据，无法估算筹码。</p>
      )}
    </Panel>
  );
}
