import { useMemo, useState } from "react";
import type { Schemas } from "@/api/client";
import { useChips } from "@/api/endpoints";
import { Candles } from "@/charts/Candles";
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
export function KlinePanel({ code, bars }: { code: string; bars: readonly Bar[] }) {
  const chips = useChips(code);
  const [hover, setHover] = useState<number | null>(null);
  const [showChips, setShowChips] = useState(true);
  const [showVp, setShowVp] = useState(false);
  const c = chips.data;
  const byDate = useMemo(() => new Map((c?.days ?? []).map((d) => [d.date, d])), [c]);
  const day = bars[hover ?? bars.length - 1];
  const chip = day ? byDate.get(day.date) : undefined;
  const range: [number, number] | undefined = c ? [c.lo, c.hi] : undefined;
  const [showVwap, setShowVwap] = useState(true);
  const [anchor, setAnchor] = useState<number | null>(null);
  const a = anchor ?? Math.max(0, bars.length - 60);
  const vwap = useMemo(() => anchoredVwap(bars, a), [bars, a]);
  const overlays = showVwap
    ? [
        { key: "vwap", values: vwap.mid, color: "#c792ea" },
        { key: "u1", values: vwap.u1, color: "#c792ea", dash: true },
        { key: "l1", values: vwap.l1, color: "#c792ea", dash: true },
        { key: "u2", values: vwap.u2, color: "#7f6a99", dash: true },
        { key: "l2", values: vwap.l2, color: "#7f6a99", dash: true },
      ]
    : [];
  return (
    <Panel
      title="日K线 · 筹码"
      sub={`近 ${bars.length} 个交易日，前复权；筹码 = 换手率衰减模型（近似）`}
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
        range={range}
        onHover={setHover}
        overlays={overlays}
        anchor={showVwap ? a : null}
        onPick={showVwap ? setAnchor : undefined}
        side={showChips && chip ? { dist: chip.dist, close: chip.close, avg: chip.avg_cost } : null}
        profile={showVp ? (c?.profile ?? null) : null}
      />
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
