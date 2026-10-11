import { useMemo, useState } from "react";
import type { Schemas } from "@/api/client";
import { useChips } from "@/api/endpoints";
import { Candles } from "@/charts/Candles";
import { Panel } from "@/ui";

type Bar = Schemas["StockProfile"]["bars"][number];

const pct = (v: number) => `${(v * 100).toFixed(1)}%`;
const band = (b: readonly number[]) => `${(b[0] ?? 0).toFixed(2)}–${(b[1] ?? 0).toFixed(2)}`;

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
