import type { Schemas } from "@/api/client";
import { TimeChart } from "@/charts/Chart";
import { Panel } from "@/ui";

type Profile = Schemas["StockProfile"];

const W = 20;

/**
 * Cumulative main-force net inflow (大单+特大单 active buy − sell, Tushare moneyflow) against
 * the close; shaded where the 20-day price change and the 20-day net flow disagree in sign.
 */
export function FlowPanel({ s }: { s: Profile }) {
  const flow = s.flow ?? [];
  if (flow.length < 2) return null;
  const close = new Map(s.bars.map((b) => [b.date, b.close]));
  const pts = flow.filter((f) => close.has(f.date));
  let acc = 0;
  const cum = pts.map((f) => {
    acc += f.main_net / 1e4; // 万元 → 亿元
    return acc;
  });
  const px = pts.map((f) => close.get(f.date) ?? null);
  const kinds: ("top" | "bottom" | null)[] = pts.map((_, i) => {
    if (i < W) return null;
    const dp = (px[i] ?? 0) - (px[i - W] ?? 0);
    const df = (cum[i] ?? 0) - (cum[i - W] ?? 0);
    return dp > 0 && df < 0 ? "top" : dp < 0 && df > 0 ? "bottom" : null;
  });
  const bands: { from: number; to: number }[] = [];
  kinds.forEach((k, i) => {
    if (!k) return;
    const last = bands[bands.length - 1];
    if (last && last.to === i - 1) last.to = i;
    else bands.push({ from: i, to: i });
  });
  const now = kinds[kinds.length - 1];
  const dates = pts.map((f) => f.date);
  return (
    <Panel
      title="主力资金"
      sub={`大单+特大单主动净买入累计（亿元，Tushare 资金流向，L2 口径）；阴影 = 近 ${W} 日价格与资金方向相反`}
    >
      <p className="chip-stats">
        <span>
          区间累计 <b className={acc >= 0 ? "up" : "down"}>{acc.toFixed(2)} 亿</b>
        </span>
        <span>
          近 {W} 日{" "}
          <b
            className={
              (cum[cum.length - 1] ?? 0) - (cum[cum.length - 1 - W] ?? 0) >= 0 ? "up" : "down"
            }
          >
            {((cum[cum.length - 1] ?? 0) - (cum[cum.length - 1 - W] ?? 0)).toFixed(2)} 亿
          </b>
        </span>
        <span>
          当前：
          <b className={now === "top" ? "down" : now === "bottom" ? "up" : ""}>
            {now === "top"
              ? "顶背离（价涨、主力净流出）"
              : now === "bottom"
                ? "底背离（价跌、主力净流入）"
                : "无背离"}
          </b>
        </span>
      </p>
      <TimeChart
        label="收盘价"
        dates={dates}
        height={130}
        bands={bands}
        format={(v) => v.toFixed(2)}
        series={[{ name: "收盘(前复权)", values: px, color: "var(--muted)" }]}
      />
      <TimeChart
        label="主力净流入累计"
        dates={dates}
        height={130}
        baseline={0}
        bands={bands}
        format={(v) => `${v.toFixed(2)}亿`}
        series={[{ name: "累计主力净流入", values: cum, color: "var(--accent)" }]}
      />
    </Panel>
  );
}
