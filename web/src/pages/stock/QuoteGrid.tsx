import type { Schemas } from "@/api/client";

type Profile = Schemas["StockProfile"];
type Cell = { k: string; v: string; tone?: "up" | "down" | "" };

const f2 = (v: number | null | undefined, d = 2) => (v == null ? "—" : v.toFixed(d));
const sign = (v: number | null | undefined): "up" | "down" | "" =>
  v == null || v === 0 ? "" : v > 0 ? "up" : "down";
const pct = (v: number | null | undefined) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${(v * 100).toFixed(2)}%`;
/** 万元 → 亿 */
const yi = (wan: number | null | undefined) =>
  wan == null ? "—" : wan >= 1e4 ? `${(wan / 1e4).toFixed(2)}亿` : `${wan.toFixed(0)}万`;

/** Dense THS/Eastmoney-style quote block: price, limits, volume, valuation, 52-week range. */
export function QuoteGrid({ s }: { s: Profile }) {
  const bars = s.bars;
  const t = bars[bars.length - 1];
  if (!t) return null;
  const q = s.quote ?? {};
  const v = s.valuation;
  const pre = q.pre_close ?? bars[bars.length - 2]?.close ?? null;
  const tone = (p: number) => (pre == null ? "" : sign(p - pre));
  const year = bars.slice(-250);
  const hi52 = Math.max(...year.map((b) => b.high));
  const lo52 = Math.min(...year.map((b) => b.low));
  const prior = bars.slice(-6, -1).map((b) => b.vol ?? 0);
  const avgPrior = prior.length ? prior.reduce((a, b) => a + b, 0) / prior.length : 0;
  const volRatio = t.vol && avgPrior ? t.vol / avgPrior : null;
  const amp = pre ? ((t.high - t.low) / pre) * 100 : null;
  const outer = q.outer ?? null;
  const inner = q.inner ?? null;
  const cells: Cell[] = [
    { k: "今开", v: f2(t.open), tone: tone(t.open) },
    { k: "最高", v: f2(t.high), tone: tone(t.high) },
    { k: "最低", v: f2(t.low), tone: tone(t.low) },
    { k: "昨收", v: f2(pre) },
    { k: "涨停", v: f2(q.limit_up), tone: "up" },
    { k: "跌停", v: f2(q.limit_down), tone: "down" },
    { k: "成交量", v: t.vol == null ? "—" : `${(t.vol / 1e4).toFixed(2)}万手` },
    { k: "成交额", v: t.amount == null ? "—" : `${(t.amount / 1e5).toFixed(2)}亿` },
    { k: "振幅", v: amp == null ? "—" : `${amp.toFixed(2)}%` },
    { k: "量比", v: f2(volRatio), tone: volRatio == null ? "" : volRatio >= 1 ? "up" : "down" },
    { k: "换手率", v: v?.turnover_rate == null ? "—" : `${v.turnover_rate.toFixed(2)}%` },
    { k: "换手(自由)", v: v?.turnover_rate_f == null ? "—" : `${v.turnover_rate_f.toFixed(2)}%` },
    { k: "外盘", v: yi(outer), tone: "up" },
    { k: "内盘", v: yi(inner), tone: "down" },
    {
      k: "外/内",
      v: outer && inner ? f2(outer / inner) : "—",
      tone: outer && inner ? (outer >= inner ? "up" : "down") : "",
    },
    { k: "总市值", v: yi(v?.total_mv) },
    { k: "流通市值", v: yi(v?.circ_mv) },
    { k: "PE(TTM)", v: f2(v?.pe_ttm, 1) },
    { k: "PB", v: f2(v?.pb) },
    { k: "PS(TTM)", v: f2(v?.ps_ttm) },
    { k: "股息率", v: v?.dv_ttm == null ? "—" : `${v.dv_ttm.toFixed(2)}%` },
    { k: "ROE", v: s.fundamentals.roe == null ? "—" : `${f2(s.fundamentals.roe)}%` },
    { k: "52周高", v: f2(hi52) },
    { k: "52周低", v: f2(lo52) },
    ...Object.entries(s.returns).map(([k, r]) => ({ k, v: pct(r), tone: sign(r) })),
  ];
  return (
    <section className="quote-grid" aria-label="行情">
      {cells.map((c) => (
        <div key={c.k} className="qg-cell">
          <span className="qg-k">{c.k}</span>
          <span className={`qg-v ${c.tone ?? ""}`}>{c.v}</span>
        </div>
      ))}
    </section>
  );
}
