import { useState } from "react";
import type { Schemas } from "@/api/client";
import { formatFixed } from "@/format/number";
import { PHONE_QUERY, useMedia } from "../panorama/useMedia";

type Profile = Schemas["StockProfile"];
type Cell = { k: string; v: string; tone?: "up" | "down" | "" };

const f2 = (v: number | null | undefined, d = 2) => (v == null ? "—" : formatFixed(v, d));
const sign = (v: number | null | undefined): "up" | "down" | "" =>
  v == null || v === 0 ? "" : v > 0 ? "up" : "down";
const pct = (v: number | null | undefined) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${formatFixed(v * 100, 2)}%`;
/** 万元 → 亿 */
const yi = (wan: number | null | undefined) =>
  wan == null ? "—" : wan >= 1e4 ? `${formatFixed(wan / 1e4, 2)}亿` : `${formatFixed(wan, 0)}万`;

/** 手 → 万手; 外/内盘 come from the 5-minute bars (tick rule), absent without them */
const lots = (v: number | null | undefined) => (v == null ? "—" : `${formatFixed(v / 1e4, 2)}万手`);

/** Phone first screen, in this order. Everything else sits behind 全部 N 项. */
const PHONE_FIRST = ["今开", "最高", "最低", "成交额", "换手率", "总市值"] as const;

/** Dense THS/Eastmoney-style quote block: price, limits, volume, valuation, 52-week range. */
export function QuoteGrid({ s }: { s: Profile }) {
  const phone = useMedia(PHONE_QUERY);
  const [open, setOpen] = useState(false);
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
    { k: "成交量", v: t.vol == null ? "—" : `${formatFixed(t.vol / 1e4, 2)}万手` },
    { k: "成交额", v: t.amount == null ? "—" : `${formatFixed(t.amount / 1e5, 2)}亿` },
    { k: "振幅", v: amp == null ? "—" : `${formatFixed(amp, 2)}%` },
    { k: "量比", v: f2(volRatio), tone: volRatio == null ? "" : volRatio >= 1 ? "up" : "down" },
    { k: "换手率", v: v?.turnover_rate == null ? "—" : `${formatFixed(v.turnover_rate, 2)}%` },
    {
      k: "换手(自由)",
      v: v?.turnover_rate_f == null ? "—" : `${formatFixed(v.turnover_rate_f, 2)}%`,
    },
    { k: "外盘", v: lots(outer), tone: "up" },
    { k: "内盘", v: lots(inner), tone: "down" },
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
    { k: "股息率", v: v?.dv_ttm == null ? "—" : `${formatFixed(v.dv_ttm, 2)}%` },
    { k: "ROE", v: s.fundamentals.roe == null ? "—" : `${f2(s.fundamentals.roe)}%` },
    { k: "52周高", v: f2(hi52) },
    { k: "52周低", v: f2(lo52) },
    ...Object.entries(s.returns).map(([k, r]) => ({ k, v: pct(r), tone: sign(r) })),
  ];
  const primary = PHONE_FIRST.map((key) => cells.find((cell) => cell.k === key)).filter(
    (cell): cell is Cell => cell !== undefined,
  );
  const shown = phone && !open ? primary : cells;
  return (
    <section aria-label="行情">
      <div className={phone ? "quote-grid phone" : "quote-grid"}>
        {shown.map((c) => (
          <div key={c.k} className="qg-cell">
            <span className="qg-k">{c.k}</span>
            <span className={`qg-v ${c.tone ?? ""}`}>{c.v}</span>
          </div>
        ))}
      </div>
      {phone ? (
        <button className="btn quote-more" type="button" onClick={() => setOpen((value) => !value)}>
          {open ? "收起" : `全部 ${cells.length} 项`}
        </button>
      ) : null}
    </section>
  );
}
