import { useEffect, useRef, useState } from "react";
import type { PanoramaData } from "@/api/endpoints";
import { squarify } from "@/charts/treemap";
import { formatAmount, formatFixed } from "@/format/number";

type Board = PanoramaData["boards"][number];

/** Red-up / green-down fill; full strength at ±3% median change. */
function fill(p: number | null | undefined): string {
  if (p == null) return "var(--surface-2)";
  const t = Math.min(1, Math.abs(p) / 3);
  const c = p >= 0 ? "var(--up)" : "var(--down)";
  return `color-mix(in srgb, ${c} ${Math.round(18 + 70 * t)}%, var(--surface))`;
}

/** Sector treemap: area = turnover, colour = median % change. */
export function Heatmap({
  boards,
  onSelect,
  selected,
  phone = false,
}: {
  boards: readonly Board[];
  /** When given, tiles become buttons that open the board's detail. */
  onSelect?: (board: Board) => void;
  selected?: string | null;
  /** iPhone: fewer, taller tiles so labels stay readable on a ~360px-wide canvas. */
  phone?: boolean;
}) {
  const ref = useRef<HTMLElement>(null);
  const [w, setW] = useState(phone ? 360 : 900);
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => {
      if (e) setW(Math.max(phone ? 240 : 300, Math.floor(e.contentRect.width)));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [phone]);
  const items = boards
    .filter((b) => (b.amount ?? 0) > 0)
    .sort((a, b) => (b.amount ?? 0) - (a.amount ?? 0))
    .slice(0, phone ? 40 : 80);
  const h = phone
    ? Math.round(Math.min(560, Math.max(380, w * 1.3)))
    : Math.round(Math.min(520, Math.max(260, w * 0.45)));
  const rects = squarify(
    items.map((b) => b.amount ?? 0),
    { x: 0, y: 0, w, h },
  );
  if (!items.length) return <p className="hint pad">没有成交额数据，无法画热力图。</p>;
  return (
    <section
      ref={ref}
      className={`heatmap${phone ? " hm-phone" : ""}`}
      style={{ height: h }}
      aria-label="板块热力图"
    >
      {items.map((b, i) => {
        const r = rects[i];
        if (!r) return null;
        const big = phone ? r.w > 46 && r.h > 30 : r.w > 70 && r.h > 34;
        return (
          <div
            key={`${b.system}:${b.board_code}`}
            className={`hm-cell${onSelect ? " hm-tap" : ""}`}
            data-selected={selected === b.board_code || undefined}
            {...(onSelect
              ? {
                  role: "button",
                  tabIndex: 0,
                  "aria-label": `${b.board_name} ${b.pct_chg_median == null ? "—" : formatFixed(b.pct_chg_median, 2)}%`,
                  onClick: () => onSelect(b),
                  onKeyDown: (e: React.KeyboardEvent) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      onSelect(b);
                    }
                  },
                }
              : {})}
            style={{
              left: r.x,
              top: r.y,
              width: r.w,
              height: r.h,
              background: fill(b.pct_chg_median),
            }}
            title={`${b.board_name}  中位涨跌 ${b.pct_chg_median == null ? "—" : formatFixed(b.pct_chg_median, 2)}%  成交 ${formatAmount(b.amount)}  领涨 ${b.leading_stock ?? "—"}`}
          >
            {big ? (
              <>
                <span className="hm-name">{b.board_name}</span>
                <span className="hm-pct num">
                  {b.pct_chg_median == null
                    ? "—"
                    : `${b.pct_chg_median > 0 ? "+" : ""}${formatFixed(b.pct_chg_median, 2)}%`}
                </span>
              </>
            ) : null}
          </div>
        );
      })}
    </section>
  );
}
