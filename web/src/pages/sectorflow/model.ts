import type { SectorFlowData } from "@/api/endpoints";
import { formatSignedFixed, formatSignedPercent } from "@/format/number";

export type Sector = SectorFlowData["sectors"][number];
export type Mode = "net" | "ratio" | "pct";

export const IN_COLORS = [
  "#ff4d4f",
  "#ff8a3d",
  "#f5b82e",
  "#ff6fa8",
  "#c9405a",
  "#e86a3a",
  "#d4577b",
  "#b8860b",
];
export const OUT_COLORS = [
  "#1fb46a",
  "#2fb8b2",
  "#7cc95a",
  "#13a8a8",
  "#5b9e72",
  "#3d9be0",
  "#6aa84f",
  "#2e8b57",
];

/** Full-session x slots: 0 = 09:25 auction, 1..121 = 09:30-11:30, 122..242 = 13:00-15:00. */
export const SLOTS = 243;

export function seriesOf(s: Sector, mode: Mode): (number | null)[] {
  if (mode === "net") return s.series;
  if (mode === "ratio") return s.ratio;
  return s.pct_series;
}

export const fmtYi = (v: number | null | undefined, digits = 1) =>
  v == null ? "—" : `${formatSignedFixed(v, digits)}亿`;
export const fmtPct = (v: number | null | undefined, digits = 2) =>
  v == null ? "—" : formatSignedPercent(v, digits);
/** A share such as 占流通: positive stays unsigned, negative uses the monitor minus. */
export function fmtRatioPct(v: number | null | undefined, digits = 2): string {
  if (v == null) return "—";
  const signed = formatSignedFixed(v, digits);
  return `${signed.startsWith("+") ? signed.slice(1) : signed}%`;
}
export const tone = (v: number | null | undefined) =>
  v == null || v === 0 ? "" : v > 0 ? "up" : "down";

/** Categorical hues for one chart. Assigned in draw order; none repeats inside that chart. */
export const LINE_COLORS = [
  "#2448D0",
  "#C47B12",
  "#6B4C9A",
  "#0E7490",
  "#9A3412",
  "#3F6212",
  "#BE185D",
  "#7C3AED",
  "#0F6E56",
  "#B45309",
  "#4338CA",
  "#A16207",
];

export function fmtValue(v: number | null | undefined, mode: Mode, _short = false): string {
  if (v == null) return "—";
  if (mode === "net") return fmtYi(v);
  return fmtPct(v, 2);
}

export function fmtAmountYi(cny: number): string {
  const yi = cny / 1e8;
  return yi >= 10000 ? `${(yi / 10000).toFixed(2)}万亿` : `${yi.toFixed(0)}亿`;
}

/** 两市成交额 card sub-line: "较昨 +X亿（+Y%）" (change vs the previous open day, 亿 CNY / %). */
export function fmtTurnoverChg(
  chg: number | null | undefined,
  pct: number | null | undefined,
): string | null {
  if (chg == null) return null;
  const p = pct == null ? "" : `（${formatSignedPercent(pct, 1)}）`;
  return `较昨 ${formatSignedFixed(chg, 1)}亿${p}`;
}

/** Default lines: top-k inflow + top-k outflow by net now. */
export function defaultSelection(sectors: Sector[], k: number): string[] {
  const ins = sectors.filter((s) => s.net > 0).slice(0, k);
  const outs = sectors
    .filter((s) => s.net < 0)
    .slice(-k)
    .reverse();
  return [...ins, ...outs].map((s) => s.name);
}

/** Categorical hues in the order the lines are drawn. A chart does not reuse a colour. */
export function colorMap(shown: Sector[]): Map<string, string> {
  const m = new Map<string, string>();
  const used = new Set<string>();
  shown.forEach((sector, index) => {
    let color = LINE_COLORS[index];
    if (!color || used.has(color)) {
      const spare = LINE_COLORS.find((item) => !used.has(item));
      color = spare ?? `hsl(${(index * 47) % 360} 45% 42%)`;
    }
    used.add(color);
    m.set(sector.name, color);
  });
  return m;
}

/** Sector series values are already 亿. Do not divide by 1e8 again. */
export function fmtSectorNet(yi: number | null | undefined): string {
  return fmtYi(yi);
}

/** Default number of inflow and of outflow sectors drawn on the main chart (phone and desktop). */
export const DEFAULT_TOP_N = 5;

/**
 * Collision nudging for line-end labels: given label y positions sorted ascending,
 * push them apart to at least `gap` px and keep them within [top, bottom].
 * If they can't fit, they are spread evenly across the range.
 */
/**
 * End labels sit in the gutter to the right of the plot, one row each.
 * `x` is the text start; it is always past `plotRight`, so the words are not
 * drawn on the curves. `y` keeps at least `gap` pixels between neighbours.
 */
export function placeEndLabels(
  points: readonly { y: number }[],
  opts: { gap: number; top: number; bottom: number; plotRight: number },
): { y: number; x: number }[] {
  const laid = layoutEndLabels(
    points.map((point) => point.y),
    opts.gap,
    opts.top,
    opts.bottom,
  );
  const x = opts.plotRight + 12;
  return laid.map((y) => ({ y, x }));
}

export function layoutEndLabels(ys: number[], gap: number, top: number, bottom: number): number[] {
  const n = ys.length;
  if (n === 0) return [];
  const g = n > 1 ? Math.min(gap, (bottom - top) / (n - 1)) : gap;
  const out = Float64Array.from(ys, (y) => Math.max(top, Math.min(bottom, y)));
  for (let i = 1; i < n; i++) out[i] = Math.max(out[i] as number, (out[i - 1] as number) + g);
  const over = (out[n - 1] as number) - bottom;
  if (over > 0) for (let i = 0; i < n; i++) out[i] = (out[i] as number) - over;
  for (let i = n - 2; i >= 0; i--) out[i] = Math.min(out[i] as number, (out[i + 1] as number) - g);
  const under = top - (out[0] as number);
  if (under > 0) for (let i = 0; i < n; i++) out[i] = (out[i] as number) + under;
  return Array.from(out);
}
