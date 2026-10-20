import type { SectorFlowData } from "@/api/endpoints";

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
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}亿`;
export const fmtPct = (v: number | null | undefined, digits = 2) =>
  v == null ? "—" : `${v > 0 ? "+" : ""}${v.toFixed(digits)}%`;
export const tone = (v: number | null | undefined) =>
  v == null || v === 0 ? "" : v > 0 ? "up" : "down";

export const LINE_COLORS = [
  "#2448D0",
  "#C47B12",
  "#6B4C9A",
  "#0E7490",
  "#9A3412",
  "#3F6212",
  "#1D4E89",
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
  const sign = (v: number) => (v > 0 ? "+" : "");
  const p = pct == null ? "" : `（${sign(pct)}${pct.toFixed(1)}%）`;
  return `较昨 ${sign(chg)}${chg.toFixed(1)}亿${p}`;
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

/** Stable colours: inflow names get warm hues, outflow cool, by rank among the shown lines. */
/** Categorical hues, stable by name. Not the red/green up-down pair. */
export function colorMap(shown: Sector[]): Map<string, string> {
  const m = new Map<string, string>();
  const names = shown.map((s) => s.name).sort((a, b) => a.localeCompare(b, "zh"));
  names.forEach((name, index) => {
    m.set(name, LINE_COLORS[index % LINE_COLORS.length] ?? "#2448D0");
  });
  return m;
}

/** Default number of inflow and of outflow sectors drawn on the main chart (phone and desktop). */
export const DEFAULT_TOP_N = 5;

/**
 * Collision nudging for line-end labels: given label y positions sorted ascending,
 * push them apart to at least `gap` px and keep them within [top, bottom].
 * If they can't fit, they are spread evenly across the range.
 */
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
