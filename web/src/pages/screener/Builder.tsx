import type { ScreenRow } from "@/api/endpoints";

/** Prototype-style condition builder over the published screen rows (client side). */
/** A built-in numeric column, or "f:<factor name>" for a factor's cross-section percentile. */
export type Field = string;
export const BASE_FIELDS: Record<string, string> = {
  pct_chg: "涨跌幅 %",
  close: "收盘价",
  pe_ttm: "PE(TTM)",
  pb: "PB",
  total_mv: "总市值(亿)",
  turnover_rate: "换手率 %",
  roe: "ROE %",
  netprofit_yoy: "净利同比 %",
  or_yoy: "营收同比 %",
};

/** Built-in fields plus one per factor seen in the rows. */
export function fieldsFor(rows: readonly ScreenRow[]): Record<string, string> {
  const out = { ...BASE_FIELDS };
  for (const r of rows)
    for (const name of Object.keys(r.factors ?? {})) out[`f:${name}`] = `因子·${name} 分位`;
  return out;
}

/** Kept for callers that only need labels of the built-ins. */
export const FIELDS = BASE_FIELDS;

export interface Filter {
  field: Field;
  op: ">=" | "<=";
  value: number;
}
export interface Rank {
  field: Field;
  desc: boolean;
  weight: number;
}

export const val = (row: ScreenRow, f: Field): number | null => {
  if (f.startsWith("f:")) return row.factors?.[f.slice(2)] ?? null;
  const v = (row as unknown as Record<string, unknown>)[f];
  return typeof v === "number" ? v : null;
};

export function applyFilters(rows: ScreenRow[], filters: Filter[]) {
  const steps: number[] = [rows.length];
  let out = rows;
  for (const f of filters) {
    out = out.filter((r) => {
      const v = val(r, f.field);
      return v !== null && (f.op === ">=" ? v >= f.value : v <= f.value);
    });
    steps.push(out.length);
  }
  return { rows: out, steps };
}

/** Weighted percentile-rank score in [0, 100]. */
export function score(rows: ScreenRow[], ranks: Rank[]): Map<ScreenRow, number> {
  const total = ranks.reduce((s, r) => s + r.weight, 0) || 1;
  const out = new Map<ScreenRow, number>(rows.map((r) => [r, 0]));
  for (const rk of ranks) {
    const sorted = [...rows].sort((a, b) => {
      const x = val(a, rk.field) ?? Number.NEGATIVE_INFINITY;
      const y = val(b, rk.field) ?? Number.NEGATIVE_INFINITY;
      return rk.desc ? x - y : y - x;
    });
    sorted.forEach((r, i) => {
      const pct = rows.length > 1 ? i / (rows.length - 1) : 1;
      out.set(r, (out.get(r) ?? 0) + (pct * 100 * rk.weight) / total);
    });
  }
  return out;
}
