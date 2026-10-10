import type { ScreenRow } from "@/api/endpoints";

/** Prototype-style condition builder over the published screen rows (client side). */
export type Field = "pct_chg" | "close";
export const FIELDS: Record<Field, string> = { pct_chg: "涨跌幅 %", close: "收盘价" };

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

const val = (row: ScreenRow, f: Field) => (f === "pct_chg" ? row.pct_chg : row.close) ?? null;

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
