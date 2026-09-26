import { EMPTY } from "@/format/number";

/** 元 → "12.35" 亿 (two decimals), "—" when missing. */
export function yi(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return EMPTY;
  }
  return (value / 1e8).toFixed(2);
}

/** A plain number with fixed decimals, "—" when missing. */
export function fixed(value: number | null | undefined, digits = 2): string {
  if (value === null || value === undefined || !Number.isFinite(value)) {
    return EMPTY;
  }
  return value.toFixed(digits);
}
