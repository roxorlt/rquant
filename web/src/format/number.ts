/** Number formatting for prices, amounts and percentages (Chinese units). */

const MINUS = "−";
export const EMPTY = "—";

function isMissing(value: number | null | undefined): value is null | undefined {
  return value === null || value === undefined || !Number.isFinite(value);
}

/** Fixed decimals with thousands separators, e.g. 12,345.60. */
export function formatNumber(value: number | null | undefined, digits = 2): string {
  if (isMissing(value)) {
    return EMPTY;
  }
  return value.toLocaleString("zh-CN", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

/** A price: always two decimals, no separators beyond the thousands. */
export function formatPrice(value: number | null | undefined): string {
  return formatNumber(value, 2);
}

/** An amount in yuan shown in 亿 (≥ 1e8) or 万 (≥ 1e4), e.g. 12.35亿, 830.2万. */
export function formatAmount(value: number | null | undefined, digits = 2): string {
  if (isMissing(value)) {
    return EMPTY;
  }
  const magnitude = Math.abs(value);
  const sign = value < 0 ? MINUS : "";
  if (magnitude >= 1e8) {
    return `${sign}${(magnitude / 1e8).toFixed(digits)}亿`;
  }
  if (magnitude >= 1e4) {
    return `${sign}${(magnitude / 1e4).toFixed(digits === 2 ? 1 : digits)}万`;
  }
  return `${sign}${magnitude.toFixed(0)}`;
}

/** A value already in percent units (2.5 means 2.50%), no sign. */
export function formatPercent(value: number | null | undefined, digits = 2): string {
  if (isMissing(value)) {
    return EMPTY;
  }
  return `${value.toFixed(digits)}%`;
}

/**
 * A change already in percent units with an explicit sign, so the direction is
 * readable without colour: +2.50%, −1.20%, 0.00%.
 */
export function formatSignedPercent(value: number | null | undefined, digits = 2): string {
  if (isMissing(value)) {
    return EMPTY;
  }
  const fixed = Math.abs(value).toFixed(digits);
  if (Number(fixed) === 0) {
    return `${(0).toFixed(digits)}%`;
  }
  return `${value > 0 ? "+" : MINUS}${fixed}%`;
}
