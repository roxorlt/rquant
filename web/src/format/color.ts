/**
 * A-share colour convention: a rise is red ("up"), a fall is green ("down").
 * Every price, change and P&L number goes through toneOf so the convention lives
 * in one place; the classes map to --up-t / --down-t in styles/base.css.
 */

export type Tone = "up" | "down" | "flat";

export function toneOf(value: number | null | undefined): Tone {
  if (value === null || value === undefined || Number.isNaN(value) || value === 0) {
    return "flat";
  }
  return value > 0 ? "up" : "down";
}

/** KPI-strip tone for a signed amount (red up / green down, flat uncoloured). */
export function kpiTone(value: number | null | undefined): "up" | "down" | undefined {
  const tone = toneOf(value);
  return tone === "flat" ? undefined : tone;
}

/** CSS class for a tone; flat numbers keep the surrounding text colour. */
export function toneClass(tone: Tone): string {
  return tone === "flat" ? "" : tone;
}
