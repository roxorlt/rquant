/**
 * The intraday axis: 241 points a day (09:30 → 0 … 11:30 / 13:00 → 120 … 15:00 → 240),
 * the API's `slot`. lightweight-charts needs a time per point, so each point gets a
 * synthetic, evenly spaced timestamp and the labels are mapped back: the noon break and
 * the nights take no room, and one day fills the whole width even mid-session.
 */

export const SLOTS_PER_DAY = 241;
/**
 * A fixed instant on a whole hour (2001-09-09 02:00 UTC): only the spacing matters, and an
 * hour-aligned start makes the chart's own tick picker land on slots 0, 60, 120, 180 and
 * 240 — 09:30, 10:30, 11:30/13:00, 14:00 and 15:00, the old panorama's ticks.
 */
const BASE = 1_000_000_800;

export function sessionTime(dayIndex: number, slot: number): number {
  return BASE + (dayIndex * SLOTS_PER_DAY + slot) * 60;
}

export function sessionPosition(time: number): { dayIndex: number; slot: number } {
  const index = Math.round((time - BASE) / 60);
  return { dayIndex: Math.floor(index / SLOTS_PER_DAY), slot: index % SLOTS_PER_DAY };
}

/** "HH:MM" of a slot; the shared noon point reads "11:30/13:00". */
export function slotLabel(slot: number): string {
  if (slot === 120) {
    return "11:30/13:00";
  }
  const minutes = slot < 120 ? 9 * 60 + 30 + slot : 13 * 60 + (slot - 120);
  return `${String(Math.floor(minutes / 60)).padStart(2, "0")}:${String(minutes % 60).padStart(2, "0")}`;
}

/** Whole-hour-ish ticks of one day: 09:30, 10:30, 11:30/13:00, 14:00, 15:00. */
export const DAY_TICKS: readonly number[] = [0, 60, 120, 180, 240];
