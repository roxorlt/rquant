/**
 * The A-share one-minute session axis: 240 bars a day, labelled by their start
 * minute — 120 in the morning (09:30–11:29) and 120 in the afternoon
 * (13:00–14:59), with the lunch break skipped. Same convention as the
 * Streamlit panorama (market_panorama.py _SESSION_BARS) and the minute bars
 * in Serving, so an intraday chart can pin its x axis to all 240 slots.
 */

import type { components } from "@/api/schema";

export const SESSION_BAR_COUNT = 240;
const MORNING_START = 9 * 60 + 30;
const AFTERNOON_START = 13 * 60;
const HALF = 120;

function minutesOf(hhmm: string): number | null {
  const match = /^(\d{2}):(\d{2})$/.exec(hhmm);
  if (match === null) {
    return null;
  }
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (hours > 23 || minutes > 59) {
    return null;
  }
  return hours * 60 + minutes;
}

/** Slot 0–239 for a bar starting at "HH:MM"; null outside the two sessions. */
export function sessionIndex(hhmm: string): number | null {
  const total = minutesOf(hhmm);
  if (total === null) {
    return null;
  }
  if (total >= MORNING_START && total < MORNING_START + HALF) {
    return total - MORNING_START;
  }
  if (total >= AFTERNOON_START && total < AFTERNOON_START + HALF) {
    return HALF + (total - AFTERNOON_START);
  }
  return null;
}

/** "HH:MM" start label of slot 0–239. */
export function sessionLabel(index: number): string {
  if (!Number.isInteger(index) || index < 0 || index >= SESSION_BAR_COUNT) {
    throw new RangeError(`session index out of range: ${index}`);
  }
  const total = index < HALF ? MORNING_START + index : AFTERNOON_START + (index - HALF);
  const hours = Math.floor(total / 60);
  const minutes = total % 60;
  return `${String(hours).padStart(2, "0")}:${String(minutes).padStart(2, "0")}`;
}

/** Market phases the API reports (rquant.web.market.MarketPhase). */
export type MarketPhase = components["schemas"]["MarketPhase"];

/** Dot colour key used by .phase[data-phase] in shell.css. */
export function phaseTone(phase: MarketPhase): "pre" | "auction" | "cont" | "noon" | "close" {
  switch (phase) {
    case "call_auction":
    case "closing_auction":
      return "auction";
    case "continuous":
      return "cont";
    case "noon_break":
      return "noon";
    case "pre_open":
      return "pre";
    default:
      return "close";
  }
}
