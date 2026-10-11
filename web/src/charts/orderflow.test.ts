import { describe, expect, it } from "vitest";
import { barMinute, deltas, phi, profile, sessionMinute, sessionVwap, stitch } from "./orderflow";

const b = (c: number, v = 100) => ({ open: c, high: c + 0.1, low: c - 0.1, close: c, vol: v });

describe("orderflow", () => {
  it("session axis skips lunch", () => {
    expect(sessionMinute("09:35")).toBe(5);
    expect(sessionMinute("11:30")).toBe(120);
    expect(sessionMinute("13:05")).toBe(125);
    expect(sessionMinute("15:00")).toBe(240);
  });
  it("vwap, deltas and profile", () => {
    const bars = [b(10), b(11, 300), b(10.5)];
    expect(sessionVwap(bars).at(-1)?.vwap).toBeCloseTo((10 * 100 + 11 * 300 + 10.5 * 100) / 500, 6);
    expect(deltas(bars, 10, "tick")).toEqual([0, 300, -100]);
    const bvc = deltas(bars, 10, "bvc");
    expect(bvc[1]).toBeGreaterThan(0);
    expect(bvc[2]).toBeLessThan(0);
    expect(phi(0)).toBeCloseTo(0.5, 6);
    const p = profile(bars, 9.8, 11.2, 14);
    expect(p.poc).toBeGreaterThan(10.8);
    expect(p.val).toBeLessThanOrEqual(p.poc);
  });
  it("auction bar sits on the left axis; 5日 stitching resets VWAP per day", () => {
    expect(barMinute("09:30", 1)).toBe(0);
    expect(barMinute("09:31", 1)).toBe(0.5);
    expect(barMinute("09:35", 5)).toBe(2.5);
    const day = (c: number) => [
      { ...b(c), t: "09:35" },
      { ...b(c + 1), t: "15:00" },
    ];
    const s = stitch([
      { date: "2026-10-08", freq: "5min", bars: day(10) },
      { date: "2026-10-09", freq: "5min", bars: day(20) },
    ]);
    expect(s.map((x) => x.x)).toEqual([2.5, 237.5, 242.5, 477.5]);
    expect(s[2]?.vwap).toBeCloseTo(20, 6);
    expect(s[3]?.date).toBe("2026-10-09");
  });
});
