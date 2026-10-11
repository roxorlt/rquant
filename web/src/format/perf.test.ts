import { describe, expect, it } from "vitest";
import { rollingSharpe, topDrawdowns, yearlyReturns } from "./perf";

describe("perf helpers", () => {
  it("finds drawdown periods deepest first", () => {
    const dd = topDrawdowns([1, 1.1, 0.99, 1.2, 1.14, 1.3, 1.0]);
    expect(dd[0]).toMatchObject({ start: 5, trough: 6, end: null });
    expect(dd[1]).toMatchObject({ start: 1, trough: 2, end: 3 });
    expect(dd).toHaveLength(3);
  });
  it("rolling sharpe fills after the window", () => {
    const r = rollingSharpe([1, 1.01, 1.0, 1.02, 1.03], 3);
    expect(r.slice(0, 3)).toEqual([null, null, null]);
    expect(r[3]).not.toBeNull();
  });
  // The first year starts from the initial NAV of 1.
  it("yearly returns chain from the prior year end", () => {
    const y = yearlyReturns([
      { date: "2025-12-30", nav: 1 },
      { date: "2025-12-31", nav: 1.1 },
      { date: "2026-01-02", nav: 1.21 },
    ]);
    expect(y[1]?.ret).toBeCloseTo(0.1);
  });
});
