import { describe, expect, it } from "vitest";
import { DEFAULT_TOP_N, layoutEndLabels } from "./model";

describe("sectorflow defaults", () => {
  it("draws 5 inflow + 5 outflow lines by default (phone too)", () => {
    expect(DEFAULT_TOP_N).toBe(5);
  });
});

describe("layoutEndLabels", () => {
  const ok = (ys: number[], gap: number, top: number, bottom: number) => {
    for (let i = 1; i < ys.length; i++)
      expect((ys[i] as number) - (ys[i - 1] as number)).toBeGreaterThanOrEqual(gap - 1e-9);
    for (const y of ys) {
      expect(y).toBeGreaterThanOrEqual(top - 1e-9);
      expect(y).toBeLessThanOrEqual(bottom + 1e-9);
    }
  };
  it("separates 10 clustered labels on a 393px phone chart", () => {
    const ys = [100, 101, 102, 103, 104, 150, 150, 151, 152, 153];
    const out = layoutEndLabels(ys, 12, 26, 276);
    ok(out, 12, 26, 276);
  });
  it("keeps labels inside when clustered at the edges", () => {
    ok(layoutEndLabels([0, 1, 2, 3, 4], 12, 26, 276), 12, 26, 276);
    ok(layoutEndLabels([300, 300, 300, 300, 300], 12, 26, 276), 12, 26, 276);
  });
  it("leaves well-spaced labels untouched", () => {
    expect(layoutEndLabels([40, 80, 120], 12, 26, 276)).toEqual([40, 80, 120]);
  });
});
