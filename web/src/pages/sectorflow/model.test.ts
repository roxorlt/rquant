import { describe, expect, it } from "vitest";
import {
  colorMap,
  DEFAULT_TOP_N,
  fmtAmountYi,
  fmtSectorNet,
  fmtTurnoverChg,
  fmtYi,
  layoutEndLabels,
  type Sector,
  tone,
} from "./model";

describe("leader net is already 亿", () => {
  it("does not divide a series value by 1e8", () => {
    expect(fmtSectorNet(2.4)).toBe("+2.4亿");
    expect(fmtYi(2.4 / 1e8)).toBe("+0.0亿");
  });
});

describe("line colours", () => {
  it("gives each drawn sector a different colour", () => {
    const shown = Array.from({ length: 10 }, (_, i) => ({ name: `板块${i}` })) as Sector[];
    const colors = [...colorMap(shown).values()];
    expect(new Set(colors).size).toBe(colors.length);
  });
});

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

describe("两市成交额 card", () => {
  it("formats 10-19 vs 10-16 (Tushare daily totals)", () => {
    expect(fmtAmountYi(14487.919 * 1e8)).toBe("1.45万亿");
    expect(fmtTurnoverChg(-1202.741, -7.67)).toBe("较昨 -1202.7亿（-7.7%）");
    expect(tone(-1202.741)).toBe("down");
  });
  it("shows a rise in red and hides the line when there is no previous day", () => {
    expect(fmtTurnoverChg(318.4, 2.01)).toBe("较昨 +318.4亿（+2.0%）");
    expect(tone(318.4)).toBe("up");
    expect(fmtTurnoverChg(null, null)).toBeNull();
    expect(fmtTurnoverChg(12, null)).toBe("较昨 +12.0亿");
  });
});
