import { describe, expect, it } from "vitest";
import { squarify } from "./treemap";

describe("squarify", () => {
  it("tiles the box with areas proportional to weight", () => {
    const rects = squarify([6, 6, 4, 3, 2, 2, 1], { x: 0, y: 0, w: 6, h: 4 });
    const areas = rects.map((r) => r.w * r.h);
    expect(areas.reduce((a, b) => a + b, 0)).toBeCloseTo(24);
    expect(areas[0]).toBeCloseTo(6);
    expect(areas[6]).toBeCloseTo(1);
  });
});
