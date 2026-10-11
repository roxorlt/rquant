import { describe, expect, it } from "vitest";
import { clampView } from "./Candles";
import { boll, kdj, macd, obv, rsi } from "./indicators";

const up = Array.from({ length: 60 }, (_, i) => 10 + i * 0.1);

describe("indicators", () => {
  it("trend up: MACD positive, RSI high, KDJ high, OBV rising, BOLL ordered", () => {
    expect(macd(up).dif.at(-1)).toBeGreaterThan(0);
    expect(rsi(up, 6).at(-1)).toBe(100);
    expect(kdj(up, up, up).k.at(-1)).toBeGreaterThan(80);
    expect(
      obv(
        up,
        up.map(() => 1),
      ).at(-1),
    ).toBe(59);
    const b = boll(up);
    expect((b.up.at(-1) ?? 0) > (b.mid.at(-1) ?? 0)).toBe(true);
  });
  it("clamps zoom windows", () => {
    expect(clampView(-5, 20, 100)).toEqual([0, 25]);
    expect(clampView(95, 110, 100)).toEqual([85, 100]);
    expect(clampView(50, 52, 100)).toEqual([50, 60]);
  });
});
