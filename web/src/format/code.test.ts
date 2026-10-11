import { describe, expect, it } from "vitest";
import { fullCode } from "./code";

describe("fullCode", () => {
  it("infers the exchange", () => {
    expect(fullCode("600519")).toBe("600519.SH");
    expect(fullCode("000001")).toBe("000001.SZ");
    expect(fullCode("830799")).toBe("830799.BJ");
    expect(fullCode(" 300750.sz ")).toBe("300750.SZ");
    expect(fullCode("茅台")).toBeNull();
  });
});
