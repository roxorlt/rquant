import { portfolioMoney, portfolioPrice } from "./portfolioFormat";

describe("PB-UI-PRICE-01 成交价格", () => {
  it.each([
    ["10.0000", "10.00"],
    ["10.0001", "10.00"],
    ["10.0049", "10.00"],
    ["10.0050", "10.01"],
    ["9.9999", "10.00"],
    ["0.0001", "0.00"],
    ["12345678901234567890.9950", "12,345,678,901,234,567,891.00"],
    [null, "—"],
    [undefined, "—"],
  ])("%s 在主表显示为 %s", (value, expected) => {
    expect(portfolioPrice(value)).toBe(expected);
  });

  it("精确详情沿用原值，不被两位价格显示改写", () => {
    expect(portfolioMoney("10.0001")).toBe("10.0001");
    expect(portfolioMoney("12345678901234567890.9950")).toBe("12,345,678,901,234,567,890.9950");
  });
});
