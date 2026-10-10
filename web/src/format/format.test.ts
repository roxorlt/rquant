import { toneClass, toneOf } from "./color";
import {
  EMPTY,
  formatAmount,
  formatNumber,
  formatPercent,
  formatPrice,
  formatSignedPercent,
} from "./number";
import {
  formatAge,
  formatShanghaiDateTime,
  formatShanghaiTime,
  shanghaiDate,
  weekdayOf,
} from "./time";

describe("red up / green down", () => {
  it("maps rises to up (red) and falls to down (green)", () => {
    expect(toneOf(1.2)).toBe("up");
    expect(toneOf(-0.01)).toBe("down");
    expect(toneOf(0)).toBe("flat");
    expect(toneOf(null)).toBe("flat");
    expect(toneOf(Number.NaN)).toBe("flat");
    expect(toneClass("up")).toBe("up");
    expect(toneClass("flat")).toBe("");
  });
});

describe("numbers", () => {
  it("formats prices, amounts and percentages", () => {
    expect(formatPrice(12.3)).toBe("12.30");
    expect(formatNumber(1234567.891, 1)).toBe("1,234,567.9");
    expect(formatAmount(1.2345e9)).toBe("12.35亿");
    expect(formatAmount(-8.302e6)).toBe("−830.2万");
    expect(formatAmount(512)).toBe("512");
    expect(formatPercent(48.26, 1)).toBe("48.3%");
    expect(formatSignedPercent(2.5)).toBe("+2.50%");
    expect(formatSignedPercent(-1.2)).toBe("−1.20%");
    expect(formatSignedPercent(-0.001)).toBe("0.00%");
    expect(formatSignedPercent(null)).toBe(EMPTY);
    expect(formatAmount(Number.POSITIVE_INFINITY)).toBe(EMPTY);
  });
});

describe("Shanghai time", () => {
  it("uses the market clock whatever the viewer's zone", () => {
    const lateUtc = new Date("2026-09-24T17:30:00Z"); // 01:30 next day in Shanghai
    expect(shanghaiDate(lateUtc)).toEqual({ date: "2026-09-25", weekday: "周五" });
    expect(formatShanghaiTime("2026-09-24T01:30:00Z")).toBe("09:30");
    expect(formatShanghaiDateTime("2026-09-24T07:31:00Z")).toBe("2026-09-24 15:31:00");
    expect(weekdayOf("2026-09-28")).toBe("周一");
  });

  it("describes ages coarsely", () => {
    expect(formatAge(12)).toBe("刚刚");
    expect(formatAge(190)).toBe("3 分钟前");
    expect(formatAge(7300)).toBe("2 小时前");
    expect(formatAge(3 * 86400 + 5)).toBe("3 天前");
  });
});
