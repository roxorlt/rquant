import { describe, expect, it } from "vitest";
import { type SurgeRules, surgeHelp, surgeSubtitle } from "./surgeCopy";

/** Defaults in src/rquant/research/surge.py. The API test checks the route returns these. */
const RULES: SurgeRules = {
  ratio_min: 8,
  amount_min: 1e8,
  acc5_min: 10,
  win_amount_min: 5e7,
  acc_ratio_min: 3,
  start_m: 5,
  cooldown_s: 900,
};

describe("surge copy follows the scanner constants", () => {
  it("renders the subtitle and the ? text from the rules object", () => {
    expect(surgeSubtitle(RULES)).toBe(
      "今天的成交量比昨天同一时刻放大很多时提醒。9:35 以后开始，同一只股票的同一种情况 15 分钟内只提醒一次。",
    );
    expect(surgeHelp(RULES)).toBe(
      [
        "看全部 A 股，满足任意一条就提醒：",
        "1. 从开盘到现在的成交量是昨天同一时刻的 8 倍以上，成交额超过 1 亿。",
        "2. 最近 5 分钟的成交量是昨天同一时段的 10 倍以上，这 5 分钟成交额超过 5000 万，而且从开盘到现在的成交量至少是昨天同一时刻的 3 倍。",
        "名字带 ST 的股票、封死涨停或跌停的股票不提醒。",
        "两种情况分开算，同一只股票 15 分钟内最多可能提醒两次。",
      ].join("\n"),
    );
  });
});
