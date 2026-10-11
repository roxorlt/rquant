import { describe, expect, it } from "vitest";
import { candidates, wordAt } from "./complete";

describe("sql completion", () => {
  const schema = {
    stock_basic: [
      ["ts_code", "VARCHAR"],
      ["name", "VARCHAR"],
    ],
    daily_bar: [["trade_date", "DATE"]],
  };
  it("finds the word at the cursor", () => {
    expect(wordAt("SELECT na", 9)).toEqual({ start: 7, word: "na" });
  });
  it("prefers columns of mentioned tables, then tables and keywords", () => {
    expect(candidates("SELECT ts FROM stock_basic", "ts", schema)).toEqual(["ts_code"]);
    expect(candidates("", "dai", schema)).toEqual(["daily_bar"]);
    expect(candidates("", "sel", schema)).toEqual(["SELECT"]);
  });
});
