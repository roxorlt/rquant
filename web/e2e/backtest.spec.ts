import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test("已发布分钟回放的记录、配置和交易在桌面与手机可用", async ({ page }, testInfo) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/backtest");
  await expect(page.getByRole("heading", { level: 1, name: "回测" })).toBeVisible();
  await expect(page.getByText("这次回放没有触发交易")).toBeVisible();
  await page.getByRole("combobox", { name: "回放记录" }).selectOption("run-earlier");
  await expect(page.getByText("这次回放未产出逐日净值")).toBeVisible();
  const groups = page.getByRole("table", { name: "配置统计" });
  await expect(groups.locator("tbody tr:not(.pad)")).toHaveCount(2);
  const trades = page.getByRole("table", { name: "交易明细" });
  await expect(trades.locator("tbody tr:not(.pad)")).toHaveCount(20);
  await trades.locator("tbody tr:not(.pad)").first().click();
  const transaction = page.getByRole("dialog");
  await expect(transaction.getByText("买入时间")).toBeVisible();
  await expect(transaction.getByText("2026-07-31 09:31")).toBeVisible();
  await expect(transaction.getByText("卖出时间")).toBeVisible();
  await expect(transaction.getByText("2026-07-31 14:31")).toBeVisible();
  await expect(transaction.getByText("止损")).toBeVisible();
  await transaction.getByRole("button", { name: "查看个股" }).click();
  await expect(page.getByRole("heading", { name: "日 K" })).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(1);
  await page.getByRole("button", { name: "关闭" }).click();
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.getByText("第 2 页")).toBeVisible();
  await expect(trades.locator("tbody tr:not(.pad)")).toHaveCount(3);
  await page.getByRole("button", { name: "上一页" }).click();
  await groups.getByText("第一次突破").click();
  await expect(page.getByText("共 22 笔")).toBeVisible();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "backtest desktop");
  if (process.env.RQ_WEB_BACKTEST_SHOTS === "1") {
    await page.screenshot({ path: testInfo.outputPath("backtest-desktop.png"), fullPage: true });
  }

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("combobox", { name: "回放记录" })).toBeVisible();
  await expect(trades).toBeVisible();
  await expectNoHorizontalOverflow(page, "backtest phone");
  await trades.locator("tbody tr:not(.pad)").first().focus();
  await page.keyboard.press("Enter");
  await expect(transaction.getByText("买入时间")).toBeVisible();
  await expect(transaction.getByText("10.10")).toBeVisible();
  await expect(transaction.getByText("卖出时间")).toBeVisible();
  await expect(transaction.getByText("10.30")).toBeVisible();
  await expect(transaction.getByText("持有到期")).toBeVisible();
  await expectNoHorizontalOverflow(page, "backtest phone trade detail");
  await transaction.getByRole("button", { name: "关闭" }).click();
  if (process.env.RQ_WEB_BACKTEST_SHOTS === "1") {
    await page.screenshot({ path: testInfo.outputPath("backtest-phone.png"), fullPage: true });
  }
  expect(watcher.problems).toEqual([]);
});
