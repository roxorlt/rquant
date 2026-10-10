import { expect, test } from "@playwright/test";

test("every MVP page opens from the rail", async ({ page }) => {
  await page.goto("./#/overview");
  await expect(page.getByText("候选股").first()).toBeVisible();
  for (const [title, text] of [
    ["选股与排序", "贵州茅台"],
    ["池子", "核心观察"],
    ["回测结果", "run-demo"],
    ["模拟盘", "paper-main"],
    ["告警", "breakout"],
    ["市场全景", "白酒"],
    ["系统健康", "rquant-monitor"],
  ] as const) {
    await page.getByRole("navigation").first().getByRole("link", { name: title }).click();
    await expect(page.getByText(text).first()).toBeVisible();
  }
});

test("acknowledging an alert goes through page control", async ({ page }) => {
  await page.goto("./#/monitor");
  await page.getByRole("button", { name: "确认" }).first().click();
  await expect(page.getByText("已确认 · owner").first()).toBeVisible();
});

test("a backtest run shows its performance block", async ({ page }) => {
  await page.goto("./#/backtest");
  await page.getByText("run-demo").first().click();
  await expect(page.getByLabel("绩效指标")).toBeVisible();
  await expect(page.getByRole("table", { name: "月度收益" })).toBeVisible();
  await expect(page.getByRole("img", { name: "净值与回撤" })).toBeVisible();
  await expect(page.getByText("超额 vs 000300.SH")).toBeVisible();
});

test("a portfolio backtest shows orders and performance", async ({ page }) => {
  await page.goto("./#/backtest");
  await page.getByText("演示组合回测").click();
  await expect(page.getByLabel("组合回测委托")).toBeVisible();
  await expect(page.getByText("组合回测逐日净值")).toBeVisible();
  await expect(page.getByLabel("过拟合指标")).toBeVisible();
  await expect(page.getByRole("table", { name: "行业暴露" })).toContainText("白酒");
  await page.getByRole("button", { name: "加入对比" }).click();
  await expect(page.getByText("再选一条回测")).toBeVisible();
});
