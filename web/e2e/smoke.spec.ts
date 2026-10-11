import { expect, test } from "@playwright/test";

test("every MVP page opens from the rail", async ({ page }) => {
  await page.goto("./#/overview");
  await expect(page.getByText("候选股").first()).toBeVisible();
  for (const [title, text] of [
    ["选股器", "贵州茅台"],
    ["池子", "核心观察"],
    ["回测", "run-demo"],
    ["模拟盘", "paper-main"],
    ["盯盘与告警", "breakout"],
    ["市场全景", "白酒"],
    ["系统健康", "rquant-monitor"],
  ] as const) {
    await page.getByRole("navigation").first().getByRole("link", { name: title }).click();
    await expect(page.getByText(text).first()).toBeVisible();
  }
});

test("acknowledging an alert goes through page control", async ({ page }) => {
  await page.goto("./#/monitor");
  await page.getByRole("button", { name: "确认", exact: true }).first().click();
  await expect(page.getByText("已确认 · owner").first()).toBeVisible();
});

test("a backtest run shows its performance block", async ({ page }) => {
  await page.goto("./#/backtest");
  await page.getByText("run-demo").first().click();
  await expect(page.getByLabel("绩效指标")).toBeVisible();
  await expect(page.getByRole("table", { name: "月度收益" })).toBeVisible();
  await expect(page.getByRole("img", { name: "净值" })).toBeVisible();
  await expect(page.getByRole("img", { name: "回撤" })).toBeVisible();
  await expect(page.getByText("超额 vs 000300.SH")).toBeVisible();
});

test("a portfolio backtest shows orders and performance", async ({ page }) => {
  await page.goto("./#/strategies");
  await expect(page.getByText("demo-breakout@1（共 1 版）")).toBeVisible();
  await page.goto("./#/experiments");
  await page.getByText("演示组合回测").click();
  await expect(page.getByLabel("组合回测委托")).toBeVisible();
  await expect(page.getByText("组合回测逐日净值")).toBeVisible();
  await expect(page.getByLabel("过拟合指标")).toBeVisible();
  await page.getByRole("button", { name: "生成解读" }).click();
  await expect(page.getByText("数字已全部核对")).toBeVisible();
  await expect(page.getByRole("table", { name: "行业暴露" })).toContainText("白酒");
  await page.getByRole("button", { name: "加入对比" }).click();
  await expect(page.getByText("再选一条回测")).toBeVisible();
  await expect(page.getByRole("table", { name: "Brinson 归因" })).toContainText("现金");
});

test("data center shows the catalog and coverage", async ({ page }) => {
  await page.goto("./#/data");
  await page.getByText("股票日线").first().click();
  await expect(page.getByText("缺 1 天")).toBeVisible();
  await expect(page.getByLabel("字段")).toBeVisible();
});

test("factor page shows IC and quantiles", async ({ page }) => {
  await page.goto("./#/factor");
  await page.getByText("演示动量因子").click();
  await expect(page.getByLabel("因子检验指标")).toBeVisible();
  await expect(page.getByRole("table", { name: "IC 衰减" })).toBeVisible();
  await expect(page.getByLabel("跟踪指标")).toBeVisible();
});

test("an alert rule saved through page control shows up", async ({ page }) => {
  await page.goto("./#/monitor");
  await page.getByLabel("规则 ID").fill("p2-l1");
  await page.getByLabel("规则名称").fill("二号池一档");
  await page.getByRole("button", { name: "保存规则" }).click();
  await expect(page.getByLabel("告警规则", { exact: true })).toContainText("二号池一档");
  await page.goto("./#/ops");
  await expect(
    page.getByRole("grid", { name: "操作记录" }).or(page.getByRole("table", { name: "操作记录" })),
  ).toContainText("启用告警规则 二号池一档");
});

test("tdx formula translates and condition hits are listed", async ({ page }) => {
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "翻译" }).click();
  await expect(page.getByLabel("翻译结果")).toContainText("ts_mean(close,5)");
  await expect(page.getByText("演示条件：站上5日线且上涨")).toBeVisible();
});

test("paper page compares against a backtest band", async ({ page }) => {
  await page.goto("./#/paper");
  await page.getByLabel("对照回测").selectOption({ label: "演示组合回测" });
  const start = new Date(Date.now() - 20 * 86400000).toISOString().slice(0, 10);
  await page.getByLabel("模拟起始日").fill(start);
  await expect(page.getByRole("table", { name: "区间对照" })).toBeVisible();
});

test("health page shows the six layers", async ({ page }) => {
  await page.goto("./#/health");
  const layers = page.getByRole("list", { name: "分层健康" });
  await expect(layers.getByRole("listitem")).toHaveCount(6);
});

test("tasks page lists units and shows a log", async ({ page }) => {
  await page.goto("./#/tasks");
  await page.getByText("rquant-daily.service").click();
  await expect(page.getByText("ingest ok rows=5412")).toBeVisible();
});

test("query page runs a read-only select", async ({ page }) => {
  await page.goto("./#/query");
  await page.getByRole("button", { name: "运行" }).click();
  await expect(
    page.getByRole("table", { name: "查询结果" }).or(page.getByRole("grid", { name: "查询结果" })),
  ).toContainText("600519.SH");
  await page.getByLabel("SQL").fill("delete from stock_basic");
  await page.getByRole("button", { name: "运行" }).click();
  await expect(page.getByRole("alert")).toContainText("只允许");
});

test("data center shows fundamentals", async ({ page }) => {
  await page.goto("./#/data");
  await expect(page.getByText("财务指标 · 2")).toBeVisible();
});

test("stock profile opens from a code link", async ({ page }) => {
  await page.goto("./#/stock/600519.SH");
  await expect(page.getByRole("heading", { name: /贵州茅台/ })).toBeVisible();
  await expect(page.getByRole("img", { name: "日K线" })).toBeVisible();
  await expect(page.getByLabel("个股财务")).toContainText("18.90");
});
