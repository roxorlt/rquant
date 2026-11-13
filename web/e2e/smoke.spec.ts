import { expect, test } from "@playwright/test";

test("every MVP page opens from the rail", async ({ page }) => {
  await page.goto("./#/overview");
  await expect(page.getByText("候选股").first()).toBeVisible();
  for (const [title, text] of [
    ["选股器", "贵州茅台"],
    ["池子", "核心观察"],
    ["因子库", "演示动量因子"],
    ["盯盘与告警", "突破"],
    ["市场全景", "白酒"],
    ["模拟盘", "paper-main"],
    ["回测", "run-demo"],
    ["系统状态", "日线采集服务"],
  ] as const) {
    await page.getByRole("navigation").first().getByRole("link", { name: title }).click();
    await expect(page.getByText(text).first()).toBeVisible();
  }
});

test("adding a stock to watchlist persists in pools", async ({ page }) => {
  await page.goto("./#/screener");
  await page
    .getByRole("row", { name: /贵州茅台/ })
    .getByRole("button", { name: "加自选" })
    .click();
  await expect(page.getByText("已加入自选")).toBeVisible();
  await page.goto("./#/pools");
  await page.getByRole("button", { name: "自选股" }).click();
  await expect(page.getByRole("row", { name: /贵州茅台/ })).toBeVisible();
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

test("unified backtest tabs show strategies and portfolio orders", async ({ page }) => {
  await page.goto("./#/backtest?tab=strategies");
  await expect(page.getByText("演示突破 1（共 1 版）")).toBeVisible();
  await page.goto("./#/backtest?tab=portfolio");
  await page.getByText("演示组合回测").click();
  await expect(page.getByLabel("组合回测委托")).toBeVisible();
  await expect(page.getByText("组合回测逐日净值")).toBeVisible();
  await expect(page.getByRole("table", { name: "行业暴露" })).toContainText("白酒");
  await page
    .getByRole("table", { name: "组合回测列表" })
    .getByRole("button", { name: "加入对比" })
    .click();
  await expect(page.getByText("再选一条就能对比")).toBeVisible();
});

test("factor page shows IC and quantiles", async ({ page }) => {
  await page.goto("./#/factor");
  await page.getByText("演示动量因子").click();
  await expect(page.getByLabel("因子检验指标")).toBeVisible();
  await expect(page.getByRole("table", { name: "IC 衰减" })).toBeVisible();
  await expect(page.getByLabel("跟踪指标")).toBeVisible();
});

test("an alert rule saved through web commands shows up", async ({ page }) => {
  await page.goto("./#/monitor");
  await page.getByRole("button", { name: "新建规则" }).click();
  await page.getByLabel("规则名称").fill("早盘一档");
  await page.getByRole("button", { name: "保存规则" }).click();
  await expect(page.getByLabel("告警规则", { exact: true })).toContainText("早盘一档");
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

test("unified system status page shows layers, unit logs, and dataset coverage", async ({
  page,
}) => {
  await page.goto("./#/health");
  const layers = page.getByRole("list", { name: "分层健康" });
  await expect(layers.getByRole("listitem")).toHaveCount(6);
  await expect(page.getByText("缺 1 天")).toBeVisible();
  await page.getByText("日线采集服务").click();
  await expect(page.getByText("ingest ok rows=5412")).toBeVisible();
  await page.goBack();
  await page.getByText("股票日线").first().click();
  await expect(page.getByLabel("字段")).toBeVisible();
});

test("stock profile opens from a code link", async ({ page }) => {
  await page.goto("./#/stock/600519.SH");
  await expect(page.getByRole("heading", { name: /贵州茅台/ })).toBeVisible();
  await expect(page.getByRole("img", { name: "日K线" })).toBeVisible();
  await expect(page.getByLabel("个股财务")).toContainText("18.90");
  await expect(page.getByText("获利比例")).toBeVisible();
});

test("one sentence becomes a validated condition", async ({ page }) => {
  await page.goto("./#/screener");
  await page.getByLabel("一句话条件").fill("站上20日线且放量一倍");
  await page.getByRole("button", { name: "一句话生成" }).click();
  await expect(page.getByLabel("生成结果")).toContainText("ts_mean(close,20)");
});

test("mode-A screener workbench switches upstream, saves custom block, and previews funnel steps", async ({
  page,
}) => {
  await page.goto("./#/screener");
  await page.getByRole("button", { name: /全市场 A 股/ }).click();
  await expect(page.getByRole("row", { name: /招商银行/ })).toBeVisible();

  await page.getByRole("button", { name: "+ 添加条件积木" }).click();
  const picker = page.getByRole("dialog", { name: "选股积木库" });
  await picker.getByRole("button", { name: /排除 ST/ }).click();

  await page.getByRole("button", { name: "★ 存为我的积木" }).click();
  await page.getByLabel("自定义积木名称").fill("E2E基础滤网");
  await page.getByRole("button", { name: "保存积木" }).click();

  await page.getByRole("button", { name: "+ 添加条件积木" }).click();
  await expect(page.getByRole("dialog", { name: "选股积木库" })).toContainText("★ E2E基础滤网");
  await page.getByRole("button", { name: "+ 添加条件积木" }).click();

  const funnel = page.getByRole("list", { name: "逐条命中" });
  await funnel.getByRole("button", { name: /全市场 A 股/ }).click();
  await expect(page.getByRole("button", { name: /返回最终结果/ })).toBeVisible();
});

test("mode-B pool dependency canvas renders DAG nodes, creates downstream node with inline filter, and toggles view", async ({
  page,
}) => {
  await page.goto("./#/screener?view=canvas");
  await expect(page.getByText("Pool 1 · 基础初筛池")).toBeVisible();
  await expect(page.getByText("Pool 2-1 · 趋势精选池")).toBeVisible();

  await page.getByRole("button", { name: "+ 新建下游节点" }).click();
  const drawer = page.getByRole("dialog", { name: "新建下游池子节点" });
  await drawer.getByLabel("节点名称").fill("E2E下游池");
  await drawer.getByRole("button", { name: "+ 添加条件积木" }).click();
  await drawer
    .getByRole("dialog", { name: "选股积木库" })
    .getByRole("button", { name: /排除北交所/ })
    .click();
  await drawer.getByRole("button", { name: "创建下游节点并连线" }).click();
  await expect(page.getByTestId("pool-canvas-stage").getByText("E2E下游池")).toBeVisible();

  await page.getByRole("button", { name: "列表工作台" }).click();
  await expect(page.getByText("条件工作台")).toBeVisible();
});
