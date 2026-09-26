import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test("单股公式预览先检查再判断，来源换代后桌面与手机要求重跑", async ({ page }, testInfo) => {
  const watcher = watch(page);
  let identity = "a".repeat(64);
  await page.route("**/api/v1/screen/blocks", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "replica";
    body.data.source = { identity, updated_at: "2026-09-24T07:31:00Z" };
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/tdx/parse", async (route) => {
    await route.fulfill({
      status: 200,
      json: {
        syntax_version: "tdx-v1",
        status: "parsed",
        capability: "parse_only",
        ast: null,
        translation: null,
        issues: [],
        unsupported: [],
      },
    });
  });
  await page.route("**/api/v1/screen/tdx/preview", async (route) => {
    const request = route.request().postDataJSON() as Schemas["TdxPreviewRequest"];
    expect(request.source_identity).toBe(identity);
    expect(request.stock_code).toBe("600001.SH");
    await route.fulfill({
      status: 200,
      json: {
        stock_code: request.stock_code,
        trade_date: request.trade_date,
        status: "match",
        reason: null,
        source_updated_at: "2026-09-24T07:31:00Z",
      },
    });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "导入公式" }).click();
  const dialog = page.getByRole("dialog", { name: "公式预览" });
  await dialog.getByRole("textbox", { name: "通达信公式" }).fill("CLOSE>MA(CLOSE,2)");
  await dialog.getByRole("textbox", { name: "股票代码" }).fill("600001.SH");
  await expect(dialog.getByRole("button", { name: "预览这只股票" })).toBeDisabled();
  await dialog.getByRole("button", { name: "检查公式" }).click();
  await expect(dialog.getByText("公式可以预览这只股票。")).toBeVisible();
  await dialog.getByRole("button", { name: "预览这只股票" }).click();
  await expect(dialog.getByRole("status")).toContainText("符合");
  expect(await dialog.innerText()).not.toContain(identity);
  expect(findJargon(await dialog.innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "formula preview desktop");
  await page.screenshot({ path: testInfo.outputPath("formula-preview-desktop.png") });

  await dialog.getByRole("button", { name: "关闭" }).click();
  await expect(dialog).toBeHidden();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "导入公式" }).click();
  await expect(dialog).toBeVisible();
  await dialog.getByRole("textbox", { name: "通达信公式" }).fill("CLOSE>MA(CLOSE,2)");
  await dialog.getByRole("textbox", { name: "股票代码" }).fill("600001.SH");
  await dialog.getByRole("button", { name: "检查公式" }).click();
  await dialog.getByRole("button", { name: "预览这只股票" }).click();
  await expect(dialog.getByRole("status")).toContainText("符合");
  await expectNoHorizontalOverflow(page, "formula preview phone");
  await page.screenshot({ path: testInfo.outputPath("formula-preview-phone.png") });
  identity = "b".repeat(64);
  await dialog.getByRole("button", { name: "刷新选股数据" }).click();
  await expect(dialog.getByRole("status")).toContainText("选股数据已更新，请重新预览");
  await dialog.getByRole("button", { name: "预览这只股票" }).click();
  await expect(dialog.getByRole("status")).toContainText("符合");
  expect(watcher.problems).toEqual([]);
});

test("中文条件筛选、翻页和个股详情在桌面与手机宽度可用", async ({ page }) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await expect(page.getByRole("heading", { level: 1, name: "选股器" })).toBeVisible();
  await expect(page.getByRole("combobox", { name: "条件目录" })).toBeVisible();

  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.getByRole("region", { name: "逐条命中" })).toContainText("排除 ST");
  const table = page.getByRole("table", { name: "选股结果" });
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(20);
  await table.locator("tbody tr:not(.pad)").first().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "关闭" }).click();

  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.getByText("第 2 页")).toBeVisible();
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(7);
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "screener desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("heading", { level: 1, name: "选股器" })).toBeVisible();
  await expect(table).toBeVisible();
  await expectNoHorizontalOverflow(page, "screener phone");
  expect(watcher.problems).toEqual([]);
});

test("排名条件可编辑、折算并按分数稳定翻页，手机上可修改前 N", async ({ page }) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await expect(page.getByRole("button", { name: "添加排名" })).toBeVisible();
  await expect(page.getByRole("option", { name: "20 日涨幅" })).toHaveCount(0);

  await page.getByRole("button", { name: "添加排名" }).click();
  await page.getByRole("combobox", { name: "第 1 项指标" }).selectOption("PCT_CHG[0]");
  await page.getByRole("button", { name: "添加排名" }).click();
  await page.getByRole("spinbutton", { name: "第 1 项权重" }).fill("60");
  await page.getByRole("spinbutton", { name: "第 2 项权重" }).fill("30");
  await page.getByRole("spinbutton", { name: "取前 N 只" }).fill("25");
  await expect(page.getByText(/权重合计 90%/)).toContainText("按比例折算为 100%");
  await page.getByRole("button", { name: "运行筛选" }).click();

  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.getByText("按排名分展示前 25 只")).toBeVisible();
  const table = page.getByRole("table", { name: "选股结果" });
  await expect(table.getByRole("columnheader", { name: "名次" })).toBeVisible();
  await expect(table.getByRole("columnheader", { name: "排名分" })).toBeVisible();
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(20);
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.getByText("第 2 页")).toBeVisible();
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(5);
  await expectNoHorizontalOverflow(page, "ranked screener desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("spinbutton", { name: "取前 N 只" })).toBeVisible();
  await page.getByRole("spinbutton", { name: "取前 N 只" }).fill("24");
  await expect(page.getByRole("status")).toContainText("条件已改，请重新运行");
  await expect(page.getByRole("button", { name: "下一页" })).toBeDisabled();
  await expectNoHorizontalOverflow(page, "ranked screener phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("手机宽度明确展示未判定股票，不把未知写成零命中", async ({ page }) => {
  const watcher = watch(page);
  await page.route("**/api/v1/screen/run", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenRunData_"];
    body.data.total = 0;
    body.data.unknown_count = 1;
    body.data.steps = [{ label: "排除 ST", count: 0, unknown_count: 1 }];
    body.data.rows = [];
    body.data.next_cursor = null;
    await route.fulfill({ response, json: body });
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("未判定 1 只")).toBeVisible();
  await expect(page.getByRole("region", { name: "逐条命中" })).toContainText("未知 1 只");
  await expect(page.getByText("没有命中股票")).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "screen unknown phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("选股来源独立换代后保留条件，失效时桌面与手机都要求重试", async ({ page }, testInfo) => {
  const watcher = watch(page);
  let identity = "a".repeat(64);
  let unavailable = false;
  let catalogReads = 0;
  await page.route("**/api/v1/screen/blocks", async (route) => {
    catalogReads += 1;
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "replica";
    body.data.source = unavailable ? null : { identity, updated_at: "2026-09-24T07:31:00Z" };
    body.data.available = !unavailable;
    if (unavailable) body.data.dates = [];
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/run", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenRunData_"];
    body.data.source = { identity, updated_at: "2026-09-24T07:31:00Z" };
    await route.fulfill({ response, json: body });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.clock.install();
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.locator(".screen-source")).toContainText("选股数据");
  expect(await page.locator("main").innerText()).not.toContain(identity);
  await page.screenshot({ path: testInfo.outputPath("screen-source-desktop.png") });

  await page.clock.fastForward(31_000);
  await page.waitForTimeout(100);
  expect(catalogReads).toBe(1);

  identity = "b".repeat(64);
  await page.getByRole("button", { name: "刷新选股数据" }).click();
  await expect.poll(() => catalogReads).toBe(2);
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expect(page.getByRole("button", { name: "下一页" })).toBeDisabled();
  await expect(page.getByRole("combobox", { name: "条件目录" })).toHaveValue("not_st");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.getByRole("status")).toHaveCount(0);

  unavailable = true;
  await page.getByRole("button", { name: "刷新选股数据" }).click();
  await expect.poll(() => catalogReads).toBe(3);
  await expect(page.getByText("选股数据暂不可用")).toBeVisible();
  await expect(page.getByRole("button", { name: "运行筛选" })).toBeDisabled();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expectNoHorizontalOverflow(page, "screen source unavailable phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(await page.locator("main").innerText()).not.toContain(identity);
  await page.screenshot({ path: testInfo.outputPath("screen-source-phone.png") });
  expect(watcher.problems).toEqual([]);
});

test("默认 Serving 换代重取选股目录，并要求旧结果重新筛选", async ({ page }) => {
  const watcher = watch(page);
  let generationId = "a".repeat(64);
  let catalogReads = 0;
  await page.route("**/api/v1/meta", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_MetaData_"];
    if (body.data.generation) body.data.generation.generation_id = generationId;
    body.serving.generation_id = generationId;
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/blocks", async (route) => {
    catalogReads += 1;
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "serving";
    body.data.source = { identity: generationId, updated_at: "2026-09-24T07:31:00Z" };
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/run", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenRunData_"];
    body.data.source = { identity: generationId, updated_at: "2026-09-24T07:31:00Z" };
    await route.fulfill({ response, json: body });
  });

  await page.clock.install();
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  expect(catalogReads).toBe(1);
  generationId = "b".repeat(64);
  await page.clock.fastForward(31_000);
  await expect.poll(() => catalogReads).toBe(2);
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expect(page.getByRole("button", { name: "下一页" })).toBeDisabled();
  await expect(page.getByRole("combobox", { name: "条件目录" })).toHaveValue("not_st");
  expect(watcher.problems).toEqual([]);
});

test("首次 Serving 数据代到来后，无需手动刷新即可运行选股", async ({ page }) => {
  const watcher = watch(page);
  let generationId: string | null = null;
  let catalogReads = 0;
  await page.route("**/api/v1/meta", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_MetaData_"];
    if (generationId === null) {
      body.data.generation = null;
      body.serving.generation_id = null;
      body.serving.state = "unavailable";
    } else {
      if (body.data.generation) body.data.generation.generation_id = generationId;
      body.serving.generation_id = generationId;
    }
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/blocks", async (route) => {
    catalogReads += 1;
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    if (generationId === null) {
      body.data.available = false;
      body.data.dates = [];
      body.data.source = null;
    } else {
      body.data.source = { identity: generationId, updated_at: "2026-09-24T07:31:00Z" };
    }
    await route.fulfill({ response, json: body });
  });

  await page.clock.install();
  await page.goto("./#/screener");
  await expect(page.getByText("选股数据暂不可用")).toBeVisible();
  await expect(page.getByRole("button", { name: "运行筛选" })).toBeDisabled();
  expect(catalogReads).toBe(1);
  generationId = "a".repeat(64);
  await page.clock.fastForward(31_000);
  await expect.poll(() => catalogReads).toBe(2);
  await expect(page.getByRole("button", { name: "运行筛选" })).toBeEnabled();
  expect(watcher.problems).toEqual([]);
});
