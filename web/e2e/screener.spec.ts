import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

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
