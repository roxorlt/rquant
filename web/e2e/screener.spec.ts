import { expect, test } from "@playwright/test";
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
