import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

test("市场脉搏、板块、成分和三档个股走势联动", async ({ page }, testInfo) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/panorama");
  await expect(page.getByRole("heading", { level: 1, name: "市场全景" })).toBeVisible();
  await expect(page.getByRole("region", { name: "市场脉搏" })).toContainText("涨停");
  await expect(page.getByRole("button", { name: "查看今日脉搏走势" })).toBeVisible();
  await page.getByRole("button", { name: "查看今日脉搏走势" }).click();
  await expect(page.getByRole("img", { name: /当日走势/ })).toHaveCount(4);
  await expect(page.getByText("上涨占比 %")).toBeVisible();
  await page.getByRole("button", { name: "查看今日脉搏走势" }).click();

  const systems = page.getByRole("group", { name: "板块体系" });
  await expect(systems.getByRole("button", { name: "开盘啦题材" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  const boards = page.getByRole("table", { name: "板块" }).locator("tbody tr:not(.pad)");
  await expect(boards.first()).toBeVisible();
  await page
    .getByRole("table", { name: "板块", exact: true })
    .getByRole("button", { name: "涨停", exact: true })
    .click();
  await expect(page.getByRole("columnheader", { name: /涨停/ }).first()).toHaveAttribute(
    "aria-sort",
    "ascending",
  );
  await expect(
    page.getByRole("table", { name: "板块成分" }).locator("tbody tr:not(.pad)").first(),
  ).toBeVisible();
  await expect(page.getByRole("group", { name: "走势周期" })).toBeVisible();
  await page.getByRole("group", { name: "走势周期" }).getByRole("button", { name: "5 日" }).click();
  await expect(page.getByRole("img", { name: /5 日/ })).toBeVisible();
  await page.getByRole("group", { name: "走势周期" }).getByRole("button", { name: "日 K" }).click();
  await expect(page.getByRole("img", { name: /日 K/ })).toBeVisible();

  await systems.getByRole("button", { name: "东财行业" }).click();
  await expect(systems.getByRole("button", { name: "东财行业" })).toHaveAttribute(
    "aria-pressed",
    "true",
  );
  await expect(boards.first()).toBeVisible();
  const pageText = await page.locator("main").innerText();
  expect(findJargon(pageText)).toEqual([]);
  await expectNoHorizontalOverflow(page, "panorama desktop");
  expect(watcher.problems).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("panorama-desktop.png"), fullPage: true });
});

test("爆量台账可切换日期、跨日搜索并查看完整分时", async ({ page }) => {
  const watcher = watch(page);
  await page.goto("./#/panorama?tab=surge");
  await expect(page.getByRole("tab", { name: "爆量记录" })).toHaveAttribute(
    "aria-selected",
    "true",
  );
  const table = page.getByRole("table", { name: "爆量记录" });
  await expect(page.getByText("统计口径：盘中累计放量，仅供观察。")).toBeVisible();
  await page.getByText("查看详情").hover();
  await expect(page.getByRole("tooltip")).toContainText("每只股票只记当天第一次确认");
  await expect(table.locator("tbody tr:not(.pad)").first()).toBeVisible();
  await table.locator("tbody tr:not(.pad)").first().click();
  await expect(page.getByRole("img", { name: /分时/ })).toBeVisible();
  await expect(page.locator(".surge-mark-line")).toHaveCount(2);
  await page.setViewportSize({ width: 1100, height: 900 });
  await expect(page.locator(".surge-mark-line")).toHaveCount(2);

  await page.getByLabel("选择日期").click();
  await page.locator('.ant-picker-cell[title="2026-09-23"]').click();
  await expect(table.locator("tbody tr:not(.pad)").first()).toContainText("600001");

  await page.getByLabel("选择日期").click();
  await expect(page.locator('.ant-picker-cell[title="2026-09-22"]')).not.toHaveClass(/disabled/);
  await page.locator('.ant-picker-cell[title="2026-09-22"]').click();
  await expect(table).toContainText(/09-22.*没有爆量记录/);

  await page.getByRole("searchbox", { name: "按代码或名称搜索爆量记录" }).fill("600001");
  await expect(page.getByText(/跨日找到/)).toBeVisible();
  await expect(page.getByText("统计口径：盘中累计放量，仅供观察。")).toBeVisible();
  await page.getByText("查看详情").hover();
  await expect(page.getByRole("tooltip")).toContainText("每只股票只记当天第一次确认");
  await expect(table.locator("tbody tr:not(.pad)").first()).toContainText("600001");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("手机宽度可以切页和刷新，不出现横向溢出", async ({ page }, testInfo) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("./#/panorama");
  await expect(page.getByRole("heading", { level: 1, name: "市场全景" })).toBeVisible();
  await expect(page.getByRole("table", { name: "板块", exact: true })).toBeVisible();
  const refreshed = page.waitForResponse((response) =>
    response.url().includes("/api/v1/panorama/pulse"),
  );
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await refreshed;
  await page.getByRole("tab", { name: "爆量记录" }).click();
  await expect(page.getByRole("table", { name: "爆量记录" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "panorama phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
  await page.getByRole("tab", { name: "市场全景" }).click();
  await expect(page.getByRole("tab", { name: "市场全景" })).toHaveAttribute(
    "aria-selected",
    "true",
  );
  await expect(page.getByRole("table", { name: "板块", exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("panorama-phone.png") });
});

test("最近异动留在页面上，同一条提示不会重复弹出", async ({ page }) => {
  await page.route("**/api/v1/panorama/pulse", async (route) => {
    const response = await route.fetch();
    const body = await response.json();
    body.data.recent_alert = body.data.alerts[0];
    await route.fulfill({ response, json: body });
  });
  await page.goto("./#/panorama");
  await expect(page.getByRole("status").filter({ hasText: "炸板潮" })).toBeVisible();
  await expect(page.locator(".ant-message-notice")).toHaveCount(1);
  expect(await page.evaluate(() => sessionStorage.getItem("rq.panorama.alerts"))).toContain(
    "2026-09-24",
  );
  expect(await page.evaluate(() => localStorage.getItem("rq.panorama.alerts"))).toBeNull();
  const refreshed = page.waitForResponse((response) =>
    response.url().includes("/api/v1/panorama/pulse"),
  );
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await refreshed;
  await expect(page.getByRole("status").filter({ hasText: "炸板潮" })).toBeVisible();
  await expect(page.locator(".ant-message-notice")).toHaveCount(1);
  await page.unrouteAll({ behavior: "wait" });
});

test("市场数据读取失败后可在各区重试", async ({ page }) => {
  const failing = new Set(["pulse", "boards", "members", "intraday", "daily"]);
  await page.route("**/api/v1/panorama/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    const kind = path.endsWith("/pulse")
      ? "pulse"
      : path.endsWith("/members")
        ? "members"
        : path.endsWith("/intraday")
          ? "intraday"
          : path.endsWith("/daily")
            ? "daily"
            : path.endsWith("/boards")
              ? "boards"
              : "";
    if (failing.has(kind)) {
      await route.fulfill({ status: 500, contentType: "application/json", body: "{}" });
    } else {
      await route.continue();
    }
  });
  await page.goto("./#/panorama");
  const error = (name: string) =>
    page.getByRole("alert").filter({ hasText: `${name}暂时无法加载` });
  await expect(error("市场脉搏")).toBeVisible();
  await expect(error("板块")).toBeVisible();
  failing.delete("pulse");
  await error("市场脉搏").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("region", { name: "市场脉搏" })).toContainText("涨停");
  failing.delete("boards");
  await error("板块").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("table", { name: "板块", exact: true })).toBeVisible();
  await expect(error("板块成分")).toBeVisible();
  failing.delete("members");
  await error("板块成分").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("table", { name: "板块成分" })).toBeVisible();
  await expect(error("分时走势")).toBeVisible();
  failing.delete("intraday");
  await error("分时走势").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("img", { name: /分时/ })).toBeVisible();
  await page.getByRole("group", { name: "走势周期" }).getByRole("button", { name: "日 K" }).click();
  await expect(error("日 K 走势")).toBeVisible();
  failing.delete("daily");
  await error("日 K 走势").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("img", { name: /日 K/ })).toBeVisible();
});

test("爆量日期与搜索失败后可重试", async ({ page }) => {
  const failing = new Set(["surge", "search"]);
  await page.route("**/api/v1/panorama/surge**", async (route) => {
    const kind = new URL(route.request().url()).pathname.endsWith("/search") ? "search" : "surge";
    if (failing.has(kind)) {
      await route.fulfill({ status: 500, contentType: "application/json", body: "{}" });
    } else {
      await route.continue();
    }
  });
  await page.goto("./#/panorama?tab=surge");
  const error = (name: string) =>
    page.getByRole("alert").filter({ hasText: `${name}暂时无法加载` });
  await expect(error("爆量记录")).toBeVisible();
  failing.delete("surge");
  await error("爆量记录").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("table", { name: "爆量记录" })).toBeVisible();
  await page.getByRole("searchbox", { name: "按代码或名称搜索爆量记录" }).fill("600001");
  await expect(error("搜索结果")).toBeVisible();
  failing.delete("search");
  await error("搜索结果").getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("table", { name: "爆量记录" })).toContainText("600001");
});
