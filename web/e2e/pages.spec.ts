import { expect, type Page, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW, REPLAY_ROOT } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

// 总览 and 系统健康 over real API responses: the synthetic generation in CI, or a replay
// copy of production (RQ_E2E_REPLAY_ROOT) locally. The API clock is pinned (env.ts).
const EXPECTED = REPLAY_ROOT
  ? { signals: 6, deliveries: 6, holdings: 2, firstStock: "天威视讯", services: 24 }
  : { signals: 2, deliveries: 2, holdings: 2, firstStock: "样本01", services: 8 };

// The browser's clock matches the API's, so relative times read as they would have then.
test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

const VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const;

async function pageText(page: Page): Promise<string> {
  return page.evaluate(() => document.querySelector("main")?.innerText ?? "");
}

async function expectNoJargon(page: Page, where: string): Promise<void> {
  const topbar = await page.locator(".topbar").innerText();
  expect(findJargon(`${topbar}\n${await pageText(page)}`), `internal wording on ${where}`).toEqual(
    [],
  );
}

function bodyRows(page: Page, table: string) {
  return page.getByRole("table", { name: table }).locator("tbody tr:not(.pad)");
}

for (const viewport of VIEWPORTS) {
  test.describe(`${viewport.name} ${viewport.width}×${viewport.height}`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("总览 shows the real rows with no errors, overflow or internal wording", async ({
      page,
    }) => {
      const watcher = watch(page);
      await page.goto("./#/overview");
      await expect(page.getByRole("heading", { level: 1, name: "总览" })).toBeVisible();
      await expect(bodyRows(page, "最新信号")).toHaveCount(EXPECTED.signals);
      await expect(bodyRows(page, "最新信号").first()).toContainText(EXPECTED.firstStock);
      await expect(bodyRows(page, "模拟盘持仓")).toHaveCount(EXPECTED.holdings);
      const deliveries = page.locator('[data-kpi="deliveries"] .val');
      await expect(deliveries).toContainText(String(EXPECTED.deliveries));
      await expect(page.getByRole("list", { name: "今日链路" }).locator("li")).toHaveCount(5);
      if (viewport.name === "phone") {
        await expect(page.getByRole("navigation", { name: "常用页面" })).toBeVisible();
      }
      await expectNoHorizontalOverflow(page, "overview");
      await expectNoJargon(page, "overview");
      expect(watcher.problems).toEqual([]);
    });

    test("系统健康 lists every service in plain words with a detail drawer", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/health");
      await expect(page.getByRole("heading", { level: 1, name: "系统健康" })).toBeVisible();
      await expect(bodyRows(page, "运行服务")).toHaveCount(EXPECTED.services);
      await expect(bodyRows(page, "数据新鲜度").first()).toBeVisible();
      await expectNoHorizontalOverflow(page, "health");
      await expectNoJargon(page, "health");

      await page.getByRole("button", { name: "只看异常" }).click();
      const attention = await bodyRows(page, "运行服务").count();
      expect(attention).toBeLessThan(EXPECTED.services);
      await page.getByRole("button", { name: "全部", exact: true }).click();

      await bodyRows(page, "运行服务").first().click();
      const drawer = page.getByRole("dialog");
      await expect(drawer).toContainText("技术名称");
      await expect(drawer.locator("dd.mono").first()).toHaveText(/\.v\d+$/);
      await expectNoHorizontalOverflow(page, "health drawer");
      expect(watcher.problems).toEqual([]);
    });
  });
}

test("tooltips carry the detail: the data chip and a service id", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/health");
  await expect(bodyRows(page, "运行服务").first()).toBeVisible();
  await page.locator(".gen-tag").hover();
  await expect(page.getByRole("tooltip")).toContainText("数据版本");
  await page.mouse.move(0, 0);
  await bodyRows(page, "运行服务").first().locator(".tip-anchor").first().hover();
  await expect(page.getByRole("tooltip").last()).toHaveText(/\.v\d+$/);
});
