import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const definitions = [
  {
    factor_id: "price_volume_factor",
    content_sha256: "a".repeat(64),
    name_zh: "价量动量",
    category_label: "技术",
    direction: "higher_is_better",
    direction_label: "偏好高值",
    version: 2,
    earliest_available_date: "2024-01-02",
    archived: false,
    expression: "ts_mean(close, 5) / ref(volume, 2)",
    dependency_columns: ["close", "volume"],
    max_history_window: 5,
  },
  {
    factor_id: "old_factor",
    content_sha256: "b".repeat(64),
    name_zh: "成交变化",
    category_label: "技术",
    direction: "lower_is_better",
    direction_label: "偏好低值",
    version: 1,
    earliest_available_date: "2025-03-04",
    archived: true,
    expression: "ts_mean(volume, 3)",
    dependency_columns: ["volume"],
    max_history_window: 3,
  },
];

test("因子库桌面和手机列表详情可读、键盘选择且无横向溢出", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:31:00Z", definitions },
        serving,
      },
    }),
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/factors");
  const list = page.getByRole("table", { name: "因子列表" });
  await expect(list).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toContainText("价量动量");
  await page.screenshot({ path: "test-results/factors-desktop.png", fullPage: true });
  const archived = list.getByRole("row", { name: /成交变化/ });
  await archived.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("region", { name: "因子详情" })).toContainText("已归档");
  await expect(page.getByText("ts_mean(volume, 3)")).toBeVisible();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expect(page.getByRole("button", { name: /运行检验|加入跟踪/ })).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "factor desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(list).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "factor phone");
  await page.screenshot({ path: "test-results/factors-phone.png", fullPage: true });
  expect(watcher.problems).toEqual([]);
});

test("因子库可信空状态与换代错误不展示旧详情", async ({ page }) => {
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "empty", available_at: "2026-09-24T07:31:00Z", definitions: [] },
        serving,
      },
    }),
  );
  await page.goto("./#/factors");
  await expect(page.getByText("还没有因子")).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toHaveCount(0);
  await page.unroute("**/api/v1/factors/definitions*");
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:31:00Z", definitions },
        serving: { ...serving, generation_id: "f".repeat(64) },
      },
    }),
  );
  await page.reload();
  await expect(page.getByText("数据已更新，请重新查看因子。")).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toHaveCount(0);
});
