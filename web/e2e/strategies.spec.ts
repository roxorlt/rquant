import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const strategies = [
  {
    strategy_id: "auction_gap",
    name: "集合竞价跳空",
    version: 1,
    registered_at: "2026-09-24T07:00:00Z",
    parameters: [{ key: "min_gap_pct", label: "跳空幅度下限", display_value: "0%" }],
  },
  {
    strategy_id: "growth_board_surge",
    name: "科创及创业板放量",
    version: 1,
    registered_at: "2026-09-24T07:00:00Z",
    parameters: [{ key: "allowed_boards", label: "适用板块", display_value: "创业板、科创板" }],
  },
  {
    strategy_id: "n_shape",
    name: "N 字形态",
    version: 1,
    registered_at: "2026-09-24T07:00:00Z",
    parameters: [{ key: "expires_seconds", label: "信号有效期", display_value: "120 秒" }],
  },
];

test("策略目录的真实字段、键盘选择和手机布局", async ({ page }) => {
  const watcher = watch(page);
  const metaResponse = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const meta = await metaResponse.json();
  await page.route("**/api/v1/strategies*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ data: { available: true, strategies }, serving: meta.serving }),
    }),
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/strategies");
  await expect(page.getByRole("table", { name: "策略列表" })).toBeVisible();
  await expect(
    page.getByRole("table", { name: "当前参数" }).getByText("跳空幅度下限"),
  ).toBeVisible();
  const growth = page.getByRole("table", { name: "策略列表" }).getByRole("row", {
    name: /科创及创业板放量/,
  });
  await growth.focus();
  await page.keyboard.press("Enter");
  await expect(
    page.getByRole("table", { name: "当前参数" }).getByText("创业板、科创板"),
  ).toBeVisible();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expect(page.getByText("年化")).toHaveCount(0);
  await expect(page.getByText("晋级阶段")).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "strategy desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("table", { name: "策略列表" })).toBeVisible();
  await expect(page.getByRole("table", { name: "当前参数" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "strategy phone");
  expect(watcher.problems).toEqual([]);
});

test("策略来源不可用与换代不沿用旧详情", async ({ page }) => {
  const metaResponse = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const meta = await metaResponse.json();
  await page.route("**/api/v1/strategies*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({ data: { available: false, strategies: [] }, serving: meta.serving }),
    }),
  );
  await page.goto("./#/strategies");
  await expect(page.getByText("策略目录暂时不可用")).toBeVisible();
  await expect(page.getByRole("table", { name: "当前参数" })).toHaveCount(0);
  await page.unroute("**/api/v1/strategies*");
  await page.route("**/api/v1/strategies*", (route) =>
    route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        data: { available: true, strategies },
        serving: { ...meta.serving, generation_id: "f".repeat(64) },
      }),
    }),
  );
  await page.reload();
  await expect(page.getByText("数据已更新，请重新查看策略。")).toBeVisible();
  await expect(page.getByRole("table", { name: "当前参数" })).toHaveCount(0);
});
