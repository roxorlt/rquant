import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test("实验记录可跨页对比真实结果，桌面与手机可读", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(`${APP_URL}api/v1/meta`);
  expect(meta.ok()).toBeTruthy();
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/experiments*", async (route) => {
    const more = new URL(route.request().url()).searchParams.has("cursor");
    await route.fulfill({
      json: {
        serving,
        data: {
          available: true,
          items: [
            {
              experiment_id: (more ? "b" : "a").repeat(64),
              hypothesis_family: more ? "突破研究" : "均线研究",
              registered_at: more ? "2026-09-23T07:20:00Z" : "2026-09-24T07:20:00Z",
              status: more ? "registered" : "succeeded",
              completed_at: more ? null : "2026-09-24T07:25:00Z",
              trade_count: more ? null : 12,
              net_return_pct: more ? null : 7.5,
              max_drawdown_pct: more ? null : 3.25,
              win_rate_pct: more ? null : 60,
            },
          ],
          retained_count: 500,
          truncated: true,
          oldest_registered_at: "2026-09-23T07:20:00Z",
          next_cursor: more ? null : "opaque-next",
        },
      },
    });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/experiments");
  await expect(page.getByRole("heading", { level: 1, name: "实验记录" })).toBeVisible();
  const table = page.getByRole("table", { name: "实验记录" });
  await expect(table.getByText("均线研究")).toBeVisible();
  await expect(table.getByText("+7.50%")).toBeVisible();
  await expect(page.locator(".exp-window")).toContainText(
    "仅显示最近 500 条实验 · 最早登记于 2026-09-23 15:20（北京时间）",
  );
  await expect(page.getByText("仅展示已发布的结果")).toBeVisible();
  await expect(page.getByRole("button", { name: "新建实验" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "对比所选" })).toBeDisabled();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "experiment desktop");

  await table.getByRole("checkbox", { name: "选择均线研究" }).focus();
  await page.keyboard.press("Space");
  await page.getByRole("button", { name: "下一页" }).focus();
  await page.keyboard.press("Enter");
  await expect(table.getByText("突破研究")).toBeVisible();
  await table.getByRole("checkbox", { name: "选择突破研究" }).check();
  await page.getByRole("button", { name: "对比所选" }).click();
  const comparison = page.getByRole("region", { name: "实验对比" });
  await expect(comparison.getByText("均线研究")).toBeVisible();
  await expect(comparison.getByText("突破研究")).toBeVisible();
  await expect(comparison.getByText("+7.50%")).toBeVisible();
  await expect(comparison.getByText(/暂不计算差值/)).toBeVisible();
  await page.setViewportSize({ width: 390, height: 844 });
  await expectNoHorizontalOverflow(page, "experiment phone");
  await page.getByRole("button", { name: "上一页" }).click();
  await expect(table.getByText("均线研究")).toBeVisible();
  await expect(comparison).toBeVisible();
  await page.getByRole("button", { name: "移除突破研究" }).click();
  await expect(comparison).toHaveCount(0);
  await expect(page.getByRole("button", { name: "对比所选" })).toBeDisabled();
  expect(watcher.problems).toEqual([]);
});
