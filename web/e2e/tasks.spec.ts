import { expect, test } from "@playwright/test";
import { tasksEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} ${viewport.width}×${viewport.height}`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("real published empty queue explains why there are no rows", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/tasks");
      await expect(page.getByRole("heading", { level: 1, name: "任务与调度" })).toBeVisible();
      await expect(page.getByText("还没有研究任务")).toBeVisible();
      await expect(page.getByRole("region", { name: "任务状态概况" })).toContainText("全部0");
      await expectNoHorizontalOverflow(page, "empty research queue");
      expect(watcher.problems).toEqual([]);
    });

    test("a populated queue keeps status and progress usable at this width", async ({ page }) => {
      const watcher = watch(page);
      await page.route("**/api/v1/tasks/jobs**", async (route) => {
        await route.fulfill({ json: tasksEnvelope() });
      });
      await page.goto("./#/tasks");
      const table = page.getByRole("table", { name: "研究任务队列" });
      await expect(table).toBeVisible();
      await expect(table).toContainText("动量参数搜索");
      await expect(table).toContainText("运行中");
      await expect(table).toContainText("25%");
      await expect(table).toContainText("15:35");
      await expect(table.getByRole("progressbar")).toHaveAttribute("value", "0.25");
      if (viewport.name === "phone") {
        await expect(table.getByRole("columnheader", { name: "状态" })).toBeHidden();
        await expect(table.locator(".tasks-mobile-status")).toBeVisible();
      }
      const body = await page.locator("main").innerText();
      expect(findJargon(body)).toEqual([]);
      expect(body).not.toContain("00000000-0000-0000-0000-000000000001");
      await expectNoHorizontalOverflow(page, "research queue");
      expect(watcher.problems).toEqual([]);
    });
  });
}
