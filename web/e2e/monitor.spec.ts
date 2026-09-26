import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const width of [1440, 390]) {
  test.describe(`最近信号 ${width}px`, () => {
    test.use({ viewport: { width, height: 844 } });

    test("shows published records and opens a stock from the keyboard", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/monitor");
      await expect(page.getByRole("heading", { level: 1, name: "盯盘与告警" })).toBeVisible();
      const timeline = page.getByRole("list", { name: "最近信号" });
      await expect(timeline).toBeVisible();
      const entries = timeline.locator(":scope > li");
      expect(await entries.count()).toBeGreaterThan(0);
      await entries.first().locator(".monitor-event-time .tip-anchor").hover();
      await expect(page.getByRole("tooltip")).toContainText("2026-09-24");
      await expect(page.getByRole("button", { name: "新建规则" })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "确认" })).toHaveCount(0);
      await expectNoHorizontalOverflow(page, "monitor");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);

      const stock = entries.first().getByRole("button", { name: /查看.+详情/ });
      await stock.focus();
      await expect(stock).toBeFocused();
      await stock.press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      await expectNoHorizontalOverflow(page, "monitor stock detail");
      expect(watcher.problems).toEqual([]);
    });
  });
}
