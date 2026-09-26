import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const width of [1440, 390]) {
  test.describe(`告警时间线 ${width}px`, () => {
    test.use({ viewport: { width, height: 844 } });

    test("shows published records and opens a stock from the keyboard", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/monitor");
      await expect(page.getByRole("heading", { level: 1, name: "盯盘与告警" })).toBeVisible();
      const timeline = page.getByRole("list", { name: "告警时间线" });
      await expect(timeline).toBeVisible();
      const entries = timeline.locator(":scope > li");
      expect(await entries.count()).toBeGreaterThan(0);
      if (process.env.RQ_E2E_REPLAY_ROOT) {
        await expect(entries).toHaveCount(3);
        await expect(entries.nth(0)).toContainText("上攻突破");
        await expect(entries.nth(1)).toContainText("爆量");
        await expect(entries.nth(2)).not.toContainText("暂无回执");
        await expect(entries.nth(2).locator('[aria-label="通知回执"]')).toHaveCount(1);
      }
      await expect(page.getByText("可向前翻看历史")).toHaveCount(0);
      await entries.first().locator(".monitor-event-time .tip-anchor").hover();
      await expect(page.getByRole("tooltip")).toContainText("2026-09-24");
      await expect(page.getByRole("button", { name: "新建规则" })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "确认" })).toHaveCount(0);
      await expectNoHorizontalOverflow(page, "monitor");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await test.info().attach(`monitor-${width}px`, {
        body: await page.screenshot({ fullPage: true }),
        contentType: "image/png",
      });

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
