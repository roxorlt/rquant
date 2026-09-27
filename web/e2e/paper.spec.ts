import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW, REPLAY_ROOT } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} paper accounts`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("shows published money and switches accounts with keyboard", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/paper");
      await expect(page.getByRole("heading", { level: 1, name: "模拟盘" })).toBeVisible();
      const table = page.getByRole("table", { name: "模拟账户持仓" });
      await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(2);
      await expect(page.getByRole("region", { name: "账户资产" })).toContainText("100,042.00");
      await expect(table).toContainText("可卖 100");
      if (viewport.name === "phone") {
        await expect(table.locator("tbody tr").first().locator(".paper-mobile-pnl")).toBeVisible();
      }
      await expectNoHorizontalOverflow(page, "paper account");
      expect(
        findJargon(await page.locator("main").innerText()),
        "paper page internal wording",
      ).toEqual([]);

      if (!REPLAY_ROOT) {
        const choices = page.getByRole("group", { name: "选择模拟账户" });
        await expect(choices.getByRole("button")).toHaveCount(2);
        const second = choices.getByRole("button", { name: "模拟账户 2" });
        await second.focus();
        await second.press("Enter");
        await expect(second).toHaveAttribute("aria-pressed", "true");
        await expect(page.getByRole("region", { name: "账户资产" })).toContainText("5,000.00");
        await expect(page.getByText("当前账户没有持仓")).toBeVisible();
        await expect(page.getByRole("table", { name: "模拟账户持仓" })).toHaveCount(0);
        await expectNoHorizontalOverflow(page, "cash-only account");
      }
      expect(watcher.problems).toEqual([]);
    });
  });
}
