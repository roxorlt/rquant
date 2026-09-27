import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} published pools`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("reads published members with keyboard and fits the page", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/pools");
      await expect(page.getByRole("heading", { level: 1, name: "池子画布" })).toBeVisible();
      const list = page.getByRole("group", { name: "池子列表" });
      await expect(list.getByRole("button")).toHaveCount(2);
      const pool = list.getByRole("button", { name: /N 字一池/ });
      await pool.focus();
      await pool.press("Enter");
      await expect(pool).toHaveAttribute("aria-pressed", "true");
      await expect(page.getByRole("region", { name: "池子详情" })).toContainText("3 只");
      await page.screenshot({
        path: join(tmpdir(), `rquant-pools-${viewport.name}.png`),
        fullPage: true,
      });
      const member = page.getByRole("table", { name: "池子成员" }).locator("tbody tr").first();
      await member.focus();
      await member.press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      await expectNoHorizontalOverflow(page, "published pools");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
