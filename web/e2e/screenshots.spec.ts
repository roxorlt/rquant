import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { expect, test } from "@playwright/test";

// Shell screenshots for review, written only when RQ_WEB_SCREENSHOT_DIR is set
// (not baselines: those are generated on the CI's Linux, plan v1 §6).
const DIRECTORY = process.env.RQ_WEB_SCREENSHOT_DIR;

test.skip(!DIRECTORY, "set RQ_WEB_SCREENSHOT_DIR to write review screenshots");

for (const width of [1440, 390] as const) {
  for (const scheme of ["light", "dark"] as const) {
    test(`shell ${width} ${scheme}`, async ({ page }) => {
      mkdirSync(DIRECTORY as string, { recursive: true });
      await page.setViewportSize({ width, height: width === 1440 ? 900 : 844 });
      await page.emulateMedia({ colorScheme: scheme, reducedMotion: "reduce" });
      await page.goto("./#/overview");
      await expect(page.getByRole("heading", { level: 1, name: "总览" })).toBeVisible();
      await expect(page.locator(".gen-tag")).toContainText("正常");
      await page.evaluate(() => document.fonts.ready);
      await page.screenshot({ path: join(DIRECTORY as string, `shell-${width}-${scheme}.png`) });
      if (width === 390) {
        await page.getByRole("button", { name: "打开导航" }).click();
        await expect(page.getByRole("navigation", { name: "页面导航" })).toBeVisible();
        await page.waitForTimeout(400);
        await page.screenshot({ path: join(DIRECTORY as string, `shell-390-${scheme}-nav.png`) });
      }
    });
  }
}
