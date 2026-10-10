import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { expect, type Page, test } from "@playwright/test";
import { API_NOW } from "./env.ts";

// Review screenshots, written only when RQ_WEB_SCREENSHOT_DIR is set (not baselines:
// those are generated on the CI's Linux, plan v1 §6). Desktop shots are full pages; on a
// phone the first screen is shot as the owner sees it (tab bar included) and the full
// page without the fixed tab bar, which would otherwise float mid-image.
const DIRECTORY = process.env.RQ_WEB_SCREENSHOT_DIR;

test.skip(!DIRECTORY, "set RQ_WEB_SCREENSHOT_DIR to write review screenshots");

// The browser's clock matches the API's, so relative times read as they would have then.
test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

const PAGES = [
  { route: "overview", title: "总览", ready: "table[aria-label='最新信号'] tbody tr" },
  { route: "health", title: "系统健康", ready: "table[aria-label='运行服务'] tbody tr" },
  { route: "datacenter", title: "数据中心", ready: ".dc-dataset" },
] as const;

async function settle(page: Page, selector: string): Promise<void> {
  await page.locator(selector).first().waitFor();
  await page.evaluate(() => document.fonts.ready);
  await page.waitForTimeout(300);
}

for (const width of [1440, 390] as const) {
  for (const scheme of ["light", "dark"] as const) {
    test(`pages ${width} ${scheme}`, async ({ page }) => {
      const directory = DIRECTORY as string;
      mkdirSync(directory, { recursive: true });
      await page.setViewportSize({ width, height: width === 1440 ? 900 : 844 });
      await page.emulateMedia({ colorScheme: scheme, reducedMotion: "reduce" });
      for (const target of PAGES) {
        await page.goto(`./#/${target.route}`);
        await expect(page.getByRole("heading", { level: 1, name: target.title })).toBeVisible();
        await settle(page, target.ready);
        const name = target.route === "datacenter" ? "shell" : target.route;
        if (width === 1440) {
          await page.screenshot({
            path: join(directory, `${name}-${width}-${scheme}.png`),
            fullPage: target.route !== "datacenter",
          });
          continue;
        }
        await page.screenshot({ path: join(directory, `${name}-${width}-${scheme}.png`) });
        if (target.route !== "datacenter") {
          const style = await page.addStyleTag({ content: ".tabbar{display:none!important}" });
          await page.screenshot({
            path: join(directory, `${name}-${width}-${scheme}-full.png`),
            fullPage: true,
          });
          await style.evaluate((node) => (node as Element).remove());
        } else {
          await page.getByRole("button", { name: "更多页面" }).click();
          await expect(page.getByRole("navigation", { name: "页面导航" })).toBeVisible();
          await page.waitForTimeout(400);
          await page.screenshot({ path: join(directory, `shell-390-${scheme}-nav.png`) });
        }
      }
    });
  }
}
