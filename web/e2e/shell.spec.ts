import { execSync } from "node:child_process";
import { expect, type Page, test } from "@playwright/test";
import { NAV_GROUPS, PAGES } from "../src/app/pages.ts";
import { REPLAY_ROOT, REPO_ROOT, SERVING_ROOT, UV_RUN } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const;

async function expectPage(page: Page, path: string, title: string): Promise<void> {
  await expect(page.getByRole("heading", { level: 1, name: title })).toBeVisible();
  await expect(page).toHaveURL(new RegExp(`#${path}$`));
  await expectNoHorizontalOverflow(page, path);
}

for (const viewport of VIEWPORTS) {
  test.describe(`${viewport.name} ${viewport.width}×${viewport.height}`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("loads under /app/ and reaches every page with no errors or overflow", async ({
      page,
    }) => {
      const watcher = watch(page);
      await page.goto("./");
      await expectPage(page, "/overview", "总览");
      await expect(page.locator(".gen-tag")).toHaveAttribute("data-state", "ready");
      await expect(page.locator(".gen-tag")).toContainText(/^数据 (刚刚|\d+ 分钟前)更新$/);

      for (const target of PAGES) {
        if (viewport.name === "desktop") {
          await page
            .getByRole("navigation", { name: "主导航" })
            .getByRole("link", { name: target.title })
            .click();
        } else {
          await page.getByRole("button", { name: "更多页面" }).click();
          const sheet = page.getByRole("navigation", { name: "页面导航" });
          await expect(sheet.getByRole("group")).toHaveCount(NAV_GROUPS.length);
          await sheet.getByRole("link", { name: target.title }).click();
          await expect(sheet).toBeHidden();
        }
        await expectPage(page, target.path, target.title);
      }

      for (const [path, title] of [
        ["/reports", "报告"],
        ["/reports/gap-status", "差距总览"],
        ["/reports/2026-09-24-quant-platform-research", "量化投研平台调研，以及 rQuant 还差什么"],
        ["/licenses", "开源许可"],
      ] as const) {
        const snapshot = path.startsWith("/reports/2026")
          ? page.waitForResponse(
              (response) =>
                response.url().endsWith(".html") && response.url().includes("/app/reports/"),
            )
          : null;
        await page.goto(`./#${path}`);
        await expectPage(page, path, title);
        if (snapshot) {
          // The snapshot loads into the sandboxed frame (no scripts, so the test does not
          // evaluate inside it) from the same origin, as UTF-8 HTML.
          const response = await snapshot;
          expect(response.status()).toBe(200);
          expect(response.headers()["content-type"]).toBe("text/html; charset=utf-8");
          expect(await response.text()).not.toMatch(/<script|javascript:|\son[a-z]+=/i);
          await expect(page.locator("iframe.report-frame")).toBeVisible();
        }
      }

      expect(watcher.problems).toEqual([]);
    });
  });
}

test("the rail collapses and the theme choice survives a reload", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  const watcher = watch(page);
  await page.goto("./#/health");
  await expect(page.getByRole("heading", { level: 1, name: "系统健康" })).toBeVisible();

  await page.getByRole("button", { name: "主题：跟随系统，点击切换" }).click();
  await page.getByRole("button", { name: "主题：浅色，点击切换" }).click();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await page.getByRole("button", { name: "收起导航" }).click();
  await expect(page.locator(".app")).toHaveClass(/rail-min/);

  await page.reload();
  await expect(page.getByRole("heading", { level: 1, name: "系统健康" })).toBeVisible();
  await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
  await expect(page.locator(".app")).toHaveClass(/rail-min/);
  const surface = await page.evaluate(() =>
    getComputedStyle(document.body).getPropertyValue("background-color"),
  );
  expect(surface).toBe("rgb(14, 17, 22)");
  expect(watcher.problems).toEqual([]);
});

test("the longest phase label and a stale-data banner fit a 390 px phone", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const watcher = watch(page);
  const generationId = "f".repeat(64);
  // Real details are long comma-joined ids with no spaces; they stay in the tooltip.
  const detail = `serving generation stale: ${Array.from(
    { length: 12 },
    (_, index) => `missing:service_${index}.source.v1`,
  ).join(",")}`;
  await page.route("**/app/api/v1/meta", (route) =>
    route.fulfill({
      json: {
        data: {
          server_time: "2026-09-24T06:58:00Z",
          viewer: "e2e",
          generation: {
            generation_id: generationId,
            built_at: "2026-09-24T06:40:00Z",
            published_at: null,
            previous_generation_id: null,
            producer_commit: "0".repeat(40),
            schema_version: 3,
            age_seconds: 1080,
          },
          datasets: [],
          projections: [],
          market: {
            trade_date: "2026-09-24",
            phase: "closing_auction",
            phase_label: "尾盘集合竞价",
            is_trading_day: true,
            previous_trading_day: "2026-09-23",
            next_trading_day: "2026-09-28",
          },
        },
        serving: {
          generation_id: generationId,
          built_at: "2026-09-24T06:40:00Z",
          age_seconds: 1080,
          state: "stale",
          message: "数据已 18 分钟没有更新，页面上的数字可能不是最新的。",
          detail,
        },
      },
    }),
  );
  await page.goto("./#/datacenter");
  const banner = page.locator(".banner");
  await expect(banner).toContainText("数据已 18 分钟没有更新");
  await expect(banner).not.toContainText("service_0");
  await expect(page.getByText("尾盘集合竞价")).toBeVisible();
  await expectNoHorizontalOverflow(page, "phone top bar and banner");
  expect(watcher.problems).toEqual([]);
});

test("a newly published generation reaches the page within 20 seconds", async ({ page }) => {
  test.skip(Boolean(REPLAY_ROOT), "the replay copy is read-only; nothing is published into it");
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/overview");
  const marker = page.locator(".gen-tag");
  await expect(marker).toHaveAttribute("data-generation", /^[0-9a-f]{12}$/);
  const before = await marker.getAttribute("data-generation");

  const output = execSync(
    `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama --publish-next`,
    { cwd: REPO_ROOT, env: { ...process.env, RQUANT_DISABLE_DOTENV: "1" }, encoding: "utf8" },
  );
  const published = JSON.parse(output.trim().split("\n").pop() ?? "{}") as {
    generation_id: string;
  };
  expect(published.generation_id.slice(0, 12)).not.toBe(before);

  await expect(marker).toHaveAttribute("data-generation", published.generation_id.slice(0, 12), {
    timeout: 20_000,
  });
});
