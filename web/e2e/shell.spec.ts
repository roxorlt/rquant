import { execSync } from "node:child_process";
import { expect, type Page, test } from "@playwright/test";
import { NAV_GROUPS, PAGES } from "../src/app/pages.ts";
import { APP_URL, REPO_ROOT, SERVING_ROOT, UV_RUN } from "./env.ts";

const VIEWPORTS = [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const;

interface PageWatch {
  problems: string[];
}

/**
 * Playwright's trace snapshotter injects a script into every frame; Chrome blocks it
 * in the report's script-less sandbox and logs this. Verified with a bare page:
 * the message appears only while tracing. The report itself carries no script (the
 * test asserts that), so this one message is not an application error.
 */
const TRACE_SANDBOX_NOISE =
  /^Blocked script execution in 'http:\/\/127\.0\.0\.1:\d+\/app\/reports\/[\w.-]+\.html' because the document's frame is sandboxed and the 'allow-scripts' permission is not set\.$/;

/** Collects console errors, uncaught errors, failed and cross-origin requests. */
function watch(page: Page): PageWatch {
  const problems: string[] = [];
  const origin = new URL(APP_URL).origin;
  page.on("console", (message) => {
    if (message.type() === "error" && !TRACE_SANDBOX_NOISE.test(message.text())) {
      problems.push(`console error: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("requestfailed", (request) => problems.push(`request failed: ${request.url()}`));
  page.on("request", (request) => {
    const url = request.url();
    if (!url.startsWith("data:") && !url.startsWith(origin)) {
      problems.push(`cross-origin request: ${url}`);
    }
  });
  page.on("response", (response) => {
    if (response.status() >= 400) {
      problems.push(`HTTP ${response.status()}: ${response.url()}`);
    }
  });
  return { problems };
}

async function expectNoHorizontalOverflow(page: Page, where: string): Promise<void> {
  const overflow = await page.evaluate(() => {
    const root = document.documentElement;
    return { scroll: root.scrollWidth, client: root.clientWidth };
  });
  expect(overflow.scroll, `horizontal overflow on ${where}`).toBeLessThanOrEqual(overflow.client);
}

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
      await expect(page.locator(".gen-tag")).toContainText("正常");
      await expect(page.locator(".gen-tag .mono")).toHaveText(/^[0-9a-f]{8}$/);

      for (const target of PAGES) {
        if (viewport.name === "desktop") {
          await page
            .getByRole("navigation", { name: "主导航" })
            .getByRole("link", { name: target.title })
            .click();
        } else {
          await page.getByRole("button", { name: "打开导航" }).click();
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

test("the longest phase label and a long unbroken serving detail fit a 390 px phone", async ({
  page,
}) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const watcher = watch(page);
  const generationId = "f".repeat(64);
  // Real degraded details are comma-joined ids with no spaces; this one has no hyphen
  // either, so the browser finds no break opportunity of its own.
  const detail = `serving generation degraded: runtime_health:degraded:${Array.from(
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
            built_at: "2026-09-24T06:57:00Z",
            published_at: null,
            previous_generation_id: null,
            producer_commit: "0".repeat(40),
            schema_version: 3,
            age_seconds: 60,
          },
          datasets: [],
          projections: [],
          market: {
            trade_date: "2026-09-24",
            phase: "closing_auction",
            phase_label: "尾盘集合竞价",
            is_trading_day: true,
          },
        },
        serving: {
          generation_id: generationId,
          built_at: "2026-09-24T06:57:00Z",
          state: "degraded",
          detail,
        },
      },
    }),
  );
  await page.goto("./#/overview");
  await expect(page.getByRole("status")).toContainText("运行时数据处于降级状态");
  await expect(page.getByText("尾盘集合竞价")).toBeVisible();
  await expectNoHorizontalOverflow(page, "phone top bar and banner");
  expect(watcher.problems).toEqual([]);
});

test("a newly published generation reaches the page within 20 seconds", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/overview");
  const marker = page.locator(".gen-tag .mono");
  await expect(marker).toHaveText(/^[0-9a-f]{8}$/);
  const before = await marker.textContent();

  const output = execSync(
    `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama --publish-next`,
    { cwd: REPO_ROOT, env: { ...process.env, RQUANT_DISABLE_DOTENV: "1" }, encoding: "utf8" },
  );
  const published = JSON.parse(output.trim().split("\n").pop() ?? "{}") as {
    generation_id: string;
  };
  expect(published.generation_id.slice(0, 8)).not.toBe(before);

  await expect(marker).toHaveText(published.generation_id.slice(0, 8), { timeout: 20_000 });
});
