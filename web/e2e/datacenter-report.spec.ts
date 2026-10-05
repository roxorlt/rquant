import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client";
import { dailyReport as report } from "./datacenter-report.fixture.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

async function installAuditRun(page: Page) {
  const metaResponse = await page.request.get("./api/v1/meta");
  expect(metaResponse.ok()).toBe(true);
  const meta: Schemas["Envelope_MetaData_"] = await metaResponse.json();
  await page.route("**/api/v1/meta*", (route) =>
    route.fulfill({ json: { ...meta, data: { ...meta.data, viewer: "tester" } } }),
  );
  await page.route("**/api/v1/data/audit-report/calendar*", (route) =>
    route.fulfill({
      json: {
        data: {
          availability: "ready",
          earliest_selectable_date: "2024-09-02",
          latest_closed_date: "2026-09-23",
          open_dates: ["2024-09-02", "2026-09-18", "2026-09-22", "2026-09-23"],
        },
        serving: meta.serving,
      },
    }),
  );
  await page.route("**/api/v1/data/report*", (route) =>
    route.fulfill({
      json: {
        data: {
          source_state: "not_published",
          dataset_state: "not_published",
          datasets: [],
          overview: null,
          months: [],
          rules: [],
          issues: [],
          progress: { availability: "empty", events: [] },
        },
        serving: meta.serving,
      },
    }),
  );
}

for (const width of [1440, 390]) {
  test(`daily-bar report shows only proven facts at ${width}px`, async ({ page }, testInfo) => {
    const observer = watch(page);
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    const metaResponse = await page.request.get("./api/v1/meta");
    expect(metaResponse.ok()).toBe(true);
    const meta: Schemas["Envelope_MetaData_"] = await metaResponse.json();
    await page.route("**/api/v1/meta*", async (route) => {
      await route.fulfill({ json: meta });
    });
    await page.route("**/api/v1/data/report*", async (route) => {
      await route.fulfill({ json: { data: report, serving: meta.serving } });
    });
    await page.goto("./#/datacenter");
    const daily = page.getByRole("button", { name: /股票日线/ });
    await daily.focus();
    await page.keyboard.press("Enter");
    const panel = page.getByRole("region", { name: "日线质量报告" });
    await expect(panel.getByText("采集未确认")).toBeVisible();
    await expect(panel.getByRole("img", { name: /按月覆盖率/ })).toBeVisible();
    await expect(panel.getByRole("table", { name: "质量规则" })).toContainText(
      "涨跌停价未确认 64 天",
    );
    await expect(panel.getByRole("table", { name: "日线质量问题" })).toContainText(
      "未停牌但零成交量",
    );
    if (width === 1440) {
      const rule = panel
        .getByRole("table", { name: "质量规则" })
        .getByRole("row", { name: /零成交量/ });
      await rule.locator("td").nth(1).locator(".tip-anchor").focus();
      await expect(page.getByRole("tooltip")).toContainText("已评估日期：2026-07-01 至 2026-09-30");
    }
    await expectNoHorizontalOverflow(page, "daily-bar report");
    if (width === 390) {
      const localOverflow = await page.evaluate(() =>
        Array.from(
          document.querySelectorAll(
            ".dc-report-rule-table .tbl-wrap, .dc-report-issue-table .tbl-wrap",
          ),
        ).map((node) => node.scrollWidth - node.clientWidth),
      );
      expect(localOverflow).toEqual([0, 0]);
      await expect(panel.locator(".dc-report-mobile-code")).toBeVisible();
    }

    const history = page.getByRole("button", { name: "查看历史审计记录" });
    await history.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("button", { name: "收起历史审计记录" })).toHaveAttribute(
      "aria-expanded",
      "true",
    );
    await expectNoHorizontalOverflow(page, "expanded history");
    await page.addStyleTag({ content: ".topbar,.tabbar{visibility:hidden!important}" });
    await panel.screenshot({ path: testInfo.outputPath(`rquant-audit-report-${width}.png`) });
    expect(observer.problems).toEqual([]);
  });
}

test.describe("390px touch report", () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

  test("tapping a rule shows its assessed date range", async ({ page }) => {
    const metaResponse = await page.request.get("./api/v1/meta");
    expect(metaResponse.ok()).toBe(true);
    const meta: Schemas["Envelope_MetaData_"] = await metaResponse.json();
    await page.route("**/api/v1/meta*", async (route) => {
      await route.fulfill({ json: meta });
    });
    await page.route("**/api/v1/data/report*", async (route) => {
      await route.fulfill({ json: { data: report, serving: meta.serving } });
    });
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: /股票日线/ }).click();

    const rule = page
      .getByRole("table", { name: "质量规则" })
      .getByRole("row", { name: /零成交量/ });
    const mobileProgress = rule.getByText("已评估 64 / 66 天");
    await expect(mobileProgress).toBeVisible();
    await mobileProgress.tap();
    await expect(page.getByRole("tooltip")).toContainText("已评估日期：2026-07-01 至 2026-09-30");
  });

  test("external keyboard opens rule details with Enter and Space", async ({ page }) => {
    const metaResponse = await page.request.get("./api/v1/meta");
    expect(metaResponse.ok()).toBe(true);
    const meta: Schemas["Envelope_MetaData_"] = await metaResponse.json();
    await page.route("**/api/v1/meta*", async (route) => {
      await route.fulfill({ json: meta });
    });
    await page.route("**/api/v1/data/report*", async (route) => {
      await route.fulfill({ json: { data: report, serving: meta.serving } });
    });
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: /股票日线/ }).click();

    const rules = page.getByRole("table", { name: "质量规则" });
    const assessed = rules.getByRole("row", { name: /零成交量/ }).getByText("已评估 64 / 66 天");
    await assessed.locator("..").focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("tooltip")).toContainText("已评估日期：2026-07-01 至 2026-09-30");

    const unassessed = rules
      .getByRole("row", { name: /收盘价上下限/ })
      .getByText("已评估 0 / 66 天");
    await unassessed.locator("..").focus();
    await page.keyboard.press("Space");
    await expect(page.getByRole("tooltip").filter({ hasText: "尚无已评估日期" })).toBeVisible();
  });
});

for (const width of [1440, 390]) {
  test(`read-only audit command works at ${width}px and stays recorded after reload`, async ({
    page,
  }, testInfo) => {
    const observer = watch(page);
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await installAuditRun(page);
    const bodies: Schemas["AuditReportCommandRequest"][] = [];
    await page.route("**/api/v1/data/audit-report/commands", async (route) => {
      expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
      const body = route.request().postDataJSON() as Schemas["AuditReportCommandRequest"];
      bodies.push(body);
      await route.fulfill({
        json: {
          command_id: body.command_id,
          task_id: "a".repeat(32),
          status: "queued",
          message: "已排队",
        },
      });
    });
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: /股票日线/ }).click();
    const panel = page.getByRole("region", { name: "日线质量报告" });
    await expect(panel.getByLabel("结束日期")).toHaveValue("2026-09-23");
    await expect(panel.getByText("还没有审计任务，选择日期后运行。")).toBeVisible();
    const run = panel.getByRole("button", { name: "运行数据审计" });
    await expect(run).toBeEnabled();
    await run.focus();
    await page.keyboard.press("Enter");
    const dialog = page.getByRole("dialog", { name: "运行数据审计" });
    await expect(dialog).toContainText("只读核对");
    await expect(dialog).toContainText("全部目录数据");
    await dialog.getByRole("button", { name: "确认排队" }).focus();
    await page.keyboard.press("Enter");
    await expect(panel.getByText("本次请求已排队")).toBeVisible();
    await expect(dialog).toBeHidden();
    expect(bodies).toHaveLength(1);
    expect(bodies[0]?.observed_through).toBe("2026-09-23");
    await expectNoHorizontalOverflow(page, "audit command");
    await panel.screenshot({ path: testInfo.outputPath(`rquant-audit-run-${width}.png`) });
    await page.reload();
    if (width === 390) await page.getByRole("button", { name: /股票日线/ }).click();
    await expect(panel.getByText("本次请求已排队")).toBeVisible();
    expect(bodies).toHaveLength(1);
    expect(observer.problems).toEqual([]);
  });
}

test.describe("390px touch audit run", () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

  test("tapping the date control and confirmation works", async ({ page }) => {
    await installAuditRun(page);
    await page.route("**/api/v1/data/audit-report/commands", async (route) => {
      const body = route.request().postDataJSON() as Schemas["AuditReportCommandRequest"];
      await route.fulfill({
        json: {
          command_id: body.command_id,
          task_id: "a".repeat(32),
          status: "queued",
          message: "已排队",
        },
      });
    });
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: /股票日线/ }).tap();
    const panel = page.getByRole("region", { name: "日线质量报告" });
    await panel.getByRole("button", { name: "运行数据审计" }).tap();
    await page
      .getByRole("dialog", { name: "运行数据审计" })
      .getByRole("button", { name: "确认排队" })
      .tap();
    await expect(panel.getByText("本次请求已排队")).toBeVisible();
    await expectNoHorizontalOverflow(page, "touch audit command");
  });
});

test("lost audit submit response is retried with the original request after reload", async ({
  page,
}) => {
  await installAuditRun(page);
  const bodies: Schemas["AuditReportCommandRequest"][] = [];
  await page.route("**/api/v1/data/audit-report/commands", async (route) => {
    const body = route.request().postDataJSON() as Schemas["AuditReportCommandRequest"];
    bodies.push(body);
    if (bodies.length === 1) {
      await route.fulfill({ status: 503, json: { detail: "unavailable" } });
    } else {
      await route.fulfill({
        json: {
          command_id: body.command_id,
          task_id: "a".repeat(32),
          status: "queued",
          message: "已排队",
        },
      });
    }
  });
  await page.goto("./#/datacenter");
  await page.getByRole("button", { name: /股票日线/ }).click();
  const panel = page.getByRole("region", { name: "日线质量报告" });
  await panel.getByRole("button", { name: "运行数据审计" }).click();
  await page
    .getByRole("dialog", { name: "运行数据审计" })
    .getByRole("button", { name: "确认排队" })
    .click();
  await expect(panel.getByText("本次提交状态待确认")).toBeVisible();
  await page.reload();
  await expect(panel.getByText("本次提交状态待确认")).toBeVisible();
  expect(bodies).toHaveLength(1);
  await panel.getByRole("button", { name: "继续核对" }).click();
  await expect(panel.getByText("本次请求已排队")).toBeVisible();
  expect(bodies).toHaveLength(2);
  expect(bodies[1]).toEqual(bodies[0]);
});
