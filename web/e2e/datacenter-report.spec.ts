import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const report: Schemas["DataAuditReportData"] = {
  source_state: "ready",
  overview: {
    report_hash: "f".repeat(64),
    schema_version: 1,
    rule_version: "daily-bar-quality-v1",
    run_status: "completed",
    collection_status: "collection_unconfirmed",
    collection_completed_through: null,
    collection_label: "采集未确认",
    coverage_conclusion: "unconfirmed",
    coverage_label: "覆盖情况待确认",
    quality_conclusion: "issues_observed",
    quality_label: "发现问题",
    current: false,
    source_mode: "production_unverified",
    source_namespace: "production",
    replica_generation_id: null,
    audit_start: "2026-07-01",
    observed_through: "2026-09-30",
    expected_open_days: 66,
    covered_open_days: 64,
    missing_open_days: 2,
    gap_count: 1,
    longest_gap_open_days: 2,
    closed_day_count: 26,
    monthly_count: 3,
    rule_count: 3,
    quality_issue_count: 1,
    indexed_issue_count: 1,
    omitted_issue_count: 0,
    unassessed_rule_days: 70,
  },
  months: [
    {
      month: "2026-07-01",
      expected_open_days: 22,
      covered_open_days: 22,
      coverage_ratio: 1,
      status: "measured",
      status_label: "已统计",
    },
    {
      month: "2026-08-01",
      expected_open_days: 22,
      covered_open_days: 22,
      coverage_ratio: 1,
      status: "measured",
      status_label: "已统计",
    },
    {
      month: "2026-09-01",
      expected_open_days: 22,
      covered_open_days: 20,
      coverage_ratio: 20 / 22,
      status: "measured",
      status_label: "已统计",
    },
  ],
  rules: [
    {
      rule_id: "daily_bar.close_limit",
      name: "收盘价上下限",
      field_name: null,
      field_label: null,
      expected_days: 66,
      checked_days: 64,
      assessed_days: 0,
      unassessed_days: 66,
      first_assessed_date: null,
      last_assessed_date: null,
      assessment_complete: false,
      unassessed_reasons: [
        { reason: "no_daily_bar", name: "缺少日线", days: 2 },
        { reason: "limits_unavailable", name: "涨跌停价未确认", days: 64 },
      ],
      issue_count: 0,
    },
    {
      rule_id: "daily_bar.zero_volume",
      name: "零成交量",
      field_name: null,
      field_label: null,
      expected_days: 66,
      checked_days: 64,
      assessed_days: 64,
      unassessed_days: 2,
      first_assessed_date: "2026-07-01",
      last_assessed_date: "2026-09-30",
      assessment_complete: false,
      unassessed_reasons: [{ reason: "no_daily_bar", name: "缺少日线", days: 2 }],
      issue_count: 1,
    },
    {
      rule_id: "daily_bar.field_null_ratio",
      name: "字段空值比例",
      field_name: "close",
      field_label: "收盘价",
      expected_days: 66,
      checked_days: 64,
      assessed_days: 64,
      unassessed_days: 2,
      first_assessed_date: "2026-07-01",
      last_assessed_date: "2026-09-30",
      assessment_complete: false,
      unassessed_reasons: [{ reason: "no_daily_bar", name: "缺少日线", days: 2 }],
      issue_count: 0,
    },
  ],
  issues: [
    {
      number: 1,
      trade_date: "2026-09-03",
      rule_id: "daily_bar.zero_volume_unsuspended",
      name: "未停牌但零成交量",
      ts_code: "000001.SZ",
      field_name: null,
      field_label: null,
      observed_value: "0",
      reference_value: null,
      null_rows: null,
      observed_rows: null,
    },
  ],
};

for (const width of [1440, 390]) {
  test(`daily-bar report shows only proven facts at ${width}px`, async ({ page }) => {
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
    await panel.screenshot({ path: `/private/tmp/rquant-audit-report-${width}.png` });
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
