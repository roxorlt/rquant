import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client";
import { dailyReport } from "./datacenter-report.fixture.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

type Dataset = Schemas["AuditReportDataset"];

// Invented UI states. Real counts, source identities and publication are covered
// by the frozen Linux receipts and the actual v2 worker→Serving→API test.
const minute: Dataset = {
  dataset_id: "minute_bar",
  name: "股票分钟线",
  source_id: `sha256:${"a".repeat(64)}`,
  source_kind: "fixed_replica",
  source_state: "ready",
  source_column_present: true,
  scope: "audit_range",
  audit_start: "2026-09-29",
  observed_through: "2026-09-30",
  as_of: "2026-10-01T00:00:00+08:00",
  contract_sha256: "b".repeat(64),
  rule_version: "catalog-dataset-audit-v1",
  visibility: "minute_as_of",
  coverage_state: "measured",
  coverage_reason: "date_presence_only",
  coverage_label: "已统计",
  completeness_state: "missing_expected_scope",
  completeness_label: "完整范围未确认",
  freshness_state: "missing_expected_scope",
  freshness_reason: "session_grid_unknown",
  freshness_label: "缺少应有范围",
  freshness_lag_sessions: null,
  observed_age_seconds: 0,
  latest_visible_date: "2026-09-30",
  latest_visible_time: "2026-09-30T15:00:00",
  latest_ingested_at: "2026-09-30T15:30:00",
  observed_rows: 2,
  visible_rows: 1,
  pending_rows: 1,
  recorded_after_as_of_rows: 1,
  unknown_source_rows: 0,
  unknown_frequency_rows: 0,
  expected_open_days: 2,
  covered_open_days: 1,
  omitted_gap_count: 0,
  omitted_gap_open_days: 0,
  omitted_closed_day_count: 0,
  omitted_closed_day_rows: 0,
  omitted_row_changes: 0,
  monthly: [
    {
      month: "2026-09-01",
      expected_open_days: 2,
      covered_open_days: 1,
      coverage_ratio: "0.5000",
      status: "measured",
    },
  ],
  gaps: [{ start: "2026-09-29", end: "2026-09-29", missing_open_days: 1 }],
  closed_day_rows: [],
  fields: [
    { field_name: "close", name: "收盘价", observed_rows: 2, null_rows: 1, required_key: false },
  ],
  frequencies: [
    { frequency: "1min", row_count: 1, visible_rows: 1 },
    { frequency: "5min", row_count: 1, visible_rows: 0 },
  ],
  row_changes: [{ day: "2026-09-30", row_count: 1, previous_rows: null }],
  rules: [
    {
      rule_id: "freshness",
      name: "更新延迟",
      state: "missing_expected_scope",
      state_label: "缺少应有范围",
      reason: "session_grid_unknown",
      checked_rows: 2,
      issue_count: 0,
      reason_label: "缺少权威交易时段和频率网格，不能用自然时间判断盘中延迟。",
    },
  ],
  conclusion: "not_fully_assessed",
  conclusion_label: "尚未完整检查",
};

const reference: Dataset = {
  ...minute,
  dataset_id: "ths_member",
  name: "同花顺板块成分",
  scope: "current_snapshot",
  visibility: "unknown",
  coverage_state: "not_applicable",
  coverage_reason: "current_snapshot",
  coverage_label: "不适用",
  completeness_state: "not_applicable",
  completeness_label: "不适用",
  freshness_state: "not_evaluated",
  freshness_reason: "visibility_unknown",
  freshness_label: "未评估",
  observed_rows: 0,
  visible_rows: 0,
  pending_rows: 0,
  recorded_after_as_of_rows: 0,
  expected_open_days: null,
  covered_open_days: null,
  latest_visible_date: null,
  latest_visible_time: null,
  latest_ingested_at: null,
  observed_age_seconds: null,
  monthly: [],
  gaps: [],
  fields: [],
  frequencies: [],
  row_changes: [],
  rules: minute.rules.map((rule) => ({
    ...rule,
    state: "not_evaluated",
    state_label: "未评估",
    reason: "no_observations",
    checked_rows: 0,
    reason_label: "没有可检查记录。",
  })),
};

if (dailyReport.overview === null) throw new Error("daily browser fixture requires an overview");

const catalogReport: Schemas["DataAuditReportData"] = {
  ...dailyReport,
  dataset_state: "ready",
  overview: {
    ...dailyReport.overview,
    schema_version: 2,
    audit_start: minute.audit_start,
    observed_through: minute.observed_through,
  },
  datasets: [
    minute,
    reference,
    { ...minute, dataset_id: "auction_bar", name: "集合竞价", frequencies: [] },
    { ...minute, dataset_id: "daily_bar", name: "股票日线", frequencies: [] },
  ],
  progress: {
    availability: "ready",
    latest_task_id: "c".repeat(32),
    latest_status: "failed",
    latest_updated_at: "2026-09-24T07:35:00Z",
    successful_report_hash: "f".repeat(64),
    successful_updated_at: "2026-09-24T07:30:00Z",
    events: [],
  },
};

async function install(page: Page, data = catalogReport) {
  const response = await page.request.get("./api/v1/meta");
  expect(response.ok()).toBe(true);
  const meta: Schemas["Envelope_MetaData_"] = await response.json();
  await page.route("**/api/v1/meta*", (route) => route.fulfill({ json: meta }));
  await page.route("**/api/v1/data/report*", (route) =>
    route.fulfill({ json: { data, serving: meta.serving } }),
  );
}

async function select(page: Page, name: string, width: number) {
  const back = page.getByRole("button", { name: "返回目录" });
  if (width === 390 && (await back.isVisible())) await back.click();
  const item = page.getByRole("button", { name: new RegExp(name) });
  await item.focus();
  await page.keyboard.press("Enter");
}

for (const width of [1440, 390]) {
  test(`目录统计切换与事实边界 ${width}px`, async ({ page }, testInfo) => {
    const observer = watch(page);
    await page.setViewportSize({ width, height: width === 390 ? 844 : 1000 });
    await install(page);
    await page.goto("./#/datacenter");
    await select(page, "股票分钟线", width);
    let panel = page.getByRole("region", { name: "数据质量报告" });
    await expect(panel.getByText("完整范围未确认", { exact: true })).toBeVisible();
    await expect(panel.getByText("尚未确认可见", { exact: true })).toBeVisible();
    await expect(panel.getByText("尚未完整检查", { exact: true })).toBeVisible();
    await expect(panel.getByRole("table", { name: "数据集月度覆盖" })).toContainText("50.0%");
    await expect(panel.getByText("1分钟 · 1 条", { exact: true })).toBeVisible();
    await expect(panel.getByText("5分钟 · 1 条", { exact: true })).toBeVisible();
    await expect(panel.getByText("最近任务未完成", { exact: true })).toBeVisible();
    const help = panel.getByRole("button", { name: "来源与范围说明" });
    await help.focus();
    await expect(page.getByRole("tooltip")).toContainText("2026-10-01");
    await page.keyboard.press("Escape");
    await panel.getByText("行数与空值", { exact: true }).click();
    const rowHelp = panel.getByRole("button", { name: "行数变化说明" });
    await rowHelp.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("tooltip").filter({ hasText: "只比较已有记录" })).toBeVisible();
    await page.keyboard.press("Escape");
    const nullHelp = panel.getByRole("button", { name: "字段空值说明" });
    await nullHelp.focus();
    await expect(
      page.getByRole("tooltip").filter({ hasText: "空值需结合字段含义判断" }),
    ).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(panel.getByRole("table", { name: "数据集行数变化" })).toContainText("—");
    await expect(panel.getByRole("table", { name: "数据集字段空值" })).toContainText("50.0%");
    await expectNoHorizontalOverflow(page, "minute dataset report");
    const visibleText = await page.locator("main").innerText();
    expect(visibleText).not.toContain("catalog-dataset-audit-v1");
    expect(visibleText).not.toContain("sha256:");
    expect(visibleText).not.toContain("盘后延迟");
    expect(visibleText).not.toContain("未知的异常阈值");
    expect(visibleText).not.toContain("合同键与代表字段");
    await page.addStyleTag({ content: ".topbar,.tabbar{visibility:hidden!important}" });
    await panel.screenshot({ path: testInfo.outputPath(`audit-minute-${width}.png`) });
    await select(page, "集合竞价", width);
    await expect(panel.getByText("完整范围未确认", { exact: true })).toBeVisible();
    await expect(panel.getByRole("heading", { name: "实际频率", exact: true })).toHaveCount(0);
    await select(page, "同花顺板块成分", width);
    await expect(panel.getByText("当前快照不检查历史逐日覆盖", { exact: true })).toBeVisible();
    await expect(
      panel.getByText("没有可检查记录，不能据此判断健康", { exact: true }),
    ).toBeVisible();
    await expect(panel.getByText("更新：未评估", { exact: true })).toBeVisible();
    await expectNoHorizontalOverflow(page, "current reference report");
    await select(page, "股票日线", width);
    panel = page.getByRole("region", { name: "日线质量报告" });
    await panel.getByText("日线规则明细", { exact: true }).click();
    await expect(panel.getByRole("table", { name: "质量规则" })).toContainText("零成交量");
    await expectNoHorizontalOverflow(page, "v2 daily details");
    expect(observer.problems).toEqual([]);
  });
}

test.describe("390px 触摸提示", () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });
  test("点击范围提示后仍可切换数据", async ({ page }) => {
    await install(page);
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: /股票分钟线/ }).tap();
    const panel = page.getByRole("region", { name: "数据质量报告" });
    await panel.getByRole("button", { name: "来源与范围说明" }).tap();
    await expect(page.getByRole("tooltip")).toContainText("按所选日期范围统计");
    await panel.getByText("行数与空值", { exact: true }).tap();
    await panel.getByRole("button", { name: "行数变化说明" }).tap();
    await expect(page.getByRole("tooltip").filter({ hasText: "只比较已有记录" })).toBeVisible();
    await panel.getByRole("button", { name: "字段空值说明" }).tap();
    await expect(
      page.getByRole("tooltip").filter({ hasText: "空值需结合字段含义判断" }),
    ).toBeVisible();
    await page.getByRole("button", { name: "返回目录" }).tap();
    await page.getByRole("button", { name: /同花顺板块成分/ }).tap();
    await expect(panel.getByText("当前快照不检查历史逐日覆盖")).toBeVisible();
    await expectNoHorizontalOverflow(page, "touch dataset switch");
  });
});

test("同代读取冲突会撤下旧目录统计", async ({ page }) => {
  await install(page);
  await page.goto("./#/datacenter");
  await select(page, "股票分钟线", 1440);
  const panel = page.getByRole("region", { name: "数据质量报告" });
  await expect(panel.getByText("1分钟 · 1 条", { exact: true })).toBeVisible();
  await page.route("**/api/v1/data/report*", (route) =>
    route.fulfill({ status: 409, json: { detail: "generation_changed" } }),
  );
  await page.reload();
  await select(page, "股票分钟线", 1440);
  await expect(panel.getByText("数据质量报告已更新，请刷新", { exact: true })).toBeVisible();
  await expect(panel.getByText("1分钟 · 1 条", { exact: true })).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "withdrawn report");
});
