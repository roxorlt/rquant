import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const planHash = "a".repeat(64);
const item: Schemas["BackfillPlanItem"] = {
  rank: 0,
  plan_hash: planHash,
  published_at: "2026-09-24T07:30:00Z",
  audit_start: "2024-09-01",
  completed_through: "2025-04-30",
  cutoff_observed_at: "2026-09-24T07:00:00Z",
  missing_day_count: 3,
  estimated_seconds: "2760",
  source_mode: "production_unverified",
  snapshot_label: "private-source-label",
  identity_verified: false,
  collection_complete_verified: false,
  quota_status: "unverified",
  executable: false,
};

const detail: Schemas["BackfillPlanDetail"] = {
  ...item,
  missing_dates: ["2024-09-02", "2024-09-03", "2024-10-08"],
  monthly: [
    { month: "2024-09-01", expected_open_days: 20, covered_open_days: 18, missing_open_days: 2 },
    { month: "2024-10-01", expected_open_days: 19, covered_open_days: 18, missing_open_days: 1 },
  ],
  gap_count: 2,
  coverage_scope: "whole_day_presence_only",
  estimate: {
    estimated_seconds: "2760",
    quota_status: "unverified",
    actual_http_calls_known: false,
    logical_operations: {
      daily: 3,
      daily_basic: 3,
      adj_factor: 3,
      namechange_context_batches: 1,
      namechange_windows: 2,
      stock_st_upper_bound: 3,
      trade_cal: 0,
      total: 13,
    },
    assumptions: {
      adapter_seconds_per_operation: "1",
      market_throttle_seconds_per_operation: "0.5",
      retry_allowance_seconds_per_operation: "0.2",
      status_throttle_seconds_per_operation: "0.5",
      status_namechange_start: "2024-09-01",
      status_source_as_of: "2026-09-24",
      status_window_years: 3,
    },
  },
  source: {
    mode: "production_unverified",
    snapshot_label: "private-source-label",
    claimed_file_sha256: "d".repeat(64),
    identity_verified: false,
    collection_complete_verified: false,
  },
};

async function installPlans(page: Page) {
  const metaResponse = await page.request.get("./api/v1/meta");
  expect(metaResponse.ok()).toBe(true);
  const meta: Schemas["Envelope_MetaData_"] = await metaResponse.json();
  await page.route("**/api/v1/data/backfill-plans?*", (route) =>
    route.fulfill({
      json: {
        data: {
          source_state: "ready",
          total: 1,
          page_size: 20,
          items: [item],
          next_cursor: null,
          progress: {
            availability: "unavailable",
            task_id: null,
            message: "任务进度尚未提供",
            logs: [],
          },
        },
        serving: meta.serving,
      },
    }),
  );
  await page.route(`**/api/v1/data/backfill-plans/${planHash}*`, (route) =>
    route.fulfill({
      json: {
        data: {
          source_state: "ready",
          plan: detail,
          progress: {
            availability: "unavailable",
            task_id: null,
            message: "任务进度尚未提供",
            logs: [],
          },
        },
        serving: meta.serving,
      },
    }),
  );
}

for (const width of [1440, 390]) {
  test(`backfill plan is readable at ${width}px`, async ({ page }) => {
    const observer = watch(page);
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    await installPlans(page);
    await page.goto("./#/datacenter");
    const switcher = page.getByRole("button", { name: "回补计划" });
    await switcher.focus();
    await page.keyboard.press("Enter");
    const preview = page.getByRole("region", { name: "计划详情" });
    await expect(preview.getByText("2024-09-02")).toBeVisible();
    await expect(preview.getByText("2024-09-03")).toBeVisible();
    await expect(preview.getByText("2024-10-08")).toBeVisible();
    await expect(preview.getByText("预计 46 分钟")).toBeVisible();
    await expect(preview.getByText("配额待确认")).toBeVisible();
    await expect(preview.getByText("暂无进度信息")).toBeVisible();
    expect(await page.locator("main").innerText()).not.toContain(planHash);
    await expect(page.getByRole("button", { name: /生成回补计划|执行回补/ })).toHaveCount(0);
    await expectNoHorizontalOverflow(page, "backfill plan");
    await page
      .locator(".dc-plan-layout")
      .screenshot({ path: `/private/tmp/rquant-backfill-plan-${width}.png` });

    const details = preview.getByText("核对信息").locator("..");
    await details.focus();
    await expect(page.getByRole("tooltip")).toContainText(planHash);
    expect(observer.problems).toEqual([]);
  });
}

test.describe("390px touch backfill plan", () => {
  test.use({ viewport: { width: 390, height: 844 }, hasTouch: true, isMobile: true });

  test("tap reveals estimate context", async ({ page }) => {
    await installPlans(page);
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: "回补计划" }).tap();
    const estimate = page.getByText("13 次逻辑操作");
    await expect(estimate).toBeVisible();
    await estimate.locator("..").tap();
    await expect(page.getByRole("tooltip")).toContainText("逻辑操作不等于实际请求次数");
    await expectNoHorizontalOverflow(page, "touch backfill plan");
  });
});
