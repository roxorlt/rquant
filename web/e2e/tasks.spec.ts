import { expect, test } from "@playwright/test";
import type { components } from "../src/api/schema";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const JOBS_ENVELOPE: components["schemas"]["Envelope_ResearchJobsData_"] = {
  serving: {
    generation_id: "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90",
    built_at: "2026-09-24T07:31:00Z",
    age_seconds: 40,
    state: "ready",
    message: null,
    detail: "serving generation verified",
  },
  data: {
    source_state: "ready",
    source_label: "研究任务",
    source_note: null,
    source_updated_at: "2026-09-24T07:30:40Z",
    total: 1,
    counts: {
      queued: 0,
      running: 1,
      checkpointed: 0,
      succeeded: 0,
      failed: 0,
      cancelled: 0,
      other: 0,
    },
    page_size: 20,
    next_cursor: null,
    items: [
      {
        job_id: "00000000-0000-0000-0000-000000000001",
        strategy_name: "动量参数搜索",
        job_type_label: "参数搜索",
        resource_label: "标准",
        status: { state: "ok", label: "运行中", reason: "任务正在运行" },
        progress_fraction: 0.25,
        terminal_shards: 1,
        total_shards: 4,
        eta_at: "2026-09-24T07:35:00Z",
        eta_low: "2026-09-24T07:33:00Z",
        eta_high: "2026-09-24T07:37:00Z",
        eta_label: "预计结束",
        updated_at: "2026-09-24T07:30:00Z",
      },
    ],
  },
};

const OVERVIEW_ENVELOPE: components["schemas"]["Envelope_TaskOverviewData_"] = {
  serving: JOBS_ENVELOPE.serving,
  data: {
    can_view_research_logs: false,
    scheduled: {
      source_state: "ready",
      source_label: "定时任务",
      source_note: null,
      source_updated_at: "2026-09-24T07:35:00Z",
      expires_at: "2026-09-24T07:37:00Z",
      remaining_seconds: 60,
      items: Array.from({ length: 16 }, (_, index) => ({
        name: index === 0 ? "日线更新" : `定时任务 ${index + 1}`,
        status: { state: "ok", label: "正常", reason: "等待下次触发" },
        last_trigger_at: "2026-09-24T07:30:00Z",
        next_at: "2026-09-25T01:30:00Z",
        duration_seconds: null,
        result_label: "未知",
        timer_unit: `rquant-sample-${index}.timer`,
        service_unit: `rquant-sample-${index}.service`,
      })),
    },
    services: {
      source_state: "ready",
      source_label: "运行服务",
      source_note: null,
      source_updated_at: "2026-09-24T07:35:00Z",
      items: [
        {
          name: "通知推送",
          plane_label: "实时",
          status: { state: "ok", label: "正常", reason: "心跳正常" },
          heartbeat_at: "2026-09-24T07:35:00Z",
          service_id: "notifier.admin.shadow.v1",
        },
      ],
    },
    resources: {
      source_state: "ready",
      source_label: "资源使用",
      source_note: null,
      source_updated_at: "2026-09-24T07:35:00Z",
      expires_at: "2026-09-24T07:37:00Z",
      remaining_seconds: 60,
      host_memory_total_bytes: 8_589_934_592,
      host_memory_available_bytes: 3_221_225_472,
      rquant_memory_current_bytes: 1_073_741_824,
      rquant_memory_peak_bytes: 2_147_483_648,
      groups: [
        {
          name: "实时服务",
          slice_unit: "rquant-live.slice",
          memory_current_bytes: 536_870_912,
          memory_peak_bytes: 805_306_368,
        },
      ],
      cpu_usage_percent: null,
      cpu_note: "暂无可信 CPU 数据",
    },
    research: JOBS_ENVELOPE.data,
  },
};

test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} ${viewport.width}×${viewport.height}`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("real published empty queue explains why there are no rows", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/tasks");
      await expect(page.getByRole("heading", { level: 1, name: "任务与调度" })).toBeVisible();
      await expect(page.getByText("还没有研究任务")).toBeVisible();
      await expect(page.getByRole("region", { name: "任务状态概况" })).toContainText("全部0");
      await expectNoHorizontalOverflow(page, "empty research queue");
      expect(watcher.problems).toEqual([]);
    });

    test("four overview sections stay readable at this width", async ({ page }, testInfo) => {
      const watcher = watch(page);
      await page.route("**/api/v1/tasks/overview**", async (route) => {
        await route.fulfill({ json: OVERVIEW_ENVELOPE });
      });
      await page.goto("./#/tasks");
      const scheduled = page.getByRole("table", { name: "定时任务" });
      await expect(scheduled).toBeVisible();
      await expect(scheduled.locator("tbody tr")).toHaveCount(16);
      await expect(scheduled).toContainText("日线更新");
      await expect(scheduled).toContainText("09-24 15:30");
      await expect(scheduled).toContainText("09-25 09:30");
      await expect(scheduled).toContainText("未知");
      await expect(page.getByRole("table", { name: "运行服务" })).toContainText("通知推送");
      await expect(page.getByRole("region", { name: "资源概况" })).toContainText("1.00 GiB");
      await expect(page.getByText("暂无可信 CPU 数据")).toBeVisible();
      const table = page.getByRole("table", { name: "研究任务队列" });
      await expect(table).toBeVisible();
      await expect(table).toContainText("动量参数搜索");
      await expect(table).toContainText("运行中");
      await expect(table).toContainText("25%");
      await expect(table).toContainText("15:35");
      await expect(table.getByRole("progressbar")).toHaveAttribute("value", "0.25");
      if (viewport.name === "phone") {
        await expect(table.getByRole("columnheader", { name: "状态" })).toBeHidden();
        await expect(table.locator(".tasks-mobile-status")).toBeVisible();
        await expect(scheduled.locator(".tasks-mobile-timer").first()).toBeVisible();
      }
      const body = await page.locator("main").innerText();
      expect(findJargon(body)).toEqual([]);
      expect(body).not.toContain("00000000-0000-0000-0000-000000000001");
      await expectNoHorizontalOverflow(page, "research queue");
      await page.screenshot({
        path: testInfo.outputPath(`task-overview-${viewport.name}.png`),
        fullPage: true,
      });
      expect(watcher.problems).toEqual([]);
    });
  });
}
