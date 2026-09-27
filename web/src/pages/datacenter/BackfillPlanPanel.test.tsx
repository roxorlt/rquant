import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

type PlanItem = Schemas["BackfillPlanItem"];
type PlanDetail = Schemas["BackfillPlanDetail"];
type PlanList = Schemas["BackfillPlansData"];
type PlanDetailData = Schemas["BackfillPlanDetailData"];

const firstHash = "a".repeat(64);
const secondHash = "b".repeat(64);
const olderHash = "c".repeat(64);
const serving = {
  generation_id: "generation-a",
  built_at: "2026-09-27T06:00:00Z",
  age_seconds: 20,
  state: "ready",
  message: null,
  detail: "",
} as const;

function item(planHash: string, rank: number): PlanItem {
  return {
    rank,
    plan_hash: planHash,
    published_at: "2026-09-27T05:30:00Z",
    audit_start: "2024-09-01",
    completed_through: "2025-04-30",
    cutoff_observed_at: "2026-09-27T05:00:00Z",
    missing_day_count: 3,
    estimated_seconds: "2760",
    source_mode: "production_unverified",
    snapshot_label: "replica-private-label",
    identity_verified: false,
    collection_complete_verified: false,
    quota_status: "unverified",
    executable: false,
  };
}

function detail(planHash: string, rank: number): PlanDetail {
  return {
    ...item(planHash, rank),
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
        status_source_as_of: "2026-09-27",
        status_window_years: 3,
      },
    },
    source: {
      mode: "production_unverified",
      snapshot_label: "replica-private-label",
      claimed_file_sha256: "d".repeat(64),
      identity_verified: false,
      collection_complete_verified: false,
    },
  };
}

function planHandlers(options: { changedOnNext?: boolean; detailMissing?: boolean } = {}) {
  const requests: URLSearchParams[] = [];
  server.use(
    http.get("*/api/v1/data/backfill-plans", ({ request }) => {
      const query = new URL(request.url).searchParams;
      requests.push(query);
      if (query.has("cursor")) {
        if (options.changedOnNext) {
          return HttpResponse.json({ detail: "数据已更新" }, { status: 409 });
        }
        if (
          query.get("generation") !== serving.generation_id ||
          query.get("cursor") !== secondHash
        ) {
          return HttpResponse.json({ detail: "参数错误" }, { status: 422 });
        }
      }
      const data: PlanList = query.has("cursor")
        ? {
            source_state: "ready",
            total: 3,
            page_size: 2,
            items: [item(olderHash, 2)],
            next_cursor: null,
            progress: {
              availability: "unavailable",
              task_id: null,
              message: "任务进度尚未提供",
              logs: [],
            },
          }
        : {
            source_state: "ready",
            total: 3,
            page_size: 2,
            items: [item(firstHash, 0), item(secondHash, 1)],
            next_cursor: secondHash,
            progress: {
              availability: "unavailable",
              task_id: null,
              message: "任务进度尚未提供",
              logs: [],
            },
          };
      return HttpResponse.json({ data, serving });
    }),
    http.get("*/api/v1/data/backfill-plans/:hash", ({ params, request }) => {
      if (options.detailMissing)
        return HttpResponse.json({ detail: "不在列表中" }, { status: 404 });
      const hash = String(params.hash);
      if (new URL(request.url).searchParams.get("generation") !== serving.generation_id) {
        return HttpResponse.json({ detail: "数据已更新" }, { status: 409 });
      }
      const data: PlanDetailData = {
        source_state: "ready",
        plan: detail(hash, hash === olderHash ? 2 : hash === secondHash ? 1 : 0),
        progress: {
          availability: "unavailable",
          task_id: null,
          message: "任务进度尚未提供",
          logs: [],
        },
      };
      return HttpResponse.json({ data, serving });
    }),
  );
  return requests;
}

async function openPlans() {
  const user = userEvent.setup();
  renderApp("/datacenter");
  await user.click(await screen.findByRole("button", { name: "回补计划" }));
  return user;
}

describe("数据中心回补计划", () => {
  it("pages within one data generation and shows every missing day without an execution affordance", async () => {
    const requests = planHandlers();
    const user = await openPlans();
    expect(await screen.findByText("2024-09-02")).toBeInTheDocument();
    const main = document.querySelector("main");
    expect(main).not.toBeNull();
    expect(within(main as HTMLElement).getByText("2024-09-03")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("2024-10-08")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("预计 46 分钟")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("13 次逻辑操作")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("配额待确认")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("暂无进度信息")).toBeInTheDocument();
    expect(within(main as HTMLElement).queryByText("未运行")).not.toBeInTheDocument();
    expect(
      within(main as HTMLElement).queryByRole("button", { name: /生成|执行/ }),
    ).not.toBeInTheDocument();
    expect(main?.textContent).not.toContain(firstHash);
    expect(main?.textContent).not.toContain("replica-private-label");
    expect(findJargon(main?.textContent ?? "")).toEqual([]);

    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByRole("button", { name: /第 3 份计划/ })).toBeInTheDocument();
    expect(requests.at(-1)?.get("cursor")).toBe(secondHash);
    expect(requests.at(-1)?.get("generation")).toBe(serving.generation_id);
    expect(await screen.findByText("历史计划")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "上一页" }));
    expect(await screen.findByRole("button", { name: /第 1 份计划/ })).toBeInTheDocument();
  });

  it.each([
    ["empty", "还没有回补计划"],
    ["not_published", "回补计划尚未发布"],
    ["unavailable", "回补计划暂时不可用"],
  ] as const)("shows %s source state honestly", async (state, message) => {
    server.use(
      http.get("*/api/v1/data/backfill-plans", () =>
        HttpResponse.json({
          data: { source_state: state, items: [], page_size: 20, total: 0, next_cursor: null },
          serving,
        }),
      ),
    );
    await openPlans();
    expect(await screen.findByText(message)).toBeInTheDocument();
  });

  it("explains that zero whole-day gaps do not prove per-stock completeness", async () => {
    planHandlers();
    server.use(
      http.get("*/api/v1/data/backfill-plans/:hash", () => {
        const noGap: PlanDetail = {
          ...detail(firstHash, 0),
          missing_day_count: 0,
          gap_count: 0,
          missing_dates: [],
          monthly: [
            {
              month: "2024-09-01",
              expected_open_days: 20,
              covered_open_days: 20,
              missing_open_days: 0,
            },
          ],
        };
        return HttpResponse.json({
          data: { source_state: "ready", plan: noGap, progress: null },
          serving,
        });
      }),
    );
    await openPlans();
    expect(await screen.findByText("这段时间没有整日缺失")).toBeInTheDocument();
    expect(screen.getByText("只代表每个交易日有记录，不代表逐股齐全")).toBeInTheDocument();
  });

  it("offers a fresh load when a later page belongs to another generation", async () => {
    planHandlers({ changedOnNext: true });
    const user = await openPlans();
    await screen.findByText("2024-09-02");
    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByText("计划列表已更新")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "重新加载计划" }));
    expect(await screen.findByRole("button", { name: /第 1 份计划/ })).toBeInTheDocument();
  });

  it("separates a vanished plan from an unreadable list", async () => {
    planHandlers({ detailMissing: true });
    const user = await openPlans();
    expect(await screen.findByText("这份计划已不在当前列表")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "数据目录" }));
    await user.click(screen.getByRole("button", { name: "回补计划" }));
    server.use(
      http.get("*/api/v1/data/backfill-plans", () =>
        HttpResponse.json({ detail: "暂不可用" }, { status: 503 }),
      ),
    );
    await user.click(screen.getByRole("button", { name: "刷新计划" }));
    expect(await screen.findByText("暂时读不到回补计划")).toBeInTheDocument();
  });
});
