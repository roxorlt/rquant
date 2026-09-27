import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { BACKFILL_PLAN_JOURNAL_KEY } from "./backfillPlanCommandSession";

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
const queuedTask = "e".repeat(32);
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
        total: 14,
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
              event_history: "unavailable",
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
              event_history: "unavailable",
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
          event_history: "unavailable",
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
  beforeEach(() => {
    window.sessionStorage.clear();
    server.use(
      http.get("*/api/v1/data/catalog", () =>
        HttpResponse.json({ data: { version: 1, datasets: [] }, serving }),
      ),
    );
  });

  it("pages within one data generation and shows every missing day without an execution affordance", async () => {
    const requests = planHandlers();
    const user = await openPlans();
    expect(await screen.findByText("2024-09-02")).toBeInTheDocument();
    const main = document.querySelector("main");
    expect(main).not.toBeNull();
    expect(within(main as HTMLElement).getByText("2024-09-03")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("2024-10-08")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("预计 46 分钟")).toBeInTheDocument();
    expect(main?.textContent).not.toContain("逻辑操作");
    expect(main?.textContent).not.toContain("非实际调用记录");
    expect(within(main as HTMLElement).getByText("配额待确认")).toBeInTheDocument();
    expect(within(main as HTMLElement).getByText("任务进度暂不可用")).toBeInTheDocument();
    expect(within(main as HTMLElement).queryByText("未运行")).not.toBeInTheDocument();
    expect(within(main as HTMLElement).getByRole("button", { name: "生成回补计划" })).toBeEnabled();
    expect(
      within(main as HTMLElement).queryByRole("button", { name: "执行回补" }),
    ).not.toBeInTheDocument();
    expect(main?.textContent).not.toContain(firstHash);
    expect(main?.textContent).not.toContain("replica-private-label");
    expect(findJargon(main?.textContent ?? "")).toEqual([]);

    await user.hover(within(main as HTMLElement).getByText("预计 46 分钟"));
    const estimate = await screen.findByRole("tooltip");
    expect(estimate).toHaveTextContent("名称变更窗口 2");
    expect(estimate).toHaveTextContent("共 14 次逻辑操作");

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

  it("queues a read-only request, keeps lagging progress honest, then reads the published result", async () => {
    let progress: Schemas["BackfillPlanProgress"] = {
      availability: "empty",
      event_history: "available",
      message: "还没有任务",
      logs: [],
    };
    let published = false;
    let sent: Schemas["BackfillPlanCommandRequest"] | null = null;
    server.use(
      http.get("*/api/v1/data/backfill-plans", () =>
        HttpResponse.json({
          data: {
            source_state: published ? "ready" : "empty",
            total: published ? 1 : 0,
            page_size: 20,
            items: published ? [item(firstHash, 0)] : [],
            next_cursor: null,
            progress,
          } satisfies PlanList,
          serving,
        }),
      ),
      http.get("*/api/v1/data/backfill-plans/:hash", () =>
        HttpResponse.json({
          data: {
            source_state: "ready",
            plan: detail(firstHash, 0),
            progress,
          } satisfies PlanDetailData,
          serving,
        }),
      ),
      http.post("*/api/v1/data/backfill-plans/commands", async ({ request }) => {
        sent = (await request.json()) as Schemas["BackfillPlanCommandRequest"];
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        expect(
          JSON.parse(window.sessionStorage.getItem(BACKFILL_PLAN_JOURNAL_KEY) ?? "{}").body,
        ).toEqual(sent);
        return HttpResponse.json({
          command_id: sent.command_id,
          status: "queued",
          task_id: queuedTask,
          message: "已排队",
        });
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/datacenter");
    await waitFor(() => expect(screen.getByRole("button", { name: "生成回补计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "生成回补计划" }));
    expect(await screen.findByText("还没有回补计划")).toBeInTheDocument();
    expect(screen.getByText("还没有生成任务")).toBeInTheDocument();
    const form = screen.getByRole("region", { name: "生成回补计划" });
    expect(within(form).getByLabelText("开始日期")).toHaveValue("2024-09-01");
    expect(within(form).getByLabelText("结束日期")).toHaveValue("2025-04-30");
    await user.click(within(form).getByRole("button", { name: "核对并生成" }));
    const dialog = screen.getByRole("dialog", { name: "生成回补计划" });
    expect(dialog).toHaveTextContent("只读核对");
    expect(dialog).toHaveTextContent("耗时");
    await user.click(within(dialog).getByRole("button", { name: "确认排队" }));
    await waitFor(() => expect(sent).not.toBeNull());
    expect(await screen.findByText("本次请求已排队，等待生成")).toBeInTheDocument();
    expect(screen.queryByText("计划已生成")).not.toBeInTheDocument();
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);

    progress = {
      availability: "ready",
      event_history: "available",
      task_id: queuedTask,
      status: "running",
      created_at: "2026-09-27T07:00:00Z",
      updated_at: "2026-09-27T07:00:30Z",
      message: "正在生成",
      logs: [],
    };
    await queryClient.invalidateQueries({ queryKey: ["data", "backfill-plans"] });
    expect(await screen.findAllByText("本次任务正在生成")).toHaveLength(2);
    expect(screen.queryByText("本次请求已排队，等待生成")).not.toBeInTheDocument();

    progress = {
      availability: "ready",
      event_history: "available",
      task_id: queuedTask,
      status: "succeeded",
      created_at: "2026-09-27T07:00:00Z",
      updated_at: "2026-09-27T07:01:00Z",
      plan_hash: firstHash,
      message: "计划已生成",
      logs: [
        {
          event_id: 1,
          event_type: "succeeded",
          attempts: 1,
          occurred_at: "2026-09-27T07:01:00Z",
          message: "svc-secret",
        },
      ],
    };
    await queryClient.invalidateQueries({ queryKey: ["data", "backfill-plans"] });
    expect(await screen.findByText("本次计划已生成，列表尚未显示")).toBeInTheDocument();

    published = true;
    await queryClient.invalidateQueries({ queryKey: ["data", "backfill-plans"] });
    expect(await screen.findByText("2024-09-02")).toBeInTheDocument();
    expect(screen.getByText("本次计划已生成")).toBeInTheDocument();
    expect(screen.queryByText("本次计划已生成，列表尚未显示")).not.toBeInTheDocument();
    expect(screen.getByText("计划已生成")).toBeInTheDocument();
    expect(document.querySelector("main")?.textContent).not.toContain("svc-secret");
  });

  it("keeps a recovered uncertain request and latest unrelated task separate", async () => {
    const saved: Schemas["BackfillPlanCommandRequest"] = {
      command_id: "web-recover",
      requested_at: "2026-09-27T07:00:00.000Z",
      audit_start: "2024-09-01",
      completed_through: "2025-04-30",
    };
    window.sessionStorage.setItem(
      BACKFILL_PLAN_JOURNAL_KEY,
      JSON.stringify({ schema: 1, body: saved, status: "unknown", taskId: null }),
    );
    planHandlers();
    server.use(
      http.get("*/api/v1/data/backfill-plans", () =>
        HttpResponse.json({
          data: {
            source_state: "empty",
            total: 0,
            page_size: 20,
            items: [],
            next_cursor: null,
            progress: {
              availability: "ready",
              event_history: "available",
              task_id: "f".repeat(32),
              status: "running",
              message: "正在生成",
              logs: [
                {
                  event_id: 2,
                  event_type: "started",
                  attempts: 1,
                  occurred_at: "2026-09-27T07:00:00Z",
                  message: "svc-secret",
                },
              ],
            },
          } satisfies PlanList,
          serving,
        }),
      ),
    );
    let retried: Schemas["BackfillPlanCommandRequest"] | null = null;
    server.use(
      http.post("*/api/v1/data/backfill-plans/commands", async ({ request }) => {
        retried = (await request.json()) as Schemas["BackfillPlanCommandRequest"];
        return HttpResponse.json({
          command_id: saved.command_id,
          status: "queued",
          task_id: queuedTask,
          message: "已排队",
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    expect(await screen.findByText("本次提交状态待确认")).toBeInTheDocument();
    expect(await screen.findByText("最新任务正在生成")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "继续核对" }));
    await waitFor(() => expect(retried).toEqual(saved));
    expect(await screen.findByText("本次请求已排队，等待生成")).toBeInTheDocument();
    expect(screen.getByText("最新任务正在生成")).toBeInTheDocument();
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
  });

  it("disables submission before login and rejects invalid dates before confirmation", async () => {
    planHandlers();
    server.use(metaHandler(metaEnvelope({ viewer: null })));
    const user = userEvent.setup();
    const first = renderApp("/datacenter");
    const disabled = await screen.findByRole("button", { name: "生成回补计划" });
    await waitFor(() =>
      expect(disabled).toHaveAttribute("aria-description", "请先登录，才能生成回补计划。"),
    );
    expect(disabled).toBeDisabled();
    first.unmount();

    server.use(metaHandler());
    renderApp("/datacenter");
    await waitFor(() => expect(screen.getByRole("button", { name: "生成回补计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "生成回补计划" }));
    const form = screen.getByRole("region", { name: "生成回补计划" });
    fireEvent.change(within(form).getByLabelText("开始日期"), { target: { value: "2010-01-01" } });
    expect(within(form).getByText("一次最多核对 3660 天。")).toBeInTheDocument();
    expect(within(form).getByRole("button", { name: "核对并生成" })).toBeDisabled();
    fireEvent.change(within(form).getByLabelText("开始日期"), { target: { value: "2024-09-01" } });
    fireEvent.change(within(form).getByLabelText("结束日期"), { target: { value: "2026-09-28" } });
    expect(within(form).getByText("结束日期须在收盘之后。")).toBeInTheDocument();
  });

  it("keeps an uncertain HTTP result on the original request until the operator retries", async () => {
    planHandlers();
    const sent: Schemas["BackfillPlanCommandRequest"][] = [];
    server.use(
      http.post("*/api/v1/data/backfill-plans/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["BackfillPlanCommandRequest"];
        sent.push(body);
        return sent.length === 1
          ? HttpResponse.json({ detail: "状态待确认" }, { status: 503 })
          : HttpResponse.json({
              command_id: body.command_id,
              status: "queued",
              task_id: queuedTask,
              message: "已排队",
            });
      }),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await waitFor(() => expect(screen.getByRole("button", { name: "生成回补计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "生成回补计划" }));
    await user.click(
      within(screen.getByRole("region", { name: "生成回补计划" })).getByRole("button", {
        name: "核对并生成",
      }),
    );
    await user.click(
      within(screen.getByRole("dialog", { name: "生成回补计划" })).getByRole("button", {
        name: "确认排队",
      }),
    );
    expect(await screen.findByText("本次提交状态待确认")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "生成回补计划" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "继续核对" }));
    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toEqual(sent[0]);
    expect(screen.getByText("本次请求已排队，等待生成")).toBeInTheDocument();
  });

  it("shows a definitive submission failure without claiming a queued task", async () => {
    planHandlers();
    server.use(
      http.post("*/api/v1/data/backfill-plans/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["BackfillPlanCommandRequest"];
        return HttpResponse.json({
          command_id: body.command_id,
          status: "failed",
          message: "未通过检查",
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/datacenter");
    await waitFor(() => expect(screen.getByRole("button", { name: "生成回补计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "生成回补计划" }));
    await user.click(
      within(screen.getByRole("region", { name: "生成回补计划" })).getByRole("button", {
        name: "核对并生成",
      }),
    );
    await user.click(
      within(screen.getByRole("dialog", { name: "生成回补计划" })).getByRole("button", {
        name: "确认排队",
      }),
    );
    expect(await screen.findByText("本次请求未通过，请调整后重试")).toBeInTheDocument();
    expect(screen.queryByText("本次请求已排队，等待生成")).not.toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("button", { name: "生成回补计划" })).toBeEnabled());
  });

  it("blocks new submission when a saved request cannot be safely read", async () => {
    window.sessionStorage.setItem(BACKFILL_PLAN_JOURNAL_KEY, "{corrupted");
    planHandlers();
    const user = userEvent.setup();
    renderApp("/datacenter");
    const button = await screen.findByRole("button", { name: "生成回补计划" });
    await waitFor(() =>
      expect(button).toHaveAttribute("aria-description", "浏览器存储不可用，无法安全提交。"),
    );
    expect(button).toBeDisabled();
    await user.click(screen.getByRole("button", { name: /^回补计划$/ }));
    expect(await screen.findByText("浏览器存储不可用，上次请求无法核对。")).toBeInTheDocument();
  });

  it("shows a failed task and at most 20 safe recent events, without raw server messages", async () => {
    const progress: Schemas["BackfillPlanProgress"] = {
      availability: "ready",
      event_history: "available",
      task_id: "f".repeat(32),
      status: "failed",
      message: "svc-internal failure",
      logs: Array.from({ length: 25 }, (_, index) => ({
        event_id: index + 1,
        event_type: "failed",
        attempts: 1,
        occurred_at: "2026-09-27T07:00:00Z",
        message: "svc-internal failure",
      })),
    };
    server.use(
      http.get("*/api/v1/data/backfill-plans", () =>
        HttpResponse.json({
          data: {
            source_state: "empty",
            total: 0,
            page_size: 20,
            items: [],
            next_cursor: null,
            progress,
          } satisfies PlanList,
          serving,
        }),
      ),
    );
    await openPlans();
    const region = await screen.findByRole("region", { name: "任务进度" });
    expect(within(region).getByText("最新任务生成失败")).toBeInTheDocument();
    expect(within(region).getAllByRole("listitem")).toHaveLength(20);
    expect(region.textContent).not.toContain("svc-internal");
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
  });
});
