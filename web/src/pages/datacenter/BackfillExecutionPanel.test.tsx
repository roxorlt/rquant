import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import type {
  DataCenterCommand,
  DataCenterConfirmation,
  DataCenterExecution,
} from "@/api/endpoints";
import { META_QUERY_KEY } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { BackfillExecutionPanel } from "./BackfillExecutionPanel";
import DataCenterPage from "./index";

const executionId = "c".repeat(64);
const planHash = "b".repeat(64);
const sources: Schemas["FinancialSourceView"][] = (
  [
    ["fina_indicator", "财务指标"],
    ["income", "利润表"],
    ["balancesheet", "资产负债表"],
    ["cashflow", "现金流量表"],
    ["forecast", "业绩预告"],
    ["express", "业绩快报"],
    ["dividend", "分红送股"],
  ] as const
).map(([api, name]) => ({
  api_name: api,
  name,
  permission_status: "verified",
  permission_label: "可用",
  evidence_source: "offline_fixture",
  scope_start: "2026-01-01",
  scope_end: "2026-12-31",
  expires_at: "2026-12-31T10:00:00Z",
  remaining_units: 17,
  total_units: 30,
  resets_at: "2026-10-07T00:00:00Z",
}));

function execution(overrides: Partial<DataCenterExecution> = {}): DataCenterExecution {
  return {
    execution_id: executionId,
    kind: "financial",
    name: "财务采集",
    status: "running",
    status_label: "采集中",
    control_sequence: 1,
    pause_requested: false,
    pause_applied: false,
    completed_tasks: 1,
    total_tasks: 3,
    current_date: "2026-09-30",
    updated_at: "2026-10-06T10:00:00Z",
    can_pause: true,
    can_resume: false,
    completion_verified: false,
    audit_report_hash: null,
    ...overrides,
  };
}

function handlers(
  options: {
    enabled?: boolean;
    executions?: DataCenterExecution[];
    events?: Schemas["ExecutionEventView"][];
  } = {},
) {
  const serving = metaEnvelope().serving;
  server.use(
    http.get("*/api/v1/data/executions", () =>
      HttpResponse.json({
        serving,
        data: {
          status: "ready",
          configured: true,
          backfill_enabled: true,
          financial_enabled: options.enabled ?? true,
          may_start: true,
          executions: options.executions ?? [],
          events: options.events ?? [],
        },
      }),
    ),
    http.get("*/api/v1/data/collection", () =>
      HttpResponse.json({
        serving,
        data: {
          status: "ready",
          report_hash: "a".repeat(64),
          datasets: [],
          coverage_label: "全市场覆盖尚未核验",
        },
      }),
    ),
    http.get("*/api/v1/data/financial-sources", () =>
      HttpResponse.json({ serving, data: { status: "ready", sources } }),
    ),
  );
}

function panel(props: Parameters<typeof BackfillExecutionPanel>[0] = { mode: "financial" }) {
  const queryClient = testQueryClient();
  queryClient.setQueryData(META_QUERY_KEY, metaEnvelope());
  const result = render(
    <AppProviders queryClient={queryClient}>
      <BackfillExecutionPanel {...props} />
    </AppProviders>,
  );
  return { ...result, queryClient };
}

function confirmation(
  body: Extract<DataCenterCommand, { kind: "prepare_financial_collection" }>,
): DataCenterConfirmation {
  return {
    kind: "financial",
    execution_id: executionId,
    intent_id: "d".repeat(64),
    prepare_command_id: body.command_id,
    plan_hash: planHash,
    start_date: body.start_date,
    end_date: body.end_date,
    security_count: 1,
    query_count: 7,
    report_periods: body.report_periods,
    expires_at: new Date(Date.now() + 300_000).toISOString(),
  };
}

async function chooseScope() {
  await screen.findByRole("region", { name: "财务接口权益" });
  fireEvent.change(screen.getByLabelText("财务开始日期"), { target: { value: "2026-01-01" } });
  fireEvent.change(screen.getByLabelText("财务结束日期"), { target: { value: "2026-09-30" } });
  fireEvent.change(screen.getByLabelText("财务报告期"), { target: { value: "2026-06-30" } });
  fireEvent.change(screen.getByLabelText("财务股票范围"), { target: { value: "600000.SH" } });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "确认财务采集范围" })).toBeEnabled(),
  );
}

it("requires a bound preview and typed confirmation before the original execute request", async () => {
  handlers();
  const requests: DataCenterCommand[] = [];
  server.use(
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (body.kind === "prepare_financial_collection")
        return HttpResponse.json({
          command_id: body.command_id,
          status: "prepared",
          message: "请确认范围",
          confirmation: confirmation(body),
        });
      if (body.kind !== "execute_financial_collection") throw new Error("unexpected command");
      expect(body.confirmed).toBe(true);
      expect(body.plan_hash).toBe(planHash);
      expect(body.prepare_command_id).toBe(requests[0]?.command_id);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "queued",
        message: "已排队",
        execution_id: executionId,
      });
    }),
  );
  const user = userEvent.setup();
  panel();
  await chooseScope();
  await user.click(screen.getByRole("button", { name: "确认财务采集范围" }));
  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByText("7 次")).toBeInTheDocument();
  expect(within(dialog).getByRole("button", { name: "开始执行" })).toBeDisabled();
  expect(requests).toHaveLength(1);
  await user.type(within(dialog).getByRole("textbox"), "财务采集");
  await user.click(within(dialog).getByRole("button", { name: "开始执行" }));
  await waitFor(() => expect(requests).toHaveLength(2));
  expect(await screen.findByText("已排队")).toBeInTheDocument();
  expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  expect(document.body.textContent).not.toContain(planHash);
});

it("retries the retained command after a lost HTTP response without another request ID", async () => {
  handlers();
  const requests: DataCenterCommand[] = [];
  server.use(
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (requests.length === 1) return HttpResponse.error();
      if (body.kind !== "prepare_financial_collection") throw new Error("unexpected command");
      return HttpResponse.json({
        command_id: body.command_id,
        status: "prepared",
        message: "请确认范围",
        confirmation: confirmation(body),
      });
    }),
  );
  const user = userEvent.setup();
  panel();
  await chooseScope();
  await user.click(screen.getByRole("button", { name: "确认财务采集范围" }));
  await user.click(await screen.findByRole("button", { name: "核对上次请求" }));
  await screen.findByRole("dialog");
  expect(requests).toHaveLength(2);
  expect(requests[1]).toEqual(requests[0]);
});

it("previews every selected quarter and rejects a report date that is not quarter end", async () => {
  handlers();
  const requests: DataCenterCommand[] = [];
  server.use(
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (body.kind !== "prepare_financial_collection") throw new Error("unexpected command");
      return HttpResponse.json({
        command_id: body.command_id,
        status: "prepared",
        message: "请确认范围",
        confirmation: { ...confirmation(body), query_count: 8 },
      });
    }),
  );
  const user = userEvent.setup();
  panel();
  await chooseScope();
  fireEvent.change(screen.getByLabelText("财务报告期"), { target: { value: "2026-07-01" } });
  expect(screen.getByRole("button", { name: "确认财务采集范围" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("财务报告期"), { target: { value: "2026-06-30" } });
  fireEvent.change(screen.getByLabelText("财务最后报告期"), { target: { value: "2026-09-30" } });
  await user.click(screen.getByRole("button", { name: "确认财务采集范围" }));
  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByText("8 次")).toBeInTheDocument();
  expect(requests[0]).toMatchObject({ report_periods: ["2026-06-30", "2026-09-30"] });
});

it.each([{ generationId: "new-data" }, { viewer: "another-owner" }])(
  "withdraws a stale confirmation when the viewer or served data changes",
  async (change) => {
    handlers();
    server.use(
      http.post("*/api/v1/data/executions/commands", async ({ request }) => {
        const body = (await request.json()) as DataCenterCommand;
        if (body.kind !== "prepare_financial_collection") throw new Error("unexpected command");
        return HttpResponse.json({
          command_id: body.command_id,
          status: "prepared",
          message: "请确认范围",
          confirmation: confirmation(body),
        });
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = panel();
    await chooseScope();
    await user.click(screen.getByRole("button", { name: "确认财务采集范围" }));
    await screen.findByRole("dialog");
    await act(async () => queryClient.setQueryData(META_QUERY_KEY, metaEnvelope(change)));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  },
);

it("does not reuse another viewer's execution rows when the served data is unchanged", async () => {
  handlers();
  let requests = 0;
  server.use(
    http.get("*/api/v1/data/executions", () => {
      requests += 1;
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          status: "ready",
          configured: true,
          backfill_enabled: true,
          financial_enabled: true,
          may_start: true,
          executions: requests === 1 ? [execution()] : [],
        },
      });
    }),
  );
  const { queryClient } = panel();
  await screen.findByRole("region", { name: "财务采集任务进度" });
  await act(async () =>
    queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "another-owner" })),
  );
  await waitFor(() => expect(requests).toBe(2));
  expect(screen.queryByRole("region", { name: "财务采集任务进度" })).not.toBeInTheDocument();
});

it("keeps unknown execution rights disabled without invented zero quota", async () => {
  handlers({ enabled: false });
  server.use(
    http.get("*/api/v1/data/financial-sources", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          status: "ready",
          sources: sources.map((value) => ({
            ...value,
            permission_status: "unknown",
            permission_label: "待核验",
            remaining_units: null,
          })),
        },
      }),
    ),
  );
  panel();
  await screen.findByRole("region", { name: "财务接口权益" });
  expect(screen.getByRole("button", { name: "确认财务采集范围" })).toBeDisabled();
  expect(screen.getAllByText("待核验")).toHaveLength(7);
  expect(screen.queryByText("0")).not.toBeInTheDocument();
});

it("shows a requested pause before it is applied and uses the original control sequence", async () => {
  const original = execution();
  handlers({ executions: [original] });
  server.use(
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      const body = (await request.json()) as DataCenterCommand;
      expect(body.kind).toBe("pause_data_center_execution");
      if (body.kind !== "pause_data_center_execution") throw new Error("unexpected command");
      expect(body.expected_sequence).toBe(1);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "control_accepted",
        message: "已请求暂停",
        execution_id: executionId,
        execution: execution({
          control_sequence: 2,
          pause_requested: true,
          can_pause: false,
          status_label: "正在暂停",
        }),
      });
    }),
  );
  const user = userEvent.setup();
  panel();
  await user.click(await screen.findByRole("button", { name: "暂停" }));
  expect(await screen.findByText("正在暂停")).toBeInTheDocument();
  expect(screen.queryByText("所选范围已完成")).not.toBeInTheDocument();
});

it("shows completion only for the original verified selected scope", async () => {
  handlers({
    executions: [
      execution({
        status: "completed",
        status_label: "已完成",
        completed_tasks: 3,
        completion_verified: true,
        can_pause: false,
        audit_report_hash: "e".repeat(64),
      }),
    ],
  });
  panel();
  expect(await screen.findByText("所选范围已完成")).toBeInTheDocument();
  expect(screen.getByRole("progressbar", { name: "数据任务进度" })).toHaveAttribute("value", "3");
  expect(document.body.textContent).not.toContain("全市场完成");
});

it("shows recent records newest first for this mode with details in the existing Tip", async () => {
  const otherId = "f".repeat(64);
  handlers({
    executions: [execution(), execution({ execution_id: otherId, kind: "backfill" })],
    events: [
      {
        event_id: "1".repeat(64),
        execution_id: executionId,
        name: "采集财务",
        status_label: "开始",
        occurred_at: "2026-10-06T09:30:00Z",
      },
      {
        event_id: "2".repeat(64),
        execution_id: executionId,
        name: "暂停",
        status_label: "已受理",
        occurred_at: "2026-10-06T09:31:00Z",
        detail: "当前批次结束后暂停。",
      },
      {
        event_id: "3".repeat(64),
        execution_id: otherId,
        name: "日线采集",
        status_label: "完成",
        occurred_at: "2026-10-06T09:32:00Z",
      },
    ],
  });
  const user = userEvent.setup();
  panel();
  const records = await screen.findByRole("region", { name: "财务采集最近运行记录" });
  const rows = within(records).getAllByRole("listitem");
  expect(rows).toHaveLength(2);
  expect(rows[0]).toHaveTextContent("暂停");
  expect(rows[1]).toHaveTextContent("采集财务");
  expect(records).not.toHaveTextContent("日线采集");
  expect(records).not.toHaveTextContent("当前批次结束后暂停。");
  expect(records).not.toHaveTextContent(executionId);
  expect(findJargon(records.textContent ?? "")).toEqual([]);
  await user.hover(within(rows[0] as HTMLElement).getByText("已受理"));
  expect(await screen.findByRole("tooltip")).toHaveTextContent("当前批次结束后暂停。");
});

it("keeps an empty recent record list truthful", async () => {
  handlers();
  panel();
  expect(await screen.findByText("还没有运行记录")).toBeInTheDocument();
  expect(screen.getByText("任务开始后显示")).toBeInTheDocument();
});

it("binds the original backfill plan and exact dates to both confirmation steps", async () => {
  const taskId = "9".repeat(32);
  const datesHash = "8".repeat(64);
  const plan: Schemas["BackfillPlanDetail"] = {
    plan_hash: planHash,
    published_at: "2026-10-06T10:00:00Z",
    audit_start: "2026-09-28",
    completed_through: "2026-09-30",
    cutoff_observed_at: "2026-10-06T10:00:00Z",
    missing_day_count: 2,
    missing_dates: ["2026-09-28", "2026-09-29"],
    monthly: [],
    gap_count: 1,
    estimated_seconds: "6",
    source_mode: "production_unverified",
    snapshot_label: "fixture-replica",
    identity_verified: false,
    collection_complete_verified: false,
    quota_status: "unverified",
    executable: false,
    coverage_scope: "whole_day_presence_only",
    estimate: {
      estimated_seconds: "6",
      quota_status: "unverified",
      actual_http_calls_known: false,
      logical_operations: {
        daily: 2,
        daily_basic: 2,
        adj_factor: 2,
        namechange_context_batches: 1,
        namechange_windows: 1,
        stock_st_upper_bound: 2,
        trade_cal: 0,
        total: 10,
      },
      assumptions: {
        adapter_seconds_per_operation: "1",
        market_throttle_seconds_per_operation: "0",
        retry_allowance_seconds_per_operation: "0",
        status_throttle_seconds_per_operation: "0",
        status_namechange_start: "2026-09-28",
        status_source_as_of: "2026-10-06",
        status_window_years: 3,
      },
    },
    source: {
      mode: "production_unverified",
      snapshot_label: "fixture-replica",
      claimed_file_sha256: "7".repeat(64),
      identity_verified: false,
      collection_complete_verified: false,
    },
  };
  handlers();
  const requests: DataCenterCommand[] = [];
  server.use(
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (body.kind === "prepare_backfill_execution") {
        expect(body.plan_task_id).toBe(taskId);
        expect(body.plan_hash).toBe(planHash);
        return HttpResponse.json({
          command_id: body.command_id,
          status: "prepared",
          message: "请确认范围",
          confirmation: {
            kind: "backfill",
            execution_id: executionId,
            intent_id: "d".repeat(64),
            prepare_command_id: body.command_id,
            plan_hash: planHash,
            plan_task_id: taskId,
            exact_dates_sha256: datesHash,
            start_date: plan.audit_start,
            end_date: plan.completed_through,
            missing_date_count: 2,
            expires_at: new Date(Date.now() + 300_000).toISOString(),
          },
        });
      }
      expect(body).toMatchObject({
        kind: "execute_backfill_plan",
        prepare_command_id: requests[0]?.command_id,
        plan_task_id: taskId,
        exact_dates_sha256: datesHash,
        plan_hash: planHash,
        confirmed: true,
      });
      return HttpResponse.json({
        command_id: body.command_id,
        execution_id: executionId,
        status: "queued",
        message: "已排队",
      });
    }),
  );
  const user = userEvent.setup();
  panel({ mode: "backfill", plan, planTaskId: taskId });
  const prepare = await screen.findByRole("button", { name: "确认日线回补范围" });
  await waitFor(() => expect(prepare).toBeEnabled());
  await user.click(prepare);
  const dialog = await screen.findByRole("dialog");
  expect(within(dialog).getByText("2 天")).toBeInTheDocument();
  const execute = within(dialog).getByRole("button", { name: "开始执行" });
  expect(execute).toBeDisabled();
  expect(requests).toHaveLength(1);
  await user.type(within(dialog).getByRole("textbox"), "日线回补");
  await user.click(execute);
  await waitFor(() => expect(requests).toHaveLength(2));
  expect(await screen.findByText("已排队")).toBeInTheDocument();
});

function lifecycleFixture(options: { loseExecuteResponse?: boolean } = {}) {
  const taskId = "9".repeat(32);
  const plan: Schemas["BackfillPlanDetail"] = {
    plan_hash: planHash,
    published_at: "2026-10-06T10:00:00Z",
    audit_start: "2026-09-28",
    completed_through: "2026-09-30",
    cutoff_observed_at: "2026-10-06T10:00:00Z",
    missing_day_count: 2,
    missing_dates: ["2026-09-28", "2026-09-29"],
    monthly: [],
    gap_count: 1,
    estimated_seconds: "6",
    source_mode: "production_unverified",
    snapshot_label: "fixture-replica",
    identity_verified: false,
    collection_complete_verified: false,
    quota_status: "unverified",
    executable: false,
    coverage_scope: "whole_day_presence_only",
    estimate: {
      estimated_seconds: "6",
      quota_status: "unverified",
      actual_http_calls_known: false,
      logical_operations: {
        daily: 2,
        daily_basic: 2,
        adj_factor: 2,
        namechange_context_batches: 1,
        namechange_windows: 1,
        stock_st_upper_bound: 2,
        trade_cal: 0,
        total: 10,
      },
      assumptions: {
        adapter_seconds_per_operation: "1",
        market_throttle_seconds_per_operation: "0",
        retry_allowance_seconds_per_operation: "0",
        status_throttle_seconds_per_operation: "0",
        status_namechange_start: "2026-09-28",
        status_source_as_of: "2026-10-06",
        status_window_years: 3,
      },
    },
    source: {
      mode: "production_unverified",
      snapshot_label: "fixture-replica",
      claimed_file_sha256: "7".repeat(64),
      identity_verified: false,
      collection_complete_verified: false,
    },
  };
  const progress: Schemas["BackfillPlanProgress"] = {
    availability: "ready",
    event_history: "available",
    task_id: taskId,
    status: "succeeded",
    plan_hash: planHash,
    message: "计划已生成",
    logs: [],
  };
  let meta = metaEnvelope();
  let rows: DataCenterExecution[] = [];
  let detailGate: Promise<void> | null = null;
  let releaseDetail = () => {};
  const requests: DataCenterCommand[] = [];
  server.use(
    http.get("*/api/v1/meta", () => HttpResponse.json(meta)),
    http.get("*/api/v1/data/catalog", () =>
      HttpResponse.json({ serving: meta.serving, data: { version: 1, datasets: [] } }),
    ),
    http.get("*/api/v1/data/backfill-plans", () =>
      HttpResponse.json({
        serving: meta.serving,
        data: {
          source_state: "ready",
          total: 1,
          page_size: 20,
          items: [{ ...plan, rank: 0 }],
          next_cursor: null,
          progress,
        },
      }),
    ),
    http.get("*/api/v1/data/backfill-plans/:hash", async ({ request }) => {
      const generation = new URL(request.url).searchParams.get("generation");
      if (generation === meta.serving.generation_id) await detailGate;
      return HttpResponse.json({
        serving: meta.serving,
        data: { source_state: "ready", plan, progress },
      });
    }),
    http.get("*/api/v1/data/executions", () =>
      HttpResponse.json({
        serving: meta.serving,
        data: {
          status: "ready",
          configured: true,
          backfill_enabled: true,
          financial_enabled: true,
          may_start: true,
          executions: meta.data.viewer === "tester" ? rows : [],
          events: [],
        },
      }),
    ),
    http.get("*/api/v1/data/collection", () =>
      HttpResponse.json({
        serving: meta.serving,
        data: {
          status: "ready",
          report_hash: "a".repeat(64),
          datasets: [],
          coverage_label: "全市场覆盖尚未核验",
        },
      }),
    ),
    http.get("*/api/v1/data/financial-sources", () =>
      HttpResponse.json({ serving: meta.serving, data: { status: "ready", sources } }),
    ),
    http.get("*/api/v1/data/fundamentals/summary", () =>
      HttpResponse.json({
        status: "not_configured",
        decision_date: null,
        waiting_for_today: false,
        source: null,
        fields: [],
      }),
    ),
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (body.kind === "prepare_backfill_execution")
        return HttpResponse.json({
          command_id: body.command_id,
          status: "prepared",
          message: "请确认范围",
          confirmation: {
            kind: "backfill",
            execution_id: executionId,
            intent_id: "d".repeat(64),
            prepare_command_id: body.command_id,
            plan_hash: body.plan_hash,
            plan_task_id: body.plan_task_id,
            exact_dates_sha256: "8".repeat(64),
            start_date: plan.audit_start,
            end_date: plan.completed_through,
            missing_date_count: 2,
            expires_at: new Date(Date.now() + 300_000).toISOString(),
          },
        });
      if (body.kind === "execute_backfill_plan") {
        rows = [execution({ kind: "backfill", status: "queued", status_label: "等待执行" })];
        if (options.loseExecuteResponse && requests.length === 2) return HttpResponse.error();
        return HttpResponse.json({
          command_id: body.command_id,
          execution_id: body.execution_id,
          status: "queued",
          message: "已排队",
        });
      }
      if (body.kind !== "pause_data_center_execution") throw new Error("unexpected command");
      rows = [
        execution({
          kind: "backfill",
          control_sequence: body.expected_sequence + 1,
          pause_requested: true,
          status_label: "正在暂停",
        }),
      ];
      return HttpResponse.json({
        command_id: body.command_id,
        execution_id: body.execution_id,
        status: "control_accepted",
        message: "已请求暂停",
        execution: rows[0],
      });
    }),
  );
  const queryClient = testQueryClient();
  queryClient.setQueryData(META_QUERY_KEY, meta);
  render(
    <AppProviders queryClient={queryClient}>
      <DataCenterPage />
    </AppProviders>,
  );
  return {
    requests,
    holdDetail() {
      detailGate = new Promise<void>((resolve) => {
        releaseDetail = resolve;
      });
    },
    releaseDetail() {
      releaseDetail();
      detailGate = null;
    },
    changeContext(overrides: Parameters<typeof metaEnvelope>[0]) {
      meta = metaEnvelope(overrides);
      act(() => queryClient.setQueryData(META_QUERY_KEY, meta));
    },
  };
}

async function startPageBackfill(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: "回补计划" }));
  const prepare = await screen.findByRole("button", { name: "确认日线回补范围" });
  await waitFor(() => expect(prepare).toBeEnabled());
  await user.click(prepare);
  const dialog = await screen.findByRole("dialog");
  await user.type(within(dialog).getByRole("textbox"), "日线回补");
  await user.click(within(dialog).getByRole("button", { name: "开始执行" }));
}

it("retains an accepted receipt through real detail loading and both page tabs", async () => {
  const fixture = lifecycleFixture();
  const user = userEvent.setup();
  await startPageBackfill(user);
  await screen.findByText("已排队");
  await waitFor(() => expect(screen.getByRole("button", { name: "暂停" })).toBeEnabled());
  fixture.holdDetail();
  fixture.changeContext({ generationId: "e".repeat(64) });
  await screen.findByRole("status", { name: "计划详情加载中" });
  expect(screen.queryByRole("button", { name: "暂停" })).not.toBeInTheDocument();
  fixture.releaseDetail();
  await waitFor(() => expect(screen.getByRole("button", { name: "暂停" })).toBeEnabled());
  expect(screen.queryByRole("button", { name: "核对上次请求" })).not.toBeInTheDocument();
  expect(fixture.requests).toHaveLength(2);
  await user.click(screen.getByRole("button", { name: "财务" }));
  await screen.findByRole("region", { name: "财务接口权益" });
  expect(screen.queryByRole("button", { name: "核对上次请求" })).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "回补计划" }));
  const pause = await screen.findByRole("button", { name: "暂停" });
  expect(pause).toBeEnabled();
  await user.click(pause);
  await screen.findByText("正在暂停");
  expect(fixture.requests).toHaveLength(3);
  expect(fixture.requests[2]).toMatchObject({
    kind: "pause_data_center_execution",
    execution_id: executionId,
    expected_sequence: 1,
  });
  expect(fixture.requests[2]?.command_id).not.toBe(fixture.requests[1]?.command_id);
});

it("keeps a lost execute response unresolved after remount and looks up its exact body", async () => {
  const fixture = lifecycleFixture({ loseExecuteResponse: true });
  const user = userEvent.setup();
  await startPageBackfill(user);
  await screen.findByRole("button", { name: "核对上次请求" });
  expect(screen.getByRole("button", { name: "暂停" })).toBeDisabled();
  fixture.holdDetail();
  fixture.changeContext({ generationId: "e".repeat(64) });
  await screen.findByRole("status", { name: "计划详情加载中" });
  fixture.releaseDetail();
  await screen.findByRole("button", { name: "核对上次请求" });
  expect(screen.getByRole("button", { name: "暂停" })).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "财务" }));
  await screen.findByRole("region", { name: "财务接口权益" });
  expect(screen.getByRole("button", { name: "确认财务采集范围" })).toBeDisabled();
  expect(fixture.requests).toHaveLength(2);
  await user.click(screen.getByRole("button", { name: "核对上次请求" }));
  await screen.findByText("已排队");
  expect(fixture.requests).toHaveLength(3);
  expect(fixture.requests[2]).toEqual(fixture.requests[1]);
  await user.click(screen.getByRole("button", { name: "回补计划" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "暂停" })).toBeEnabled());
});

it("withdraws an old prepared confirmation while its detail is absent", async () => {
  const fixture = lifecycleFixture();
  const user = userEvent.setup();
  await user.click(screen.getByRole("button", { name: "回补计划" }));
  const prepare = await screen.findByRole("button", { name: "确认日线回补范围" });
  await waitFor(() => expect(prepare).toBeEnabled());
  await user.click(prepare);
  await screen.findByRole("dialog");
  fixture.holdDetail();
  fixture.changeContext({ generationId: "e".repeat(64) });
  await screen.findByRole("status", { name: "计划详情加载中" });
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  fixture.releaseDetail();
  await waitFor(() =>
    expect(screen.getByRole("button", { name: "确认日线回补范围" })).toBeEnabled(),
  );
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "核对上次请求" })).not.toBeInTheDocument();
  expect(fixture.requests).toHaveLength(1);
});

it("withholds an accepted receipt when the page owner changes across tabs", async () => {
  const fixture = lifecycleFixture();
  const user = userEvent.setup();
  await startPageBackfill(user);
  await screen.findByText("已排队");
  await user.click(screen.getByRole("button", { name: "财务" }));
  await screen.findByRole("region", { name: "财务接口权益" });
  fixture.changeContext({ viewer: "another-owner" });
  await waitFor(() => expect(screen.queryByText("已排队")).not.toBeInTheDocument());
  expect(screen.queryByRole("button", { name: "核对上次请求" })).not.toBeInTheDocument();
  expect(fixture.requests).toHaveLength(2);
  fixture.changeContext({ viewer: "tester" });
  await screen.findByRole("button", { name: "核对上次请求" });
  expect(fixture.requests).toHaveLength(2);
  await user.click(screen.getByRole("button", { name: "核对上次请求" }));
  await screen.findByText("已排队");
  expect(fixture.requests[2]).toEqual(fixture.requests[1]);
});

it("blocks control during a real index refresh and then uses its new sequence", async () => {
  handlers({ executions: [execution()] });
  let release = () => {};
  let reads = 0;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  const requests: DataCenterCommand[] = [];
  server.use(
    http.get("*/api/v1/data/executions", async () => {
      reads += 1;
      if (reads === 2) await gate;
      return HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          status: "ready",
          configured: true,
          backfill_enabled: true,
          financial_enabled: true,
          may_start: true,
          executions: [execution({ control_sequence: reads === 1 ? 1 : 2 })],
          events: [],
        },
      });
    }),
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (body.kind !== "pause_data_center_execution") throw new Error("unexpected command");
      return HttpResponse.json({
        command_id: body.command_id,
        status: "control_accepted",
        message: "已请求暂停",
        execution_id: executionId,
        execution: execution({ control_sequence: 3, pause_requested: true, can_pause: false }),
      });
    }),
  );
  const user = userEvent.setup();
  panel();
  const pause = await screen.findByRole("button", { name: "暂停" });
  await waitFor(() => expect(pause).toBeEnabled());
  act(() => {
    fireEvent.click(screen.getByRole("button", { name: "刷新状态" }));
    fireEvent.click(pause);
  });
  await waitFor(() => expect(reads).toBe(2));
  await waitFor(() => expect(screen.getByRole("button", { name: "暂停" })).toBeDisabled());
  fireEvent.click(screen.getByRole("button", { name: "暂停" }));
  expect(requests).toHaveLength(0);
  release();
  await waitFor(() => expect(screen.getByRole("button", { name: "暂停" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "暂停" }));
  await waitFor(() => expect(requests).toHaveLength(1));
  expect(requests[0]).toMatchObject({
    kind: "pause_data_center_execution",
    execution_id: executionId,
    expected_sequence: 2,
  });
});

it("blocks acknowledged controls after the served source changes", async () => {
  handlers({ executions: [execution()] });
  const requests: DataCenterCommand[] = [];
  server.use(
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "control_accepted",
        message: "已请求暂停",
        execution_id: executionId,
        execution: execution({
          control_sequence: 2,
          status: "paused",
          can_pause: false,
          can_resume: true,
          pause_requested: true,
          pause_applied: true,
        }),
      });
    }),
  );
  const user = userEvent.setup();
  const { queryClient } = panel();
  await user.click(await screen.findByRole("button", { name: "暂停" }));
  const resume = await screen.findByRole("button", { name: "继续" });
  await waitFor(() => expect(resume).toBeEnabled());
  act(() => {
    queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ generationId: "e".repeat(64) }));
    fireEvent.click(resume);
  });
  await waitFor(() => expect(screen.getByRole("button", { name: "继续" })).toBeDisabled());
  fireEvent.click(screen.getByRole("button", { name: "继续" }));
  expect(requests).toHaveLength(1);
});

it("never compares an old acknowledged sequence with another task ID", async () => {
  const nextId = "f".repeat(64);
  let current = execution({ control_sequence: 8 });
  const requests: DataCenterCommand[] = [];
  handlers();
  server.use(
    http.get("*/api/v1/data/executions", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          status: "ready",
          configured: true,
          backfill_enabled: true,
          financial_enabled: true,
          may_start: true,
          executions: [current],
          events: [],
        },
      }),
    ),
    http.post("*/api/v1/data/executions/commands", async ({ request }) => {
      const body = (await request.json()) as DataCenterCommand;
      requests.push(body);
      if (body.kind !== "pause_data_center_execution") throw new Error("unexpected command");
      const accepted = execution({
        execution_id: body.execution_id,
        control_sequence: body.expected_sequence + 1,
        can_pause: false,
        pause_requested: true,
      });
      current = execution({ execution_id: nextId, control_sequence: 1 });
      return HttpResponse.json({
        command_id: body.command_id,
        status: "control_accepted",
        message: "已请求暂停",
        execution_id: body.execution_id,
        execution: accepted,
      });
    }),
  );
  const user = userEvent.setup();
  panel();
  await user.click(await screen.findByRole("button", { name: "暂停" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "暂停" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "暂停" }));
  await waitFor(() => expect(requests).toHaveLength(2));
  expect(requests[0]).toMatchObject({ execution_id: executionId, expected_sequence: 8 });
  expect(requests[1]).toMatchObject({ execution_id: nextId, expected_sequence: 1 });
});
