import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { deadlineFromRemaining, pinnedTaskDeadline } from "@/api/endpoints";
import { metaEnvelope, tasksEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

type Overview = Schemas["TaskOverviewData"];

beforeEach(() => {
  server.use(
    http.get("*/api/v1/tasks/control-capabilities", () =>
      HttpResponse.json({
        generation_id: tasksEnvelope().serving.generation_id,
        units: [],
        can_control_scheduling: false,
        can_recover_units: false,
        can_recover_scheduling: false,
        scheduling: { available: false, note: "调度状态尚未发布。" },
        note: "任务操作尚未开放。",
      }),
    ),
  );
});

const second: Schemas["ResearchJobItem"] = {
  job_id: "00000000-0000-0000-0000-000000000002",
  strategy_name: "历史回放",
  job_type_label: "策略回放",
  resource_label: "快速",
  status: { state: "waiting", label: "排队中", reason: "正在等待研究资源" },
  progress_fraction: 0,
  terminal_shards: 0,
  total_shards: 4,
  eta_at: null,
  eta_low: null,
  eta_high: null,
  eta_label: "排队中",
  updated_at: "2026-09-24T07:29:00Z",
};

function firstResearchJob(): Schemas["ResearchJobItem"] {
  const row = tasksEnvelope().data.items[0];
  if (row === undefined) throw new Error("synthetic research job is missing");
  return row;
}

function overviewEnvelope(
  overrides: Partial<Overview> = {},
): Schemas["Envelope_TaskOverviewData_"] {
  return {
    serving: tasksEnvelope().serving,
    data: {
      can_control_research_jobs: false,
      can_view_research_logs: false,
      scheduled: {
        source_state: "ready",
        source_label: "定时任务",
        source_note: null,
        source_updated_at: "2026-09-24T07:31:00Z",
        expires_at: "2026-09-24T07:33:00Z",
        remaining_seconds: 100,
        items: [
          {
            name: "日线更新",
            status: { state: "ok", label: "正常", reason: "等待下次触发" },
            last_trigger_at: "2026-09-24T07:30:00Z",
            next_at: "2026-09-25T01:30:00Z",
            duration_seconds: null,
            result_label: "未知",
            origin_label: "待确认",
            timer_unit: "rquant-daily.timer",
            service_unit: "rquant-daily.service",
          },
        ],
      },
      services: {
        source_state: "ready",
        source_label: "运行服务",
        source_note: null,
        source_updated_at: "2026-09-24T07:31:00Z",
        items: [
          {
            name: "通知推送",
            plane_label: "实时",
            status: { state: "ok", label: "正常", reason: "心跳正常" },
            heartbeat_at: "2026-09-24T07:30:45Z",
            service_id: "notifier.admin.shadow.v1",
          },
        ],
      },
      resources: {
        source_state: "ready",
        source_label: "资源使用",
        source_note: null,
        source_updated_at: "2026-09-24T07:31:00Z",
        expires_at: "2026-09-24T07:33:00Z",
        remaining_seconds: 100,
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
            cpu_note: "暂无可信 CPU 数据",
          },
        ],
        cpu_usage_percent: null,
        cpu_note: "暂无可信 CPU 数据",
      },
      research: tasksEnvelope().data,
      scheduling: { available: false, note: "调度状态尚未发布。" },
      ...overrides,
    },
  };
}

function overviewHandler(envelope = overviewEnvelope()) {
  return http.get("*/api/v1/tasks/overview", () => HttpResponse.json(envelope));
}

describe("任务中心原请求和全局调度", () => {
  function taskCapabilities(writer = false): Schemas["TaskControlCapabilitiesData"] {
    return {
      generation_id: tasksEnvelope().serving.generation_id,
      units: [
        {
          unit: "rquant-daily.service",
          can_request: true,
          requires_confirmation: writer,
          reason: "执行前会再次核验。",
        },
      ],
      can_control_scheduling: true,
      can_recover_units: true,
      can_recover_scheduling: true,
      scheduling: {
        available: true,
        desired_version: 0,
        applied_version: 0,
        desired_paused: false,
        applied_paused: false,
        draining_count: 0,
        note: "研究调度正常。",
      },
      note: "执行前会再次核验。",
    };
  }

  it("keeps the exact original UUID after a lost unit reply and only looks it up", async () => {
    const submitted: Schemas["RequestUnitRun"][] = [];
    const looked: Schemas["RequestUnitRun"][] = [];
    server.use(
      overviewHandler(),
      http.get("*/api/v1/tasks/control-capabilities", () => HttpResponse.json(taskCapabilities())),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run", async ({ request }) => {
        submitted.push((await request.json()) as Schemas["RequestUnitRun"]);
        return HttpResponse.json({ detail: "结果待确认，请核验原请求。" }, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestUnitRun"];
        looked.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "started",
          message: "本次运行已开始。",
          can_resume: true,
          started_at: "2026-09-24T07:31:30Z",
          invocation_id: "a".repeat(32),
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    expect(await screen.findByText("运行结果待确认，请核验原请求。")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "核验日线更新原请求" }));
    expect(await screen.findByText("本次运行已开始。")).toBeVisible();
    expect(submitted).toHaveLength(1);
    expect(looked).toEqual(submitted);
    expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeDisabled();
  });

  it("global scheduling waits for applied state and never pauses an individual job", async () => {
    const controls: Schemas["SetLabSchedulingPaused"][] = [];
    let applied = false;
    const ready = overviewEnvelope({ scheduling: taskCapabilities().scheduling });
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(
          applied
            ? overviewEnvelope({
                scheduling: {
                  available: true,
                  desired_version: 1,
                  applied_version: 1,
                  desired_paused: true,
                  applied_paused: true,
                  draining_count: 0,
                  note: "研究调度已暂停。",
                },
              })
            : ready,
        ),
      ),
      http.get("*/api/v1/tasks/control-capabilities", () => HttpResponse.json(taskCapabilities())),
      http.post("*/api/v1/tasks/scheduling/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["SetLabSchedulingPaused"];
        controls.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "submitted",
          message: "请求已受理，等待调度应用。",
          can_resume: true,
          desired_version: 1,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "暂停研究调度" }));
    await user.click(await screen.findByRole("button", { name: "确认暂停" }));
    expect(await screen.findByText("请求已受理，等待调度应用。")).toBeVisible();
    expect(screen.queryByText("研究调度已暂停。")).not.toBeInTheDocument();
    expect(controls).toHaveLength(1);
    expect(controls[0]).toMatchObject({
      kind: "set_lab_scheduling_paused",
      expected_version: 0,
      paused: true,
    });
    expect(controls[0]).not.toHaveProperty("job_id");
    applied = true;
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await screen.findByText("研究调度已暂停。")).toBeVisible();
  });

  it("another administrator CAS cannot settle a lost original scheduling UUID", async () => {
    const submitted: Schemas["SetLabSchedulingPaused"][] = [];
    const looked: Schemas["SetLabSchedulingPaused"][] = [];
    let shared = taskCapabilities().scheduling;
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(overviewEnvelope({ scheduling: shared })),
      ),
      http.get("*/api/v1/tasks/control-capabilities", () =>
        HttpResponse.json({ ...taskCapabilities(), scheduling: shared }),
      ),
      http.post("*/api/v1/tasks/scheduling/commands", async ({ request }) => {
        submitted.push((await request.json()) as Schemas["SetLabSchedulingPaused"]);
        return HttpResponse.json({ detail: "结果待确认，请核验原请求。" }, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as Schemas["SetLabSchedulingPaused"];
        looked.push(body);
        if (looked.length === 1) {
          const other = { ...body, command_id: "00000000-0000-0000-0000-000000000001" };
          return HttpResponse.json({
            command_id: other.command_id,
            original_request: other,
            status: "applied",
            message: "研究调度已暂停。",
            can_resume: false,
            desired_version: 1,
          });
        }
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: looked.length === 2 ? "submitted" : "applied",
          message: looked.length === 2 ? "请求已受理，等待核验。" : "研究调度已暂停。",
          can_resume: looked.length === 2,
          desired_version: looked.length === 2 ? null : 1,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "暂停研究调度" }));
    await user.click(await screen.findByRole("button", { name: "确认暂停" }));
    expect(await screen.findByText("调度结果待确认，请核验原请求。")).toBeVisible();
    const original = submitted[0];
    if (original === undefined) throw new Error("original scheduling request missing");
    expect(submitted).toHaveLength(1);
    expect(original).toMatchObject({ expected_version: 0, paused: true });
    expect(screen.getByRole("button", { name: "恢复研究调度" })).toBeDisabled();

    shared = {
      available: true,
      desired_version: 1,
      applied_version: 1,
      desired_paused: true,
      applied_paused: true,
      draining_count: 0,
      note: "研究调度已暂停。",
    };
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await screen.findByText("研究调度已暂停。")).toBeVisible();
    expect(screen.getByText("调度结果待确认，请核验原请求。")).toBeVisible();
    expect(screen.getByRole("button", { name: "暂停研究调度" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "恢复研究调度" })).toBeDisabled();

    for (let attempt = 1; attempt <= 2; attempt += 1) {
      await user.click(screen.getByRole("button", { name: "核验调度原请求" }));
      await waitFor(() => expect(looked).toHaveLength(attempt));
      await waitFor(() =>
        expect(screen.getByRole("button", { name: "核验调度原请求" })).toBeEnabled(),
      );
      expect(screen.getByRole("button", { name: "恢复研究调度" })).toBeDisabled();
      expect(looked[attempt - 1]).toEqual(original);
      expect(submitted).toHaveLength(1);
    }
    expect(await screen.findByText("请求已受理，等待核验。")).toBeVisible();
    await user.click(screen.getByRole("button", { name: "核验调度原请求" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "恢复研究调度" })).toBeEnabled());
    expect(screen.queryByRole("button", { name: "核验调度原请求" })).not.toBeInTheDocument();
    expect(looked).toEqual([original, original, original]);
    expect(submitted).toEqual([original]);
  });

  it("writer preparation binds the original run and cancellation sends no start", async () => {
    const preparations: Schemas["PrepareUnitRun"][] = [];
    const runs: Schemas["RequestUnitRun"][] = [];
    const csrf: (string | null)[] = [];
    server.use(
      overviewHandler(),
      http.get("*/api/v1/tasks/control-capabilities", () =>
        HttpResponse.json(taskCapabilities(true)),
      ),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run/prepare", async ({ request }) => {
        const body = (await request.json()) as Schemas["PrepareUnitRun"];
        preparations.push(body);
        csrf.push(request.headers.get("X-Rquant-Csrf"));
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "prepared",
          message: "准备已完成，请确认本次运行。",
          can_resume: false,
          confirmation_id: "00000000-0000-4000-8000-000000000003",
          confirmation_expires_at: new Date(Date.now() + 60_000).toISOString(),
        });
      }),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestUnitRun"];
        runs.push(body);
        csrf.push(request.headers.get("X-Rquant-Csrf"));
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "started",
          message: "本次运行已开始。",
          can_resume: true,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    const first = await screen.findByRole("dialog", { name: "运行日线更新" });
    expect(within(first).getByRole("button", { name: "确认运行" })).toBeDisabled();
    await user.click(within(first).getByRole("button", { name: /取\s*消/ }));
    expect(runs).toHaveLength(0);
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toHaveFocus(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    const second = await screen.findByRole("dialog", { name: "运行日线更新" });
    await user.type(within(second).getByRole("textbox"), "日线更新");
    await user.click(within(second).getByRole("button", { name: "确认运行" }));
    expect(await screen.findByText("本次运行已开始。")).toBeVisible();
    expect(preparations).toHaveLength(2);
    expect(runs).toHaveLength(1);
    expect(runs[0]).toMatchObject({
      ...preparations[1]?.run,
      confirmation_id: "00000000-0000-4000-8000-000000000003",
    });
    expect(runs[0]?.command_id).not.toBe(preparations[0]?.run.command_id);
    expect(csrf).toEqual(["1", "1", "1"]);
  });

  it("a definite 422 refusal releases the run control instead of locking unknown", async () => {
    let attempts = 0;
    server.use(
      overviewHandler(),
      http.get("*/api/v1/tasks/control-capabilities", () => HttpResponse.json(taskCapabilities())),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run", () => {
        attempts += 1;
        return HttpResponse.json({ detail: "private-implementation-error" }, { status: 422 });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    expect(await screen.findByText("请求已拒绝，请刷新状态后再操作。")).toBeVisible();
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    expect(screen.queryByRole("button", { name: "核验日线更新原请求" })).toBeNull();
    expect(attempts).toBe(1);
    expect(document.body).not.toHaveTextContent("private-implementation-error");
  });

  it("retains original recovery when the current scheduled source loses its row", async () => {
    const submitted: Schemas["RequestUnitRun"][] = [];
    const looked: Schemas["RequestUnitRun"][] = [];
    let missing = false;
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(
          missing
            ? overviewEnvelope({
                scheduled: {
                  ...overviewEnvelope().data.scheduled,
                  source_state: "unavailable",
                  source_label: "任务状态暂不可用",
                  items: [],
                  remaining_seconds: null,
                },
              })
            : overviewEnvelope(),
        ),
      ),
      http.get("*/api/v1/tasks/control-capabilities", () =>
        HttpResponse.json(missing ? { ...taskCapabilities(), units: [] } : taskCapabilities()),
      ),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run", async ({ request }) => {
        submitted.push((await request.json()) as Schemas["RequestUnitRun"]);
        return HttpResponse.json({ detail: "unknown" }, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestUnitRun"];
        looked.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "unknown",
          message: "本次结果待确认。",
          can_resume: true,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    expect(await screen.findByText("运行结果待确认，请核验原请求。")).toBeVisible();
    missing = true;
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await screen.findByText("任务状态暂不可用")).toBeVisible();
    await user.click(await screen.findByRole("button", { name: "核验日线更新原请求" }));
    expect(await screen.findByText("本次结果待确认。")).toBeVisible();
    expect(looked).toEqual(submitted);
    expect(submitted).toHaveLength(1);
    expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeDisabled();
  });

  it("a new same-owner generation keeps only the old UUID recovery and never retargets it", async () => {
    const oldGeneration = tasksEnvelope().serving.generation_id;
    let generation = oldGeneration;
    const submitted: Schemas["RequestUnitRun"][] = [];
    const looked: Schemas["RequestUnitRun"][] = [];
    const grants: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json({
          ...overviewEnvelope(),
          serving: { ...tasksEnvelope().serving, generation_id: generation },
        }),
      ),
      http.get("*/api/v1/tasks/control-capabilities", ({ request }) => {
        grants.push(new URL(request.url).searchParams.get("generation_id") ?? "");
        return HttpResponse.json({ ...taskCapabilities(), generation_id: generation });
      }),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run", async ({ request }) => {
        submitted.push((await request.json()) as Schemas["RequestUnitRun"]);
        return HttpResponse.json({}, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestUnitRun"];
        looked.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "unknown",
          message: "原代结果待确认。",
          can_resume: true,
        });
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    expect(await screen.findByText("运行结果待确认，请核验原请求。")).toBeVisible();
    generation = "b".repeat(64);
    queryClient.setQueryData(["meta"], metaEnvelope({ generationId: generation }));
    await waitFor(() => expect(grants).toContain(generation));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "核验日线更新原请求" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "核验日线更新原请求" }));
    expect(await screen.findByText("原代结果待确认。")).toBeVisible();
    expect(looked).toEqual(submitted);
    expect(looked[0]?.generation_id).toBe(oldGeneration);
    expect(submitted).toHaveLength(1);
  });

  it("a viewer change drops a late unit reply and clears the previous private UUID", async () => {
    let viewer = "alice";
    const deferred: { release: (() => void) | null } = { release: null };
    const submitted: Schemas["RequestUnitRun"][] = [];
    server.use(
      overviewHandler(),
      http.get("*/api/v1/tasks/control-capabilities", () =>
        HttpResponse.json(
          viewer === "alice"
            ? taskCapabilities()
            : {
                ...taskCapabilities(),
                units: [],
                can_control_scheduling: false,
                can_recover_units: false,
                can_recover_scheduling: false,
              },
        ),
      ),
      http.post("*/api/v1/tasks/units/rquant-daily.service/run", async ({ request }) => {
        const body = (await request.json()) as Schemas["RequestUnitRun"];
        submitted.push(body);
        await new Promise<void>((resolve) => {
          deferred.release = resolve;
        });
        return HttpResponse.json({
          command_id: body.command_id,
          original_request: body,
          status: "started",
          message: "此前本人的敏感回执",
          can_resume: true,
        });
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行日线更新" })).toBeEnabled(),
    );
    await user.click(screen.getByRole("button", { name: "立即运行日线更新" }));
    await user.click(await screen.findByRole("button", { name: "确认运行" }));
    await waitFor(() => expect(submitted).toHaveLength(1));
    viewer = "bob";
    queryClient.setQueryData(["meta"], metaEnvelope({ viewer }));
    await waitFor(() =>
      expect(screen.queryByRole("button", { name: "核验日线更新原请求" })).toBeNull(),
    );
    if (deferred.release === null) throw new Error("unit request was not actually pending");
    deferred.release();
    await user.click(await screen.findByRole("button", { name: "刷新" }));
    await waitFor(() =>
      expect(screen.queryByRole("button", { name: "立即运行日线更新" })).toBeNull(),
    );
    expect(document.body).not.toHaveTextContent("此前本人的敏感回执");
    expect(submitted).toHaveLength(1);
  });

  it("renders measured zero CPU for the host and each group without turning unknown into zero", async () => {
    const original = overviewEnvelope().data.resources;
    const firstGroup = original.groups[0];
    if (!firstGroup) throw new Error("synthetic resource group is missing");
    server.use(
      overviewHandler(
        overviewEnvelope({
          resources: {
            ...original,
            cpu_usage_percent: 0,
            groups: [
              { ...firstGroup, cpu_usage_percent: 0 },
              {
                name: "维护任务",
                slice_unit: "rquant-maintenance.slice",
                memory_current_bytes: 0,
                memory_peak_bytes: 0,
                cpu_usage_percent: null,
                cpu_note: "空组尚无完整计数",
              },
            ],
          },
        }),
      ),
    );
    renderApp("/tasks");
    const groups = await screen.findByRole("table", { name: "资源分组" });
    expect(within(groups).getByRole("row", { name: /实时服务/ })).toHaveTextContent("0.0%");
    expect(within(groups).getByRole("row", { name: /维护任务/ })).not.toHaveTextContent("0.0%");
    expect(screen.getByRole("region", { name: "资源概况" })).toHaveTextContent("CPU0.0%");
  });

  it("keeps the exact invocation in every result-log request and rejects a swapped log scope", async () => {
    const original = overviewEnvelope().data.scheduled;
    const firstTask = original.items[0];
    if (!firstTask) throw new Error("synthetic scheduled task is missing");
    const invocation = "d".repeat(32);
    const requests: URL[] = [];
    server.use(
      overviewHandler(
        overviewEnvelope({
          scheduled: {
            ...original,
            items: [
              {
                ...firstTask,
                invocation_id: invocation,
                origin_label: "手动运行",
                result_label: "已完成",
              },
            ],
          },
        }),
      ),
      http.get("*/api/v1/tasks/services/log-capabilities", () =>
        HttpResponse.json({ units: ["rquant-daily.service"] }),
      ),
      http.get("*/api/v1/tasks/services/:unit/logs", ({ request }) => {
        requests.push(new URL(request.url));
        return HttpResponse.json({
          service_label: "每日任务",
          scope: "本机本次开机以来的服务日志（含手动运行）",
          invocation_id: "e".repeat(32),
          entries: [{ at: "2026-09-24T07:31:00Z", level: "信息", text: "任务已完成" }],
          next_cursor: null,
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
    expect(await screen.findByText("日志已更新，请重新查看。")).toBeVisible();
    expect(requests).toHaveLength(1);
    expect(requests[0]?.searchParams.get("invocation_id")).toBe(invocation);
    expect(screen.queryByText("任务已完成")).toBeNull();
  });
});

function eventsData(
  overrides: Partial<Schemas["ResearchTaskEventsData"]> = {},
): Schemas["ResearchTaskEventsData"] {
  return {
    state: "ready",
    note: "任务进展",
    generation_id: tasksEnvelope().serving.generation_id,
    updated_at: "2026-09-24T07:31:00Z",
    truncated: false,
    events: [
      {
        event_id: 1,
        label: "任务已开始",
        status_label: "运行中",
        occurred_at: "2026-09-24T07:30:00Z",
      },
    ],
    ...overrides,
  };
}

describe("任务与运行状态总览", () => {
  beforeEach(() => server.use(overviewHandler()));

  it("uses one overview request for four sections without exposing IDs or inventing results", async () => {
    const user = userEvent.setup();
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", ({ request }) => {
        requests.push(new URL(request.url).pathname);
        return HttpResponse.json(overviewEnvelope());
      }),
    );
    renderApp("/tasks");
    const schedule = await screen.findByRole("table", { name: "定时任务" });
    expect(schedule).toHaveTextContent("日线更新");
    expect(schedule).toHaveTextContent("09-24 15:30");
    expect(schedule).toHaveTextContent("09-25 09:30");
    expect(schedule).toHaveTextContent("未知");
    expect(schedule).toHaveTextContent("—");
    expect(screen.getByRole("table", { name: "运行服务" })).toHaveTextContent("通知推送");
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("动量参数搜索");
    expect(screen.getByRole("region", { name: "资源概况" })).toHaveTextContent("1.00 GiB");
    expect(screen.getByRole("region", { name: "资源概况" })).toHaveTextContent("2.00 GiB");
    expect(screen.getByText("暂无可信 CPU 数据")).toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("CPU 0%");
    expect(requests).toHaveLength(1);
    expect(screen.queryByRole("button", { name: /暂停|日志|立即运行/ })).toBeNull();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
    await user.hover(within(schedule).getByText("日线更新"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("rquant-daily.timer");
  });

  it("pages research within the same overview and refreshes from the first page", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        requests.push(cursor ?? "first");
        return HttpResponse.json(
          overviewEnvelope({
            research: cursor
              ? { ...tasksEnvelope().data, items: [second], next_cursor: null }
              : tasksEnvelope().data,
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await screen.findByRole("table", { name: "研究任务队列" });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await screen.findByText("第 2 页");
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("历史回放");
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await screen.findByText("第 1 页");
    await waitFor(() => expect(requests.at(-1)).toBe("first"));
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("动量参数搜索");
    expect(requests).toEqual(["first", "fixture-next", "first"]);
  });

  it("clears every old section on cursor 409 and automatically fetches a fresh first page", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        requests.push(cursor ?? "first");
        if (cursor) {
          return HttpResponse.json(
            { detail: "任务数据已更新，请从第一页重新查看。" },
            { status: 409 },
          );
        }
        const fresh = requests.length > 2;
        return HttpResponse.json(
          overviewEnvelope({
            scheduled: {
              ...overviewEnvelope().data.scheduled,
              items: fresh ? [] : overviewEnvelope().data.scheduled.items,
            },
            research: fresh
              ? { ...tasksEnvelope().data, items: [second], next_cursor: null }
              : tasksEnvelope().data,
          }),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await screen.findByText("日线更新");
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await waitFor(() => expect(requests).toEqual(["first", "fixture-next", "first"]));
    expect(await screen.findByText("历史回放")).toBeInTheDocument();
    expect(screen.queryByText("日线更新")).toBeNull();
    expect(screen.queryByText("动量参数搜索")).toBeNull();
    expect(screen.getByText("第 1 页")).toBeInTheDocument();
  });

  it("keeps independent sections visible when ops data is unavailable", async () => {
    const base = overviewEnvelope().data;
    server.use(
      overviewHandler(
        overviewEnvelope({
          scheduled: {
            ...base.scheduled,
            source_state: "unavailable",
            source_label: "定时任务暂不可用",
            source_note: "任务状态已过期，等待下一次更新。",
            remaining_seconds: 0,
            items: [],
          },
          resources: {
            ...base.resources,
            source_state: "unavailable",
            source_label: "资源状态暂不可用",
            remaining_seconds: null,
            rquant_memory_current_bytes: null,
            rquant_memory_peak_bytes: null,
            host_memory_total_bytes: null,
            host_memory_available_bytes: null,
            groups: [],
          },
        }),
      ),
    );
    renderApp("/tasks");
    expect(await screen.findByText("定时任务暂不可用")).toBeInTheDocument();
    expect(screen.getByText("资源状态暂不可用")).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "运行服务" })).toHaveTextContent("通知推送");
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("动量参数搜索");
    expect(screen.queryByText("1.00 GiB")).toBeNull();
  });

  it("keeps current ops visible when services or research have no source", async () => {
    const base = overviewEnvelope().data;
    server.use(
      overviewHandler(
        overviewEnvelope({
          services: {
            ...base.services,
            source_state: "unavailable",
            source_label: "运行服务暂不可用",
            source_note: "服务状态等待发布。",
            items: [],
          },
          research: {
            ...base.research,
            source_state: "not_published",
            source_label: "研究任务尚未发布",
            source_note: null,
            total: null,
            counts: null,
            items: [],
            next_cursor: null,
          },
        }),
      ),
    );
    renderApp("/tasks");
    expect(await screen.findByText("运行服务暂不可用")).toBeInTheDocument();
    expect(screen.getByText("研究任务尚未发布")).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "定时任务" })).toHaveTextContent("日线更新");
    expect(screen.getByRole("region", { name: "资源概况" })).toHaveTextContent("1.00 GiB");
    expect(screen.queryByRole("table", { name: "运行服务" })).toBeNull();
    expect(screen.queryByRole("region", { name: "任务状态概况" })).toBeNull();
  });

  it("expires ops on monotonic time without clearing services or research", async () => {
    const base = overviewEnvelope().data;
    server.use(
      overviewHandler(
        overviewEnvelope({
          scheduled: { ...base.scheduled, remaining_seconds: 0.5 },
          resources: { ...base.resources, remaining_seconds: 0.5 },
        }),
      ),
    );
    renderApp("/tasks");
    expect(await screen.findByText("日线更新")).toBeInTheDocument();
    await waitFor(() => expect(screen.queryByText("日线更新")).toBeNull(), { timeout: 2000 });
    expect(screen.getByText("定时任务状态已过期")).toBeInTheDocument();
    expect(screen.getByText("资源状态已过期")).toBeInTheDocument();
    expect(screen.getByRole("table", { name: "运行服务" })).toHaveTextContent("通知推送");
    expect(screen.getByRole("table", { name: "研究任务队列" })).toHaveTextContent("动量参数搜索");
  });

  it("subtracts in-flight time from the server TTL and reports whole-request errors", async () => {
    expect(deadlineFromRemaining("unavailable", null, 100, 500)).toBeNull();
    expect(deadlineFromRemaining("ready", 1, 100, 700)).toBe(1100);
    expect(deadlineFromRemaining("ready", 0.2, 100, 700)).toBe(700);
    expect(pinnedTaskDeadline({ key: "same", deadline: 1100 }, "same", 1600)).toBe(1100);
    expect(pinnedTaskDeadline({ key: "same", deadline: 1100 }, "new", 1600)).toBe(1600);
    server.use(http.get("*/api/v1/tasks/overview", () => HttpResponse.error()));
    renderApp("/tasks");
    expect(await screen.findByText("任务总览暂时无法加载")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "研究任务队列" })).toBeNull();
  });
});

describe("研究任务控制", () => {
  beforeEach(() => {
    window.localStorage.clear();
    server.use(
      overviewHandler(
        overviewEnvelope({
          can_control_research_jobs: true,
          research: {
            ...tasksEnvelope().data,
            items: [
              {
                ...firstResearchJob(),
                job_version: 7,
                available_actions: ["pause", "cancel"],
              },
            ],
          },
        }),
      ),
      http.get("*/api/v1/tasks/jobs/control-capabilities", () =>
        HttpResponse.json({ can_control: true }),
      ),
    );
  });

  it("keeps the original command after an uncertain reply and retries the identical request", async () => {
    const user = userEvent.setup();
    const bodies: Schemas["LabControlRequest"][] = [];
    server.use(
      http.post("*/api/v1/tasks/jobs/commands", async ({ request }) => {
        bodies.push((await request.json()) as Schemas["LabControlRequest"]);
        return bodies.length === 1
          ? HttpResponse.json({ detail: "提交状态待确认" }, { status: 503 })
          : HttpResponse.json({
              command_id: bodies[0]?.command_id,
              status: "submitted",
              message: "已提交，等待状态更新。",
            });
      }),
    );
    const { unmount } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "暂停动量参数搜索" }));
    expect(await screen.findByText("提交状态待确认，请查询或重试原请求。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "刷新任务" })).toBeNull();
    expect(bodies).toHaveLength(1);
    unmount();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查询或重试动量参数搜索" }));
    await screen.findByText("已提交，等待状态更新。");
    expect(bodies).toHaveLength(2);
    expect(bodies[1]).toEqual(bodies[0]);
    expect(bodies[0]?.action).toBe("pause");
    expect(bodies[0]?.expected_version).toBe(7);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("rechecks current task and grant before a new request after terminal failure", async () => {
    const user = userEvent.setup();
    let overviewReads = 0;
    let grantReads = 0;
    const bodies: Schemas["LabControlRequest"][] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", () => {
        overviewReads += 1;
        return HttpResponse.json(
          overviewEnvelope({
            can_control_research_jobs: true,
            research: {
              ...tasksEnvelope().data,
              items: [
                { ...firstResearchJob(), job_version: 7, available_actions: ["pause", "cancel"] },
              ],
            },
          }),
        );
      }),
      http.get("*/api/v1/tasks/jobs/control-capabilities", () => {
        grantReads += 1;
        return HttpResponse.json({ can_control: true });
      }),
      http.post("*/api/v1/tasks/jobs/commands", async ({ request }) => {
        bodies.push((await request.json()) as Schemas["LabControlRequest"]);
        return HttpResponse.json(
          bodies.length === 1
            ? {
                command_id: bodies[0]?.command_id,
                status: "failed",
                message: "设备时间可能不准，请校准后刷新任务。",
              }
            : {
                command_id: bodies[1]?.command_id,
                status: "submitted",
                message: "已提交，等待状态更新。",
              },
        );
      }),
    );
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "暂停动量参数搜索" }));
    expect(await screen.findByText("设备时间可能不准，请校准后刷新任务。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "查询或重试动量参数搜索" })).toBeNull();
    expect(screen.queryByRole("button", { name: "暂停动量参数搜索" })).toBeNull();
    const before = [overviewReads, grantReads];
    await user.click(screen.getByRole("button", { name: "刷新任务" }));
    await waitFor(() => {
      expect(overviewReads).toBeGreaterThan(before[0] ?? 0);
      expect(grantReads).toBeGreaterThan(before[1] ?? 0);
    });
    expect(await screen.findByText("任务状态已刷新，请核对后再操作。")).toBeInTheDocument();
    expect(bodies).toHaveLength(1);
    await user.click(screen.getByRole("button", { name: "已核对" }));
    await user.click(await screen.findByRole("button", { name: "暂停动量参数搜索" }));
    await screen.findByText("已提交，等待状态更新。");
    expect(bodies).toHaveLength(2);
    expect(bodies[1]?.command_id).not.toBe(bodies[0]?.command_id);
    expect(bodies[1]?.expected_version).toBe(7);
  });

  it("does not rearm an action removed from the refreshed task", async () => {
    const user = userEvent.setup();
    let actions: Schemas["ResearchJobItem"]["available_actions"] = ["pause"];
    const bodies: unknown[] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(
          overviewEnvelope({
            can_control_research_jobs: true,
            research: {
              ...tasksEnvelope().data,
              items: [{ ...firstResearchJob(), job_version: 7, available_actions: actions }],
            },
          }),
        ),
      ),
      http.post("*/api/v1/tasks/jobs/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["LabControlRequest"];
        bodies.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          status: "failed",
          message: "本次请求失败，请刷新任务后再确认。",
        });
      }),
    );
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "暂停动量参数搜索" }));
    await screen.findByText("本次请求失败，请刷新任务后再确认。");
    actions = [];
    await user.click(screen.getByRole("button", { name: "刷新任务" }));
    await screen.findByText("任务状态已刷新，请核对后再操作。");
    await user.click(screen.getByRole("button", { name: "已核对" }));
    expect(screen.queryByRole("button", { name: "暂停动量参数搜索" })).toBeNull();
    expect(bodies).toHaveLength(1);
  });

  it("keeps the original request available after the published task version changes", async () => {
    const user = userEvent.setup();
    let version = 7;
    const bodies: Schemas["LabControlRequest"][] = [];
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(
          overviewEnvelope({
            can_control_research_jobs: true,
            research: {
              ...tasksEnvelope().data,
              items: [
                {
                  ...firstResearchJob(),
                  job_version: version,
                  available_actions: ["pause", "cancel"],
                },
              ],
            },
          }),
        ),
      ),
      http.post("*/api/v1/tasks/jobs/commands", async ({ request }) => {
        bodies.push((await request.json()) as Schemas["LabControlRequest"]);
        return HttpResponse.json({ detail: "提交状态待确认" }, { status: 503 });
      }),
    );
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "暂停动量参数搜索" }));
    await screen.findByText("提交状态待确认，请查询或重试原请求。");
    version = 8;
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await screen.findByText("任务状态已更新，请核对结果。");
    await user.click(screen.getByRole("button", { name: "查询或重试动量参数搜索" }));
    await waitFor(() => expect(bodies).toHaveLength(2));
    expect(bodies[1]).toEqual(bodies[0]);
  });

  it("confirms cancellation before posting", async () => {
    const user = userEvent.setup();
    const posts: unknown[] = [];
    server.use(
      http.post("*/api/v1/tasks/jobs/commands", async ({ request }) => {
        posts.push(await request.json());
        return HttpResponse.json({ detail: "待确认" }, { status: 503 });
      }),
    );
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "取消动量参数搜索" }));
    expect(posts).toHaveLength(0);
    expect(screen.getByRole("dialog")).toHaveTextContent("取消后无法继续当前任务");
    await user.click(screen.getByRole("button", { name: "确认取消" }));
    await waitFor(() => expect(posts).toHaveLength(1));
  });

  it("keeps old generations read only even for an operator", async () => {
    server.use(
      overviewHandler(
        overviewEnvelope({
          can_control_research_jobs: true,
          research: {
            ...tasksEnvelope().data,
            items: [firstResearchJob()],
          },
        }),
      ),
    );
    renderApp("/tasks");
    await screen.findByRole("table", { name: "研究任务队列" });
    expect(screen.queryByRole("button", { name: /暂停动量参数搜索|取消动量参数搜索/ })).toBeNull();
  });

  it("removes controls when the independent live grant is revoked", async () => {
    let granted = true;
    server.use(
      http.get("*/api/v1/tasks/jobs/control-capabilities", () =>
        HttpResponse.json({ can_control: granted }),
      ),
    );
    const { queryClient } = renderApp("/tasks");
    expect(await screen.findByRole("button", { name: "暂停动量参数搜索" })).toBeInTheDocument();
    granted = false;
    await queryClient.invalidateQueries({ queryKey: ["tasks", "lab-control-capabilities"] });
    await waitFor(() =>
      expect(screen.queryByRole("button", { name: "暂停动量参数搜索" })).toBeNull(),
    );
  });

  it("hides controls if the live grant cannot be refreshed", async () => {
    let reachable = true;
    server.use(
      http.get("*/api/v1/tasks/jobs/control-capabilities", () =>
        reachable ? HttpResponse.json({ can_control: true }) : HttpResponse.error(),
      ),
    );
    const { queryClient } = renderApp("/tasks");
    expect(await screen.findByRole("button", { name: "暂停动量参数搜索" })).toBeInTheDocument();
    reachable = false;
    await queryClient.invalidateQueries({ queryKey: ["tasks", "lab-control-capabilities"] });
    await waitFor(() =>
      expect(screen.queryByRole("button", { name: "暂停动量参数搜索" })).toBeNull(),
    );
  });
});

describe("服务运行日志", () => {
  beforeEach(() => server.use(overviewHandler()));

  it("keeps every entry hidden until an independent capability names the exact unit", async () => {
    renderApp("/tasks");
    await screen.findByRole("table", { name: "定时任务" });
    expect(screen.queryByRole("button", { name: /运行日志/ })).toBeNull();
  });

  it("opens only matching timer and service rows, paginates, filters, and restores focus", async () => {
    const requests: URL[] = [];
    server.use(
      overviewHandler(
        overviewEnvelope({
          services: {
            ...overviewEnvelope().data.services,
            items: [
              {
                name: "备份服务",
                plane_label: "维护",
                status: { state: "ok", label: "正常", reason: "正常" },
                heartbeat_at: "2026-09-24T07:30:45Z",
                service_id: "rquant-backup.service",
              },
            ],
          },
        }),
      ),
      http.get("*/api/v1/tasks/services/log-capabilities", () =>
        HttpResponse.json({ units: ["rquant-daily.service", "rquant-backup.service"] }),
      ),
      http.get("*/api/v1/tasks/services/:unit/logs", ({ request }) => {
        requests.push(new URL(request.url));
        const cursor = new URL(request.url).searchParams.get("cursor");
        return HttpResponse.json({
          service_label: "每日任务",
          scope: "本机本次开机以来的服务日志（含手动运行）",
          entries: [
            {
              at: "2026-09-28T04:00:00Z",
              level: "信息",
              text: cursor ? "任务已完成" : "任务已开始",
            },
          ],
          next_cursor: cursor ? null : "signed-page-cursor",
          raw_message: "Bearer private token",
        });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    const timerButton = await screen.findByRole("button", { name: "查看日线更新的运行日志" });
    expect(screen.getByRole("button", { name: "查看备份服务的运行日志" })).toBeInTheDocument();
    await user.click(timerButton);
    const dialog = await screen.findByRole("dialog", { name: /本机本次开机以来的服务日志/ });
    expect(within(dialog).getByText("任务已开始")).toBeInTheDocument();
    expect(within(dialog).getByText("2026-09-28 12:00:00")).toBeInTheDocument();
    expect(
      within(within(dialog).getByRole("list", { name: "服务日志" })).getByText("信息"),
    ).toBeInTheDocument();
    expect(dialog).not.toHaveTextContent("rquant-daily.service");
    expect(dialog).not.toHaveTextContent("Bearer private token");
    await user.click(within(dialog).getByRole("button", { name: "加载更早记录" }));
    expect(await within(dialog).findByText("任务已完成")).toBeInTheDocument();
    expect(requests[1]?.searchParams.get("cursor")).toBe("signed-page-cursor");
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "日志级别" }), "warning");
    await waitFor(() => expect(within(dialog).queryByText("任务已完成")).toBeNull());
    expect(requests.at(-1)?.searchParams.get("level")).toBe("warning");
    expect(requests.at(-1)?.searchParams.get("cursor")).toBeNull();
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "时间范围" }), "hour");
    expect(requests.at(-1)?.searchParams.has("since")).toBe(true);
    await user.keyboard("{Escape}");
    await waitFor(() => expect(timerButton).toHaveFocus());
  });

  it("clears old pages on 409 and revokes an open drawer on permission loss", async () => {
    let status = 200;
    let units = ["rquant-daily.service"];
    server.use(
      http.get("*/api/v1/tasks/services/log-capabilities", () => HttpResponse.json({ units })),
      http.get("*/api/v1/tasks/services/:unit/logs", () =>
        status === 200
          ? HttpResponse.json({
              service_label: "每日任务",
              scope: "本机本次开机以来的服务日志（含手动运行）",
              entries: [{ at: "2026-09-28T04:00:00Z", level: "信息", text: "任务已完成" }],
              next_cursor: "signed-page-cursor",
            })
          : HttpResponse.json({ detail: "private-journal-message" }, { status }),
      ),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    const button = await screen.findByRole("button", { name: "查看日线更新的运行日志" });
    await user.click(button);
    expect(await screen.findByText("任务已完成")).toBeInTheDocument();
    status = 409;
    await user.click(screen.getByRole("button", { name: "加载更早记录" }));
    expect(await screen.findByText("日志已更新，请重新查看。")).toBeInTheDocument();
    expect(screen.queryByText("任务已完成")).toBeNull();
    expect(document.body).not.toHaveTextContent("private-journal-message");
    units = [];
    await queryClient.invalidateQueries({ queryKey: ["tasks", "service-log-capabilities"] });
    await waitFor(() => expect(screen.queryByRole("dialog", { name: /服务日志/ })).toBeNull());
    expect(screen.queryByRole("button", { name: "查看日线更新的运行日志" })).toBeNull();
    await waitFor(() => expect(screen.getByRole("button", { name: "刷新" })).toHaveFocus());
  });

  it("drops the open page when a capability refresh withdraws its unit", async () => {
    let units = ["rquant-daily.service"];
    server.use(
      http.get("*/api/v1/tasks/services/log-capabilities", () => HttpResponse.json({ units })),
      http.get("*/api/v1/tasks/services/:unit/logs", () =>
        HttpResponse.json({
          service_label: "每日任务",
          scope: "本机本次开机以来的服务日志（含手动运行）",
          entries: [{ at: "2026-09-28T04:00:00Z", level: "信息", text: "任务已完成" }],
          next_cursor: "signed-page-cursor",
        }),
      ),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
    expect(await screen.findByText("任务已完成")).toBeInTheDocument();
    units = [];
    await queryClient.invalidateQueries({ queryKey: ["tasks", "service-log-capabilities"] });
    await waitFor(() => expect(screen.queryByText("任务已完成")).toBeNull());
    expect(screen.queryByRole("dialog", { name: /服务日志/ })).toBeNull();
    expect(screen.queryByRole("button", { name: "查看日线更新的运行日志" })).toBeNull();
    await waitFor(() => expect(screen.getByRole("button", { name: "刷新" })).toHaveFocus());
  });

  it("expires a seven-day continuation before sending an out-of-range since", async () => {
    const instant = Date.now();
    const clock = vi.spyOn(Date, "now").mockReturnValue(instant);
    const requests: URL[] = [];
    try {
      server.use(
        http.get("*/api/v1/tasks/services/log-capabilities", () =>
          HttpResponse.json({ units: ["rquant-daily.service"] }),
        ),
        http.get("*/api/v1/tasks/services/:unit/logs", ({ request }) => {
          requests.push(new URL(request.url));
          return HttpResponse.json({
            service_label: "每日任务",
            scope: "本机本次开机以来的服务日志（含手动运行）",
            entries: [{ at: "2026-09-28T04:00:00Z", level: "信息", text: "任务已开始" }],
            next_cursor: "signed-page-cursor",
          });
        }),
      );
      const user = userEvent.setup();
      renderApp("/tasks");
      await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
      const dialog = await screen.findByRole("dialog", { name: /服务日志/ });
      await user.selectOptions(within(dialog).getByRole("combobox", { name: "时间范围" }), "week");
      await waitFor(() => expect(requests).toHaveLength(2));
      const oldSince = requests[1]?.searchParams.get("since");
      expect(oldSince).not.toBeNull();
      clock.mockReturnValue(Date.parse(oldSince ?? "") + 7 * 86_400_000 + 1000);
      await user.click(within(dialog).getByRole("button", { name: "加载更早记录" }));
      expect(
        await within(dialog).findByText("日志筛选范围已过期，请重新查看。"),
      ).toBeInTheDocument();
      expect(within(dialog).queryByText("任务已开始")).toBeNull();
      expect(requests).toHaveLength(2);
      await user.click(within(dialog).getByRole("button", { name: "重新查看" }));
      await waitFor(() => expect(requests).toHaveLength(3));
      expect(requests[2]?.searchParams.get("cursor")).toBeNull();
      expect(Date.parse(requests[2]?.searchParams.get("since") ?? "")).toBeGreaterThan(
        Date.parse(oldSince ?? ""),
      );
    } finally {
      clock.mockRestore();
    }
  });

  it("recovers a seven-day page rejected as expired by the server", async () => {
    const requests: URL[] = [];
    server.use(
      http.get("*/api/v1/tasks/services/log-capabilities", () =>
        HttpResponse.json({ units: ["rquant-daily.service"] }),
      ),
      http.get("*/api/v1/tasks/services/:unit/logs", ({ request }) => {
        const url = new URL(request.url);
        requests.push(url);
        return url.searchParams.has("cursor")
          ? HttpResponse.json({ detail: "private validation detail" }, { status: 422 })
          : HttpResponse.json({
              service_label: "每日任务",
              scope: "本机本次开机以来的服务日志（含手动运行）",
              entries: [{ at: "2026-09-28T04:00:00Z", level: "信息", text: "任务已开始" }],
              next_cursor: "signed-page-cursor",
            });
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
    const dialog = await screen.findByRole("dialog", { name: /服务日志/ });
    await user.selectOptions(within(dialog).getByRole("combobox", { name: "时间范围" }), "week");
    await within(dialog).findByText("任务已开始");
    await user.click(within(dialog).getByRole("button", { name: "加载更早记录" }));
    expect(await within(dialog).findByText("日志筛选范围已过期，请重新查看。")).toBeInTheDocument();
    expect(within(dialog).queryByText("任务已开始")).toBeNull();
    await user.click(within(dialog).getByRole("button", { name: "重新查看" }));
    await waitFor(() => expect(requests).toHaveLength(4));
    expect(requests[3]?.searchParams.get("cursor")).toBeNull();
    expect(document.body).not.toHaveTextContent("private validation detail");
  });

  it.each([
    { filterName: "时间范围", value: "day" },
    { filterName: "日志级别", value: "warning" },
  ])(
    "starts a fresh page when changing $filterName after expiry",
    async ({ filterName, value }) => {
      const instant = Date.now();
      const clock = vi.spyOn(Date, "now").mockReturnValue(instant);
      const requests: URL[] = [];
      try {
        server.use(
          http.get("*/api/v1/tasks/services/log-capabilities", () =>
            HttpResponse.json({ units: ["rquant-daily.service"] }),
          ),
          http.get("*/api/v1/tasks/services/:unit/logs", ({ request }) => {
            const url = new URL(request.url);
            requests.push(url);
            if (url.searchParams.has("cursor")) {
              return HttpResponse.json({ detail: "private validation detail" }, { status: 422 });
            }
            return HttpResponse.json({
              service_label: "每日任务",
              scope: "本机本次开机以来的服务日志（含手动运行）",
              entries: [
                {
                  at: "2026-09-28T04:00:00Z",
                  level: "信息",
                  text: requests.length === 4 ? "任务已完成" : "任务已开始",
                },
              ],
              next_cursor: "signed-page-cursor",
            });
          }),
        );
        const user = userEvent.setup();
        renderApp("/tasks");
        await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
        const dialog = await screen.findByRole("dialog", { name: /服务日志/ });
        await user.selectOptions(
          within(dialog).getByRole("combobox", { name: "时间范围" }),
          "week",
        );
        await waitFor(() => expect(requests).toHaveLength(2));
        const oldSince = requests[1]?.searchParams.get("since");
        await user.click(within(dialog).getByRole("button", { name: "加载更早记录" }));
        expect(
          await within(dialog).findByText("日志筛选范围已过期，请重新查看。"),
        ).toBeInTheDocument();
        expect(within(dialog).queryByText("任务已开始")).toBeNull();

        clock.mockReturnValue(instant + 1000);
        await user.selectOptions(within(dialog).getByRole("combobox", { name: filterName }), value);
        expect(within(dialog).queryByText("所选范围还没有可显示的日志。")).toBeNull();
        expect(await within(dialog).findByText("任务已完成")).toBeInTheDocument();
        expect(within(dialog).queryByText("任务已开始")).toBeNull();
        expect(requests).toHaveLength(4);
        expect(requests[3]?.searchParams.get("cursor")).toBeNull();
        expect(Date.parse(requests[3]?.searchParams.get("since") ?? "")).toBeGreaterThan(
          Date.parse(oldSince ?? ""),
        );
        if (filterName === "日志级别") {
          expect(requests[3]?.searchParams.get("level")).toBe("warning");
        }
      } finally {
        clock.mockRestore();
      }
    },
  );

  it("shows a safe 429 state and retry without displaying transport detail", async () => {
    let busy = true;
    server.use(
      http.get("*/api/v1/tasks/services/log-capabilities", () =>
        HttpResponse.json({ units: ["rquant-daily.service"] }),
      ),
      http.get("*/api/v1/tasks/services/:unit/logs", () =>
        busy
          ? HttpResponse.json({ detail: "Bearer private" }, { status: 429 })
          : HttpResponse.json({
              service_label: "每日任务",
              scope: "本机本次开机以来的服务日志（含手动运行）",
              entries: [],
              next_cursor: null,
            }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
    expect(await screen.findByText("请求较多，请稍后重试。")).toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("Bearer private");
    busy = false;
    await user.click(screen.getByRole("button", { name: "重试" }));
    expect(await screen.findByText("所选范围还没有可显示的日志。")).toBeInTheDocument();
  });

  it("never prints an unexpected message or level from a malformed response", async () => {
    server.use(
      http.get("*/api/v1/tasks/services/log-capabilities", () =>
        HttpResponse.json({ units: ["rquant-daily.service"] }),
      ),
      http.get("*/api/v1/tasks/services/:unit/logs", () =>
        HttpResponse.json({
          service_label: "每日任务",
          scope: "本机本次开机以来的服务日志（含手动运行）",
          entries: [
            {
              at: "2026-09-28T04:00:00Z",
              level: "internal-state",
              text: "Bearer private journal MESSAGE",
            },
          ],
          next_cursor: null,
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
    expect(await screen.findByText("该条内容暂不可显示")).toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("Bearer private journal MESSAGE");
    expect(document.body).not.toHaveTextContent("internal-state");
  });

  it("drops the open log page as soon as the confirmed viewer changes", async () => {
    server.use(
      http.get("*/api/v1/tasks/services/log-capabilities", () =>
        HttpResponse.json({ units: ["rquant-daily.service"] }),
      ),
      http.get("*/api/v1/tasks/services/:unit/logs", () =>
        HttpResponse.json({
          service_label: "每日任务",
          scope: "本机本次开机以来的服务日志（含手动运行）",
          entries: [{ at: "2026-09-28T04:00:00Z", level: "信息", text: "任务已完成" }],
          next_cursor: null,
        }),
      ),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看日线更新的运行日志" }));
    expect(await screen.findByText("任务已完成")).toBeInTheDocument();
    queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "other" }));
    await waitFor(() => expect(screen.queryByText("任务已完成")).toBeNull());
    expect(screen.queryByRole("button", { name: "查看日线更新的运行日志" })).toBeNull();
    expect(screen.getByText("当前身份已变化，运行日志已关闭。")).toBeInTheDocument();
  });
});

describe("研究任务进展", () => {
  beforeEach(() => server.use(overviewHandler()));

  it("hides entry when overview denies it", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/tasks/jobs/:jobId/events", ({ request }) => {
        requests.push(request.url);
        return HttpResponse.json(eventsData());
      }),
    );
    renderApp("/tasks");
    await screen.findByRole("table", { name: "研究任务队列" });
    expect(screen.queryByRole("button", { name: /进展/ })).toBeNull();
    expect(requests).toEqual([]);
  });

  it("hides entry when the source has no published jobs", async () => {
    server.use(
      overviewHandler(
        overviewEnvelope({
          can_view_research_logs: true,
          research: { ...tasksEnvelope().data, items: [], next_cursor: null },
        }),
      ),
    );
    renderApp("/tasks");
    await screen.findByRole("table", { name: "研究任务队列" });
    expect(screen.queryByRole("button", { name: /进展/ })).toBeNull();
  });

  it("pins the request to overview data, renders plain Chinese events, and restores focus on Escape", async () => {
    const requests: URL[] = [];
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", ({ request }) => {
        requests.push(new URL(request.url));
        return HttpResponse.json(eventsData());
      }),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    const trigger = await screen.findByRole("button", { name: "查看动量参数搜索的进展" });
    await user.click(trigger);
    const dialog = await screen.findByRole("dialog", { name: /任务进展/ });
    expect(within(dialog).getByText("任务已开始")).toBeInTheDocument();
    expect(within(dialog).getByText("2026-09-24 15:30:00")).toBeInTheDocument();
    expect(within(dialog).getByText("运行中")).toBeInTheDocument();
    expect(requests).toHaveLength(1);
    expect(requests[0]?.pathname).toContain("/00000000-0000-0000-0000-000000000001/events");
    expect(requests[0]?.searchParams.get("generation_id")).toBe(
      tasksEnvelope().serving.generation_id,
    );
    expect(dialog).not.toHaveTextContent("00000000-0000-0000-0000-000000000001");
    await user.keyboard("{Escape}");
    await waitFor(() => expect(trigger).toHaveFocus());
  });

  it("clears an open drawer when the overview generation changes", async () => {
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () => HttpResponse.json(eventsData())),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("任务已开始")).toBeInTheDocument();
    queryClient.setQueryData(
      ["meta"],
      metaEnvelope({
        generationId: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      }),
    );
    await waitFor(() => expect(screen.queryByText("任务已开始")).toBeNull());
    expect(await screen.findByText("数据已更新，请重新打开任务进展。")).toBeInTheDocument();
  });

  it("closes and hides progress when the refreshed overview revokes access", async () => {
    let permitted = true;
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(overviewEnvelope({ can_view_research_logs: permitted })),
      ),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () => HttpResponse.json(eventsData())),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("任务已开始")).toBeInTheDocument();
    permitted = false;
    await queryClient.invalidateQueries({ queryKey: ["tasks", "overview"] });
    await waitFor(() => expect(screen.queryByText("任务已开始")).toBeNull());
    expect(screen.queryByRole("button", { name: "查看动量参数搜索的进展" })).toBeNull();
    expect(screen.getByText("当前无法查看任务进展。")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByRole("button", { name: "刷新" })).toHaveFocus());
  });

  it.each(["other-viewer", null])(
    "drops private progress and refetches overview when meta viewer becomes %s in the same generation",
    async (nextViewer) => {
      let overviewRequests = 0;
      server.use(
        http.get("*/api/v1/tasks/overview", () => {
          overviewRequests += 1;
          return HttpResponse.json(
            overviewEnvelope({ can_view_research_logs: overviewRequests === 1 }),
          );
        }),
        http.get("*/api/v1/tasks/jobs/:jobId/events", () => HttpResponse.json(eventsData())),
      );
      const user = userEvent.setup();
      const { queryClient } = renderApp("/tasks");
      await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
      expect(await screen.findByText("任务已开始")).toBeInTheDocument();
      queryClient.setQueryData(["meta"], metaEnvelope({ viewer: nextViewer }));
      await waitFor(() => expect(screen.queryByText("任务已开始")).toBeNull());
      expect(screen.getByText("当前身份已变化，任务进展已关闭。")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "查看动量参数搜索的进展" })).toBeNull();
      await waitFor(() => expect(overviewRequests).toBeGreaterThan(1));
      await waitFor(() => expect(screen.getByRole("button", { name: "刷新" })).toHaveFocus());
      await screen.findByRole("table", { name: "研究任务队列" });
      expect(screen.queryByRole("button", { name: "查看动量参数搜索的进展" })).toBeNull();
    },
  );

  it("drops private progress on meta failure and refetches overview after recovery", async () => {
    let overviewRequests = 0;
    server.use(
      http.get("*/api/v1/tasks/overview", () => {
        overviewRequests += 1;
        return HttpResponse.json(overviewEnvelope({ can_view_research_logs: true }));
      }),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () => HttpResponse.json(eventsData())),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("任务已开始")).toBeInTheDocument();
    server.use(http.get("*/api/v1/meta", () => HttpResponse.error()));
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
    await waitFor(() => expect(screen.queryByText("任务已开始")).toBeNull());
    expect(screen.getByText("当前身份暂无法确认，任务进展已关闭。")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "查看动量参数搜索的进展" })).toBeNull();
    expect(screen.getByText("当前身份暂无法确认")).toBeInTheDocument();
    expect(overviewRequests).toBe(1);
    server.use(http.get("*/api/v1/meta", () => HttpResponse.json(metaEnvelope())));
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await waitFor(() => expect(overviewRequests).toBeGreaterThan(1));
    expect(
      await screen.findByRole("button", { name: "查看动量参数搜索的进展" }),
    ).toBeInTheDocument();
  });

  it("closes the stale drawer on 409 and never renders an older response", async () => {
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () =>
        HttpResponse.json({ detail: "stale-internal-id" }, { status: 409 }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(
      await screen.findByText("数据已更新。新数据发布后点「刷新」，再打开任务进展。"),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: /任务进展/ })).toBeNull();
    expect(document.body).not.toHaveTextContent("stale-internal-id");
  });

  it("blocks repeated 409 requests until a newer overview generation succeeds", async () => {
    let generation = tasksEnvelope().serving.generation_id;
    let eventRequests = 0;
    let overviewRequests = 0;
    const nextGeneration = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    server.use(
      http.get("*/api/v1/tasks/overview", () => {
        overviewRequests += 1;
        return HttpResponse.json({
          ...overviewEnvelope({ can_view_research_logs: true }),
          serving: { ...tasksEnvelope().serving, generation_id: generation },
        });
      }),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () => {
        eventRequests += 1;
        return generation === nextGeneration
          ? HttpResponse.json(eventsData({ generation_id: nextGeneration }))
          : HttpResponse.json({ detail: "stale-internal-id" }, { status: 409 });
      }),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText(/新数据发布后点「刷新」/)).toBeInTheDocument();
    await waitFor(() => expect(overviewRequests).toBeGreaterThan(1));
    expect(screen.queryByRole("button", { name: "查看动量参数搜索的进展" })).toBeNull();
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(eventRequests).toBe(1);
    expect(screen.queryByRole("button", { name: "查看动量参数搜索的进展" })).toBeNull();
    generation = nextGeneration;
    queryClient.setQueryData(["meta"], metaEnvelope({ generationId: nextGeneration }));
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("任务已开始")).toBeInTheDocument();
    expect(eventRequests).toBe(2);
  });

  it("returns focus to page refresh when the selected row leaves the current overview", async () => {
    let items = tasksEnvelope().data.items;
    server.use(
      http.get("*/api/v1/tasks/overview", () =>
        HttpResponse.json(
          overviewEnvelope({
            can_view_research_logs: true,
            research: { ...tasksEnvelope().data, items },
          }),
        ),
      ),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () => HttpResponse.json(eventsData())),
    );
    const user = userEvent.setup();
    const { queryClient } = renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("任务已开始")).toBeInTheDocument();
    items = [second];
    await queryClient.invalidateQueries({ queryKey: ["tasks", "overview"] });
    await waitFor(() => expect(screen.queryByText("任务已开始")).toBeNull());
    await waitFor(() => expect(screen.getByRole("button", { name: "刷新" })).toHaveFocus());
  });

  it("closes a 200 response that names another data generation", async () => {
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () =>
        HttpResponse.json(eventsData({ generation_id: "other", events: [] })),
      ),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(
      await screen.findByText("数据已更新。新数据发布后点「刷新」，再打开任务进展。"),
    ).toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: /任务进展/ })).toBeNull();
  });

  it.each([
    ["not_published", "任务进展尚未发布。"],
    ["not_included", "当前数据未包含该任务。"],
    ["unavailable", "任务进展暂时无法读取，请稍后重试。"],
  ] as const)("explains %s without inventing records", async (state, note) => {
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () =>
        HttpResponse.json(eventsData({ state, note, events: [] })),
      ),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    const dialog = await screen.findByRole("dialog", { name: /任务进展/ });
    expect(within(dialog).getByText(note)).toBeInTheDocument();
    expect(within(dialog).queryByRole("list", { name: "最近进展" })).toBeNull();
  });

  it("shows empty and truncated source states and bounds rendering to 500 rows", async () => {
    const user = userEvent.setup();
    let response = eventsData({ state: "empty", note: "还没有进展记录。", events: [] });
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () => HttpResponse.json(response)),
    );
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("还没有进展记录。")).toBeInTheDocument();
    response = eventsData({
      state: "truncated",
      note: "仅显示最近记录。",
      truncated: true,
      events: Array.from({ length: 501 }, (_, index) => ({
        event_id: index + 1,
        label: `任务进展 ${index + 1}`,
        status_label: "运行中",
        occurred_at: "2026-09-24T07:30:00Z",
      })),
    });
    await user.click(screen.getByRole("button", { name: "刷新进展" }));
    expect(await screen.findByText("仅显示最近记录。")).toBeInTheDocument();
    expect(
      within(screen.getByRole("list", { name: "最近进展" })).getAllByRole("listitem"),
    ).toHaveLength(500);
    expect(screen.queryByText("任务进展 501")).toBeNull();
  });

  it.each([401, 403, 503])(
    "shows a safe %i error without displaying server detail",
    async (status) => {
      server.use(
        overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
        http.get("*/api/v1/tasks/jobs/:jobId/events", () =>
          HttpResponse.json({ detail: "secret-internal-error" }, { status }),
        ),
      );
      const user = userEvent.setup();
      renderApp("/tasks");
      await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
      expect(await screen.findByRole("dialog", { name: /任务进展/ })).toBeInTheDocument();
      expect(await screen.findByRole("alert")).toBeInTheDocument();
      expect(document.body).not.toHaveTextContent("secret-internal-error");
    },
  );

  it("retries an unavailable request without replaying server details", async () => {
    let unavailable = true;
    server.use(
      overviewHandler(overviewEnvelope({ can_view_research_logs: true })),
      http.get("*/api/v1/tasks/jobs/:jobId/events", () =>
        unavailable
          ? HttpResponse.json({ detail: "private-error" }, { status: 503 })
          : HttpResponse.json(eventsData()),
      ),
    );
    const user = userEvent.setup();
    renderApp("/tasks");
    await user.click(await screen.findByRole("button", { name: "查看动量参数搜索的进展" }));
    expect(await screen.findByText("进展暂时不可用，请稍后重试。")).toBeInTheDocument();
    unavailable = false;
    await user.click(screen.getByRole("button", { name: "重试" }));
    expect(await screen.findByText("任务已开始")).toBeInTheDocument();
    expect(document.body).not.toHaveTextContent("private-error");
  });
});
