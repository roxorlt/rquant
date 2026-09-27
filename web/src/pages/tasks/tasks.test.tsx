import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { deadlineFromRemaining, pinnedTaskDeadline } from "@/api/endpoints";
import { metaEnvelope, tasksEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

type Overview = Schemas["TaskOverviewData"];

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

function overviewEnvelope(
  overrides: Partial<Overview> = {},
): Schemas["Envelope_TaskOverviewData_"] {
  return {
    serving: tasksEnvelope().serving,
    data: {
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
          },
        ],
        cpu_usage_percent: null,
        cpu_note: "暂无可信 CPU 数据",
      },
      research: tasksEnvelope().data,
      ...overrides,
    },
  };
}

function overviewHandler(envelope = overviewEnvelope()) {
  return http.get("*/api/v1/tasks/overview", () => HttpResponse.json(envelope));
}

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
    expect(await screen.findByText("数据已更新，请重新打开任务进展。")).toBeInTheDocument();
    expect(screen.queryByRole("dialog", { name: /任务进展/ })).toBeNull();
    expect(document.body).not.toHaveTextContent("stale-internal-id");
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
    expect(await screen.findByText("数据已更新，请重新打开任务进展。")).toBeInTheDocument();
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
