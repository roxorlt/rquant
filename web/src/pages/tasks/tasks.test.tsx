import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { deadlineFromRemaining, pinnedTaskDeadline } from "@/api/endpoints";
import { tasksEnvelope } from "@/test/fixtures";
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
