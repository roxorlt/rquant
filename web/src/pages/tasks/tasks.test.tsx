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
