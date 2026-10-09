import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { channelsEnvelope, metaEnvelope, monitorEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { channelsHandler, metaHandler, monitorHandler, server } from "@/test/server";

const publishedChannels = channelsEnvelope({
  state: "ready",
  channels: [
    {
      channel: "pushdeer",
      channel_label: "PushDeer",
      today_submitted: 2,
      seven_day_attempts: 3,
      seven_day_submitted: 2,
      seven_day_success_pct: 66.7,
      last_success_at: "2026-09-24T07:30:00Z",
    },
    {
      channel: "pushplus",
      channel_label: "PushPlus",
      today_submitted: 0,
      seven_day_attempts: 0,
      seven_day_submitted: 0,
      seven_day_success_pct: null,
      last_success_at: null,
    },
  ],
});

const unavailableRuntime: Schemas["MonitorRuntimeData"] = {
  state: "unavailable",
  source_label: "暂不可用",
  source_note: "通知来源尚未发布。",
  mode: "unknown",
  mode_label: "未确认",
  builtins: [],
  channels: [],
};

function runtimeHandler(data: Schemas["MonitorRuntimeData"], meta = metaEnvelope()) {
  return http.get("*/api/v1/monitor/runtime", () =>
    HttpResponse.json({ serving: meta.serving, data }),
  );
}

beforeEach(() => {
  server.use(
    runtimeHandler(unavailableRuntime),
    ...["price-rules", "price-rules/runtime", "price-rules/events"].map((path) =>
      http.get(`*/api/v1/monitor/${path}`, () => new HttpResponse(null, { status: 503 })),
    ),
    http.get("*/api/v1/tasks/control-capabilities", () =>
      HttpResponse.json({
        generation_id: metaEnvelope().data.generation?.generation_id,
        units: [],
        can_control_scheduling: false,
        can_recover_units: false,
        can_recover_scheduling: false,
        scheduling: { available: false, note: "尚未配置" },
        notifier_mode: {
          available: false,
          can_request: false,
          can_set_live: false,
          note: "尚未配置",
        },
        monitor_builtins: [],
        note: "尚未配置",
      } satisfies Schemas["TaskControlCapabilitiesData"]),
    ),
  );
});

const currentRuntime: Schemas["MonitorRuntimeData"] = {
  ...unavailableRuntime,
  state: "ready",
  source_label: "已核对",
  source_note: "当前完整范围",
  mode: "live",
  mode_label: "正式推送",
  applied_revision: 2,
  channels: [
    {
      channel: "pushdeer",
      channel_label: "PushDeer",
      mode: "live",
      covered_from: "2026-09-24T02:00:00Z",
      covered_through: "2026-09-24T03:00:00Z",
      logical_count: 7,
      member_attempts: 8,
      member_retries: 1,
      physical_requests: 3,
      accepted_count: 1,
      rejected_count: 1,
      physical_unknown_count: 1,
      possible_requests: 1,
      accepted_pct: null,
      last_accepted_at: null,
    },
  ],
};

describe("盯盘当前来源", () => {
  it("keeps logical, member, physical and unknown facts separate from legacy channel percentages", async () => {
    server.use(runtimeHandler(currentRuntime), channelsHandler(publishedChannels));
    const user = userEvent.setup();
    renderApp("/monitor");
    const current = await screen.findByRole("region", { name: "当前通道尝试" });
    await within(current).findByRole("article", { name: "PushDeer当前通知" });
    expect(current).toHaveTextContent("逻辑通知7");
    expect(current).toHaveTextContent("成员尝试8");
    expect(current).toHaveTextContent("实际请求3");
    expect(current).toHaveTextContent("成员重试1");
    expect(current).toHaveTextContent("结果未明1");
    expect(current).toHaveTextContent("可能已请求1");
    expect(current).not.toHaveTextContent("33.3%");
    expect(await screen.findByText("66.7%")).toBeInTheDocument();
    await user.hover(within(current).getByText("窗口提交成功率"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("不代表手机送达");
  });

  it("renders market events without a fake stock and independently permits eligible builtin ACK", async () => {
    server.use(
      runtimeHandler(currentRuntime),
      monitorHandler(
        monitorEnvelope({
          total: null,
          unacknowledged: {
            state: "unavailable",
            label: "暂不可用",
            count: null,
            count_as_of: null,
            note: "旧范围未核对",
          },
          builtin_unacknowledged: {
            state: "ready",
            label: "已核对",
            count: 1,
            count_as_of: "2026-09-24T02:00:00Z",
            note: null,
          },
          items: [
            {
              kind: "builtin",
              event_key: "sealed-market",
              at: "2026-09-24T02:00:00Z",
              builtin_id: "pulse",
              event_label: "市场异动",
              subject: "market",
              code: null,
              name: null,
              before: 12,
              after: 18,
              threshold: null,
              comparison_unit: "count",
              source_note: "原全市场聚合",
              acknowledgment: {
                state: "unconfirmed",
                eligible: true,
                alert_id: "f".repeat(64),
                label: "待确认",
              },
            },
            {
              kind: "channel_attempt",
              event_key: "possible-request",
              at: "2026-09-24T01:59:00Z",
              channel_label: "PushDeer",
              mode: "live",
              state: "possible",
              state_label: "可能已请求",
              logical_count: 7,
              attempt_no: 1,
              source_note: "可能已请求，不能自动重发。",
            },
          ],
        }),
      ),
    );
    renderApp("/monitor");
    const timeline = await screen.findByRole("list", { name: "告警时间线" });
    expect(timeline).toHaveTextContent("全市场");
    expect(timeline).toHaveTextContent("12 → 18");
    expect(timeline).toHaveTextContent("可能已请求");
    expect(within(timeline).queryByRole("button", { name: /^查看.*详情$/ })).toBeNull();
    expect(within(timeline).getByRole("button", { name: "确认" })).toBeEnabled();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("uses the supplied percent, multiplier and price units without inventing old missing units", async () => {
    const unavailable = {
      state: "unavailable",
      eligible: false,
      label: "暂不可确认",
    } satisfies Schemas["AlertAcknowledgmentView"];
    server.use(
      runtimeHandler(currentRuntime),
      monitorHandler(
        monitorEnvelope({
          items: [
            {
              kind: "builtin",
              event_key: "original-ratio",
              at: "2026-09-24T02:00:00Z",
              builtin_id: "pulse",
              event_label: "涨跌占比突变",
              subject: "market",
              code: null,
              name: null,
              before: 38.75,
              after: 54.125,
              comparison_unit: "percent",
              source_note: "原市场占比",
              acknowledgment: unavailable,
            },
            {
              kind: "builtin",
              event_key: "original-multiple",
              at: "2026-09-24T01:59:00Z",
              builtin_id: "surge",
              event_label: "爆量",
              subject: "stock",
              code: "300001.SZ",
              name: "量能样本",
              price: 100.11,
              threshold: 8.25,
              threshold_unit: "multiple",
              source_note: "原相对量",
              acknowledgment: unavailable,
            },
            {
              kind: "builtin",
              event_key: "original-price",
              at: "2026-09-24T01:58:00Z",
              builtin_id: "pool2_levels",
              event_label: "档位提醒",
              subject: "stock",
              code: "600001.SH",
              name: "档位样本",
              price: 10.21,
              threshold: 10.4,
              threshold_unit: "CNY",
              source_note: "原档位价格",
              acknowledgment: unavailable,
            },
            {
              kind: "builtin",
              event_key: "old-no-unit",
              at: "2026-09-24T01:57:00Z",
              builtin_id: "pool2_levels",
              event_label: "旧原值",
              subject: "stock",
              code: "600002.SH",
              name: "旧样本",
              price: 11.33,
              threshold: 2.75,
              source_note: "旧记录没有单位",
              acknowledgment: unavailable,
            },
          ],
        }),
      ),
    );
    renderApp("/monitor");
    const timeline = await screen.findByRole("list", { name: "告警时间线" });
    const rows = within(timeline).getAllByRole("listitem");
    expect(rows[0]).toHaveTextContent("38.75% → 54.13%");
    expect(rows[0]).not.toHaveTextContent("3,875");
    expect(rows[1]).toHaveTextContent("相对量 8.25倍");
    expect(rows[1]).not.toHaveTextContent("参考价");
    expect(rows[2]).toHaveTextContent("参考价 10.40");
    expect(rows[3]).toHaveTextContent("阈值 2.75");
    expect(rows[3]).not.toHaveTextContent("参考价");
  });

  it("removes private rows and counts immediately when the viewer and generation change", async () => {
    server.use(runtimeHandler(currentRuntime));
    const view = renderApp("/monitor");
    const current = await screen.findByRole("region", { name: "当前通道尝试" });
    await within(current).findByRole("article", { name: "PushDeer当前通知" });
    expect(current).toHaveTextContent("逻辑通知7");
    const next = metaEnvelope({ generationId: "b".repeat(64), viewer: "bob" });
    server.use(metaHandler(next), runtimeHandler(unavailableRuntime, next));
    act(() => {
      view.queryClient.setQueryData(["meta"], next);
    });
    await waitFor(() => expect(screen.queryByText("逻辑通知7")).toBeNull());
    expect(
      view.queryClient.getQueryCache().findAll({ queryKey: ["monitor", "runtime", "tester"] }),
    ).toHaveLength(0);
    expect(
      view.queryClient.getQueryCache().findAll({ queryKey: ["monitor", "timeline", "tester"] }),
    ).toHaveLength(0);
    expect(
      view.queryClient
        .getQueryCache()
        .findAll({ queryKey: ["monitor", "runtime", "bob", "b".repeat(64)] }).length,
    ).toBeGreaterThan(0);
  });

  it.each([null, "failed"] as const)(
    "does not fetch or write private controls when identity is %s",
    async (identity) => {
      let privateReads = 0,
        writes = 0;
      server.use(
        identity === null
          ? metaHandler(metaEnvelope({ viewer: null }))
          : http.get("*/api/v1/meta", () => new HttpResponse(null, { status: 503 })),
        http.get("*/api/v1/monitor/runtime", () => {
          privateReads += 1;
          return HttpResponse.json({ serving: metaEnvelope().serving, data: currentRuntime });
        }),
        http.post("*/api/v1/tasks/notifications/*", () => {
          writes += 1;
          return new HttpResponse(null, { status: 403 });
        }),
      );
      const user = userEvent.setup();
      renderApp("/monitor");
      const switchMode = await screen.findByRole("button", { name: "切换通知模式" });
      expect(switchMode).toBeDisabled();
      await user.click(switchMode);
      expect(screen.queryByRole("button", { name: "立即运行测试推送" })).toBeNull();
      expect(screen.queryByRole("article", { name: "PushDeer当前通知" })).toBeNull();
      expect(privateReads).toBe(0);
      expect(writes).toBe(0);
    },
  );
});

describe("盯盘与告警", () => {
  it("shows only verified submission facts, honest zero, and a delivery caveat", async () => {
    server.use(channelsHandler(publishedChannels));
    const user = userEvent.setup();
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "推送通道状态" });
    expect(within(section).getByRole("heading", { name: "通道提交记录" })).toBeInTheDocument();
    const deer = await within(section).findByRole("article", { name: "PushDeer" });
    const plus = within(section).getByRole("article", { name: "PushPlus" });
    expect(deer).toHaveTextContent("66.7%");
    expect(deer).toHaveTextContent("今日成功提交");
    expect(deer).toHaveTextContent("2");
    expect(plus).toHaveTextContent("近 7 日无提交记录");
    expect(plus).toHaveTextContent("0");
    expect(plus).not.toHaveTextContent("100%");
    await user.hover(within(deer).getByText("近 7 日提交成功率"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("不代表手机送达");
    expect(await screen.findByRole("tooltip")).toHaveTextContent("新信号通知");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("shows an independent retry when channel records cannot be verified", async () => {
    const user = userEvent.setup();
    renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "推送通道状态" });
    expect(await within(section).findByText("推送记录暂无法核对")).toBeInTheDocument();
    expect(within(section).queryByRole("article")).toBeNull();
    server.use(channelsHandler(publishedChannels));
    await user.click(within(section).getByRole("button", { name: "重试" }));
    expect(await within(section).findByRole("article", { name: "PushDeer" })).toBeInTheDocument();
  });

  it("clears old counts when the data generation changes", async () => {
    server.use(channelsHandler(publishedChannels));
    const view = renderApp("/monitor");
    const section = await screen.findByRole("region", { name: "推送通道状态" });
    expect(await within(section).findByText("66.7%")).toBeInTheDocument();
    server.use(metaHandler(metaEnvelope({ generationId: "b".repeat(64) })));
    act(() => {
      view.queryClient.setQueryData(["meta"], metaEnvelope({ generationId: "b".repeat(64) }));
    });
    await waitFor(() => expect(within(section).queryByText("66.7%")).toBeNull());
  });

  it("clears old counts after a failed refresh", async () => {
    server.use(channelsHandler(publishedChannels));
    const user = userEvent.setup();
    renderApp("/monitor");
    const freshSection = await screen.findByRole("region", { name: "推送通道状态" });
    expect(await within(freshSection).findByText("66.7%")).toBeInTheDocument();
    server.use(
      http.get("*/api/v1/monitor/channels", () => new HttpResponse(null, { status: 503 })),
    );
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await within(freshSection).findByText("推送记录暂无法核对")).toBeInTheDocument();
    expect(within(freshSection).queryByText("66.7%")).toBeNull();
  });

  it.each([
    ["has_receipts", "有回执", "通知回执已更新"],
    ["truncated", "仅部分", "回执较多，仅显示部分，状态未确认"],
    ["no_receipts", "暂无", "这些信号还没有通知回执"],
    ["not_published", "未就绪", "通知来源尚未发布"],
  ] as const)(
    "keeps %s receipt detail available without a long KPI value",
    async (state, short, full) => {
      server.use(
        http.get("*/api/v1/monitor/timeline", () =>
          HttpResponse.json(monitorEnvelope({ receipt_state: state, receipt_label: full })),
        ),
      );
      const user = userEvent.setup();
      renderApp("/monitor");
      const receiptKpi = await screen.findByText("信号回执");
      expect(
        within(receiptKpi.closest('[data-kpi="receipts"]') as HTMLElement).getByText(short),
      ).toBeInTheDocument();
      await user.hover(receiptKpi);
      expect(await screen.findByRole("tooltip")).toHaveTextContent(full);
      expect(await screen.findByRole("tooltip")).toHaveTextContent("旧通知记录另列");
      if (state === "no_receipts" || state === "truncated") {
        expect(screen.getByRole("status", { name: "" })).toHaveTextContent(full);
      }
    },
  );

  it("shows trigger and surge records beside signals without claiming delivery", async () => {
    renderApp("/monitor");
    const timeline = await screen.findByRole("list", { name: "告警时间线" });
    expect(timeline.querySelectorAll(":scope > li")).toHaveLength(4);
    expect(timeline).toHaveTextContent("上攻突破");
    expect(timeline).toHaveTextContent("爆量");
    expect(timeline).toHaveTextContent("12.34");
    expect(timeline).toHaveTextContent("+3.15%");
    expect(timeline.querySelectorAll('[aria-label="通知回执"]')).toHaveLength(1);
    expect(timeline.lastElementChild).toHaveTextContent("暂无回执");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("shows old channel attempts independently without a stock or delivery claim", async () => {
    const original = monitorEnvelope();
    server.use(
      http.get("*/api/v1/monitor/timeline", () =>
        HttpResponse.json(
          monitorEnvelope({
            total: 6,
            items: [
              {
                kind: "notification",
                event_key: "notification:internal-1",
                at: "2026-09-24T02:06:00Z",
                scene_label: "价位提醒",
                channel_label: "PushDeer",
                submitted: true,
                submission_label: "提交成功",
              },
              {
                kind: "notification",
                event_key: "notification:internal-2",
                at: "2026-09-24T02:05:00Z",
                scene_label: "价位提醒",
                channel_label: "PushPlus",
                submitted: false,
                submission_label: "提交失败",
              },
              ...original.data.items,
            ],
          }),
        ),
      ),
    );
    renderApp("/monitor");
    const timeline = await screen.findByRole("list", { name: "告警时间线" });
    const entries = timeline.querySelectorAll(":scope > li");
    expect(entries).toHaveLength(6);
    expect(entries[0]).toHaveTextContent("价位提醒");
    expect(entries[0]).toHaveTextContent("PushDeer");
    expect(entries[0]).toHaveTextContent("提交成功");
    expect(entries[0]?.querySelector(".monitor-stock")).toBeNull();
    expect(entries[1]).toHaveTextContent("提交失败");
    expect(timeline).not.toHaveTextContent("已送达");
    expect(document.body.textContent).not.toContain("notification:internal-1");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("shows recent published signals, honest receipt states and a stock detail entry", async () => {
    const user = userEvent.setup();
    renderApp("/monitor");
    const timeline = await screen.findByRole("list", { name: "告警时间线" });
    expect(timeline.querySelectorAll(":scope > li")).toHaveLength(4);
    expect(timeline).toHaveTextContent("天威视讯");
    expect(timeline).toHaveTextContent("竞价跳空买入意向");
    expect(timeline).toHaveTextContent("PushDeer · 送达未确认");
    const times = timeline.querySelectorAll(".monitor-event-time .tip-anchor");
    expect(times).toHaveLength(4);
    await user.hover(times[0] as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("2026-09-24 13:05:14");
    await user.unhover(times[0] as HTMLElement);
    await user.hover(times[3] as HTMLElement);
    expect(await screen.findByText("2026-09-23 09:47:00")).toBeInTheDocument();
    expect(screen.getByText("今天休市，显示历史告警")).toBeInTheDocument();
    expect(screen.getByText("新信号通知")).toBeInTheDocument();
    expect(screen.queryByText("已送达")).not.toBeInTheDocument();
    expect(within(timeline).queryByRole("button", { name: /确认/ })).not.toBeInTheDocument();
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);

    await user.click(within(timeline).getByRole("button", { name: "查看天威视讯详情" }));
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
  });

  it("pages through every signal and can return to the previous page", async () => {
    server.use(
      http.get("*/api/v1/monitor/timeline", ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        return HttpResponse.json(
          cursor
            ? monitorEnvelope({ items: monitorEnvelope().data.items.slice(1), next_cursor: null })
            : monitorEnvelope(),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/monitor");
    await screen.findByRole("list", { name: "告警时间线" });
    expect(screen.getByText("可向前翻看历史")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByText("第 2 页")).toBeInTheDocument();
    expect(screen.getByRole("list", { name: "告警时间线" })).not.toHaveTextContent("天威视讯");
    expect(screen.queryByText("可向前翻看历史")).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "上一页" }));
    expect(await screen.findByText("第 1 页")).toBeInTheDocument();
    expect(screen.getByRole("list", { name: "告警时间线" })).toHaveTextContent("天威视讯");
  });

  it("refreshes the newest page without replaying the old page cursor", async () => {
    const requests: string[] = [];
    server.use(
      http.get("*/api/v1/monitor/timeline", ({ request }) => {
        const cursor = new URL(request.url).searchParams.get("cursor");
        requests.push(cursor ?? "first");
        return HttpResponse.json(
          cursor
            ? monitorEnvelope({ items: monitorEnvelope().data.items.slice(1), next_cursor: null })
            : monitorEnvelope(),
        );
      }),
    );
    const user = userEvent.setup();
    renderApp("/monitor");
    await screen.findByRole("list", { name: "告警时间线" });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    await screen.findByText("第 2 页");
    requests.length = 0;
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await screen.findByText("第 1 页");
    expect(requests).toEqual(["first"]);
  });

  it("distinguishes no Serving, an unpublished signal source, and no signals", async () => {
    server.use(
      monitorHandler(
        monitorEnvelope({
          source_state: "unavailable",
          source_label: "暂时读不到页面数据",
          total: null,
          items: [],
        }),
      ),
    );
    const view = renderApp("/monitor");
    expect(await screen.findByText("暂时读不到页面数据")).toBeInTheDocument();
    expect(screen.queryByText("可向前翻看历史")).not.toBeInTheDocument();
    view.unmount();

    server.use(
      monitorHandler(
        monitorEnvelope({
          source_state: "not_published",
          source_label: "告警来源暂未发布",
          total: null,
          items: [],
        }),
      ),
    );
    const unpublished = renderApp("/monitor");
    expect(await screen.findByText("告警来源暂未发布")).toBeInTheDocument();
    unpublished.unmount();

    server.use(
      monitorHandler(
        monitorEnvelope({ source_state: "empty", source_label: "还没有告警", total: 0, items: [] }),
      ),
    );
    renderApp("/monitor");
    expect(await screen.findByText("还没有告警")).toBeInTheDocument();
    expect(screen.queryByText("可向前翻看历史")).not.toBeInTheDocument();
  });

  it("offers a clean restart when the published generation changes during paging", async () => {
    server.use(
      http.get("*/api/v1/monitor/timeline", ({ request }) =>
        new URL(request.url).searchParams.has("cursor")
          ? HttpResponse.json({ detail: "数据已更新，请重新查看告警时间线。" }, { status: 409 })
          : HttpResponse.json(monitorEnvelope()),
      ),
    );
    const user = userEvent.setup();
    renderApp("/monitor");
    await screen.findByRole("list", { name: "告警时间线" });
    await user.click(screen.getByRole("button", { name: "下一页" }));
    expect(await screen.findByText("数据已更新，请从第一页重新查看。")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "返回最新" }));
    expect(await screen.findByText("第 1 页")).toBeInTheDocument();
  });

  it("does not call missing event sources an empty day", async () => {
    server.use(
      monitorHandler(
        monitorEnvelope({
          source_state: "empty",
          source_label: "告警数据暂不完整",
          source_note: "当前数据缺少盯盘触发记录、爆量记录，仅显示已有记录",
          total: 0,
          items: [],
        }),
      ),
    );
    renderApp("/monitor");
    expect(await screen.findByText("告警数据暂不完整")).toBeInTheDocument();
    expect(screen.getByText(/仅显示已有记录/)).toBeInTheDocument();
    expect(screen.getByText("请稍后刷新，或查看系统健康。")).toBeInTheDocument();
    expect(screen.queryByText("盘中出现新记录后会显示，也可稍后刷新。")).not.toBeInTheDocument();
  });
});
