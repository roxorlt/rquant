import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { monitorEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { monitorHandler, server } from "@/test/server";

describe("盯盘与告警", () => {
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
      const receiptKpi = await screen.findByText("本页回执");
      expect(
        within(receiptKpi.closest('[data-kpi="receipts"]') as HTMLElement).getByText(short),
      ).toBeInTheDocument();
      await user.hover(receiptKpi);
      expect(await screen.findByRole("tooltip")).toHaveTextContent(full);
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
    expect(screen.getByText("当前通知方式")).toBeInTheDocument();
    expect(screen.queryByText("已送达")).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: /确认|新建规则|发测试推送/ }),
    ).not.toBeInTheDocument();
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
