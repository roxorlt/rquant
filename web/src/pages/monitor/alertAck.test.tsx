import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope, monitorEnvelope, overviewEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, monitorHandler, overviewHandler, server } from "@/test/server";

const CUTOFF = "2026-09-24T05:10:00Z";
const READY: Schemas["UnacknowledgedSummary"] = {
  state: "ready",
  count: 12,
  count_as_of: CUTOFF,
  label: "12 条待确认",
  note: "已核对全部告警来源。",
};

function countCard(): HTMLElement {
  return document.querySelector('[data-kpi="unacknowledged"]') as HTMLElement;
}

describe("告警确认状态展示", () => {
  it("shows every event state and keeps notification submissions separate", async () => {
    const original = monitorEnvelope();
    const states: Schemas["AlertAcknowledgmentView"][] = [
      { state: "unconfirmed", eligible: true, alert_id: "1".repeat(64), label: "待确认" },
      {
        state: "confirmed",
        eligible: false,
        alert_id: "2".repeat(64),
        confirmation_id: "confirmed-first",
        label: "已确认",
        confirmed_at: "2026-09-24T05:06:00Z",
      },
      { state: "historical", eligible: false, label: "历史告警", note: "启用前的告警" },
      {
        state: "unavailable",
        eligible: false,
        label: "确认状态暂不可用",
        note: "告警来源尚未核对完整。",
      },
    ];
    server.use(
      monitorHandler(
        monitorEnvelope({
          unacknowledged: READY,
          items: [
            ...original.data.items.map((item, index) => {
              const acknowledgment = states[index];
              if (!acknowledgment)
                throw new Error("Original fixture event has no expected acknowledgment state");
              return { ...item, acknowledgment };
            }),
            {
              kind: "notification",
              event_key: "notification:old",
              at: "2026-09-23T02:06:00Z",
              scene_label: "价位提醒",
              channel_label: "PushDeer",
              submitted: true,
              submission_label: "提交成功",
            },
          ],
        }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/monitor");
    const timeline = await screen.findByRole("list", { name: "告警时间线" });
    const entries = timeline.querySelectorAll(":scope > li");
    expect(within(entries[0] as HTMLElement).getByText("待确认")).toBeInTheDocument();
    expect(within(entries[1] as HTMLElement).getByText("已确认")).toBeInTheDocument();
    expect(within(entries[2] as HTMLElement).getByText("历史告警")).toBeInTheDocument();
    expect(within(entries[3] as HTMLElement).getByText("暂不可用")).toBeInTheDocument();
    expect(entries[4]).toHaveTextContent("通知记录");
    expect(entries[4]?.textContent).not.toContain("待确认");
    await user.hover(within(entries[1] as HTMLElement).getByText("已确认"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("2026-09-24 13:06:00");
    await user.unhover(within(entries[1] as HTMLElement).getByText("已确认"));
    await user.hover(within(entries[2] as HTMLElement).getByText("历史告警"));
    await waitFor(() =>
      expect(
        screen.getAllByRole("tooltip").some((tip) => tip.textContent?.includes("启用前的告警")),
      ).toBe(true),
    );
    expect(screen.getAllByRole("button", { name: /^确认$/ })).toHaveLength(1);
  });

  it("uses the same published count and cutoff on timeline and overview", async () => {
    server.use(
      monitorHandler(monitorEnvelope({ unacknowledged: READY })),
      overviewHandler(overviewEnvelope({ unacknowledged: READY })),
    );
    const user = userEvent.setup();
    const timeline = renderApp("/monitor");
    await screen.findByRole("list", { name: "告警时间线" });
    expect(countCard()).toHaveTextContent("待确认12条截至");
    await user.hover(within(countCard()).getByText("待确认"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("已核对全部告警来源");
    timeline.unmount();

    renderApp("/overview");
    await screen.findByRole("table", { name: "最新信号" });
    expect(countCard()).toHaveTextContent("待确认12条截至");
    await user.hover(within(countCard()).getByText("待确认"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("已核对全部告警来源");
    await user.unhover(within(countCard()).getByText("待确认"));
    await user.hover(countCard().querySelector(".sub .tip-anchor") as HTMLElement);
    await waitFor(() =>
      expect(
        screen
          .getAllByRole("tooltip")
          .some((tip) => tip.textContent?.includes("2026-09-24 13:10:00")),
      ).toBe(true),
    );
  });

  it.each(["unavailable", "source_incomplete"] as const)(
    "shows an unknown count on both pages when the source is %s",
    async (state) => {
      const unknown: Schemas["UnacknowledgedSummary"] = {
        state,
        count: null,
        count_as_of: null,
        label: "待确认数暂不可用",
        note: "告警来源尚未核对完整。",
      };
      server.use(
        monitorHandler(monitorEnvelope({ unacknowledged: unknown })),
        overviewHandler(overviewEnvelope({ unacknowledged: unknown })),
      );
      const timeline = renderApp("/monitor");
      await screen.findByRole("list", { name: "告警时间线" });
      expect(countCard()).toHaveTextContent("待确认—数量未知");
      expect(countCard()).not.toHaveTextContent("0条");
      timeline.unmount();

      renderApp("/overview");
      await screen.findByRole("table", { name: "最新信号" });
      expect(countCard()).toHaveTextContent("待确认—数量未知");
      expect(countCard()).not.toHaveTextContent("2条");
    },
  );

  it("withdraws an old overview count after the page request fails", async () => {
    server.use(overviewHandler(overviewEnvelope({ unacknowledged: READY })));
    const user = userEvent.setup();
    renderApp("/overview");
    await screen.findByRole("table", { name: "最新信号" });
    expect(countCard()).toHaveTextContent("12");
    server.use(http.get("*/api/v1/overview", () => new HttpResponse(null, { status: 503 })));
    await user.click(screen.getByRole("button", { name: "刷新" }));
    await screen.findByText("暂时读不到总览数据");
    expect(document.querySelector('[data-kpi="unacknowledged"]')).toBeNull();
  });

  it("withdraws a count from the previous data version while a new one loads", async () => {
    server.use(monitorHandler(monitorEnvelope({ unacknowledged: READY })));
    const view = renderApp("/monitor");
    await screen.findByRole("list", { name: "告警时间线" });
    expect(countCard()).toHaveTextContent("12");

    server.use(
      metaHandler(metaEnvelope({ generationId: "b".repeat(64) })),
      http.get("*/api/v1/monitor/timeline", async () => {
        await new Promise<void>(() => undefined);
        return HttpResponse.json(monitorEnvelope());
      }),
    );
    await view.queryClient.invalidateQueries({ queryKey: ["meta"] });
    await screen.findByRole("status", { name: "告警时间线加载中" });
    expect(document.querySelector('[data-kpi="unacknowledged"]')).toBeNull();
  });
});
