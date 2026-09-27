import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope, monitorEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, monitorHandler, server } from "@/test/server";

const IDS = ["1".repeat(64), "2".repeat(64), "3".repeat(64)];
const READY: Schemas["UnacknowledgedSummary"] = {
  state: "ready",
  count: 3,
  count_as_of: "2026-09-24T05:10:00Z",
  label: "3 条待确认",
  note: "来源齐全",
};

function eligibleEnvelope(summary = READY) {
  const original = monitorEnvelope();
  return monitorEnvelope({
    unacknowledged: summary,
    items: original.data.items.map((item, index) => ({
      ...item,
      acknowledgment:
        index < 3
          ? { state: "unconfirmed" as const, eligible: true, alert_id: IDS[index], label: "待确认" }
          : { state: "historical" as const, eligible: false, label: "历史告警" },
    })),
  });
}

beforeEach(() => window.sessionStorage.clear());

it("offers exactly three eligible trigger actions, never a notification action", async () => {
  const envelope = eligibleEnvelope();
  server.use(
    monitorHandler({
      ...envelope,
      data: {
        ...envelope.data,
        items: [
          ...envelope.data.items,
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
      },
    }),
  );
  renderApp("/monitor");
  const timeline = await screen.findByRole("list", { name: "告警时间线" });
  expect(within(timeline).getAllByRole("button", { name: /^确认$/ })).toHaveLength(3);
  expect(timeline.lastElementChild).toHaveTextContent("通知记录");
  expect(timeline.lastElementChild).not.toHaveTextContent("确认");
  expect(findJargon(document.body.textContent ?? "")).toEqual([]);
});

it.each([
  ["no viewer", metaEnvelope({ viewer: null }), eligibleEnvelope()],
  [
    "stale page",
    metaEnvelope(),
    { ...eligibleEnvelope(), serving: { ...monitorEnvelope().serving, state: "stale" as const } },
  ],
  [
    "incomplete source",
    metaEnvelope(),
    eligibleEnvelope({
      state: "source_incomplete",
      count: null,
      count_as_of: null,
      label: "数量未知",
      note: "来源不全",
    }),
  ],
  [
    "missing alert identity",
    metaEnvelope(),
    monitorEnvelope({
      unacknowledged: READY,
      items: monitorEnvelope().data.items.map((item) => ({
        ...item,
        acknowledgment: { state: "unconfirmed" as const, eligible: true, label: "待确认" },
      })),
    }),
  ],
] as const)("does not offer confirmation for %s", async (_reason, meta, timeline) => {
  server.use(metaHandler(meta), monitorHandler(timeline));
  renderApp("/monitor");
  await screen.findByRole("list", { name: "告警时间线" });
  expect(screen.queryByRole("button", { name: /^确认$/ })).not.toBeInTheDocument();
});

it("persists a successful receipt as syncing, then waits for a new data version and matching fact", async () => {
  const sent: Schemas["AckCommandRequest"][] = [];
  server.use(
    monitorHandler(eligibleEnvelope()),
    http.post("*/api/v1/monitor/ack", async ({ request }) => {
      const body = (await request.json()) as Schemas["AckCommandRequest"];
      sent.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "succeeded",
        confirmation_id: "first-confirmation",
        message: "已受理，正在同步",
      });
    }),
  );
  const user = userEvent.setup();
  const view = renderApp("/monitor");
  await screen.findByRole("list", { name: "告警时间线" });
  await user.click(screen.getAllByRole("button", { name: /^确认$/ })[0] as HTMLElement);
  await screen.findByText("已受理，正在同步");
  expect(sent).toHaveLength(1);
  expect(screen.getByRole("list", { name: "告警时间线" }).firstElementChild).not.toHaveTextContent(
    "已确认",
  );
  expect(document.querySelector('[data-kpi="unacknowledged"]')).toHaveTextContent("3条");
  expect(screen.getAllByRole("button", { name: /^确认$/ })).toHaveLength(2);

  const newGeneration = "c".repeat(64);
  const original = eligibleEnvelope();
  const newer = {
    ...original,
    serving: { ...original.serving, generation_id: newGeneration },
    data: {
      ...original.data,
      unacknowledged: { ...READY, count: 2 },
      items: original.data.items.map((item, index) =>
        index === 0
          ? {
              ...item,
              acknowledgment: {
                state: "confirmed" as const,
                eligible: false,
                alert_id: IDS[0],
                confirmation_id: "first-confirmation",
                label: "已确认",
              },
            }
          : item,
      ),
    },
  };
  const mismatched = {
    ...newer,
    data: {
      ...newer.data,
      items: newer.data.items.map((item, index) =>
        index === 0
          ? {
              ...item,
              acknowledgment: {
                state: "confirmed" as const,
                eligible: false,
                alert_id: IDS[0],
                confirmation_id: "different-confirmation",
                label: "已确认",
              },
            }
          : item,
      ),
    },
  };
  server.use(
    metaHandler(metaEnvelope({ generationId: newGeneration })),
    monitorHandler(mismatched),
  );
  await view.queryClient.invalidateQueries({ queryKey: ["meta"] });
  await waitFor(() => {
    expect(document.querySelector('[data-kpi="unacknowledged"]')).toHaveTextContent("2条");
    expect(screen.getByRole("list", { name: "告警时间线" }).firstElementChild).toHaveTextContent(
      "已受理，正在同步",
    );
  });
  server.use(monitorHandler(newer));
  await user.click(screen.getByRole("button", { name: "刷新" }));
  await waitFor(() => {
    const timeline = screen.getByRole("list", { name: "告警时间线" });
    expect(timeline.firstElementChild).toHaveTextContent("已确认");
    expect(document.querySelector('[data-kpi="unacknowledged"]')).toHaveTextContent("2条");
  });
});

it("retries the original request after an uncertain response and reload", async () => {
  const sent: Schemas["AckCommandRequest"][] = [];
  server.use(
    monitorHandler(eligibleEnvelope()),
    http.post("*/api/v1/monitor/ack", async ({ request }) => {
      const body = (await request.json()) as Schemas["AckCommandRequest"];
      sent.push(body);
      return sent.length === 1
        ? new HttpResponse(null, { status: 503 })
        : HttpResponse.json({
            command_id: body.command_id,
            status: "succeeded",
            confirmation_id: "first-confirmation",
            message: "已受理，正在同步",
          });
    }),
  );
  const user = userEvent.setup();
  const first = renderApp("/monitor");
  await screen.findByRole("list", { name: "告警时间线" });
  await user.click(screen.getAllByRole("button", { name: /^确认$/ })[0] as HTMLElement);
  await screen.findByText("状态待核对");
  first.unmount();
  renderApp("/monitor");
  await screen.findByText("已受理，正在同步");
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
});

it.each([
  [403, "当前账号无权确认"],
  [409, "告警状态已变化"],
  [503, "确认状态待核对"],
] as const)("keeps the original request after HTTP %s", async (status, message) => {
  const sent: Schemas["AckCommandRequest"][] = [];
  server.use(
    monitorHandler(eligibleEnvelope()),
    http.post("*/api/v1/monitor/ack", async ({ request }) => {
      const body = (await request.json()) as Schemas["AckCommandRequest"];
      sent.push(body);
      return sent.length === 1
        ? new HttpResponse(null, { status })
        : HttpResponse.json({
            command_id: body.command_id,
            status: "succeeded",
            confirmation_id: "first-confirmation",
            message: "已受理，正在同步",
          });
    }),
  );
  const user = userEvent.setup();
  renderApp("/monitor");
  await screen.findByRole("list", { name: "告警时间线" });
  await user.click(screen.getAllByRole("button", { name: /^确认$/ })[0] as HTMLElement);
  await screen.findByText(message, { exact: false });
  await user.click(screen.getByRole("button", { name: "继续核对" }));
  await screen.findByText("已受理，正在同步");
  expect(sent[1]).toEqual(sent[0]);
});
