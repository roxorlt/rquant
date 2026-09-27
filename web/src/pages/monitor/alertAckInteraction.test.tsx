import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope, monitorEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, monitorHandler, server } from "@/test/server";
import { AlertAckCommandSession } from "./alertAckCommandSession";

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

beforeEach(() => {
  window.localStorage.clear();
  window.sessionStorage.clear();
});

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

it("ends a directly rejected old-version request and starts a new command only on the refreshed version", async () => {
  const posts: Schemas["AckCommandRequest"][] = [];
  server.use(
    monitorHandler(eligibleEnvelope()),
    http.post("*/api/v1/monitor/ack", async ({ request }) => {
      const body = (await request.json()) as Schemas["AckCommandRequest"];
      posts.push(body);
      return body.generation_id === monitorEnvelope().serving.generation_id
        ? HttpResponse.json(
            { detail: "数据已更新，请刷新告警时间线。", code: "stale_generation_no_effect" },
            { status: 409 },
          )
        : HttpResponse.json({
            command_id: body.command_id,
            status: "succeeded",
            confirmation_id: "first-confirmation",
            message: "已受理，正在同步",
          });
    }),
  );
  const user = userEvent.setup();
  const view = renderApp("/monitor");
  const timeline = await screen.findByRole("list", { name: "告警时间线" });
  await user.click(
    within(timeline.firstElementChild as HTMLElement).getByRole("button", { name: "确认" }),
  );
  await screen.findByText("数据已更新，请刷新后重新确认。");
  expect(
    within(timeline.firstElementChild as HTMLElement).queryByRole("button", { name: "继续核对" }),
  ).toBeNull();
  expect(
    within(timeline.firstElementChild as HTMLElement).queryByRole("button", { name: "重新确认" }),
  ).toBeNull();

  const generation = "c".repeat(64);
  const original = eligibleEnvelope();
  server.use(
    metaHandler(metaEnvelope({ generationId: generation })),
    monitorHandler({
      ...original,
      serving: { ...original.serving, generation_id: generation },
    }),
  );
  await view.queryClient.invalidateQueries({ queryKey: ["meta"] });
  const retry = await screen.findByRole("button", { name: "重新确认" });
  await user.click(retry);
  await screen.findByText("已受理，正在同步");
  expect(posts).toHaveLength(2);
  expect(posts[1]?.generation_id).toBe(generation);
  expect(posts[1]?.alert_id).toBe(posts[0]?.alert_id);
  expect(posts[1]?.command_id).not.toBe(posts[0]?.command_id);
});

it("recovers a lost old-version request after a proved no-effect retry on the refreshed version", async () => {
  const posts: Schemas["AckCommandRequest"][] = [];
  server.use(
    monitorHandler(eligibleEnvelope()),
    http.post("*/api/v1/monitor/ack", async ({ request }) => {
      const body = (await request.json()) as Schemas["AckCommandRequest"];
      posts.push(body);
      if (posts.length === 1) return new HttpResponse(null, { status: 503 });
      if (posts.length === 2)
        return HttpResponse.json(
          { detail: "数据已更新，请刷新告警时间线。", code: "stale_generation_no_effect" },
          { status: 409 },
        );
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
  const timeline = await screen.findByRole("list", { name: "告警时间线" });
  await user.click(
    within(timeline.firstElementChild as HTMLElement).getByRole("button", { name: "确认" }),
  );
  await screen.findByText("状态待核对");

  const generation = "c".repeat(64);
  const original = eligibleEnvelope();
  server.use(
    metaHandler(metaEnvelope({ generationId: generation })),
    monitorHandler({
      ...original,
      serving: { ...original.serving, generation_id: generation },
    }),
  );
  await Promise.all([
    view.queryClient.invalidateQueries({ queryKey: ["meta"] }),
    view.queryClient.invalidateQueries({ queryKey: ["monitor"] }),
  ]);
  await user.click(screen.getByRole("button", { name: "继续核对" }));
  await screen.findByText("数据已更新，请刷新后重新确认。");
  expect(posts[1]).toEqual(posts[0]);
  await user.click(screen.getByRole("button", { name: "重新确认" }));
  await screen.findByText("已受理，正在同步");
  expect(posts[2]?.generation_id).toBe(generation);
  expect(posts[2]?.command_id).not.toBe(posts[0]?.command_id);
});

it("does not resume the previous login's saved request after the browser identity changes", async () => {
  server.use(metaHandler(metaEnvelope({ viewer: "alice" })));
  const view = renderApp("/overview");
  await screen.findByRole("table", { name: "最新信号" });
  const saved = new AlertAckCommandSession(
    window.localStorage,
    "alice",
    async (body) => ({ command_id: body.command_id, status: "pending", message: "等待处理" }),
    () => "alice-command",
    () => "2026-09-28T07:00:00.000Z",
  );
  await saved.start(monitorEnvelope().serving.generation_id as string, IDS[0] as string);

  const posts = vi.fn(async ({ request }: { request: Request }) => {
    const body = (await request.json()) as Schemas["AckCommandRequest"];
    return HttpResponse.json({
      command_id: body.command_id,
      status: "pending",
      message: "等待处理",
    });
  });
  server.use(
    metaHandler(metaEnvelope({ viewer: "bob" })),
    monitorHandler(eligibleEnvelope()),
    http.post("*/api/v1/monitor/ack", posts),
  );
  await view.router.navigate("/monitor");
  await screen.findByRole("list", { name: "告警时间线" });
  await waitFor(() =>
    expect(screen.queryByText("请先登录，才能确认告警。")).not.toBeInTheDocument(),
  );
  expect(posts).not.toHaveBeenCalled();
  expect(screen.getAllByRole("button", { name: /^确认$/ })).toHaveLength(3);
});
