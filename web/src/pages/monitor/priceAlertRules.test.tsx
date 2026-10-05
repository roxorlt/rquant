import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import { PRICE_RULE_JOURNAL } from "./priceAlertRuleCommandSession";

type Command = Schemas["PriceAlertRuleCommandRequest"];
type List = Schemas["Envelope_PriceAlertRuleListData_"];
const PREFIX = "*/api/v1/monitor/price-rules";
const list = (): List => ({
  serving: metaEnvelope().serving,
  data: {
    availability: "ready",
    available_at: "2026-09-24T07:31:00Z",
    message: "",
    can_write: true,
    write_message: "",
    members: [{ ts_code: "600001.SH", version: 1, expires_at: null }],
    priority_options: [
      { value: "P0", label: "紧急" },
      { value: "P1", label: "重要" },
      { value: "P2", label: "普通" },
      { value: "P3", label: "提示" },
    ],
    items: [
      {
        rule_id: "rule-a",
        version: 1,
        ts_code: "600001.SH",
        membership_version: 1,
        name: "突破提醒",
        priority: "P2",
        priority_label: "普通",
        enabled: true,
        comparison: "gte",
        threshold: "10.123456",
        valid_from: "09:30:01.123456",
        valid_until: "14:57:02",
        updated_at: "2026-09-24T07:30:00Z",
        scope_status: "bound",
        status_label: "未运行",
        scope_message: "行情评估接通后才会提醒。",
      },
    ],
  },
});
beforeEach(() => {
  localStorage.clear();
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (_name: string, _options: unknown, callback: () => Promise<void>) =>
        callback(),
    },
  });
  server.use(http.get(PREFIX, () => HttpResponse.json(list())));
});

it("opens an editor with exact original price and seconds, and guards unsaved drafts", async () => {
  renderApp("/monitor");
  const trigger = await screen.findByRole("button", { name: "编辑 突破提醒" });
  trigger.focus();
  fireEvent.click(trigger);
  expect(await screen.findByLabelText("阈值价格")).toHaveValue("10.123456");
  expect(screen.getByLabelText("开始时间")).toHaveValue("09:30:01.123456");
  fireEvent.change(screen.getByLabelText("规则名称"), { target: { value: "未保存草稿" } });
  fireEvent.click(screen.getByRole("button", { name: "取消编辑" }));
  expect(await screen.findByText("放弃未保存的修改？")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "放弃修改" }));
  await waitFor(() => expect(screen.queryByLabelText("阈值价格")).toBeNull());
  await waitFor(() => expect(screen.getByRole("button", { name: "编辑 突破提醒" })).toHaveFocus());
});

it("saves a new rule after durable recording and shows syncing instead of completion", async () => {
  const seen: Command[] = [];
  server.use(
    http.post(`${PREFIX}/commands`, async ({ request }) => {
      const value = (await request.json()) as Command;
      seen.push(value);
      const stored = JSON.parse(
        localStorage.getItem(`${PRICE_RULE_JOURNAL}:tester:${value.command_id}`) ?? "{}",
      );
      expect(stored.body).toEqual(value);
      return HttpResponse.json({
        command_id: value.command_id,
        rule_id: value.rule_id,
        action: value.action,
        status: "saved_syncing",
        version: 1,
        message: "设置已写入，等待同步。",
      });
    }),
  );
  renderApp("/monitor");
  await waitFor(() => expect(screen.getByRole("button", { name: "新建规则" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
  fireEvent.change(await screen.findByLabelText("阈值价格"), { target: { value: "8.987654" } });
  fireEvent.click(screen.getByRole("button", { name: "保存规则" }));
  expect(await screen.findAllByText("设置已写入，等待同步")).not.toHaveLength(0);
  expect(seen[0]?.rule?.threshold).toBe("8.987654");
  expect(seen[0]).not.toHaveProperty("owner_id");
  expect(screen.queryByText("已保存")).toBeNull();
});

it("runs enable and delete through the original actions and precise versions", async () => {
  const seen: Command[] = [];
  server.use(
    http.post(`${PREFIX}/commands`, async ({ request }) => {
      const value = (await request.json()) as Command;
      seen.push(value);
      return HttpResponse.json({
        command_id: value.command_id,
        rule_id: value.rule_id,
        action: value.action,
        status: "published",
        version: 2,
        message: "已完成。",
      });
    }),
  );
  renderApp("/monitor");
  fireEvent.click(await screen.findByRole("switch", { name: "启停 突破提醒" }));
  await waitFor(() => expect(seen).toHaveLength(1));
  expect(seen[0]).toMatchObject({ action: "set_enabled", expected_version: 1, enabled: false });
  fireEvent.click(screen.getByRole("button", { name: "删除 突破提醒" }));
  const modal = await screen.findByRole("dialog", { name: "删除到价规则？" });
  fireEvent.click(within(modal).getByRole("button", { name: "删除规则" }));
  await waitFor(() => expect(seen).toHaveLength(2));
  expect(seen[1]).toMatchObject({ action: "delete", expected_version: 1 });
  expect(seen[1]).not.toHaveProperty("enabled");
});

it("a late save cannot overwrite another draft or show current success", async () => {
  let release: () => void = () => undefined;
  const pending = new Promise<void>((resolve) => {
    release = resolve;
  });
  server.use(
    http.post(`${PREFIX}/commands`, async ({ request }) => {
      const value = (await request.json()) as Command;
      await pending;
      return HttpResponse.json({
        command_id: value.command_id,
        rule_id: value.rule_id,
        action: value.action,
        status: "published",
        version: 2,
        message: "已保存。",
      });
    }),
  );
  renderApp("/monitor");
  fireEvent.click(await screen.findByRole("button", { name: "编辑 突破提醒" }));
  fireEvent.click(await screen.findByRole("button", { name: "保存规则" }));
  fireEvent.click(screen.getByRole("button", { name: "取消编辑" }));
  await waitFor(() => expect(screen.queryByLabelText("规则名称")).toBeNull());
  fireEvent.click(screen.getByRole("button", { name: "新建规则" }));
  fireEvent.change(await screen.findByLabelText("规则名称"), { target: { value: "另一份草稿" } });
  await act(async () => {
    release();
    await pending;
  });
  await waitFor(() => expect(screen.getByLabelText("规则名称")).toHaveValue("另一份草稿"));
  expect(
    within(screen.getByRole("dialog", { name: "新建到价规则" })).queryByText("已保存"),
  ).toBeNull();
});

it("owner change hides the previous drawer and original records", async () => {
  const app = renderApp("/monitor");
  fireEvent.click(await screen.findByRole("button", { name: "编辑 突破提醒" }));
  expect(await screen.findByLabelText("规则名称")).toHaveValue("突破提醒");
  server.use(metaHandler(metaEnvelope({ viewer: "bob" })));
  act(() => app.queryClient.setQueryData(["meta"], metaEnvelope({ viewer: "bob" })));
  await waitFor(() => expect(screen.queryByLabelText("规则名称")).toBeNull());
});

it("invalid time keeps the draft and sends no configuration command", async () => {
  const sent = vi.fn();
  server.use(
    http.post(`${PREFIX}/commands`, () => {
      sent();
      return HttpResponse.json({});
    }),
  );
  renderApp("/monitor");
  fireEvent.click(await screen.findByRole("button", { name: "编辑 突破提醒" }));
  fireEvent.change(await screen.findByLabelText("开始时间"), { target: { value: "15:00" } });
  fireEvent.click(screen.getByRole("button", { name: "保存规则" }));
  expect(await screen.findByText("请填写有效时间，开始须早于结束。")).toBeInTheDocument();
  expect(sent).not.toHaveBeenCalled();
  expect(screen.getByLabelText("开始时间")).toHaveValue("15:00");
});

it.each(["empty", "unavailable", "not_activated"])(
  "renders an honest %s state and a next action",
  async (state) => {
    const response = list();
    response.data.items = [];
    if (state !== "empty") {
      response.data.availability = state as "unavailable" | "not_activated";
      response.data.can_write = false;
      response.data.message =
        state === "not_activated" ? "规则尚未开放。" : "规则暂不可用，请稍后重试。";
      response.data.write_message = response.data.message;
    }
    server.use(http.get(PREFIX, () => HttpResponse.json(response)));
    renderApp("/monitor");
    const panel = await screen.findByRole("region", { name: "告警规则" });
    expect(
      await within(panel).findByText(state === "empty" ? "还没有到价规则" : response.data.message),
    ).toBeInTheDocument();
    if (state === "empty")
      await waitFor(() =>
        expect(within(panel).getByRole("button", { name: "新建规则" })).toBeEnabled(),
      );
    else expect(within(panel).getByRole("button", { name: "新建规则" })).toBeDisabled();
  },
);

it("keyboard focus exposes the reason and preserves distinct enable intent", async () => {
  renderApp("/monitor");
  const status = await screen.findByText("未运行");
  fireEvent.focus(status.closest(".tip-anchor") ?? status);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("行情评估接通后才会提醒。");
  expect(await screen.findByRole("switch", { name: "启停 突破提醒" })).toHaveAttribute(
    "aria-checked",
    "true",
  );
  expect(screen.queryByText("正在监控")).toBeNull();
});
