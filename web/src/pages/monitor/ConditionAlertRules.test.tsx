import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { Schemas } from "@/api/client";
import { ConditionAlertRules, conditionDeliveryLabel } from "./ConditionAlertRules";

type List = Schemas["ConditionAlertRuleListData"];
type Command = Schemas["ConditionAlertRuleCommandRequest"];
const lifecycle = vi.hoisted(() => ({
  callback: undefined as ((open: boolean) => void) | undefined,
}));
vi.mock("@/ui", async (original) => {
  const actual = await original<typeof import("@/ui")>();
  return {
    ...actual,
    SideDrawer: (props: Parameters<typeof actual.SideDrawer>[0]) => {
      lifecycle.callback = props.afterOpenChange;
      return <actual.SideDrawer {...props} />;
    },
  };
});
const api = vi.hoisted(() => ({
  owner: "alice" as string | null,
  generation: "a".repeat(64),
  data: null as List | null,
  post: vi.fn(),
  fetchDraft: vi.fn(),
  refresh: vi.fn(),
}));
vi.mock("@/api/screen", async (original) => ({
  ...(await original<typeof import("@/api/screen")>()),
  fetchScreenAlertDraft: (...args: unknown[]) => api.fetchDraft(...args),
}));
vi.mock("@/api/conditionAlerts", async (original) => ({
  ...(await original<typeof import("@/api/conditionAlerts")>()),
  useConditionRules: () => ({
    owner: api.owner,
    generation: api.generation,
    data: api.data,
    serverTime: "2026-10-05T02:00:00Z",
    loading: false,
    refresh: api.refresh,
  }),
  postConditionRule: (...args: unknown[]) => api.post(...args),
}));

function list(): List {
  return {
    availability: "ready",
    available_at: "2026-10-05T02:00:00Z",
    message: "",
    can_write: true,
    can_enable: false,
    write_message: "",
    enable_message: "盘中数据暂不可用。",
    triggers: [],
    ranking_metrics: [],
    blocks: [
      {
        key: "not_st",
        label: "排除 ST",
        hint: "排除特殊处理股票。",
        category: "basic",
        category_label: "基础",
        parameters: [],
      },
    ],
    scopes: [
      {
        label: "全市场",
        scope: { kind: "market", universe_policy: "trusted_current" },
        available: false,
        member_count: null,
        message: "盘中数据暂不可用。",
      },
    ],
    items: [
      {
        rule_id: "rule-a",
        version: 2,
        rule: {
          schema_version: 1,
          rule_id: "rule-a",
          name: "完整条件",
          enabled: false,
          priority: "P2",
          conditions: [{ name: "not_st", args: {} }],
          ranking: null,
          scope: { kind: "market", universe_policy: "trusted_current" },
          frequency: { kind: "every_evaluation" },
          governance: { channels: ["pushdeer"], dedup_window_seconds: 60, notify_recovery: false },
          trading_hours: {
            timezone: "Asia/Shanghai",
            windows: [
              { start: "09:30:00", end: "11:30:00" },
              { start: "13:00:00", end: "14:57:00" },
            ],
          },
          source_policy: {
            condition_semantics_version: "screen-registry/v1",
            daily_anchor: "previous_closed_session",
            intraday_contract_id: "intraday-pit",
            minimum_intraday_contract_version: 3,
          },
        },
        updated_at: "2026-10-05T02:00:00Z",
        scope_status: "unavailable",
        scope_message: "盘中数据暂不可用。",
        status_label: "未运行",
        matched_count: null,
        unknown_count: null,
      },
    ],
  };
}
beforeEach(() => {
  lifecycle.callback = undefined;
  localStorage.clear();
  window.history.replaceState(null, "", "#/monitor");
  api.owner = "alice";
  api.generation = "a".repeat(64);
  api.data = list();
  api.post.mockReset();
  api.fetchDraft.mockReset();
  api.refresh.mockReset();
});

it.each([false, true])(
  "returns to the caller only after the editor closes (discard=%s)",
  async (discard) => {
    const user = userEvent.setup();
    render(<ConditionAlertRules />);
    const trigger = screen.getByRole("button", { name: "编辑条件 完整条件" });
    await user.click(trigger);
    const field = await screen.findByLabelText("条件规则名称");
    expect(screen.getByRole("button", { name: "编辑条件 完整条件" })).not.toHaveFocus();
    if (discard) await user.type(field, " 修改");
    await user.click(screen.getByRole("button", { name: "取消编辑条件" }));
    if (discard) {
      expect(field).toBeInTheDocument();
      await user.click(await screen.findByRole("button", { name: "放弃修改" }));
    }
    await waitFor(() => expect(screen.queryByLabelText("条件规则名称")).toBeNull());
    // The real dialog may disappear before any false motion callback.
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "编辑条件 完整条件" })).toHaveFocus(),
    );
    await act(async () => lifecycle.callback?.(false));
    expect(screen.getByRole("button", { name: "编辑条件 完整条件" })).toHaveFocus();
    expect(api.post).not.toHaveBeenCalled();
  },
);

it.each(["another owner", "removed rule", "unmounted page"])(
  "does not focus %s when the close callback arrives",
  async (change) => {
    const user = userEvent.setup();
    const view = render(<ConditionAlertRules />);
    await user.click(screen.getByRole("button", { name: "编辑条件 完整条件" }));
    await screen.findByLabelText("条件规则名称");
    if (change === "another owner") api.owner = "bob";
    else {
      await user.click(screen.getByRole("button", { name: "取消编辑条件" }));
      if (change === "removed rule") api.data = { ...list(), items: [] };
    }
    if (change === "unmounted page") view.unmount();
    else view.rerender(<ConditionAlertRules />);
    await waitFor(() => expect(screen.queryByLabelText("条件规则名称")).toBeNull());
    const focus = vi.spyOn(HTMLElement.prototype, "focus");
    await act(async () => lifecycle.callback?.(false));
    expect(focus).not.toHaveBeenCalled();
    focus.mockRestore();
  },
);

it("reloads the actual private import when its link changes without changing owner or generation", async () => {
  const fields = api.data?.items[0]?.rule;
  if (!fields) throw new Error("rule missing");
  const conditions = fields.conditions;
  const sourcePolicy = fields.source_policy;
  const firstId = "1".repeat(24);
  const secondId = "2".repeat(24);
  function reply(id: string): Schemas["ScreenQueryReadData"] {
    return {
      available: true,
      owner_scope_tag: "3".repeat(64),
      presets: [],
      daily_run_evidence: [],
      alert_draft: {
        schema_version: 1,
        draft_id: id,
        created_at: "2026-10-05T02:00:00Z",
        expires_at: "2026-10-06T02:00:00Z",
        suggested_name: `选股 ${id.slice(0, 1)}`,
        conditions,
        ranking: { top_n: 7, conditions: [{ metric: "amount", ascending: false, weight: 0.4 }] },
        preferred_scope: { kind: "market", universe_policy: "trusted_current" },
        source_policy: sourcePolicy,
        definition: {
          schema_version: 1,
          description: "完整原条件",
          mode: "daily",
          trade_date: "2026-09-30",
          source_kind: "replica",
          source_identity: "4".repeat(64),
          conditions,
        },
        origin: {
          draft_id: id,
          execution_id: "original-screen-command",
          command_hash: "5".repeat(64),
          definition_hash: "6".repeat(64),
          result_digest: "7".repeat(64),
          member_rank_digest: "8".repeat(64),
          source_identity: "4".repeat(64),
          mode: "daily",
          trade_date: "2026-09-30",
        },
        capabilities: {
          condition_count: 1,
          ranking_imported: true,
          consumer_state: "awaiting_consumer",
          message: "提醒草稿已生成，尚未生效。",
        },
        content_hash: "9".repeat(64),
      },
    };
  }
  api.fetchDraft.mockImplementation(async (id: string) => reply(id));
  window.history.replaceState(null, "", `#/monitor?conditionDraft=${firstId}`);
  render(<ConditionAlertRules />);
  expect(await screen.findByRole("button", { name: "继续设置条件提醒" })).toBeEnabled();
  window.history.replaceState(null, "", `#/monitor?conditionDraft=${secondId}`);
  fireEvent(window, new HashChangeEvent("hashchange"));
  await waitFor(() =>
    expect(api.fetchDraft).toHaveBeenCalledWith(secondId, expect.any(AbortSignal)),
  );
  fireEvent.click(screen.getByRole("button", { name: "继续设置条件提醒" }));
  expect(await screen.findByLabelText("条件规则名称")).toHaveValue("选股 2");
  expect(screen.getByLabelText("排名权重 1")).toHaveValue(0.4);
  expect(api.post).not.toHaveBeenCalled();
});

it("distinguishes original shadow receipts, provider acceptance and missing receipts", () => {
  const target: Schemas["ConditionAlertTargetReceipt"] = {
    channel: "pushdeer",
    recipient_id: "admin",
    state: "succeeded",
    attempted_at: "2026-10-05T02:00:00Z",
    provider_receipt: "shadow:offline",
  };
  expect(conditionDeliveryLabel({ delivery_state: "succeeded", targets: [target] })).toBe("已记录");
  expect(
    conditionDeliveryLabel({
      delivery_state: "succeeded",
      targets: [{ ...target, provider_receipt: "runtime-condition-notification/v1:receipt" }],
    }),
  ).toBe("已提交");
  expect(
    conditionDeliveryLabel({
      delivery_state: "succeeded",
      targets: [{ ...target, provider_receipt: null }],
    }),
  ).toBe("待核对");
});

it("shows unknown counts and keeps saving separate from unproved activation", async () => {
  render(<ConditionAlertRules />);
  expect(screen.getByText("未运行")).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "编辑条件 完整条件" }));
  expect(await screen.findByLabelText("条件规则名称")).toHaveValue("完整条件");
  expect(screen.getByLabelText("启用条件规则")).toBeDisabled();
  expect(screen.getByRole("button", { name: "保存条件规则" })).toBeEnabled();
  api.post.mockImplementation(async (body: Command) => ({
    command_id: body.command_id,
    rule_id: body.rule_id,
    action: body.action,
    status: "published",
    version: 3,
    consumer_state: "unknown",
    message: "设置已保存，运行待核对。",
  }));
  fireEvent.change(screen.getByLabelText("条件规则名称"), { target: { value: "保留完整条件" } });
  fireEvent.click(screen.getByRole("button", { name: "保存条件规则" }));
  await screen.findByText("设置已保存，运行待核对。");
  expect(api.post).toHaveBeenCalledTimes(1);
  const [body, resume] = api.post.mock.calls[0] ?? [];
  expect(body.expected_version).toBe(2);
  expect(body.rule.conditions).toEqual([{ name: "not_st", args: {} }]);
  expect(body.rule.enabled).toBe(false);
  expect(resume).toBe(false);
});

it("retains the original request after lost acknowledgement and only resumes it", async () => {
  api.post.mockRejectedValue(new Error("lost ack"));
  const view = render(<ConditionAlertRules />);
  fireEvent.click(screen.getByRole("button", { name: "编辑条件 完整条件" }));
  await screen.findByLabelText("条件规则名称");
  fireEvent.click(screen.getByRole("button", { name: "保存条件规则" }));
  await screen.findByText("状态待核对，请继续核对原操作。");
  const original = api.post.mock.calls[0]?.[0];
  view.unmount();
  api.post.mockResolvedValue({
    command_id: original.command_id,
    rule_id: original.rule_id,
    action: "save",
    status: "saved_syncing",
    version: 3,
    consumer_state: "unknown",
    message: "设置已写入，等待同步。",
  });
  render(<ConditionAlertRules />);
  await waitFor(() => expect(api.post).toHaveBeenCalledTimes(2));
  expect(api.post.mock.calls[1]).toEqual([original, true]);
  expect(await screen.findByText("设置已写入，等待同步。")).toBeInTheDocument();
});

it("blocks stale drafts on a generation change and clears them on an owner change", async () => {
  const view = render(<ConditionAlertRules />);
  fireEvent.click(screen.getByRole("button", { name: "编辑条件 完整条件" }));
  await screen.findByLabelText("条件规则名称");
  api.generation = "b".repeat(64);
  view.rerender(<ConditionAlertRules />);
  expect(screen.getByRole("button", { name: "保存条件规则" })).toBeDisabled();
  api.owner = "bob";
  view.rerender(<ConditionAlertRules />);
  await waitFor(() => expect(screen.queryByLabelText("条件规则名称")).toBeNull());
  expect(api.post).not.toHaveBeenCalled();
  api.generation = "a".repeat(64);
});

it("requires the actual market scope name before enabling and preserves the original version", async () => {
  if (!api.data) throw new Error("list missing");
  api.data.can_enable = true;
  const row = api.data.items[0];
  const scope = api.data.scopes[0];
  if (!row || !scope) throw new Error("scope missing");
  row.scope_status = "bound";
  scope.available = true;
  scope.member_count = 5557;
  api.post.mockImplementation(async (body: Command) => ({
    command_id: body.command_id,
    rule_id: body.rule_id,
    action: body.action,
    status: "published",
    version: 3,
    consumer_state: "unknown",
    message: "设置已保存，运行待核对。",
  }));
  render(<ConditionAlertRules />);
  fireEvent.click(screen.getByRole("button", { name: "启用" }));
  const confirm = await screen.findByRole("button", { name: "确认启用" });
  expect(confirm).toBeDisabled();
  expect(api.post).not.toHaveBeenCalled();
  fireEvent.change(screen.getByRole("textbox"), { target: { value: "全市场" } });
  expect(confirm).toBeEnabled();
  fireEvent.click(confirm);
  await waitFor(() => expect(api.post).toHaveBeenCalledTimes(1));
  expect(api.post.mock.calls[0]?.[0]).toMatchObject({
    action: "set_enabled",
    expected_version: 2,
    enabled: true,
    rule_id: "rule-a",
  });
});

it("retains imported rank weights and saves full governance settings without claiming a send", async () => {
  if (!api.data?.items[0]) throw new Error("rule missing");
  api.data.ranking_metrics = [
    { value: "pct_chg", label: "涨幅" },
    { value: "amount", label: "成交额" },
  ];
  api.data.items[0].rule.ranking = {
    top_n: 10,
    conditions: [
      { metric: "pct_chg", weight: 0.7, ascending: false },
      { metric: "amount", weight: 0.3, ascending: true },
    ],
  };
  api.post.mockImplementation(async (body: Command) => {
    expect(localStorage.getItem("rquant.condition-command.v1:alice")).toBe(JSON.stringify(body));
    return {
      command_id: body.command_id,
      rule_id: body.rule_id,
      action: body.action,
      status: "saved_syncing",
      version: 3,
      consumer_state: "unknown",
      message: "设置已写入，等待同步。",
    };
  });
  render(<ConditionAlertRules />);
  fireEvent.click(screen.getByRole("button", { name: "编辑条件 完整条件" }));
  expect(await screen.findByLabelText("排名权重 1")).toHaveValue(0.7);
  fireEvent.change(screen.getByLabelText("提醒频率"), { target: { value: "per_symbol_minutes" } });
  fireEvent.change(screen.getByLabelText("提醒间隔分钟"), { target: { value: "12" } });
  fireEvent.click(screen.getByLabelText("条件恢复时提醒"));
  fireEvent.click(screen.getByLabelText("PushPlus"));
  fireEvent.click(screen.getByRole("button", { name: "保存条件规则" }));
  await screen.findByText("设置已写入，等待同步。");
  const body = api.post.mock.calls[0]?.[0];
  expect(body.rule.ranking).toEqual(api.data.items[0].rule.ranking);
  expect(body.rule.frequency).toEqual({ kind: "per_symbol_minutes", minutes: 12 });
  expect(body.rule.governance.channels).toEqual(["pushdeer", "pushplus"]);
  expect(body.rule.governance.notify_recovery).toBe(true);
  expect(screen.queryByText("已发送")).toBeNull();
});
