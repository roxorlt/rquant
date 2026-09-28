import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { PRICE_RULE_JOURNAL_KEY } from "@/api/priceAlertRuleCommand";
import { META_QUERY_KEY } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { PriceRulePanel } from "./PriceRulePanel";

const GENERATION = metaEnvelope().serving.generation_id as string;
type Rule = Schemas["PriceAlertRuleItemData"];

const row: Rule = {
  rule_id: "web-existing",
  version: 2,
  deleted: false,
  ts_code: "600001.SH",
  membership_version: 2,
  name: "上破提醒",
  priority: "P2",
  enabled: true,
  comparison: "gte",
  threshold: "12.50",
  valid_from: "09:30:00",
  valid_until: "14:57:00",
  scope_status: "valid",
  updated_at: "2026-09-24T07:30:00Z",
};

function prepare(items: Rule[] = []) {
  server.use(
    http.get("*/api/v1/monitor/rules", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          evaluation_running: false,
          items,
          message: "",
        },
      }),
    ),
    http.get("*/api/v1/watchlist", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          items: [
            {
              ts_code: "600001.SH",
              version: 2,
              source: "detail",
              price_levels: [],
              expires_at: "2026-09-24T08:00:00Z",
              updated_at: "2026-09-24T07:30:00Z",
            },
          ],
          message: "",
        },
      }),
    ),
    http.get("*/api/v1/watchlist/600001.SH", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          ts_code: "600001.SH",
          status: "active",
          version: 2,
          source: "detail",
          price_levels: [],
          expires_at: "2026-09-24T08:00:00Z",
          updated_at: "2026-09-24T07:30:00Z",
          message: "",
        },
      }),
    ),
  );
}

beforeEach(() => {
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: {
      request: async (_name: string, _options: unknown, task: () => Promise<void>) => task(),
    },
  });
});

function renderScopedPanel(viewer: string, generationId = GENERATION) {
  const client = testQueryClient();
  const ui = (owner: string, generation: string) => (
    <AppProviders queryClient={client}>
      <PriceRulePanel
        viewer={owner}
        generationId={generation}
        fresh={true}
        refreshMeta={() => undefined}
      />
    </AppProviders>
  );
  const view = render(ui(viewer, generationId));
  return {
    ...view,
    changeScope(owner: string, generation: string) {
      act(() => {
        client.setQueryData(
          META_QUERY_KEY,
          metaEnvelope({ viewer: owner, generationId: generation }),
        );
        view.rerender(ui(owner, generation));
      });
    },
  };
}

it("drops an Alice draft before Bob can save it, and drops it on a generation change", async () => {
  prepare();
  const post = vi.fn();
  server.use(http.post("*/api/v1/monitor/rules/commands", post));
  const view = renderScopedPanel("tester");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "新建规则" })).toBeEnabled(),
  );
  fireEvent.click(within(panel).getByRole("button", { name: "新建规则" }));
  fireEvent.change(within(panel).getByLabelText("名称"), { target: { value: "Alice 的草稿" } });
  fireEvent.change(within(panel).getByLabelText("价格"), { target: { value: "12.50" } });
  view.changeScope("bob", GENERATION);
  expect(within(panel).queryByLabelText("名称")).toBeNull();
  view.changeScope("tester", GENERATION);
  expect(within(panel).queryByLabelText("名称")).toBeNull();
  fireEvent.click(within(panel).getByRole("button", { name: "新建规则" }));
  view.changeScope("tester", "b".repeat(64));
  expect(within(panel).queryByLabelText("名称")).toBeNull();
  expect(post).not.toHaveBeenCalled();
});

it("closes a pending delete confirmation across viewer and generation boundaries", async () => {
  prepare([row]);
  const post = vi.fn();
  server.use(http.post("*/api/v1/monitor/rules/commands", post));
  const view = renderScopedPanel("tester");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  const rule = await within(panel).findByRole("listitem", { name: "上破提醒" });
  fireEvent.click(within(rule).getByRole("button", { name: "删除上破提醒" }));
  expect(screen.getByRole("button", { name: "删除规则" })).toBeInTheDocument();
  view.changeScope("bob", GENERATION);
  await waitFor(() => expect(screen.queryByRole("button", { name: "删除规则" })).toBeNull());
  view.changeScope("tester", GENERATION);
  fireEvent.click(within(rule).getByRole("button", { name: "删除上破提醒" }));
  view.changeScope("tester", "b".repeat(64));
  await waitFor(() => expect(screen.queryByRole("button", { name: "删除规则" })).toBeNull());
  expect(post).not.toHaveBeenCalled();
});

it.each([
  ["viewer", "bob", GENERATION],
  ["generation", "tester", "b".repeat(64)],
])(
  "does not post an in-flight save after its %s changes",
  async (_boundary, nextViewer, nextGeneration) => {
    prepare();
    let entered: () => void = () => undefined;
    const requested = new Promise<void>((resolve) => {
      entered = resolve;
    });
    let release: () => void = () => undefined;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let completed: () => void = () => undefined;
    const requestFinished = new Promise<void>((resolve) => {
      completed = resolve;
    });
    Object.defineProperty(navigator, "locks", {
      configurable: true,
      value: {
        request: async (_name: string, _options: unknown, task: () => Promise<void>) => {
          await task();
          completed();
        },
      },
    });
    server.use(
      http.get("*/api/v1/watchlist/600001.SH", async () => {
        entered();
        await gate;
        return HttpResponse.json({
          serving: metaEnvelope().serving,
          data: {
            availability: "ready",
            available_at: "2026-09-24T07:31:00Z",
            ts_code: "600001.SH",
            status: "active",
            version: 2,
            source: "detail",
            price_levels: [],
            expires_at: "2026-09-24T08:00:00Z",
            updated_at: "2026-09-24T07:30:00Z",
            message: "",
          },
        });
      }),
    );
    const post = vi.fn();
    server.use(http.post("*/api/v1/monitor/rules/commands", post));
    const view = renderScopedPanel("tester");
    const panel = await screen.findByRole("region", { name: "价格提醒规则" });
    await waitFor(() =>
      expect(within(panel).getByRole("button", { name: "新建规则" })).toBeEnabled(),
    );
    fireEvent.click(within(panel).getByRole("button", { name: "新建规则" }));
    fireEvent.change(within(panel).getByLabelText("名称"), { target: { value: "旧身份草稿" } });
    fireEvent.change(within(panel).getByLabelText("价格"), { target: { value: "12.50" } });
    fireEvent.click(within(panel).getByRole("button", { name: "保存规则" }));
    await requested;
    view.changeScope(nextViewer, nextGeneration);
    release();
    await requestFinished;
    expect(within(panel).queryByLabelText("名称")).toBeNull();
    expect(post).not.toHaveBeenCalled();
    expect(
      Object.keys(window.localStorage).filter((key) => key.startsWith(PRICE_RULE_JOURNAL_KEY)),
    ).toEqual([]);
  },
);

it("creates a single-stock price rule, persists the request, and never claims alerts are running", async () => {
  prepare();
  const requests: Array<Record<string, unknown>> = [];
  server.use(
    http.post("*/api/v1/monitor/rules/commands", async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body = (await request.json()) as Record<string, unknown>;
      requests.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        kind: body.kind,
        rule_id: (body.rule as { rule_id: string }).rule_id,
        status: "saved_syncing",
        version: 1,
        reason: null,
        message: "已保存，正在同步。",
      });
    }),
  );
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  expect(await within(panel).findByText("价格提醒尚未运行")).toBeInTheDocument();
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "新建规则" })).toBeEnabled(),
  );
  fireEvent.click(within(panel).getByRole("button", { name: "新建规则" }));
  fireEvent.change(within(panel).getByLabelText("名称"), { target: { value: "跌破提醒" } });
  fireEvent.change(within(panel).getByLabelText("价格"), { target: { value: "12.50" } });
  fireEvent.click(within(panel).getByRole("button", { name: "保存规则" }));
  await waitFor(() => expect(requests).toHaveLength(1));
  expect(requests[0]).toMatchObject({
    kind: "save_price_alert_rule",
    generation_id: GENERATION,
    ts_code: "600001.SH",
    membership_version: 2,
    expected_version: null,
  });
  expect(await within(panel).findByText("已保存，正在同步")).toBeInTheDocument();
  expect(within(panel).queryByText("正在提醒")).toBeNull();
});

it("offers a new command after a durable failed create without claiming the rule is live", async () => {
  prepare();
  const sent: Array<Record<string, unknown>> = [];
  server.use(
    http.post("*/api/v1/monitor/rules/commands", async ({ request }) => {
      const body = (await request.json()) as Record<string, unknown>;
      sent.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        kind: body.kind,
        rule_id: (body.rule as { rule_id: string }).rule_id,
        status: sent.length === 1 ? "failed" : "saved_syncing",
        version: sent.length === 1 ? null : 1,
        reason: null,
        message: sent.length === 1 ? "未保存" : "已保存，正在同步。",
      });
    }),
  );
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "新建规则" })).toBeEnabled(),
  );
  fireEvent.click(within(panel).getByRole("button", { name: "新建规则" }));
  fireEvent.change(within(panel).getByLabelText("名称"), { target: { value: "失败后重试" } });
  fireEvent.change(within(panel).getByLabelText("价格"), { target: { value: "12.50" } });
  fireEvent.click(within(panel).getByRole("button", { name: "保存规则" }));
  const recovery = await within(panel).findByRole("listitem", { name: "失败后重试" });
  expect(within(recovery).getByText("未保存，可重试")).toBeInTheDocument();
  fireEvent.click(within(recovery).getByRole("button", { name: "重新尝试" }));
  await waitFor(() => expect(sent).toHaveLength(2));
  expect(sent[1]).toMatchObject({ generation_id: GENERATION, expected_version: null });
  expect(sent[1]?.command_id).not.toBe(sent[0]?.command_id);
  expect(within(panel).queryByText("正在提醒")).toBeNull();
});

it("offers edit, switch, and delete for a valid rule, without changing the displayed fact early", async () => {
  prepare([row]);
  const requests: Array<Record<string, unknown>> = [];
  server.use(
    http.post("*/api/v1/monitor/rules/commands", async ({ request }) => {
      const body = (await request.json()) as Record<string, unknown>;
      requests.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        kind: body.kind,
        rule_id: "web-existing",
        status: "saved_syncing",
        version: 3,
        reason: null,
        message: "已保存，正在同步。",
      });
    }),
  );
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  const rule = await within(panel).findByRole("listitem", { name: "上破提醒" });
  expect(within(rule).getByRole("switch", { name: "上破提醒规则开关" })).toHaveAttribute(
    "aria-checked",
    "true",
  );
  fireEvent.click(within(rule).getByRole("button", { name: "编辑上破提醒" }));
  expect(within(panel).getByLabelText("名称")).toHaveValue("上破提醒");
  fireEvent.click(within(panel).getByRole("button", { name: "取消编辑" }));
  fireEvent.click(within(rule).getByRole("switch", { name: "上破提醒规则开关" }));
  await waitFor(() => expect(requests).toHaveLength(1));
  expect(requests[0]).toMatchObject({
    kind: "set_price_alert_rule_enabled",
    rule_id: "web-existing",
    expected_version: 2,
    enabled: false,
  });
  expect(within(rule).getByRole("switch", { name: "上破提醒规则开关" })).toHaveAttribute(
    "aria-checked",
    "true",
  );
  expect(within(rule).getByText("已保存，正在同步")).toBeInTheDocument();
  expect(within(rule).getByRole("button", { name: "删除上破提醒" })).toBeDisabled();
});

it("distinguishes unavailable rule source from an empty list", async () => {
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  expect(await within(panel).findByText("规则尚未就绪，请稍后刷新。")).toBeInTheDocument();
  expect(within(panel).queryByText("还没有价格规则")).toBeNull();
});

it("does not offer writes from an incomplete published rule", async () => {
  prepare([{ ...row, membership_version: null }]);
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  const rule = await within(panel).findByRole("listitem", { name: "上破提醒" });
  expect(within(rule).getByRole("switch", { name: "上破提醒规则开关" })).toBeDisabled();
  expect(within(rule).getByRole("button", { name: "编辑上破提醒" })).toBeDisabled();
  expect(within(rule).getByRole("button", { name: "删除上破提醒" })).toBeDisabled();
});

it("keeps the edited values when the exact watchlist version changed before submission", async () => {
  prepare();
  server.use(
    http.get("*/api/v1/watchlist/600001.SH", () =>
      HttpResponse.json({
        serving: metaEnvelope().serving,
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          ts_code: "600001.SH",
          status: "active",
          version: 3,
          source: "detail",
          price_levels: [],
          expires_at: "2026-09-24T08:00:00Z",
          updated_at: "2026-09-24T07:30:00Z",
        },
      }),
    ),
  );
  const post = vi.fn();
  server.use(http.post("*/api/v1/monitor/rules/commands", post));
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  await waitFor(() =>
    expect(within(panel).getByRole("button", { name: "新建规则" })).toBeEnabled(),
  );
  fireEvent.click(within(panel).getByRole("button", { name: "新建规则" }));
  fireEvent.change(within(panel).getByLabelText("名称"), { target: { value: "我的提醒" } });
  fireEvent.change(within(panel).getByLabelText("价格"), { target: { value: "12.50" } });
  fireEvent.click(within(panel).getByRole("button", { name: "保存规则" }));
  expect(await within(panel).findByText("规则或名单已更新，请刷新后重试。")).toBeInTheDocument();
  expect(within(panel).getByLabelText("名称")).toHaveValue("我的提醒");
  expect(post).not.toHaveBeenCalled();
});

it("edits with the published version and deletes only after confirmation", async () => {
  prepare([row]);
  const requests: Array<Record<string, unknown>> = [];
  server.use(
    http.post("*/api/v1/monitor/rules/commands", async ({ request }) => {
      const body = (await request.json()) as Record<string, unknown>;
      requests.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        kind: body.kind,
        rule_id: "web-existing",
        status: "saved_syncing",
        version: 3,
        reason: null,
        message: "",
      });
    }),
  );
  const view = renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  const rule = await within(panel).findByRole("listitem", { name: "上破提醒" });
  await waitFor(() =>
    expect(within(rule).getByRole("button", { name: "编辑上破提醒" })).toBeEnabled(),
  );
  fireEvent.click(within(rule).getByRole("button", { name: "编辑上破提醒" }));
  fireEvent.change(within(panel).getByLabelText("价格"), { target: { value: "13.00" } });
  fireEvent.click(within(panel).getByRole("button", { name: "保存规则" }));
  await waitFor(() => expect(requests).toHaveLength(1));
  expect(requests[0]).toMatchObject({
    kind: "save_price_alert_rule",
    expected_version: 2,
    rule: { rule_id: "web-existing", threshold: "13.00" },
  });
  expect(within(rule).getByText("12.50")).toBeInTheDocument();

  view.unmount();
  window.localStorage.clear();
  renderApp("/monitor");
  const again = await screen.findByRole("region", { name: "价格提醒规则" });
  const current = await within(again).findByRole("listitem", { name: "上破提醒" });
  fireEvent.click(within(current).getByRole("button", { name: "删除上破提醒" }));
  expect(requests).toHaveLength(1);
  fireEvent.click(await screen.findByRole("button", { name: "删除规则" }));
  await waitFor(() => expect(requests).toHaveLength(2));
  expect(requests[1]).toMatchObject({
    kind: "delete_price_alert_rule",
    rule_id: "web-existing",
    expected_version: 2,
  });
  expect(within(current).getByText("上破提醒")).toBeInTheDocument();
});

it("recovers the exact original request after a tab reopens even while rules GET is unavailable", async () => {
  const body: Schemas["SavePriceAlertRuleRequest"] = {
    kind: "save_price_alert_rule",
    command_id: "web-original",
    requested_at: "2026-09-24T07:31:30.000Z",
    generation_id: GENERATION,
    ts_code: "600001.SH",
    membership_version: 2,
    expected_version: null,
    rule: {
      rule_id: "web-recovery",
      name: "跌破提醒",
      priority: "P2",
      enabled: true,
      comparison: "lte",
      threshold: "12.50",
      valid_from: "09:30:00",
      valid_until: "14:57:00",
    },
  };
  window.localStorage.setItem(
    `${PRICE_RULE_JOURNAL_KEY}:tester:web-recovery`,
    JSON.stringify({
      schema: 1,
      body,
      status: "unknown",
      version: null,
      reason: null,
    }),
  );
  const requests: unknown[] = [];
  server.use(
    http.post("*/api/v1/monitor/rules/commands", async ({ request }) => {
      requests.push(await request.json());
      return HttpResponse.json(
        {
          command_id: body.command_id,
          kind: body.kind,
          rule_id: body.rule.rule_id,
          status: "uncertain",
          version: null,
          reason: null,
          message: "状态待核对",
        },
        { status: 503 },
      );
    }),
  );
  renderApp("/monitor");
  const panel = await screen.findByRole("region", { name: "价格提醒规则" });
  expect(await within(panel).findByRole("listitem", { name: "跌破提醒" })).toHaveTextContent(
    "状态待核对",
  );
  await waitFor(() => expect(requests).toHaveLength(1));
  expect(requests[0]).toEqual(body);
  expect(within(panel).getByText("规则尚未就绪，请稍后刷新。")).toBeInTheDocument();
});
