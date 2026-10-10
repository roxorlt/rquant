import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { PaperAccountsData } from "@/api/endpoints";
import { metaEnvelope, paperEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, paperHandler, server } from "@/test/server";

beforeEach(() => {
  server.use(
    http.get("*/api/v1/paper-portfolios", ({ request }) => {
      const envelope = paperEnvelope();
      return HttpResponse.json({
        ...envelope,
        serving: {
          ...envelope.serving,
          generation_id:
            new URL(request.url).searchParams.get("generation_id") ??
            envelope.serving.generation_id,
        },
        data: { availability: "unavailable", available_at: null, accounts: [] },
      });
    }),
  );
});

function readyHistory(): PaperAccountsData["history"] {
  return {
    source_state: "ready",
    source_updated_at: "2026-09-24T07:31:00Z",
    source_note: null,
    account_id: "shadow-main",
    total_orders: 2,
    has_more: false,
    newest_updated_at: "2026-09-24T07:30:00Z",
    oldest_updated_at: "2026-09-23T02:00:00Z",
    orders: [
      {
        order_id: "order-fixture-current",
        code: "600005.SH",
        name: "样本05",
        side: "BUY",
        side_label: "买入",
        order_type: "LIMIT",
        quantity: 300,
        filled_quantity: 100,
        average_fill_price: "12.3456",
        status: "PARTIALLY_FILLED",
        status_label: "部分成交",
        reject_reason: null,
        reject_message: null,
        created_at: "2026-09-24T01:30:00Z",
        updated_at: "2026-09-24T07:30:00Z",
        fills: [
          {
            fill_id: "fill-fixture-current",
            sequence: 1,
            quantity: 100,
            price: "12.3456",
            commission: "1.2345",
            transfer_fee: "0.10",
            tax: "0",
            total_fees: "1.3345",
            executed_at: "2026-09-24T01:32:00Z",
            persisted_at: "2026-09-24T01:32:01Z",
          },
        ],
      },
      {
        order_id: "order-fixture-older",
        code: "600001.SH",
        name: "样本01",
        side: "SELL",
        side_label: "卖出",
        order_type: "MARKET",
        quantity: 100,
        filled_quantity: 0,
        average_fill_price: null,
        status: "REJECTED",
        status_label: "未接受",
        reject_reason: "SUSPENDED",
        reject_message: "股票停牌",
        created_at: "2026-09-23T01:30:00Z",
        updated_at: "2026-09-23T02:00:00Z",
        fills: [],
      },
    ],
  };
}

function truncatedHistory(): PaperAccountsData["history"] {
  const history = readyHistory();
  const older = history.orders[1];
  if (!older) throw new Error("fixture order is missing");
  return {
    ...history,
    total_orders: 201,
    has_more: true,
    oldest_updated_at: "2026-09-22T02:00:00Z",
    orders: [
      ...history.orders,
      ...Array.from({ length: 198 }, (_, index) => ({
        ...older,
        order_id: `older-${index}`,
        name: "较早记录",
        created_at: "2026-09-22T01:30:00Z",
        updated_at: "2026-09-22T02:00:00Z",
      })),
    ],
  };
}

describe("模拟盘", () => {
  it("shows one published account, its holdings and valuation note without fake controls", async () => {
    const user = userEvent.setup();
    renderApp("/paper");
    const table = await screen.findByRole("table", { name: "模拟账户持仓" });
    expect(screen.getByRole("region", { name: "账户资产" })).toHaveTextContent("总资产100,042.00");
    expect(screen.getByRole("region", { name: "账户资产" })).toHaveTextContent("现金97,620.00");
    expect(screen.getByRole("region", { name: "账户资产" })).toHaveTextContent("持仓市值2,422.00");
    expect(screen.getByRole("region", { name: "账户资产" })).toHaveTextContent("浮动盈亏+42.00");
    expect(document.querySelector('[data-kpi="pnl"] .val .up')).not.toBeNull();
    expect(table).toHaveTextContent("样本05");
    expect(table).toHaveTextContent("可卖 100");
    expect(table).toHaveTextContent("可卖 0");
    expect(screen.queryByRole("button", { name: /对账|暂停|下单/ })).toBeNull();
    expect(document.body).not.toHaveTextContent("shadow-main");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
    await user.hover(screen.getByText("持仓市值"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("最近成交价");
  });

  it("uses A-share green for a loss", async () => {
    const base = paperEnvelope();
    const first = base.data.accounts[0];
    if (!first) throw new Error("fixture account is missing");
    server.use(paperHandler(paperEnvelope({ accounts: [{ ...first, unrealized_pnl: -42 }] })));
    renderApp("/paper");
    await screen.findByRole("region", { name: "账户资产" });
    expect(document.querySelector('[data-kpi="pnl"] .val .down')).not.toBeNull();
  });

  it("switches accounts and explains an empty holding list", async () => {
    const user = userEvent.setup();
    renderApp("/paper");
    await screen.findByRole("table", { name: "模拟账户持仓" });
    const choices = screen.getByRole("group", { name: "选择模拟账户" });
    const second = within(choices).getByRole("button", { name: "模拟账户 2" });
    act(() => second.focus());
    await user.keyboard("{Enter}");
    expect(second).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByRole("region", { name: "账户资产" })).toHaveTextContent("总资产5,000.00");
    expect(screen.getByText("当前账户没有持仓")).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "模拟账户持仓" })).toBeNull();
  });

  it.each([
    ["empty", "还没有模拟账户", "账户发布后会显示，请稍后刷新。"],
    ["not_published", "模拟账户尚未发布", "账户来源发布后会显示，请稍后刷新。"],
    ["unavailable", "暂时读不到页面数据", "请稍后刷新，或查看系统健康。"],
  ] as const)("explains %s without showing zero assets", async (state, title, hint) => {
    server.use(paperHandler(paperEnvelope({ source_state: state, accounts: [] })));
    renderApp("/paper");
    expect(await screen.findByText(title)).toBeInTheDocument();
    expect(screen.getByText(hint)).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "账户资产" })).toBeNull();
  });

  it("shows stale source data and clears old details on request failure", async () => {
    const user = userEvent.setup();
    server.use(
      paperHandler(paperEnvelope({ source_note: "模拟账户更新延迟，以下金额可能不是最新的。" })),
    );
    renderApp("/paper");
    expect(await screen.findByText("模拟账户更新延迟，以下金额可能不是最新的。")).toBeVisible();
    server.use(http.get("*/api/v1/paper/accounts", () => HttpResponse.error()));
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await screen.findByText("模拟账户暂时无法加载")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "账户资产" })).toBeNull();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
  });

  it("does not show a prior account when metadata is already on a newer generation", async () => {
    server.use(metaHandler(metaEnvelope({ generationId: "b".repeat(64) })));
    renderApp("/paper");
    expect(await screen.findByText("账户数据更新中")).toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "账户资产" })).toBeNull();
    expect(screen.queryByRole("table", { name: "模拟账户持仓" })).toBeNull();
  });

  it("offers retry when fetching a new generation fails", async () => {
    server.use(metaHandler(metaEnvelope({ generationId: "b".repeat(64) })));
    const user = userEvent.setup();
    renderApp("/paper");
    await screen.findByText("账户数据更新中");
    server.use(
      http.get("*/api/v1/paper/accounts", () =>
        HttpResponse.json({ detail: "模拟账户数据暂时无法读取" }, { status: 503 }),
      ),
    );
    await user.click(screen.getByRole("button", { name: "刷新" }));
    expect(await screen.findByText("模拟账户暂时无法加载")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试" })).toBeInTheDocument();
  });

  it("shows the source day's instructions, bounded recent window and actual fill detail", async () => {
    const user = userEvent.setup();
    server.use(paperHandler(paperEnvelope({ history: readyHistory() })));
    renderApp("/paper");

    const today = await screen.findByRole("table", { name: "当日模拟指令" });
    expect(screen.getByRole("group", { name: "筛选模拟指令" })).toHaveTextContent("09-24");
    expect(screen.getByText("覆盖 09-23 10:00 至 09-24 15:30")).toBeVisible();
    expect(screen.getByText("最近 2 / 2 条")).toBeVisible();
    expect(today).toHaveTextContent("样本05");
    expect(today).not.toHaveTextContent("样本01");
    expect(screen.queryByText(/今天没有指令/)).toBeNull();

    const row = within(today).getByRole("row", { name: /样本05/ });
    row.focus();
    await user.keyboard("{Enter}");
    const detail = await screen.findByRole("dialog", { name: /样本05.*指令详情/ });
    expect(detail).toHaveTextContent("12.35");
    expect(detail).toHaveTextContent("1.33");
    expect(detail).not.toHaveTextContent("12.3456");
    expect(detail).not.toHaveTextContent("1.3345");
    const roundedPrice = within(detail).getAllByText("12.35")[0];
    if (!roundedPrice) throw new Error("rounded price is missing");
    await user.hover(roundedPrice);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("12.3456");
    expect(detail).toHaveTextContent("09:32");
    expect(detail).toHaveTextContent("已成交 100 / 300");
    expect(screen.getByRole("main")).not.toHaveTextContent("order-fixture-current");

    await user.keyboard("{Escape}");
    await user.click(screen.getByRole("button", { name: /最近指令/ }));
    const recent = screen.getByRole("table", { name: "最近模拟指令" });
    expect(recent).toHaveTextContent("样本01");
    await user.click(within(recent).getByRole("row", { name: /样本01/ }));
    const rejected = await screen.findByRole("dialog", { name: /样本01.*指令详情/ });
    expect(rejected).toHaveTextContent("股票停牌");
    expect(rejected).not.toHaveTextContent("SUSPENDED");
  });

  it("marks a full 200-order window as truncated without claiming the day's history is complete", async () => {
    server.use(paperHandler(paperEnvelope({ history: truncatedHistory() })));
    renderApp("/paper");
    expect(await screen.findByText("最近 200 / 201 条")).toBeVisible();
    expect(screen.getByText("覆盖 09-22 10:00 至 09-24 15:30")).toBeVisible();
    expect(screen.getByText(/更早的指令未包含/)).toBeVisible();
  });

  it("isolates the selected account and drops its old detail when switching", async () => {
    const user = userEvent.setup();
    server.use(paperHandler(paperEnvelope({ history: readyHistory() })));
    renderApp("/paper");
    const table = await screen.findByRole("table", { name: "当日模拟指令" });
    await user.click(within(table).getByRole("row", { name: /样本05/ }));
    expect(await screen.findByRole("dialog", { name: /样本05.*指令详情/ })).toBeVisible();

    const choices = screen.getByRole("group", { name: "选择模拟账户" });
    await user.click(within(choices).getByRole("button", { name: "模拟账户 2" }));
    expect(await screen.findByText("当前账户的指令记录尚未发布")).toBeVisible();
    expect(screen.queryByRole("table", { name: "当日模拟指令" })).toBeNull();
    expect(screen.queryByRole("dialog", { name: /样本05.*指令详情/ })).toBeNull();
    await user.click(within(choices).getByRole("button", { name: "模拟账户 1" }));
    expect(await screen.findByRole("table", { name: "当日模拟指令" })).toBeVisible();
    expect(screen.queryByRole("dialog", { name: /样本05.*指令详情/ })).toBeNull();
  });

  it("uses the Shanghai date when a source window crosses UTC midnight", async () => {
    const history = readyHistory();
    const current = history.orders[0];
    const older = history.orders[1];
    if (!current || !older) throw new Error("fixture orders are missing");
    server.use(
      paperHandler(
        paperEnvelope({
          history: {
            ...history,
            source_updated_at: "2026-09-23T16:05:00Z",
            newest_updated_at: "2026-09-23T16:04:00Z",
            orders: [
              {
                ...current,
                created_at: "2026-09-23T16:01:00Z",
                updated_at: "2026-09-23T16:04:00Z",
                filled_quantity: 0,
                average_fill_price: null,
                status: "ACCEPTED",
                status_label: "待成交",
                fills: [],
              },
              older,
            ],
          },
        }),
      ),
    );
    renderApp("/paper");
    const today = await screen.findByRole("table", { name: "当日模拟指令" });
    expect(screen.getByRole("group", { name: "筛选模拟指令" })).toHaveTextContent("09-24");
    expect(today).toHaveTextContent("样本05");
    expect(today).not.toHaveTextContent("样本01");
  });

  it("drops order detail immediately when metadata advances to another generation", async () => {
    const user = userEvent.setup();
    server.use(paperHandler(paperEnvelope({ history: readyHistory() })));
    const { queryClient } = renderApp("/paper");
    const table = await screen.findByRole("table", { name: "当日模拟指令" });
    await user.click(within(table).getByRole("row", { name: /样本05/ }));
    expect(await screen.findByRole("dialog", { name: /样本05.*指令详情/ })).toBeVisible();
    server.use(metaHandler(metaEnvelope({ generationId: "b".repeat(64) })));
    await act(async () => {
      await queryClient.invalidateQueries({ queryKey: ["meta"] });
    });
    expect(await screen.findByText("账户数据更新中")).toBeVisible();
    expect(screen.queryByRole("dialog", { name: /样本05.*指令详情/ })).toBeNull();
  });

  it.each([
    ["not_published", "指令记录尚未发布"],
    ["empty", "还没有指令记录"],
    ["unavailable", "指令记录暂时不可用"],
  ] as const)(
    "explains %s history without claiming there were zero instructions",
    async (state, title) => {
      server.use(
        paperHandler(
          paperEnvelope({
            history: {
              ...readyHistory(),
              source_state: state,
              account_id: state === "not_published" ? null : "shadow-main",
              total_orders: state === "not_published" ? null : 0,
              has_more: false,
              newest_updated_at: null,
              oldest_updated_at: null,
              orders: [],
            },
          }),
        ),
      );
      renderApp("/paper");
      expect(await screen.findByText(title)).toBeVisible();
      expect(screen.queryByRole("table", { name: "当日模拟指令" })).toBeNull();
    },
  );
});
