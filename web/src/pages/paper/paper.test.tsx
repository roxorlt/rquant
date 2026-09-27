import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { metaEnvelope, paperEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { metaHandler, paperHandler, server } from "@/test/server";

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
});
