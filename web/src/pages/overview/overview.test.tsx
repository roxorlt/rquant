import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { overviewEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { overviewHandler, server } from "@/test/server";

async function renderOverview() {
  const utils = renderApp("/overview");
  await screen.findByRole("table", { name: "最新信号" });
  return utils;
}

describe("总览", () => {
  it("shows the last trading day on a holiday, with why in a tooltip", async () => {
    const user = userEvent.setup();
    await renderOverview();
    const note = screen.getByText(/09-24 周四 · 最近交易日/);
    await user.hover(note);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "今天休市，显示最近一个交易日；下一交易日 09-28 周一",
    );
  });

  it("draws the pipeline and the key numbers", async () => {
    await renderOverview();
    const pipe = screen.getByRole("list", { name: "今日链路" });
    expect(within(pipe).getAllByRole("listitem")).toHaveLength(2);
    expect(pipe).toHaveTextContent("参考数据已完成5,556 只");
    const kpis = screen.getByRole("region", { name: "今日关键数字" });
    expect(kpis).toHaveTextContent("候选3只N 字一池 2 · 竞价跳空 1");
    expect(kpis).toHaveTextContent("信号2条买入意向 1 · 观察 1");
    expect(kpis).toHaveTextContent("推送2条 · 仅记录正式推送未开通 · 失败 1");
    expect(kpis).toHaveTextContent("模拟盘净值99,990.00浮动盈亏 −10.00 · 持仓 1 只");
    expect(kpis).toHaveTextContent("服务11/ 24 正常异常 1 · 注意 1");
    expect(kpis).toHaveTextContent("数据按时6/ 10日线、分钟线没按时");
    expect(kpis.querySelector('[data-kpi="deliveries"] .val')).toHaveAttribute("data-tone", "crit");
  });

  it("lists signals with plain labels and the delivery state", async () => {
    await renderOverview();
    const table = screen.getByRole("table", { name: "最新信号" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(2);
    expect(rows[0]).toHaveTextContent("13:05天威视讯 002238.SZ竞价跳空买入意向仅记录");
    expect(rows[0]).not.toHaveTextContent("已送达");
    expect(rows[1]).toHaveTextContent("失败");
    expect(rows[1]?.querySelector('.status[data-state="crit"]')).not.toBeNull();
  });

  it("explains shadow deliveries and the paper valuation in tooltips", async () => {
    const user = userEvent.setup();
    await renderOverview();
    const table = screen.getByRole("table", { name: "最新信号" });
    await user.hover(within(table).getAllByText("仅记录")[0] as HTMLElement);
    expect(await screen.findByRole("tooltip")).toHaveTextContent("正式推送开通前只记录不发送");
    await user.unhover(within(table).getAllByText("仅记录")[0] as HTMLElement);
    await user.hover(screen.getByText("99,990.00"));
    await waitFor(() =>
      expect(
        screen.getAllByRole("tooltip").some((tip) => tip.textContent?.includes("最近成交价")),
      ).toBe(true),
    );
  });

  it("renders every missing value as a dash", async () => {
    await renderOverview();
    const table = screen.getByRole("table", { name: "候选" });
    const row = within(table).getByText("002238.SZ").closest("tr") as HTMLElement;
    const cells = within(row).getAllByRole("cell");
    // No name → the code alone; no close or change → "—".
    expect(cells[0]).toHaveTextContent(/^002238\.SZ$/);
    expect(cells[2]).toHaveTextContent("—");
    expect(cells[3]).toHaveTextContent("—");
    expect(cells[4]).toHaveTextContent("13:04");
  });

  it("filters candidates by source", async () => {
    const user = userEvent.setup();
    await renderOverview();
    const table = screen.getByRole("table", { name: "候选" });
    expect(within(table).getAllByRole("row")).toHaveLength(4);
    await user.click(screen.getByRole("button", { name: "竞价跳空" }));
    expect(within(table).getAllByRole("row")).toHaveLength(2);
  });

  it("shows holdings and the attention list with where to go", async () => {
    await renderOverview();
    const holdings = screen.getByRole("table", { name: "模拟盘持仓" });
    expect(within(holdings).getAllByRole("row")).toHaveLength(2);
    expect(holdings).toHaveTextContent("丽岛新材 603937.SH10012.7612.711,270.64−5.00−0.39%");
    const attention = screen.getByText("1 条推送失败").closest("li") as HTMLElement;
    expect(attention).toHaveTextContent("需处理1 条推送失败手机可能没有收到这些信号看健康");
    expect(screen.queryByText(/模拟账户/)).not.toBeInTheDocument();
    expect(within(attention).getByRole("link", { name: "看健康" })).toHaveAttribute(
      "href",
      "/health",
    );
  });

  it("says why a list is empty and what happens next", async () => {
    const base = overviewEnvelope();
    server.use(
      overviewHandler(
        overviewEnvelope({
          session: {
            ...base.data.session,
            is_today: true,
            phase: "pre_open",
            today: "2026-09-28",
            trade_date: "2026-09-28",
          },
          signals: { total: 0, by_action: [], items: [] },
          attention: [],
          paper: null,
        }),
      ),
    );
    await renderOverview();
    expect(screen.getByText("今天还没有信号")).toBeInTheDocument();
    expect(screen.getByText("09:30 开盘后出现")).toBeInTheDocument();
    expect(screen.getByText("一切正常")).toBeInTheDocument();
    expect(screen.getByText("还没有模拟账户数据")).toBeInTheDocument();
  });

  it("keeps ids and hashes out of the page text", async () => {
    await renderOverview();
    await waitFor(() => expect(document.querySelector(".page-skel")).toBeNull());
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });

  it("shows a skeleton, not a spinner, while loading", async () => {
    renderApp("/overview");
    expect(await screen.findByRole("status", { name: "总览加载中" })).toBeInTheDocument();
    await screen.findByRole("table", { name: "最新信号" });
  });
});
