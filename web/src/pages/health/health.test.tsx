import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";

async function renderHealth() {
  const utils = renderApp("/health");
  await screen.findByRole("table", { name: "运行服务" });
  return utils;
}

describe("系统健康", () => {
  it("counts services by plain state", async () => {
    await renderHealth();
    const kpis = screen.getByRole("region", { name: "服务与数据" });
    expect(kpis).toHaveTextContent("服务4个");
    expect(kpis).toHaveTextContent("异常1");
    expect(kpis).toHaveTextContent("未运行1其中 1 个是盘中服务，不在交易时段");
  });

  it("names services in plain words; the technical id is in the tooltip", async () => {
    const user = userEvent.setup();
    await renderHealth();
    const table = screen.getByRole("table", { name: "运行服务" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows.map((row) => within(row).getAllByRole("cell")[0]?.textContent)).toEqual([
      "参考数据发布",
      "通知推送",
      "竞价撮合数据",
      "信号路由",
    ]);
    expect(table).not.toHaveTextContent("notifier.admin.shadow.v1");
    await user.hover(within(table).getByText("通知推送"));
    expect(await screen.findByRole("tooltip")).toHaveTextContent("notifier.admin.shadow.v1");
  });

  it("shows expected off-session idleness as 等待开盘, not red", async () => {
    await renderHealth();
    const badge = screen.getByText("等待开盘").closest(".status");
    expect(badge).toHaveAttribute("data-state", "waiting");
  });

  it("filters to what needs attention", async () => {
    const user = userEvent.setup();
    await renderHealth();
    await user.click(screen.getByRole("button", { name: "只看异常" }));
    const table = screen.getByRole("table", { name: "运行服务" });
    expect(within(table).getAllByRole("row")).toHaveLength(3);
    expect(table).not.toHaveTextContent("信号路由");
  });

  it("opens a detail drawer with the technical fields", async () => {
    const user = userEvent.setup();
    await renderHealth();
    await user.click(screen.getByText("参考数据发布", { selector: "td .nm" }));
    const drawer = await screen.findByRole("dialog");
    expect(drawer).toHaveTextContent("reference-slow.publisher.v1");
    expect(drawer).toHaveTextContent("连续失败239");
    expect(drawer).toHaveTextContent("ReferenceSlowRuntimeError");
  });

  it("shows data freshness, page data and recent errors", async () => {
    await renderHealth();
    const freshness = screen.getByRole("table", { name: "数据新鲜度" });
    expect(freshness).toHaveTextContent("分钟线09-23 周三延迟");
    expect(freshness).toHaveTextContent("研究任务—未发布");
    expect(screen.getByText("2 个")).toBeInTheDocument();
    expect(screen.getByText("连续失败 239 次", { selector: ".what" })).toBeInTheDocument();
  });

  it("keeps ids and hashes out of the page text", async () => {
    await renderHealth();
    await waitFor(() => expect(document.querySelector(".page-skel")).toBeNull());
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  });
});
