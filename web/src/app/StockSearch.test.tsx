import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { StockSearch } from "./StockSearch";

const serving = metaEnvelope().serving;

function stockHandlers() {
  server.use(
    http.get("*/api/v1/stocks/search", ({ request }) => {
      const query = new URL(request.url).searchParams.get("q") ?? "";
      return HttpResponse.json({
        data: {
          query,
          available: true,
          rows:
            query.includes("600001") || query.includes("样本01")
              ? [{ ts_code: "600001.SH", name: "样本01" }]
              : [],
          truncated: false,
        },
        serving,
      });
    }),
    http.get("*/api/v1/stocks/600001.SH/summary", () =>
      HttpResponse.json({
        data: {
          ts_code: "600001.SH",
          name: "样本01",
          price: 8.8,
          as_of: "2026-09-24T07:00:03Z",
          pools: ["N 字一池"],
        },
        serving,
      }),
    ),
    http.get("*/api/v1/panorama/stocks/600001.SH/daily", () =>
      HttpResponse.json({
        data: { ts_code: "600001.SH", name: "样本01", bars: [] },
        serving,
      }),
    ),
  );
}

describe("顶栏个股搜索", () => {
  it("支持键盘选择，抽屉显示价格与池子，无日 K 时说明原因", async () => {
    stockHandlers();
    const user = userEvent.setup();
    renderApp("/overview");

    const input = screen.getByRole("combobox", { name: "搜索股票" });
    await user.type(input, "600001");
    await screen.findByRole("option", { name: /样本01.*600001\.SH/ });
    await user.keyboard("{ArrowDown}{Enter}");

    const drawer = await screen.findByRole("dialog", { name: /样本01/ });
    expect(within(drawer).getByText("8.80")).toBeInTheDocument();
    expect(within(drawer).getByText("N 字一池")).toBeInTheDocument();
    expect(within(drawer).getByText("暂无日 K 数据")).toBeInTheDocument();
    await user.keyboard("{Escape}");
    expect(drawer).not.toBeVisible();
  });

  it("明确显示无结果，而不打开空抽屉", async () => {
    stockHandlers();
    const user = userEvent.setup();
    renderApp("/overview");
    await user.type(screen.getByRole("combobox", { name: "搜索股票" }), "没有这只");
    expect(await screen.findByText("没有找到股票")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  });

  it("读取失败时可重试", async () => {
    let failing = true;
    stockHandlers();
    server.use(
      http.get("*/api/v1/stocks/search", ({ request }) =>
        failing
          ? HttpResponse.json({}, { status: 500 })
          : HttpResponse.json({
              data: {
                query: new URL(request.url).searchParams.get("q"),
                available: true,
                rows: [{ ts_code: "600001.SH", name: "样本01" }],
                truncated: false,
              },
              serving,
            }),
      ),
    );
    const user = userEvent.setup();
    renderApp("/overview");
    await user.type(screen.getByRole("combobox", { name: "搜索股票" }), "600001");
    const error = await screen.findByRole("alert", { name: "搜索暂时无法加载" });
    failing = false;
    await user.click(within(error).getByRole("button", { name: "重试" }));
    expect(await screen.findByRole("option", { name: /样本01/ })).toBeInTheDocument();
  });

  it("已有结果重新读取失败后不能用 Enter 选中旧结果，重试成功后可以", async () => {
    let failing = false;
    stockHandlers();
    server.use(
      http.get("*/api/v1/stocks/search", ({ request }) =>
        failing
          ? HttpResponse.json({}, { status: 500 })
          : HttpResponse.json({
              data: {
                query: new URL(request.url).searchParams.get("q"),
                available: true,
                rows: [{ ts_code: "600001.SH", name: "样本01" }],
                truncated: false,
              },
              serving,
            }),
      ),
    );
    const client = testQueryClient();
    const user = userEvent.setup();
    render(
      <AppProviders queryClient={client}>
        <StockSearch />
      </AppProviders>,
    );

    const input = screen.getByRole("combobox", { name: "搜索股票" });
    await user.type(input, "600001");
    expect(await screen.findByRole("option", { name: /样本01/ })).toBeInTheDocument();
    await user.keyboard("{ArrowDown}");

    failing = true;
    await act(async () => {
      await client.invalidateQueries({ queryKey: ["stocks", "search", "600001"] });
    });
    const error = await screen.findByRole("alert", { name: "搜索暂时无法加载" });
    expect(screen.queryByRole("option")).not.toBeInTheDocument();
    await user.keyboard("{Enter}");
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();

    failing = false;
    await user.click(within(error).getByRole("button", { name: "重试" }));
    expect(await screen.findByRole("option", { name: /样本01/ })).toBeInTheDocument();
    await user.click(input);
    await user.keyboard("{Enter}");
    expect(await screen.findByRole("dialog", { name: /样本01/ })).toBeInTheDocument();
  });
});
