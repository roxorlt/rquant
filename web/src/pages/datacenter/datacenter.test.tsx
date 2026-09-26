import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

type CatalogList = Schemas["CatalogList"];
type CatalogDataset = Schemas["CatalogDataset"];

const daily: CatalogDataset = {
  dataset_id: "daily_bar",
  table_name: "daily_bar",
  name: "股票日线",
  purpose: "查看股票每天的开收盘价、成交量与涨跌",
  category: "行情",
  sources: ["Tushare Pro"],
  update_note: "1 个交易日内更新",
  visibility_note: "下一交易日可见",
  primary_key: ["ts_code", "trade_date"],
  schema_available: true,
  sample_available: false,
  fields: [
    {
      key: "ts_code",
      name: "证券代码",
      description: "证券代码",
      data_type: "VARCHAR",
      unit: null,
      is_primary_key: true,
    },
    {
      key: "pct_chg",
      name: "涨跌幅",
      description: "相对前收盘价的涨跌百分比",
      data_type: "DOUBLE",
      unit: "%",
      is_primary_key: false,
    },
  ],
};

const descriptions: CatalogList = {
  version: 1,
  datasets: [
    {
      dataset_id: daily.dataset_id,
      name: daily.name,
      purpose: daily.purpose,
      category: daily.category,
      sources: daily.sources,
      schema_available: true,
    },
    {
      dataset_id: "adj_factor",
      name: "复权因子",
      purpose: "调整历史价格",
      category: "行情",
      sources: ["Tushare Pro"],
      schema_available: true,
    },
    {
      dataset_id: "ths_member",
      name: "同花顺板块成分",
      purpose: "查找股票所属板块",
      category: "板块",
      sources: ["Tushare Pro"],
      schema_available: false,
    },
  ],
};

function catalogHandlers(list: CatalogList = descriptions) {
  server.use(
    http.get("*/api/v1/catalog/datasets", () =>
      HttpResponse.json({
        data: list,
        serving: {
          generation_id: null,
          built_at: null,
          age_seconds: null,
          state: "ready",
          message: null,
          detail: "static",
        },
      }),
    ),
    http.get("*/api/v1/catalog/datasets/:id", ({ params }) =>
      HttpResponse.json({
        data:
          params.id === "daily_bar"
            ? daily
            : {
                ...daily,
                dataset_id: String(params.id),
                name: params.id === "ths_member" ? "同花顺板块成分" : "复权因子",
                schema_available: params.id !== "ths_member",
                fields: params.id === "ths_member" ? [] : daily.fields,
              },
        serving: {
          generation_id: null,
          built_at: null,
          age_seconds: null,
          state: "ready",
          message: null,
          detail: "static",
        },
      }),
    ),
  );
}

describe("数据中心目录", () => {
  it("filters by category and keyword, then opens a real field dictionary", async () => {
    const user = userEvent.setup();
    catalogHandlers();
    renderApp("/datacenter");
    expect(await screen.findByRole("button", { name: /股票日线/ })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "板块" }));
    expect(screen.getByRole("button", { name: /同花顺板块成分/ })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /股票日线/ })).not.toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "全部" }));
    await user.type(screen.getByRole("searchbox", { name: "搜索数据集" }), "日线");
    expect(screen.getByRole("button", { name: /股票日线/ })).toBeInTheDocument();
    await waitFor(() =>
      expect(screen.queryByRole("button", { name: /复权因子/ })).not.toBeInTheDocument(),
    );
    await user.click(screen.getByRole("button", { name: /股票日线/ }));
    expect(await screen.findByRole("table", { name: "字段字典" })).toHaveTextContent("涨跌幅");
    expect(screen.getByText("下一交易日可见")).toBeInTheDocument();
    expect(screen.getByText("样例数据尚未发布")).toBeInTheDocument();
    const table = screen.getByRole("table", { name: "字段字典" });
    expect(within(table).getByText("DOUBLE")).toBeInTheDocument();
    expect(within(table).getByText("%")).toBeInTheDocument();
    await user.type(screen.getByRole("searchbox", { name: "搜索字段" }), "涨跌");
    await waitFor(() => expect(within(table).getAllByRole("row")).toHaveLength(2));
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
  });

  it("shows an honest schema gap, empty catalog and API error", async () => {
    const user = userEvent.setup();
    catalogHandlers();
    const view = renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: /同花顺板块成分/ }));
    expect(await screen.findByText("字段结构待发布")).toBeInTheDocument();
    view.unmount();

    catalogHandlers({ version: 1, datasets: [] });
    const empty = renderApp("/datacenter");
    expect(await screen.findByText("还没有数据集说明")).toBeInTheDocument();
    empty.unmount();

    server.use(
      http.get("*/api/v1/catalog/datasets", () =>
        HttpResponse.json({ detail: "数据目录暂时不可用" }, { status: 503 }),
      ),
    );
    renderApp("/datacenter");
    expect(await screen.findByText("暂时读不到数据目录")).toBeInTheDocument();
  });
});
