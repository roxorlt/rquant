import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";

type CatalogList = Schemas["CatalogList"];
type CatalogDataset = Schemas["CatalogDatasetDetail"];

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
  sample: { state: "unpublished", rows: [] },
  sample_fields: [],
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
    http.get("*/api/v1/data/health", () =>
      HttpResponse.json({
        data: { source_state: "not_published", latest_attempt: null, latest_success: null },
        serving: {
          generation_id: null,
          built_at: null,
          age_seconds: null,
          state: "unavailable",
          message: null,
          detail: "no audit fixture",
        },
      }),
    ),
    http.get("*/api/v1/data/catalog", () =>
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
    http.get("*/api/v1/data/catalog/:id", ({ params }) =>
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

function auditHandlers(
  health: Schemas["DataAuditHealthData"],
  issueItems: Schemas["DataAuditIssueItem"][] = [],
) {
  const serving = {
    generation_id: "generation-a",
    built_at: "2026-09-24T07:31:00Z",
    age_seconds: 20,
    state: "ready",
    message: null,
    detail: "",
  };
  server.use(
    http.get("*/api/v1/data/health", () => HttpResponse.json({ data: health, serving })),
    http.get("*/api/v1/data/issues", ({ request }) => {
      const query = new URL(request.url).searchParams;
      if (query.get("generation") !== serving.generation_id) {
        return HttpResponse.json({ detail: "审计数据已更新" }, { status: 409 });
      }
      const selected = query.get("dataset") === "daily_bar" ? issueItems : [];
      return HttpResponse.json({
        data: {
          source_state: "ready",
          dataset_name: query.get("dataset") === "daily_bar" ? "股票日线" : "复权因子",
          total_count: selected.length,
          partial: false,
          issues: selected,
        },
        serving,
      });
    }),
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
      http.get("*/api/v1/data/catalog", () =>
        HttpResponse.json({ detail: "数据目录暂时不可用" }, { status: 503 }),
      ),
    );
    renderApp("/datacenter");
    expect(await screen.findByText("暂时读不到数据目录")).toBeInTheDocument();
  });

  it("renders only approved sample fields and never prints hostile metadata", async () => {
    catalogHandlers();
    const hostile = {
      ...daily,
      sample_available: true,
      fields: [
        ...daily.fields,
        {
          key: "conflict_reason",
          name: "冲突原因",
          description: "核对来源",
          data_type: "VARCHAR",
          unit: null,
          is_primary_key: false,
        },
      ],
      sample_fields: daily.fields,
      sample: {
        state: "available",
        rows: [
          {
            ts_code: "000001.SZ",
            pct_chg: 3.14,
            source_file: "/private/secrets/a.json",
            conflict_reason: "notifier.admin.shadow.v1",
            snapshot_hash: "a".repeat(64),
          },
        ],
      },
    };
    server.use(
      http.get("*/api/v1/data/catalog/daily_bar", () =>
        HttpResponse.json({
          data: hostile,
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
    renderApp("/datacenter");
    const table = await screen.findByRole("table", { name: "样例数据" });
    expect(within(table).getByText("000001.SZ")).toBeInTheDocument();
    expect(within(table).getByText("3.14")).toBeInTheDocument();
    expect(within(table).queryAllByText("a".repeat(64))).toHaveLength(0);
    const body = document.querySelector("main")?.textContent ?? "";
    for (const secret of [
      "/private/secrets/a.json",
      "notifier.admin.shadow.v1",
      "conflict_reason",
      "source_file",
      "a".repeat(64),
    ]) {
      expect(body).not.toContain(secret);
    }
    expect(findJargon(body)).toEqual([]);
  });

  it.each([
    ["empty", "这份数据暂时没有记录"],
    ["missing", "这份数据尚未接入样例"],
    ["stale", "样例数据需要更新"],
    ["error", "暂时读不到样例数据"],
  ] as const)("shows the %s sample empty state", async (state, copy) => {
    catalogHandlers();
    server.use(
      http.get("*/api/v1/data/catalog/daily_bar", () =>
        HttpResponse.json({
          data: { ...daily, sample: { state, rows: [] } },
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
    renderApp("/datacenter");
    expect(await screen.findByText(copy)).toBeInTheDocument();
  });
});

describe("数据中心审计", () => {
  it("marks completed audits with findings as attention, not healthy", async () => {
    catalogHandlers();
    auditHandlers(
      {
        source_state: "ready",
        latest_attempt: {
          status: "completed",
          label: "已完成",
          observed_at: "2026-09-24T07:20:00Z",
          completed_at: "2026-09-24T07:21:00Z",
        },
        latest_success: {
          as_of_date: "2026-09-23",
          range_start: "2026-09-01",
          range_end: "2026-09-23",
          completed_at: "2026-09-24T07:21:00Z",
          finding_count: 1,
          p0_count: 0,
        },
      },
      [{ number: 1, name: "分钟线缺少日线", severity: "P1", status: "待处理" }],
    );
    renderApp("/datacenter");
    expect(await screen.findByText("发现问题")).toBeInTheDocument();
    expect(screen.getByText("发现问题").closest(".status")).toHaveAttribute("data-state", "warn");
  });

  it("keeps a failed attempt and the last successful issue list separate", async () => {
    const user = userEvent.setup();
    catalogHandlers();
    auditHandlers(
      {
        source_state: "ready",
        latest_attempt: {
          status: "failed",
          label: "审计失败",
          observed_at: "2026-09-24T07:20:00Z",
          completed_at: "2026-09-24T07:21:00Z",
        },
        latest_success: {
          as_of_date: "2026-09-23",
          range_start: "2026-09-01",
          range_end: "2026-09-23",
          completed_at: "2026-09-24T07:10:00Z",
          finding_count: 1,
          p0_count: 0,
        },
      },
      [{ number: 1, name: "分钟线缺少日线", severity: "P1", status: "待处理" }],
    );
    renderApp("/datacenter");
    expect(await screen.findByText("审计失败")).toBeInTheDocument();
    expect(screen.getByText(/上次完成/)).toBeInTheDocument();
    expect(await screen.findByRole("table", { name: "审计问题" })).toHaveTextContent(
      "分钟线缺少日线",
    );
    expect(screen.getByText("全部 1 条")).toBeInTheDocument();
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
    await user.click(screen.getByRole("button", { name: /复权因子/ }));
    expect(await screen.findByText("这份数据没有审计问题")).toBeInTheDocument();
    expect(screen.queryByText("分钟线缺少日线")).not.toBeInTheDocument();
  });

  it("tells apart no audit, running, and unavailable source", async () => {
    catalogHandlers();
    auditHandlers({ source_state: "ready", latest_attempt: null, latest_success: null });
    const empty = renderApp("/datacenter");
    expect(await screen.findByText("尚未审计")).toBeInTheDocument();
    empty.unmount();

    auditHandlers({
      source_state: "ready",
      latest_attempt: {
        status: "running",
        label: "审计中",
        observed_at: "2026-09-24T07:20:00Z",
        completed_at: null,
      },
      latest_success: null,
    });
    const running = renderApp("/datacenter");
    expect(await screen.findByText("审计中")).toBeInTheDocument();
    running.unmount();

    auditHandlers({ source_state: "unavailable", latest_attempt: null, latest_success: null });
    renderApp("/datacenter");
    expect(await screen.findByText("审计结果暂时不可用")).toBeInTheDocument();
  });
});
