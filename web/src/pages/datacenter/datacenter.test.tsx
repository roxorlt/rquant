import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { AUDIT_REPORT_JOURNAL_KEY } from "./auditReportCommandSession";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

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
    http.get("*/api/v1/data/audit-report/calendar", () =>
      HttpResponse.json({
        data: {
          availability: "unavailable",
          earliest_selectable_date: null,
          latest_closed_date: null,
          open_dates: [],
        },
        serving: metaEnvelope().serving,
      }),
    ),
    http.get("*/api/v1/data/report", () =>
      HttpResponse.json({
        data: { source_state: "not_published", overview: null, months: [], rules: [], issues: [] },
        serving: metaEnvelope().serving,
      }),
    ),
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
    expect(within(table).getByText("3.14%")).toBeInTheDocument();
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

  it("shows timezone-aware sample timestamps in Shanghai time", async () => {
    const queriedAt = {
      key: "queried_at",
      name: "查询时间",
      description: "完成查询的时间",
      data_type: "TIMESTAMP WITH TIME ZONE",
      unit: null,
      is_primary_key: false,
    };
    const coverage: CatalogDataset = {
      ...daily,
      dataset_id: "stock_suspend_coverage",
      name: "停复牌采集记录",
      fields: [queriedAt],
      sample_fields: [queriedAt],
      sample_available: true,
      sample: { state: "available", rows: [{ queried_at: "2026-09-25T02:00:00+00:00" }] },
    };
    catalogHandlers({
      version: 1,
      datasets: [
        {
          dataset_id: coverage.dataset_id,
          name: coverage.name,
          purpose: coverage.purpose,
          category: coverage.category,
          sources: coverage.sources,
          schema_available: true,
        },
      ],
    });
    server.use(
      http.get("*/api/v1/data/catalog/stock_suspend_coverage", () =>
        HttpResponse.json({
          data: coverage,
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
    expect(within(table).getByText("2026-09-25 10:00:00")).toBeInTheDocument();
    expect(within(table).queryByText("2026-09-25T02:00:00+00:00")).not.toBeInTheDocument();
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

const report: Schemas["DataAuditReportData"] = {
  source_state: "ready",
  dataset_state: "not_published",
  overview: {
    report_hash: "f".repeat(64),
    schema_version: 1,
    rule_version: "daily-bar-quality-v1",
    run_status: "completed",
    collection_status: "collection_unconfirmed",
    collection_completed_through: null,
    collection_label: "采集未确认",
    coverage_conclusion: "unconfirmed",
    coverage_label: "覆盖情况待确认",
    quality_conclusion: "issues_observed",
    quality_label: "发现问题",
    current: false,
    source_mode: "production_unverified",
    source_namespace: "production",
    replica_generation_id: null,
    audit_start: "2026-08-01",
    observed_through: "2026-09-30",
    expected_open_days: 42,
    covered_open_days: 40,
    missing_open_days: 2,
    gap_count: 1,
    longest_gap_open_days: 2,
    closed_day_count: 19,
    monthly_count: 2,
    rule_count: 3,
    quality_issue_count: 1,
    indexed_issue_count: 1,
    omitted_issue_count: 0,
    unassessed_rule_days: 46,
  },
  months: [
    {
      month: "2026-08-01",
      expected_open_days: 21,
      covered_open_days: 21,
      coverage_ratio: 1,
      status: "measured",
      status_label: "已统计",
    },
    {
      month: "2026-09-01",
      expected_open_days: 21,
      covered_open_days: 19,
      coverage_ratio: 19 / 21,
      status: "measured",
      status_label: "已统计",
    },
  ],
  rules: [
    {
      rule_id: "daily_bar.close_limit",
      name: "收盘价上下限",
      field_name: null,
      field_label: null,
      expected_days: 42,
      checked_days: 40,
      assessed_days: 0,
      unassessed_days: 42,
      first_assessed_date: null,
      last_assessed_date: null,
      assessment_complete: false,
      unassessed_reasons: [
        { reason: "no_daily_bar", name: "缺少日线", days: 2 },
        { reason: "limits_unavailable", name: "涨跌停价未确认", days: 40 },
      ],
      issue_count: 0,
    },
    {
      rule_id: "daily_bar.zero_volume",
      name: "零成交量",
      field_name: null,
      field_label: null,
      expected_days: 42,
      checked_days: 40,
      assessed_days: 40,
      unassessed_days: 2,
      first_assessed_date: "2026-08-03",
      last_assessed_date: "2026-09-30",
      assessment_complete: false,
      unassessed_reasons: [{ reason: "no_daily_bar", name: "缺少日线", days: 2 }],
      issue_count: 1,
    },
    {
      rule_id: "daily_bar.field_null_ratio",
      name: "字段空值比例",
      field_name: "close",
      field_label: "收盘价",
      expected_days: 42,
      checked_days: 40,
      assessed_days: 40,
      unassessed_days: 2,
      first_assessed_date: "2026-08-03",
      last_assessed_date: "2026-09-30",
      assessment_complete: false,
      unassessed_reasons: [{ reason: "no_daily_bar", name: "缺少日线", days: 2 }],
      issue_count: 0,
    },
  ],
  issues: [
    {
      number: 1,
      trade_date: "2026-09-03",
      rule_id: "daily_bar.zero_volume_unsuspended",
      name: "未停牌但零成交量",
      ts_code: "000001.SZ",
      field_name: null,
      field_label: null,
      observed_value: "0",
      reference_value: null,
      null_rows: null,
      observed_rows: null,
    },
  ],
};

function reportHandler(data: Schemas["DataAuditReportData"] = report) {
  server.use(
    http.get("*/api/v1/data/report", () =>
      HttpResponse.json({ data, serving: metaEnvelope().serving }),
    ),
  );
}

function calendarHandler(
  data: Schemas["AuditReportCalendarData"] = {
    availability: "ready",
    earliest_selectable_date: "2024-09-02",
    latest_closed_date: "2026-09-23",
    open_dates: ["2024-09-02", "2026-09-18", "2026-09-22", "2026-09-23"],
  },
  generationId?: string,
) {
  server.use(
    http.get("*/api/v1/data/audit-report/calendar", () =>
      HttpResponse.json({ data, serving: metaEnvelope({ generationId }).serving }),
    ),
  );
}

describe("运行日线审计", () => {
  it("uses the verified calendar, confirms the read-only range, and persists one original request", async () => {
    const user = userEvent.setup();
    const requests: Schemas["AuditReportCommandRequest"][] = [];
    catalogHandlers();
    calendarHandler();
    server.use(
      http.post("*/api/v1/data/audit-report/commands", async ({ request }) => {
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        const body = (await request.json()) as Schemas["AuditReportCommandRequest"];
        requests.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          task_id: "a".repeat(32),
          status: "queued",
          message: "queued",
        });
      }),
    );
    renderApp("/datacenter");
    await screen.findByRole("button", { name: /运行数据审计/ });
    const panel = screen.getByRole("region", { name: "日线质量报告" });
    await waitFor(() => expect(within(panel).getByLabelText("结束日期")).toHaveValue("2026-09-23"));
    await user.click(within(panel).getByRole("button", { name: "运行数据审计" }));
    expect(screen.getByText(/只读核对.*全部目录数据/)).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "确认排队" }));
    expect(await within(panel).findByText("本次请求已排队")).toBeInTheDocument();
    expect(requests).toHaveLength(1);
    expect(requests[0]?.observed_through).toBe("2026-09-23");
    expect(JSON.parse(localStorage.getItem(AUDIT_REPORT_JOURNAL_KEY) ?? "{}").body).toEqual(
      requests[0],
    );
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
  });

  it("refuses a date absent from the verified trading calendar", async () => {
    const user = userEvent.setup();
    catalogHandlers();
    calendarHandler();
    renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    const end = await within(panel).findByLabelText("结束日期");
    await user.clear(end);
    await user.type(end, "2026-09-19");
    expect(within(panel).getByRole("button", { name: "运行数据审计" })).toBeDisabled();
    expect(within(panel).getByText(/尚未核实为交易日/)).toBeInTheDocument();
  });

  it("disables submission when the calendar belongs to a different data generation", async () => {
    catalogHandlers();
    calendarHandler(undefined, "b".repeat(64));
    renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    expect(await within(panel).findByRole("button", { name: "运行数据审计" })).toBeDisabled();
    expect(within(panel).getByText(/交易日历暂不可用/)).toBeInTheDocument();
  });

  it("marks a report as this run only after matching published task and report evidence", async () => {
    const taskId = "a".repeat(32);
    localStorage.setItem(
      AUDIT_REPORT_JOURNAL_KEY,
      JSON.stringify({
        schema: 1,
        body: {
          command_id: "web-original",
          requested_at: "2026-09-24T07:00:00Z",
          audit_start: "2024-09-02",
          observed_through: "2026-09-23",
        },
        status: "queued",
        taskId,
      }),
    );
    catalogHandlers();
    calendarHandler();
    reportHandler({
      ...report,
      progress: {
        availability: "ready",
        latest_task_id: taskId,
        latest_status: "succeeded",
        successful_task_id: taskId,
        successful_report_hash: report.overview?.report_hash ?? null,
        successful_created_at: "2026-09-24T07:00:00Z",
        successful_updated_at: "2026-09-24T07:05:00Z",
        events: [
          { event_type: "succeeded", label: "检查完成", occurred_at: "2026-09-24T07:05:00Z" },
        ],
      },
    });
    renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    expect(await within(panel).findByText("本次报告已发布")).toBeInTheDocument();
    expect(within(panel).getByRole("heading", { name: "本次报告" })).toBeInTheDocument();
  });

  it("keeps a published result tied to its request after a newer task fails", async () => {
    const taskId = "a".repeat(32);
    localStorage.setItem(
      AUDIT_REPORT_JOURNAL_KEY,
      JSON.stringify({
        schema: 1,
        body: {
          command_id: "web-original",
          requested_at: "2026-09-24T07:00:00Z",
          audit_start: "2024-09-02",
          observed_through: "2026-09-23",
        },
        status: "queued",
        taskId,
      }),
    );
    catalogHandlers();
    calendarHandler();
    reportHandler({
      ...report,
      progress: {
        availability: "ready",
        latest_task_id: "b".repeat(32),
        latest_status: "failed",
        successful_task_id: taskId,
        successful_report_hash: report.overview?.report_hash ?? null,
        successful_updated_at: "2026-09-24T07:05:00Z",
        events: [
          { event_type: "failed", label: "检查未完成", occurred_at: "2026-09-24T07:07:00Z" },
        ],
      },
    });
    renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    expect(await within(panel).findByText("本次报告已发布")).toBeInTheDocument();
    expect(within(panel).getByRole("heading", { name: "本次报告" })).toBeInTheDocument();
    expect(within(panel).getByText("最近任务未完成")).toBeInTheDocument();
    expect(within(panel).getByRole("button", { name: "运行数据审计" })).toBeEnabled();
  });

  it("shows a real empty task state while the report is unpublished", async () => {
    catalogHandlers();
    calendarHandler();
    reportHandler({
      source_state: "not_published",
      dataset_state: "not_published",
      overview: null,
      months: [],
      rules: [],
      issues: [],
      progress: { availability: "empty", events: [] },
    });
    renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    expect(await within(panel).findByText("还没有审计任务，选择日期后运行。")).toBeInTheDocument();
    expect(within(panel).queryByText(/近期任务暂不可查看/)).not.toBeInTheDocument();
  });

  it("blocks stale meta data after a failed refresh", async () => {
    catalogHandlers();
    calendarHandler();
    reportHandler();
    const { queryClient } = renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    await waitFor(() =>
      expect(within(panel).getByRole("button", { name: "运行数据审计" })).toBeEnabled(),
    );
    server.use(
      http.get("*/api/v1/meta", () =>
        HttpResponse.json({ detail: "unavailable" }, { status: 503 }),
      ),
    );
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
    await waitFor(() => expect(queryClient.getQueryState(["meta"])?.error).toBeTruthy());
    expect(queryClient.getQueryState(["meta"])?.data).toBeDefined();
    expect(within(panel).getByRole("button", { name: "运行数据审计" })).toBeDisabled();
    expect(within(panel).queryByRole("img", { name: /按月覆盖率/ })).not.toBeInTheDocument();
  });

  it("withdraws a cached success and blocks submission after report refresh conflicts", async () => {
    const taskId = "a".repeat(32);
    localStorage.setItem(
      AUDIT_REPORT_JOURNAL_KEY,
      JSON.stringify({
        schema: 1,
        body: {
          command_id: "web-original",
          requested_at: "2026-09-24T07:00:00Z",
          audit_start: "2024-09-02",
          observed_through: "2026-09-23",
        },
        status: "queued",
        taskId,
      }),
    );
    catalogHandlers();
    calendarHandler();
    reportHandler({
      ...report,
      progress: {
        availability: "ready",
        latest_task_id: taskId,
        latest_status: "succeeded",
        successful_task_id: taskId,
        successful_report_hash: report.overview?.report_hash ?? null,
        events: [],
      },
    });
    const { queryClient } = renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    expect(await within(panel).findByText("本次报告已发布")).toBeInTheDocument();
    server.use(
      http.get("*/api/v1/data/report", () =>
        HttpResponse.json({ detail: "changed" }, { status: 409 }),
      ),
    );
    await queryClient.invalidateQueries({ queryKey: ["data", "audit", "report"] });
    await waitFor(() =>
      expect(
        queryClient.getQueryState(["data", "audit", "report", metaEnvelope().serving.generation_id])
          ?.error,
      ).toBeTruthy(),
    );
    expect(
      queryClient.getQueryState(["data", "audit", "report", metaEnvelope().serving.generation_id])
        ?.data,
    ).toBeDefined();
    expect(within(panel).getByRole("button", { name: "运行数据审计" })).toBeDisabled();
    expect(within(panel).queryByText("本次报告已发布")).not.toBeInTheDocument();
    expect(within(panel).queryByRole("img", { name: /按月覆盖率/ })).not.toBeInTheDocument();
  });

  it("keeps an unknown original request for manual retry and retains the previous report after failure", async () => {
    const user = userEvent.setup();
    const original = {
      command_id: "web-original",
      requested_at: "2026-09-24T07:00:00Z",
      audit_start: "2024-09-02",
      observed_through: "2026-09-23",
    };
    localStorage.setItem(
      AUDIT_REPORT_JOURNAL_KEY,
      JSON.stringify({ schema: 1, body: original, status: "unknown", taskId: null }),
    );
    const requests: Schemas["AuditReportCommandRequest"][] = [];
    catalogHandlers();
    calendarHandler();
    reportHandler({
      ...report,
      progress: {
        availability: "ready",
        latest_task_id: "b".repeat(32),
        latest_status: "failed",
        latest_status_label: "失败",
        latest_hint: "核对未完成，请稍后重试。",
        latest_created_at: "2026-09-24T06:00:00Z",
        latest_updated_at: "2026-09-24T06:01:00Z",
        successful_task_id: "c".repeat(32),
        successful_report_hash: report.overview?.report_hash ?? null,
        successful_created_at: "2026-09-23T06:00:00Z",
        successful_updated_at: "2026-09-23T06:01:00Z",
        events: [{ event_type: "failed", label: "核对失败", occurred_at: "2026-09-24T06:01:00Z" }],
      },
    });
    server.use(
      http.post("*/api/v1/data/audit-report/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["AuditReportCommandRequest"];
        requests.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          task_id: null,
          status: "pending",
          message: "pending",
        });
      }),
    );
    renderApp("/datacenter");
    const panel = await screen.findByRole("region", { name: "日线质量报告" });
    expect(await within(panel).findByText("本次提交状态待确认")).toBeInTheDocument();
    expect(within(panel).getByRole("img", { name: /按月覆盖率/ })).toBeInTheDocument();
    expect(within(panel).getByText("最近任务未完成")).toBeInTheDocument();
    expect(requests).toHaveLength(0);
    await user.click(within(panel).getByRole("button", { name: "继续核对" }));
    await waitFor(() => expect(requests).toEqual([original]));
    expect(await within(panel).findByText("本次提交状态待确认")).toBeInTheDocument();
    expect(within(panel).getByRole("img", { name: /按月覆盖率/ })).toBeInTheDocument();
  });
});

describe("日线质量报告", () => {
  it("shows measured coverage, per-rule assessed range and the bounded issue list", async () => {
    catalogHandlers();
    reportHandler();
    renderApp("/datacenter");
    await screen.findByText("采集未确认");
    const panel = screen.getByRole("region", { name: "日线质量报告" });
    expect(screen.queryByRole("heading", { name: "数据审计" })).not.toBeInTheDocument();
    expect(within(panel).getByText("采集未确认")).toBeInTheDocument();
    expect(within(panel).getByText("覆盖情况待确认")).toBeInTheDocument();
    expect(within(panel).getByText(/尚未完整检查/)).toBeInTheDocument();
    const summary = within(panel).getByRole("region", { name: "日线报告摘要" });
    expect(within(summary).getByText("42")).toBeInTheDocument();
    expect(within(summary).getByText("40")).toBeInTheDocument();
    expect(within(summary).getByText("2")).toBeInTheDocument();
    expect(within(panel).getByRole("img", { name: /按月覆盖率/ })).toBeInTheDocument();
    expect(within(panel).getByRole("table", { name: "月度覆盖" })).toHaveTextContent("2026-09");
    const rules = within(panel).getByRole("table", { name: "质量规则" });
    expect(rules).toHaveTextContent("收盘价上下限");
    expect(rules).toHaveTextContent("0 / 42");
    expect(rules).toHaveTextContent("涨跌停价未确认 40 天");
    const issues = within(panel).getByRole("table", { name: "日线质量问题" });
    expect(issues).toHaveTextContent("未停牌但零成交量");
    expect(within(panel).queryByText(/仅列出/)).not.toBeInTheDocument();
    expect(findJargon(document.querySelector("main")?.textContent ?? "")).toEqual([]);
    expect(document.querySelector("main")?.textContent).not.toContain("f".repeat(64));
  });

  it("shows the total issue count when the visible index stops at 256", async () => {
    catalogHandlers();
    const overview = report.overview;
    if (overview === null) throw new Error("报告测试数据缺少摘要");
    const issue = report.issues[0];
    if (issue === undefined) throw new Error("报告测试数据缺少问题");
    reportHandler({
      ...report,
      overview: {
        ...overview,
        quality_issue_count: 301,
        indexed_issue_count: 256,
        omitted_issue_count: 45,
      },
      rules: report.rules.map((rule) =>
        rule.rule_id === "daily_bar.zero_volume" ? { ...rule, issue_count: 301 } : rule,
      ),
      issues: Array.from({ length: 256 }, (_, index) => ({ ...issue, number: index + 1 })),
    });
    renderApp("/datacenter");
    expect(await screen.findByText("仅列出 256 / 301 条")).toBeInTheDocument();
    const panel = screen.getByRole("region", { name: "日线质量报告" });
    expect(within(panel).getByRole("table", { name: "日线质量问题" })).toBeInTheDocument();
    expect(within(panel).getByRole("region", { name: "日线报告摘要" })).toHaveTextContent("301");
  });

  it("keeps missing and unassessed days visible even when no issue was observed", async () => {
    catalogHandlers();
    const overview = report.overview;
    if (overview === null) throw new Error("报告测试数据缺少摘要");
    reportHandler({
      ...report,
      overview: {
        ...overview,
        quality_conclusion: "not_fully_assessed",
        quality_label: "尚未完整检查",
        quality_issue_count: 0,
        indexed_issue_count: 0,
        omitted_issue_count: 0,
      },
      rules: report.rules.map((item) => ({ ...item, issue_count: 0 })),
      issues: [],
    });
    renderApp("/datacenter");
    await screen.findByText("尚未完整检查");
    const panel = screen.getByRole("region", { name: "日线质量报告" });
    expect(within(panel).getByText("尚未完整检查")).toBeInTheDocument();
    expect(within(panel).getByText(/仍有 46 个规则日未评估/)).toBeInTheDocument();
    expect(within(panel).queryByText("正常")).not.toBeInTheDocument();
    expect(within(panel).getByText("本次记录没有质量问题；未评估日期仍需检查")).toBeInTheDocument();
  });

  it.each([
    ["not_published", "日线质量报告尚未发布"],
    ["unavailable", "日线质量报告暂时不可用"],
  ] as const)("shows %s without a fabricated zero", async (state, title) => {
    catalogHandlers();
    reportHandler({
      source_state: state,
      dataset_state: state,
      overview: null,
      months: [],
      rules: [],
      issues: [],
    });
    renderApp("/datacenter");
    expect(await screen.findByText(title)).toBeInTheDocument();
    expect(screen.queryByRole("table", { name: "月度覆盖" })).not.toBeInTheDocument();
  });

  it.each([503, 409])("withdraws the report after HTTP %s", async (status) => {
    catalogHandlers();
    server.use(
      http.get("*/api/v1/data/report", () => HttpResponse.json({ detail: "internal" }, { status })),
    );
    renderApp("/datacenter");
    expect(
      await screen.findByText(
        status === 409 ? "日线质量报告已更新，请刷新" : "日线质量报告暂时不可用",
      ),
    ).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: /按月覆盖率/ })).not.toBeInTheDocument();
  });

  it("withdraws a newer report envelope until the page observes that data generation", async () => {
    catalogHandlers();
    reportHandler();
    const { queryClient } = renderApp("/datacenter");
    expect(await screen.findByRole("img", { name: /按月覆盖率/ })).toBeInTheDocument();
    server.use(
      http.get("*/api/v1/data/report", () =>
        HttpResponse.json({
          data: report,
          serving: metaEnvelope({ generationId: "b".repeat(64) }).serving,
        }),
      ),
    );
    await queryClient.invalidateQueries({ queryKey: ["data", "audit", "report"] });
    expect(await screen.findByText("日线质量报告已更新，请刷新")).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: /按月覆盖率/ })).not.toBeInTheDocument();
  });

  it("shows other catalog report states and clears the old report after a generation swap", async () => {
    const user = userEvent.setup();
    catalogHandlers();
    reportHandler();
    const { queryClient } = renderApp("/datacenter");
    expect(await screen.findByRole("img", { name: /按月覆盖率/ })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /复权因子/ }));
    expect(screen.queryByRole("region", { name: "日线质量报告" })).not.toBeInTheDocument();
    expect(screen.getByRole("region", { name: "数据质量报告" })).toBeInTheDocument();
    expect(await screen.findByText("这份数据尚未审计")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /股票日线/ }));
    expect(await screen.findByRole("img", { name: /按月覆盖率/ })).toBeInTheDocument();
    server.use(
      http.get("*/api/v1/meta", () =>
        HttpResponse.json(metaEnvelope({ generationId: "b".repeat(64) })),
      ),
      http.get("*/api/v1/data/report", () =>
        HttpResponse.json({
          data: {
            source_state: "not_published",
            overview: null,
            months: [],
            rules: [],
            issues: [],
          },
          serving: metaEnvelope({ generationId: "b".repeat(64) }).serving,
        }),
      ),
    );
    await queryClient.invalidateQueries({ queryKey: ["meta"] });
    expect(await screen.findByText("日线质量报告尚未发布")).toBeInTheDocument();
    expect(screen.queryByRole("img", { name: /按月覆盖率/ })).not.toBeInTheDocument();
  });
});

describe("数据中心审计", () => {
  it("marks completed audits with findings as attention, not healthy", async () => {
    const user = userEvent.setup();
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
    await user.click(await screen.findByRole("button", { name: "查看历史审计记录" }));
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
    await user.click(await screen.findByRole("button", { name: "查看历史审计记录" }));
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
    const user = userEvent.setup();
    catalogHandlers();
    auditHandlers({ source_state: "ready", latest_attempt: null, latest_success: null });
    const empty = renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "查看历史审计记录" }));
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
    await user.click(await screen.findByRole("button", { name: "查看历史审计记录" }));
    expect(await screen.findByText("审计中")).toBeInTheDocument();
    running.unmount();

    auditHandlers({ source_state: "unavailable", latest_attempt: null, latest_success: null });
    renderApp("/datacenter");
    await user.click(await screen.findByRole("button", { name: "查看历史审计记录" }));
    expect(await screen.findByText("审计结果暂时不可用")).toBeInTheDocument();
  });
});
