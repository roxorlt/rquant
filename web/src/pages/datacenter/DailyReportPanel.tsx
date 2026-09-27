import { ApiError } from "@/api/client";
import {
  type AuditReportIssue,
  type AuditReportMonth,
  type AuditReportRule,
  type DataAuditReportData,
  useDataAuditReport,
} from "@/api/endpoints";
import { useMeta } from "@/api/useMeta";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import { baseOption } from "@/charts/options";
import type { ChartColors } from "@/charts/tokens";
import { formatCount, formatPercent } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, type Kpi, KpiStrip, PageSkeleton, Panel, StatusBadge, Tip } from "@/ui";

type Overview = NonNullable<DataAuditReportData["overview"]>;

function monthLabel(value: string): string {
  return value.slice(0, 7);
}

function monthOption(months: readonly AuditReportMonth[], colors: ChartColors): EChartOption {
  const firstShown = Math.max(0, 100 - (18 / months.length) * 100);
  return {
    ...baseOption(colors, false),
    grid: { left: 42, right: 12, top: 12, bottom: 58 },
    tooltip: {
      ...baseOption(colors, false).tooltip,
      axisPointer: { type: "shadow" },
      valueFormatter: (value) => (typeof value === "number" ? formatPercent(value, 1) : "无交易日"),
    },
    xAxis: {
      type: "category",
      data: months.map((month) => monthLabel(month.month)),
      axisLabel: { color: colors.muted, fontSize: 11, interval: "auto" },
      axisLine: { lineStyle: { color: colors.rule } },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value",
      min: 0,
      max: 100,
      interval: 50,
      axisLabel: { color: colors.muted, fontSize: 11, formatter: "{value}%" },
      splitLine: { lineStyle: { color: colors.grid } },
    },
    dataZoom: [
      { type: "inside", start: firstShown, end: 100 },
      {
        type: "slider",
        start: firstShown,
        end: 100,
        bottom: 7,
        height: 18,
        showDetail: false,
        borderColor: colors.rule,
        fillerColor: colors.grid,
        textStyle: { color: colors.muted },
      },
    ],
    series: [
      {
        type: "bar",
        name: "覆盖率",
        data: months.map((month) =>
          month.coverage_ratio === null ? null : Number((month.coverage_ratio * 100).toFixed(1)),
        ),
        barMaxWidth: 24,
        itemStyle: { color: colors.accent, borderRadius: [2, 2, 0, 0] },
      },
    ],
  };
}

const MONTH_COLUMNS: DataColumn<AuditReportMonth>[] = [
  { id: "month", header: "月份", value: (row) => row.month, cell: (row) => monthLabel(row.month) },
  {
    id: "expected",
    header: "应有",
    value: (row) => row.expected_open_days,
    cell: (row) => `${formatCount(row.expected_open_days)} 天`,
    numeric: true,
  },
  {
    id: "covered",
    header: "已有",
    value: (row) => row.covered_open_days,
    cell: (row) => `${formatCount(row.covered_open_days)} 天`,
    numeric: true,
  },
  {
    id: "missing",
    header: "缺失",
    value: (row) => row.expected_open_days - row.covered_open_days,
    cell: (row) => `${formatCount(row.expected_open_days - row.covered_open_days)} 天`,
    numeric: true,
  },
  {
    id: "ratio",
    header: "覆盖率",
    value: (row) => row.coverage_ratio,
    cell: (row) =>
      row.coverage_ratio === null ? row.status_label : formatPercent(row.coverage_ratio * 100, 1),
    numeric: true,
  },
];

function assessedRange(rule: AuditReportRule): string {
  if (rule.first_assessed_date === null || rule.last_assessed_date === null) {
    return "尚无已评估日期";
  }
  return `已评估日期：${rule.first_assessed_date} 至 ${rule.last_assessed_date}`;
}

const RULE_COLUMNS: DataColumn<AuditReportRule>[] = [
  {
    id: "name",
    header: "检查项",
    value: (row) => row.name,
    cell: (row) => (
      <span className="dc-report-rule-name">
        {row.name}
        {row.field_label ? <small>{row.field_label}</small> : null}
        <small className="dc-report-mobile-progress">
          已评估 {formatCount(row.assessed_days)} / {formatCount(row.expected_days)} 天
        </small>
      </span>
    ),
    wrap: true,
  },
  {
    id: "progress",
    header: "已评估 / 应有",
    value: (row) => row.assessed_days,
    cell: (row) => (
      <Tip content={`已检查 ${formatCount(row.checked_days)} 天 · ${assessedRange(row)}`}>
        <span className="num dc-report-progress">
          {formatCount(row.assessed_days)} / {formatCount(row.expected_days)}
        </span>
      </Tip>
    ),
    secondary: true,
  },
  {
    id: "unassessed",
    header: "未评估",
    value: (row) => row.unassessed_days,
    cell: (row) =>
      row.unassessed_days === 0 ? (
        <span className="dc-report-muted">—</span>
      ) : (
        <span className="dc-report-reasons">
          <strong className="num">{formatCount(row.unassessed_days)} 天</strong>
          <span>
            {row.unassessed_reasons
              .map((reason) => `${reason.name} ${formatCount(reason.days)} 天`)
              .join("；")}
          </span>
        </span>
      ),
    wrap: true,
  },
  {
    id: "issues",
    header: "问题",
    value: (row) => row.issue_count,
    cell: (row) => <span className="num">{formatCount(row.issue_count)}</span>,
    numeric: true,
    secondary: true,
  },
];

function issueDetail(issue: AuditReportIssue): string {
  if (issue.null_rows !== null && issue.observed_rows !== null) {
    return `${issue.field_label ?? "字段"} ${formatCount(issue.null_rows)} / ${formatCount(issue.observed_rows)} 行为空`;
  }
  if (issue.observed_value !== null && issue.reference_value !== null) {
    return `实测 ${issue.observed_value} · 参考 ${issue.reference_value}`;
  }
  return issue.observed_value === null ? "—" : `实测 ${issue.observed_value}`;
}

const ISSUE_COLUMNS: DataColumn<AuditReportIssue>[] = [
  {
    id: "date",
    header: "日期",
    value: (row) => row.trade_date,
    cell: (row) => <span className="mono">{row.trade_date}</span>,
  },
  {
    id: "name",
    header: "问题",
    value: (row) => row.name,
    cell: (row) => (
      <span className="dc-report-issue-name">
        {row.name}
        <small className="dc-report-mobile-code mono">{row.ts_code ?? "—"}</small>
      </span>
    ),
    wrap: true,
  },
  {
    id: "code",
    header: "股票",
    value: (row) => row.ts_code,
    cell: (row) => <span className="mono">{row.ts_code ?? "—"}</span>,
    secondary: true,
  },
  { id: "detail", header: "记录", value: (row) => issueDetail(row), wrap: true, secondary: true },
];

function ReportContent({ data, overview }: { data: DataAuditReportData; overview: Overview }) {
  const items: Kpi[] = [
    {
      key: "expected",
      label: "应有交易日",
      value: formatCount(overview.expected_open_days),
      tip: "按交易日历统计",
    },
    {
      key: "covered",
      label: "有记录",
      value: formatCount(overview.covered_open_days),
      tip: "当天至少有一条日线记录；不代表全市场股票齐全",
    },
    {
      key: "missing",
      label: "缺失交易日",
      value: formatCount(overview.missing_open_days),
      tone: overview.missing_open_days > 0 ? "warn" : undefined,
    },
    {
      key: "issues",
      label: "已发现问题",
      value: formatCount(overview.quality_issue_count),
      tone: overview.quality_issue_count > 0 ? "warn" : undefined,
      tip: "仅统计本次实际完成检查的结果",
    },
  ];
  return (
    <div className="dc-report">
      <div className="dc-report-intro">
        <span className="dc-report-dates mono">
          {overview.audit_start} 至 {overview.observed_through}
        </span>
        <div className="dc-report-states">
          <StatusBadge
            state="warn"
            label={overview.collection_label}
            reason="生产采集完成与入库记录还没有贯通核对"
          />
          <StatusBadge
            state="warn"
            label={overview.coverage_label}
            reason="已观察到的记录可查看，但不能证明当前数据采集完整"
          />
          <StatusBadge
            state="warn"
            label={overview.quality_label}
            reason="只表示实际评估日期的结果，不代表全区间已检查"
          />
        </div>
      </div>
      <KpiStrip items={items} label="日线报告摘要" compact />
      {overview.unassessed_rule_days > 0 ? (
        <p className="dc-report-caveat">
          尚未完整检查 · 仍有 {formatCount(overview.unassessed_rule_days)} 个规则日未评估
        </p>
      ) : null}
      <section className="dc-report-section" aria-label="月度覆盖情况">
        <div className="dc-report-section-head">
          <h3>按月覆盖</h3>
          <Tip content="图中百分比为有日线记录的交易日 / 应有交易日；拖动下方滑块查看历史。">
            <span className="dc-audit-help">说明</span>
          </Tip>
        </div>
        <EChart
          build={(colors) => monthOption(data.months, colors)}
          label={`按月覆盖率，${data.months.length} 个月；各月交易日数量见下表`}
          className="chart sm dc-report-chart"
        />
        <DataTable
          label="月度覆盖"
          rows={data.months}
          columns={MONTH_COLUMNS}
          rowKey={(month) => month.month}
          height={data.months.length > 12 ? 270 : undefined}
        />
      </section>
      <section className="dc-report-section" aria-label="质量规则情况">
        <div className="dc-report-section-head">
          <h3>质量规则</h3>
          <Tip content="已评估指有足够依据完成判断；已检查但缺依据的日期仍算未评估。悬停数字可看已检查天数。">
            <span className="dc-audit-help">说明</span>
          </Tip>
        </div>
        <div className="dc-report-rule-table">
          <DataTable
            label="质量规则"
            rows={data.rules}
            columns={RULE_COLUMNS}
            rowKey={(rule) => `${rule.rule_id}:${rule.field_name ?? ""}`}
          />
        </div>
      </section>
      <section className="dc-report-section" aria-label="质量问题情况">
        <div className="dc-report-section-head">
          <h3>质量问题</h3>
          {overview.omitted_issue_count > 0 ? (
            <span className="dc-report-partial">
              仅列出 {formatCount(overview.indexed_issue_count)} /{" "}
              {formatCount(overview.quality_issue_count)} 条
            </span>
          ) : null}
        </div>
        <div className="dc-report-issue-table">
          <DataTable
            label="日线质量问题"
            rows={data.issues}
            columns={ISSUE_COLUMNS}
            rowKey={(issue) => String(issue.number)}
            height={data.issues.length > 10 ? 360 : undefined}
            virtualizeFrom={300}
            emptyText={
              <EmptyState
                title={
                  overview.unassessed_rule_days > 0
                    ? "本次记录没有质量问题；未评估日期仍需检查"
                    : "已检查范围没有质量问题"
                }
              />
            }
          />
        </div>
      </section>
    </div>
  );
}

export function DailyReportPanel() {
  const meta = useMeta();
  const expectedGeneration = meta.data?.serving.generation_id;
  const report = useDataAuditReport(expectedGeneration);

  if (meta.isLoading || report.isLoading) {
    return (
      <Panel title="日线质量报告" label="日线质量报告">
        <PageSkeleton label="日线质量报告加载中" />
      </Panel>
    );
  }
  if (meta.error) {
    return (
      <Panel title="日线质量报告" label="日线质量报告">
        <EmptyState title="暂时无法确认当前数据" hint="稍后刷新页面再试" />
      </Panel>
    );
  }
  if (report.error) {
    return (
      <Panel title="日线质量报告" label="日线质量报告">
        <EmptyState
          title={
            report.error instanceof ApiError && report.error.status === 409
              ? "日线质量报告已更新，请刷新"
              : "日线质量报告暂时不可用"
          }
          hint="稍后刷新页面再试"
        />
      </Panel>
    );
  }
  if (report.serving?.generation_id !== expectedGeneration) {
    return (
      <Panel title="日线质量报告" label="日线质量报告">
        <EmptyState title="日线质量报告已更新，请刷新" hint="稍后刷新页面再试" />
      </Panel>
    );
  }
  if (report.data?.source_state === "not_published") {
    return (
      <Panel title="日线质量报告" label="日线质量报告">
        <EmptyState title="日线质量报告尚未发布" hint="发布后会显示覆盖与质量记录" />
      </Panel>
    );
  }
  if (report.data?.source_state !== "ready" || report.data.overview === null) {
    return (
      <Panel title="日线质量报告" label="日线质量报告">
        <EmptyState title="日线质量报告暂时不可用" hint="稍后刷新页面再试" />
      </Panel>
    );
  }
  return (
    <Panel title="日线质量报告" label="日线质量报告" flush>
      <ReportContent data={report.data} overview={report.data.overview} />
    </Panel>
  );
}
