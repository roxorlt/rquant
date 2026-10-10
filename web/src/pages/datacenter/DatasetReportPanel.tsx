import type { Schemas } from "@/api/client";
import { useDataCollection } from "@/api/endpoints";
import { formatCount, formatPercent } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, KpiStrip, StatusBadge, Tip } from "@/ui";

type Dataset = Schemas["AuditReportDataset"];
type Month = Dataset["monthly"][number];
type Rule = Dataset["rules"][number];
type Field = Dataset["fields"][number];
type Change = Dataset["row_changes"][number];

const months: DataColumn<Month>[] = [
  { id: "month", header: "月份", value: (r) => r.month, cell: (r) => r.month.slice(0, 7) },
  {
    id: "days",
    header: "有记录 / 应有",
    value: (r) => r.covered_open_days,
    cell: (r) => `${formatCount(r.covered_open_days)} / ${formatCount(r.expected_open_days)}`,
  },
  {
    id: "ratio",
    header: "记录覆盖",
    value: (r) => r.coverage_ratio,
    cell: (r) =>
      r.coverage_ratio === null ? "—" : formatPercent(Number(r.coverage_ratio) * 100, 1),
    numeric: true,
  },
];
const rules: DataColumn<Rule>[] = [
  { id: "name", header: "检查项", value: (r) => r.name, wrap: true },
  {
    id: "state",
    header: "结果",
    value: (r) => r.state_label,
    cell: (r) => (
      <Tip content={r.reason_label}>
        <span>{r.state_label}</span>
      </Tip>
    ),
    wrap: true,
  },
  {
    id: "issues",
    header: "发现",
    value: (r) => r.issue_count,
    cell: (r) => <span className="num">{formatCount(r.issue_count)}</span>,
    numeric: true,
  },
];
const fields: DataColumn<Field>[] = [
  { id: "field", header: "字段", value: (r) => r.name, wrap: true },
  {
    id: "nulls",
    header: "空值 / 记录",
    value: (r) => r.null_rows,
    cell: (r) => `${formatCount(r.null_rows)} / ${formatCount(r.observed_rows)}`,
  },
  {
    id: "ratio",
    header: "空值比例",
    value: (r) => (r.observed_rows ? r.null_rows / r.observed_rows : null),
    cell: (r) => (r.observed_rows ? formatPercent((100 * r.null_rows) / r.observed_rows, 1) : "—"),
    numeric: true,
  },
];
const changes: DataColumn<Change>[] = [
  {
    id: "day",
    header: "交易日",
    value: (r) => r.day,
    cell: (r) => <span className="mono">{r.day}</span>,
  },
  {
    id: "rows",
    header: "记录",
    value: (r) => r.row_count,
    cell: (r) => formatCount(r.row_count),
    numeric: true,
  },
  {
    id: "change",
    header: "较前日",
    value: (r) => r.change_rows ?? null,
    cell: (r) => {
      const change = r.change_rows ?? null;
      return change === null ? "—" : `${change > 0 ? "+" : ""}${formatCount(change)}`;
    },
    numeric: true,
  },
];

function frequencyLabel(value: string): string {
  const labels: Record<string, string> = {
    "1min": "1分钟",
    "5min": "5分钟",
    "15min": "15分钟",
    "30min": "30分钟",
    "60min": "60分钟",
  };
  return labels[value] ?? "未知频率";
}

export function DatasetReportContent({ dataset }: { dataset: Dataset }) {
  const sourceDetail = `可见性时点：${dataset.as_of}。${
    dataset.scope === "named_partitions"
      ? "只限具名分区，不证明全湖。"
      : dataset.scope === "current_snapshot"
        ? "只限当前快照。"
        : "按所选日期范围统计。"
  }来源：${dataset.source_id}；规则版本：${dataset.rule_version}；合同摘要：${dataset.contract_sha256}`;
  const coverage =
    dataset.expected_open_days === null || dataset.covered_open_days === null
      ? "—"
      : `${formatCount(dataset.covered_open_days)} / ${formatCount(dataset.expected_open_days)}`;
  const closedRows =
    dataset.closed_day_rows.reduce((sum, d) => sum + d.row_count, 0) +
    dataset.omitted_closed_day_rows;
  const gaps = dataset.gaps.length + dataset.omitted_gap_count;
  return (
    <div className="dc-report dc-dataset-report">
      <div className="dc-report-intro">
        <span className="dc-report-dates mono">
          {dataset.audit_start} 至 {dataset.observed_through}
        </span>
        <Tip content={sourceDetail} interactive>
          <button type="button" className="dc-dataset-help" aria-label="来源与范围说明">
            来源与范围
          </button>
        </Tip>
      </div>
      <div className="dc-report-states">
        <StatusBadge
          state={dataset.conclusion === "issues_observed" ? "crit" : "warn"}
          label={dataset.conclusion_label}
          reason="只解释已检查事实，零问题不代表健康或采集完整。"
        />
        <StatusBadge
          state="warn"
          label={dataset.completeness_label}
          reason="完整证券范围、分钟网格或采集完成依据缺失时保留未确认。"
        />
        <StatusBadge
          state={dataset.freshness_state === "delayed" ? "crit" : "warn"}
          label={`更新：${dataset.freshness_label}`}
          reason={dataset.rules.find((r) => r.rule_id === "freshness")?.reason_label}
        />
      </div>
      <KpiStrip
        compact
        label="数据集报告摘要"
        items={[
          { key: "rows", label: "已有记录", value: formatCount(dataset.observed_rows) },
          { key: "visible", label: "已确认可见", value: formatCount(dataset.visible_rows) },
          { key: "pending", label: "尚未确认可见", value: formatCount(dataset.pending_rows) },
          { key: "days", label: "有记录交易日", value: coverage },
        ]}
      />
      {dataset.observed_rows === 0 ? (
        <p className="dc-report-caveat">没有可检查记录，不能据此判断健康</p>
      ) : null}
      {dataset.source_state !== "ready" ? (
        <EmptyState
          title={
            dataset.source_state === "missing_source" ? "缺少这份数据来源" : "来源与数据合同不符"
          }
          hint="来源齐备后可重新审计"
        />
      ) : null}
      <section className="dc-report-section" aria-label="数据集覆盖情况">
        <div className="dc-report-section-head">
          <h3>按月覆盖</h3>
          <Tip content="只统计交易日是否有可见记录。它不证明每只证券和每个分钟齐全；尚未结束的交易日不算缺口。">
            <span className="dc-audit-help">说明</span>
          </Tip>
        </div>
        {dataset.monthly.length ? (
          <DataTable
            label="数据集月度覆盖"
            rows={dataset.monthly}
            columns={months}
            rowKey={(r) => r.month}
            height={dataset.monthly.length > 12 ? 270 : undefined}
          />
        ) : (
          <p className="dc-report-muted">
            {dataset.scope === "current_snapshot"
              ? "当前快照不检查历史逐日覆盖"
              : dataset.coverage_label}
          </p>
        )}
        {gaps || closedRows ? (
          <p className="dc-report-caveat">
            记录缺口 {formatCount(gaps)} 段 · 休市日记录 {formatCount(closedRows)} 条
          </p>
        ) : null}
        {dataset.gaps.length ? (
          <details className="dc-dataset-details">
            <summary>查看缺口</summary>
            <ul className="dc-dataset-gaps">
              {dataset.gaps.map((g) => (
                <li key={g.start}>
                  <span className="mono">
                    {g.start} 至 {g.end}
                  </span>
                  <span>{formatCount(g.missing_open_days)} 个交易日</span>
                </li>
              ))}
            </ul>
            {dataset.omitted_gap_count ? (
              <p>另有 {formatCount(dataset.omitted_gap_count)} 段未列出</p>
            ) : null}
          </details>
        ) : null}
      </section>
      {dataset.frequencies.length ? (
        <section className="dc-report-section" aria-label="实际分钟频率">
          <h3>实际频率</h3>
          <div className="dc-dataset-frequencies">
            {dataset.frequencies.map((f) => (
              <Tip
                key={f.frequency}
                content={`实际 ${f.frequency}；已确认可见 ${formatCount(f.visible_rows)} 条。完整频率网格未确认。`}
              >
                <span>
                  {frequencyLabel(f.frequency)} · {formatCount(f.row_count)} 条
                </span>
              </Tip>
            ))}
          </div>
        </section>
      ) : null}
      <section className="dc-report-section" aria-label="数据集检查情况">
        <h3>检查情况</h3>
        <DataTable
          label="数据集检查项"
          rows={dataset.rules}
          columns={rules}
          rowKey={(r) => r.rule_id}
        />
        {dataset.recorded_after_as_of_rows ? (
          <p className="dc-report-caveat">
            {formatCount(dataset.recorded_after_as_of_rows)} 条记录在报告时点后写入
            <Tip content="这是相对于报告可见性时点的实际写入记录。它不证明源采集已完成，也不把盘后时间当成分钟延迟。">
              <span className="dc-audit-help">说明</span>
            </Tip>
          </p>
        ) : null}
      </section>
      <details className="dc-dataset-details">
        <summary>行数与空值</summary>
        <section className="dc-report-section" aria-label="数据集行数变化">
          <div className="dc-report-section-head">
            <h3>行数变化</h3>
            <Tip content="只比较已有记录，增减不一定是问题。" interactive>
              <button type="button" className="dc-dataset-help" aria-label="行数变化说明">
                说明
              </button>
            </Tip>
          </div>
          <DataTable
            label="数据集行数变化"
            rows={dataset.row_changes}
            columns={changes}
            rowKey={(r) => r.day}
            height={dataset.row_changes.length > 12 ? 270 : undefined}
            emptyText="没有适用的历史逐日对比"
          />
          {dataset.omitted_row_changes ? (
            <p className="dc-report-muted">
              仅列最近 {formatCount(dataset.row_changes.length)} 个交易日
            </p>
          ) : null}
        </section>
        <section className="dc-report-section" aria-label="数据集字段空值情况">
          <div className="dc-report-section-head">
            <h3>字段空值</h3>
            <Tip content="检查必要字段和部分常用字段，空值需结合字段含义判断。" interactive>
              <button type="button" className="dc-dataset-help" aria-label="字段空值说明">
                说明
              </button>
            </Tip>
          </div>
          <DataTable
            label="数据集字段空值"
            rows={dataset.fields}
            columns={fields}
            rowKey={(r) => r.field_name}
            emptyText="尚无可检查字段"
          />
        </section>
      </details>
    </div>
  );
}

export function CollectionScopeBadge({
  datasetId,
  generation,
}: {
  datasetId: string;
  generation: string | null;
}) {
  const collection = useDataCollection();
  const evidence =
    generation && collection.serving?.generation_id === generation
      ? collection.data?.datasets.find((item) => item.dataset_id === datasetId)
      : undefined;
  return (
    <section className="dc-collection-scope" aria-label="实际采集范围">
      <span>采集范围</span>
      <StatusBadge
        state={evidence?.status === "verified" ? "ok" : "warn"}
        label={evidence?.status_label ?? "采集尚未确认"}
        reason={
          evidence?.scopes.length
            ? evidence.scopes
                .map((scope) =>
                  scope.scope === "actual_receipt_set"
                    ? `本次原回执合计 ${formatCount(scope.row_count)} 条实际记录`
                    : `${scope.trade_date}：${formatCount(scope.row_count)} 条实际记录`,
                )
                .join("；")
            : "缺少可核对的原采集回执，不能确认完成。"
        }
      />

      <Tip content="每个交易日有记录，不代表全市场逐股齐全。实际采集范围只按原回执显示。">
        <span>全市场覆盖尚未核验</span>
      </Tip>
    </section>
  );
}
