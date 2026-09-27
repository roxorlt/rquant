import { useState } from "react";
import { type DataAuditIssueItem, useDataAuditHealth, useDataAuditIssues } from "@/api/endpoints";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, PageSkeleton, Panel, RelativeTime, Segmented, StatusBadge, Tip } from "@/ui";

const SEVERITY_LABELS = { P0: "紧急", P1: "严重", P2: "注意", P3: "提示" } as const;

const ISSUE_COLUMNS: DataColumn<DataAuditIssueItem>[] = [
  { id: "name", header: "问题", value: (row) => row.name, wrap: true },
  {
    id: "severity",
    header: "级别",
    value: (row) => row.severity,
    cell: (row) => <span>{SEVERITY_LABELS[row.severity]}</span>,
  },
  {
    id: "status",
    header: "处理",
    value: (row) => row.status,
    cell: (row) => (
      <StatusBadge
        state={row.status === "已处理" ? "ok" : "warn"}
        label={row.status}
        reason={row.status === "已处理" ? "这条问题后来已处理" : "这条问题仍待处理"}
      />
    ),
  },
];

export function AuditPanel({ datasetId }: { datasetId: string }) {
  const health = useDataAuditHealth();
  const generation =
    health.data?.source_state === "ready" ? (health.serving?.generation_id ?? null) : null;
  const issues = useDataAuditIssues(datasetId, generation);
  const [filter, setFilter] = useState("全部");

  if (health.isLoading) return <PageSkeleton label="审计结果加载中" />;

  if (health.error || !health.data || health.data.source_state === "unavailable") {
    return (
      <Panel title="数据审计">
        <EmptyState title="审计结果暂时不可用" hint="稍后刷新页面再试" />
      </Panel>
    );
  }
  if (health.data.source_state === "not_published") {
    return (
      <Panel title="数据审计">
        <EmptyState title="审计结果尚未发布" hint="发布后会在这里显示" />
      </Panel>
    );
  }
  if (issues.error || issues.data?.source_state === "unavailable") {
    return (
      <Panel title="数据审计">
        <EmptyState title="审计数据已更新或暂时无法读取" hint="刷新页面后重试" />
      </Panel>
    );
  }

  const { latest_attempt: attempt, latest_success: success } = health.data;
  if (attempt === null) {
    return (
      <Panel title="数据审计">
        <EmptyState title="尚未审计" hint="完成首次审计后显示结果" />
      </Panel>
    );
  }

  const selected =
    issues.data?.issues.filter((item) => filter === "全部" || item.status === filter) ?? [];
  const attemptState =
    attempt.status === "failed"
      ? "crit"
      : attempt.status === "running"
        ? "waiting"
        : (success?.p0_count ?? 0) > 0
          ? "crit"
          : (success?.finding_count ?? 0) > 0
            ? "warn"
            : "ok";
  const attemptLabel =
    attempt.status === "completed" && (success?.finding_count ?? 0) > 0
      ? "发现问题"
      : attempt.label;
  return (
    <Panel
      title="数据审计"
      actions={
        <Tip content="问题来自最近一次已完成的审计；处理状态反映当前记录。">
          <span className="dc-audit-help">说明</span>
        </Tip>
      }
    >
      <div className="dc-audit-summary">
        <StatusBadge
          state={attemptState}
          label={attemptLabel}
          reason={
            attempt.status === "failed"
              ? "最近一次审计未完成"
              : attempt.status === "running"
                ? "最近一次审计仍在运行"
                : "最近一次审计已完成"
          }
        />
        <span className="dc-audit-time">
          {attempt.status === "running" ? "开始于" : "结束于"}{" "}
          <RelativeTime at={attempt.completed_at ?? attempt.observed_at} />
        </span>
      </div>
      {success === null ? (
        <EmptyState
          title="还没有已完成的审计"
          hint={attempt.status === "running" ? "完成后会显示问题" : "下次审计完成后会显示问题"}
        />
      ) : (
        <>
          <div className="dc-audit-success">
            <span>{attempt.status === "completed" ? "本次完成" : "上次完成"}</span>
            <strong>
              {success.range_start} — {success.range_end}
            </strong>
            <span>
              <RelativeTime at={success.completed_at} suffix="完成" />
            </span>
            <span className="num">发现 {success.finding_count} 条</span>
          </div>
          {issues.isLoading || !issues.data ? (
            <PageSkeleton label="审计问题加载中" />
          ) : issues.data.source_state !== "ready" ? (
            <EmptyState title="审计问题暂时不可用" hint="稍后刷新页面再试" />
          ) : issues.data.total_count === 0 ? (
            <EmptyState
              title="这份数据没有审计问题"
              hint={
                attempt.status === "completed"
                  ? "最近一次审计未发现这份数据的问题"
                  : "上次完成的审计未发现这份数据的问题"
              }
            />
          ) : (
            <div className="dc-audit-issues">
              <div className="dc-audit-controls">
                <span className="num">
                  {issues.data.partial
                    ? `仅显示 ${issues.data.issues.length} / ${issues.data.total_count} 条`
                    : `全部 ${issues.data.total_count} 条`}
                </span>
                <Segmented
                  label="按处理状态筛选审计问题"
                  options={["全部", "待处理", "已处理"].map((value) => ({ value, label: value }))}
                  value={filter}
                  onChange={setFilter}
                />
              </div>
              <DataTable
                label="审计问题"
                rows={selected}
                columns={ISSUE_COLUMNS}
                rowKey={(row) => String(row.number)}
                emptyText={<EmptyState title="没有匹配的问题" hint="换个处理状态试试" />}
              />
            </div>
          )}
        </>
      )}
    </Panel>
  );
}
