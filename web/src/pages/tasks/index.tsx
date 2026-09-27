import { useEffect, useState } from "react";
import { ApiError } from "@/api/client";
import { type ResearchJobItem, type ResearchJobsData, useTaskOverview } from "@/api/endpoints";
import { formatCount, formatPercent } from "@/format/number";
import { formatShanghaiDateTime, formatShanghaiTime } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  EmptyState,
  type Kpi,
  KpiStrip,
  PageHeader,
  PageSkeleton,
  Panel,
  RelativeTime,
  StatusBadge,
  Tip,
} from "@/ui";
import { OverviewSections } from "./OverviewSections";
import "./tasks.css";

function metrics(data: ResearchJobsData): Kpi[] {
  const counts = data.counts;
  if (counts === null) return [];
  const items: Kpi[] = [
    { key: "all", label: "全部", value: formatCount(data.total) },
    { key: "running", label: "运行中", value: formatCount(counts.running) },
    { key: "queued", label: "排队中", value: formatCount(counts.queued) },
    { key: "paused", label: "已暂停", value: formatCount(counts.checkpointed) },
    { key: "done", label: "已完成", value: formatCount(counts.succeeded) },
    {
      key: "failed",
      label: "失败",
      value: formatCount(counts.failed),
      tone: counts.failed ? "crit" : undefined,
    },
  ];
  if (counts.cancelled) {
    items.push({ key: "cancelled", label: "已取消", value: formatCount(counts.cancelled) });
  }
  if (counts.other) {
    items.push({ key: "other", label: "待确认", value: formatCount(counts.other), tone: "warn" });
  }
  return items;
}

function Eta({ row }: { row: ResearchJobItem }) {
  if (row.eta_at === null) {
    return <span className="muted">{row.eta_label}</span>;
  }
  const bounds =
    row.eta_low && row.eta_high
      ? `预计范围 ${formatShanghaiDateTime(row.eta_low)} 至 ${formatShanghaiDateTime(row.eta_high)}`
      : null;
  return (
    <Tip content={bounds ?? formatShanghaiDateTime(row.eta_at)}>
      <span className="num">{formatShanghaiTime(row.eta_at)}</span>
    </Tip>
  );
}

function TaskName({ row }: { row: ResearchJobItem }) {
  return (
    <div className="tasks-name-cell">
      <Tip content={`任务编号 ${row.job_id}`}>
        <strong className="tasks-name">{row.strategy_name}</strong>
      </Tip>
      <span className="tasks-meta">
        {row.job_type_label} · {row.resource_label}
      </span>
      <span className="tasks-mobile-status">
        <StatusBadge state={row.status.state} label={row.status.label} reason={row.status.reason} />
        <RelativeTime at={row.updated_at} />
      </span>
    </div>
  );
}

function Progress({ row }: { row: ResearchJobItem }) {
  return (
    <div className="tasks-progress-cell">
      <div className="tasks-progress-numbers">
        <strong className="num">{formatPercent(row.progress_fraction * 100, 0)}</strong>
        <span className="num muted">
          {formatCount(row.terminal_shards)} / {formatCount(row.total_shards)}
        </span>
      </div>
      <progress aria-label={`${row.strategy_name}完成进度`} max={1} value={row.progress_fraction} />
      <span className="tasks-mobile-eta">
        预计结束 <Eta row={row} />
      </span>
    </div>
  );
}

const COLUMNS: DataColumn<ResearchJobItem>[] = [
  {
    id: "task",
    header: "任务",
    value: (row) => row.strategy_name,
    cell: (row) => <TaskName row={row} />,
    wrap: true,
  },
  {
    id: "progress",
    header: "进度",
    value: (row) => row.progress_fraction,
    cell: (row) => <Progress row={row} />,
  },
  {
    id: "status",
    header: "状态",
    value: (row) => row.status.label,
    cell: (row) => (
      <StatusBadge state={row.status.state} label={row.status.label} reason={row.status.reason} />
    ),
    secondary: true,
  },
  {
    id: "eta",
    header: "预计结束",
    value: (row) => row.eta_at,
    cell: (row) => <Eta row={row} />,
    secondary: true,
  },
  {
    id: "updated",
    header: "更新",
    value: (row) => row.updated_at,
    cell: (row) => <RelativeTime at={row.updated_at} />,
    secondary: true,
  },
];

function emptyHint(state: ResearchJobsData["source_state"]): string {
  if (state === "empty") return "有新研究任务发布后会显示，也可稍后刷新。";
  if (state === "not_published") return "任务来源发布后会显示，请稍后刷新。";
  return "请稍后刷新，或查看系统健康。";
}

export default function TasksPage() {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [refreshKey, setRefreshKey] = useState(0);
  const [, setClockPulse] = useState(0);
  const pageIndex = cursors.length - 1;
  const result = useTaskOverview(cursors[pageIndex] ?? null, refreshKey);
  const snapshot = result.data;
  const data = snapshot?.overview;
  const changed = result.error instanceof ApiError && result.error.status === 409;
  const now = performance.now();
  const scheduledDeadline = snapshot?.scheduledDeadline ?? null;
  const resourcesDeadline = snapshot?.resourcesDeadline ?? null;
  const scheduledFresh =
    data?.scheduled.source_state === "ready" &&
    scheduledDeadline !== null &&
    scheduledDeadline > now;
  const resourcesFresh =
    data?.resources.source_state === "ready" &&
    resourcesDeadline !== null &&
    resourcesDeadline > now;

  useEffect(() => {
    if (!changed || pageIndex === 0) return;
    setCursors([null]);
    setRefreshKey((value) => value + 1);
  }, [changed, pageIndex]);

  useEffect(() => {
    let timer: number | undefined;
    const schedule = () => {
      const current = performance.now();
      const next = [snapshot?.scheduledDeadline, snapshot?.resourcesDeadline]
        .filter(
          (deadline): deadline is number =>
            deadline !== null && deadline !== undefined && deadline > current,
        )
        .sort((left, right) => left - right)[0];
      if (next === undefined) return;
      timer = window.setTimeout(
        () => {
          setClockPulse((value) => value + 1);
          schedule();
        },
        Math.ceil(next - current),
      );
    };
    schedule();
    return () => window.clearTimeout(timer);
  }, [snapshot?.scheduledDeadline, snapshot?.resourcesDeadline]);

  useEffect(() => {
    const wake = () => setClockPulse((value) => value + 1);
    window.addEventListener("focus", wake);
    document.addEventListener("visibilitychange", wake);
    return () => {
      window.removeEventListener("focus", wake);
      document.removeEventListener("visibilitychange", wake);
    };
  }, []);

  function refresh() {
    setCursors([null]);
    setRefreshKey((value) => value + 1);
  }

  return (
    <>
      <PageHeader
        eyebrow="运维"
        title="任务与调度"
        note="定时任务、运行服务、资源和研究队列"
        actions={
          <Button size="sm" variant="ghost" onClick={refresh} disabled={result.isFetching}>
            刷新
          </Button>
        }
      />
      {result.isLoading || (changed && pageIndex > 0) ? (
        <PageSkeleton label="任务总览加载中" />
      ) : result.error ? (
        <Panel title="任务总览">
          <EmptyState
            title={changed ? "数据已更新，请重新查看。" : "任务总览暂时无法加载"}
            hint={
              <Button size="sm" onClick={result.refetch}>
                重试
              </Button>
            }
          />
        </Panel>
      ) : data ? (
        <div className="tasks-content">
          <OverviewSections
            data={data}
            scheduledFresh={scheduledFresh}
            resourcesFresh={resourcesFresh}
          />
          {data.research.counts ? (
            <KpiStrip items={metrics(data.research)} label="任务状态概况" compact />
          ) : null}
          <Panel
            title="研究任务"
            sub={
              data.research.source_updated_at ? (
                <span>
                  数据 <RelativeTime at={data.research.source_updated_at} />
                  更新
                </span>
              ) : undefined
            }
          >
            {data.research.source_note ? (
              <p className="tasks-notice" role="status">
                {data.research.source_note}
              </p>
            ) : null}
            {data.research.source_state !== "ready" ? (
              <EmptyState
                title={data.research.source_label}
                hint={emptyHint(data.research.source_state)}
              />
            ) : (
              <>
                <DataTable
                  rows={data.research.items}
                  columns={COLUMNS}
                  rowKey={(row) => row.job_id}
                  label="研究任务队列"
                  emptyText="本页没有更多任务，请返回上一页。"
                />
                <nav className="tasks-pages" aria-label="任务翻页">
                  <span className="hint">第 {pageIndex + 1} 页</span>
                  <Button
                    size="sm"
                    disabled={pageIndex === 0 || result.isFetching}
                    onClick={() => setCursors((current) => current.slice(0, -1))}
                  >
                    上一页
                  </Button>
                  <Button
                    size="sm"
                    disabled={data.research.next_cursor === null || result.isFetching}
                    onClick={() =>
                      setCursors((current) =>
                        data.research.next_cursor
                          ? [...current, data.research.next_cursor]
                          : current,
                      )
                    }
                  >
                    下一页
                  </Button>
                </nav>
              </>
            )}
          </Panel>
        </div>
      ) : null}
    </>
  );
}
