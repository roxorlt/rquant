import { useMemo } from "react";
import type {
  ResourceGroupItem,
  RuntimeServiceItem,
  ScheduledTaskItem,
  TaskOverviewData,
} from "@/api/endpoints";
import { EMPTY, formatNumber, formatPercent } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  EmptyState,
  type Kpi,
  KpiStrip,
  Panel,
  RelativeTime,
  StatusBadge,
  Tip,
} from "@/ui";

function Timestamp({ at }: { at: string | null }) {
  if (at === null) return <span className="muted">{EMPTY}</span>;
  const full = formatShanghaiDateTime(at);
  return (
    <Tip content={full}>
      <time className="num tasks-when" dateTime={at}>
        {full.slice(5, 16)}
      </time>
    </Tip>
  );
}

function Duration({ seconds }: { seconds: number | null }) {
  return <span className="num">{seconds === null ? EMPTY : `${formatNumber(seconds, 1)} 秒`}</span>;
}

type OpenLog = (unit: string, name: string, trigger: HTMLButtonElement) => void;

function LogButton({ unit, name, onLog }: { unit: string; name: string; onLog: OpenLog }) {
  return (
    <Button
      size="sm"
      variant="ghost"
      className="tasks-progress-link"
      aria-label={`查看${name}的运行日志`}
      onClick={(event) => onLog(unit, name, event.currentTarget)}
    >
      运行日志
    </Button>
  );
}

function TimerName({ row, onLog }: { row: ScheduledTaskItem; onLog?: OpenLog }) {
  return (
    <div className="tasks-name-cell">
      <div className="tasks-name-head">
        <Tip content={`${row.timer_unit} · ${row.service_unit}`}>
          <strong className="tasks-name">{row.name}</strong>
        </Tip>
        {onLog ? <LogButton unit={row.service_unit} name={row.name} onLog={onLog} /> : null}
      </div>
      <div className="tasks-mobile-timer">
        <StatusBadge state={row.status.state} label={row.status.label} reason={row.status.reason} />
        <span className="tasks-timer-times">
          <span>
            上次 <Timestamp at={row.last_trigger_at} />
          </span>
          <span>
            下次 <Timestamp at={row.next_at} />
          </span>
        </span>
        <span className="tasks-timer-outcome">
          结果 {row.result_label} · 耗时 <Duration seconds={row.duration_seconds} />
        </span>
      </div>
    </div>
  );
}

const TIMER_COLUMNS: DataColumn<ScheduledTaskItem>[] = [
  {
    id: "task",
    header: "任务",
    value: (row) => row.name,
    cell: (row) => <TimerName row={row} />,
    wrap: true,
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
    id: "last",
    header: "上次触发",
    value: (row) => row.last_trigger_at,
    cell: (row) => <Timestamp at={row.last_trigger_at} />,
    secondary: true,
  },
  {
    id: "next",
    header: "下次触发",
    value: (row) => row.next_at,
    cell: (row) => <Timestamp at={row.next_at} />,
    secondary: true,
  },
  {
    id: "result",
    header: "上次结果",
    value: (row) => row.result_label,
    secondary: true,
  },
  {
    id: "duration",
    header: "耗时",
    value: (row) => row.duration_seconds,
    cell: (row) => <Duration seconds={row.duration_seconds} />,
    secondary: true,
  },
];

function ServiceName({ row, onLog }: { row: RuntimeServiceItem; onLog?: OpenLog }) {
  return (
    <div className="tasks-name-cell">
      <div className="tasks-name-head">
        <Tip content={row.service_id}>
          <strong className="tasks-name">{row.name}</strong>
        </Tip>
        {onLog ? <LogButton unit={row.service_id} name={row.name} onLog={onLog} /> : null}
      </div>
      <span className="tasks-meta">{row.plane_label}</span>
      <span className="tasks-mobile-status">
        <StatusBadge state={row.status.state} label={row.status.label} reason={row.status.reason} />
        <RelativeTime at={row.heartbeat_at} />
      </span>
    </div>
  );
}

const SERVICE_COLUMNS: DataColumn<RuntimeServiceItem>[] = [
  {
    id: "service",
    header: "服务",
    value: (row) => row.name,
    cell: (row) => <ServiceName row={row} />,
    wrap: true,
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
    id: "plane",
    header: "位置",
    value: (row) => row.plane_label,
    secondary: true,
  },
  {
    id: "heartbeat",
    header: "最近心跳",
    value: (row) => row.heartbeat_at,
    cell: (row) => <RelativeTime at={row.heartbeat_at} />,
    secondary: true,
  },
];

function memory(bytes: number | null): string {
  return bytes === null ? EMPTY : `${formatNumber(bytes / 1024 ** 3, 2)} GiB`;
}

const GROUP_COLUMNS: DataColumn<ResourceGroupItem>[] = [
  {
    id: "group",
    header: "资源组",
    value: (row) => row.name,
    cell: (row) => (
      <Tip content={row.slice_unit}>
        <strong className="tasks-name">{row.name}</strong>
      </Tip>
    ),
    wrap: true,
  },
  {
    id: "current",
    header: "当前占用",
    value: (row) => row.memory_current_bytes,
    cell: (row) => <span className="num">{memory(row.memory_current_bytes)}</span>,
    numeric: true,
  },
  {
    id: "peak",
    header: "本次峰值",
    value: (row) => row.memory_peak_bytes,
    cell: (row) => <span className="num">{memory(row.memory_peak_bytes)}</span>,
    numeric: true,
  },
];

function SourceTime({ at }: { at: string | null }) {
  return at === null ? undefined : (
    <span>
      数据 <RelativeTime at={at} suffix="更新" />
    </span>
  );
}

function SourceNote({ note }: { note: string | null }) {
  return note ? (
    <p className="tasks-notice" role="status">
      {note}
    </p>
  ) : null;
}

export function OverviewSections({
  data,
  scheduledFresh,
  resourcesFresh,
  logUnits,
  onLog,
}: {
  data: TaskOverviewData;
  scheduledFresh: boolean;
  resourcesFresh: boolean;
  logUnits: ReadonlySet<string>;
  onLog: OpenLog;
}) {
  const { scheduled, services, resources } = data;
  const timerColumns = useMemo(
    () =>
      TIMER_COLUMNS.map((column) =>
        column.id === "task"
          ? {
              ...column,
              cell: (row: ScheduledTaskItem) => (
                <TimerName row={row} onLog={logUnits.has(row.service_unit) ? onLog : undefined} />
              ),
            }
          : column,
      ),
    [logUnits, onLog],
  );
  const serviceColumns = useMemo(
    () =>
      SERVICE_COLUMNS.map((column) =>
        column.id === "service"
          ? {
              ...column,
              cell: (row: RuntimeServiceItem) => (
                <ServiceName row={row} onLog={logUnits.has(row.service_id) ? onLog : undefined} />
              ),
            }
          : column,
      ),
    [logUnits, onLog],
  );
  const memoryKpis: Kpi[] = [
    { key: "host-total", label: "主机内存", value: memory(resources.host_memory_total_bytes) },
    { key: "host-free", label: "主机可用", value: memory(resources.host_memory_available_bytes) },
    {
      key: "rquant-current",
      label: "当前占用",
      value: memory(resources.rquant_memory_current_bytes),
    },
    { key: "rquant-peak", label: "本次峰值", value: memory(resources.rquant_memory_peak_bytes) },
  ];
  return (
    <>
      <Panel
        title="定时任务"
        label="定时任务区"
        sub={<SourceTime at={scheduled.source_updated_at} />}
      >
        {scheduled.source_state !== "ready" ? (
          <EmptyState
            title={scheduled.source_label}
            hint={scheduled.source_note ?? "请稍后刷新。"}
          />
        ) : !scheduledFresh ? (
          <EmptyState title="定时任务状态已过期" hint="刷新查看最新状态。" />
        ) : (
          <>
            <SourceNote note={scheduled.source_note} />
            <DataTable
              rows={scheduled.items.slice(0, 32)}
              columns={timerColumns}
              rowKey={(row) => row.timer_unit}
              label="定时任务"
              emptyText={<EmptyState title="当前没有定时任务" hint="请稍后刷新。" />}
            />
          </>
        )}
      </Panel>
      <Panel
        title="运行服务"
        label="运行服务区"
        sub={<SourceTime at={services.source_updated_at} />}
      >
        {services.source_state !== "ready" ? (
          <EmptyState title={services.source_label} hint={services.source_note ?? "请稍后刷新。"} />
        ) : (
          <>
            <SourceNote note={services.source_note} />
            <DataTable
              rows={services.items.slice(0, 32)}
              columns={serviceColumns}
              rowKey={(row) => row.service_id}
              label="运行服务"
              emptyText={<EmptyState title="还没有服务心跳" hint="服务启动后会显示。" />}
            />
          </>
        )}
      </Panel>
      <Panel
        title="资源概况"
        label="资源概况"
        sub={<SourceTime at={resources.source_updated_at} />}
      >
        {resources.source_state !== "ready" ? (
          <EmptyState
            title={resources.source_label}
            hint={resources.source_note ?? "请稍后刷新。"}
          />
        ) : !resourcesFresh ? (
          <EmptyState title="资源状态已过期" hint="刷新查看最新状态。" />
        ) : (
          <>
            <SourceNote note={resources.source_note} />
            <div className="tasks-resource-kpis">
              <KpiStrip items={memoryKpis} label="内存概况" compact />
            </div>
            <div className="tasks-cpu">
              <span>CPU</span>
              {resources.cpu_usage_percent === null ? (
                <span className="muted">{resources.cpu_note}</span>
              ) : (
                <strong className="num">{formatPercent(resources.cpu_usage_percent, 1)}</strong>
              )}
            </div>
            <div className="tasks-groups">
              <DataTable
                rows={resources.groups.slice(0, 4)}
                columns={GROUP_COLUMNS}
                rowKey={(row) => row.slice_unit}
                label="资源分组"
                emptyText={<EmptyState title="暂无资源分项" hint="采集更新后显示。" />}
              />
            </div>
          </>
        )}
      </Panel>
    </>
  );
}
