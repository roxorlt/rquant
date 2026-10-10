import { type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type ResearchJobItem,
  type ResearchJobsData,
  useInvalidateServiceLogCapabilities,
  useLabControlCapabilities,
  useServiceLogCapabilities,
  useTaskOverview,
} from "@/api/endpoints";
import { useTaskControlCapabilities } from "@/api/taskControls";
import { useCurrentMeta } from "@/api/useMeta";
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
import { LabJobControls } from "./LabJobControls";
import { LabSchedulingControls } from "./LabSchedulingControls";
import { OverviewSections } from "./OverviewSections";
import { type SelectedServiceLog, ServiceLogDrawer } from "./ServiceLogDrawer";
import { type SelectedTask, TaskProgressDrawer } from "./TaskProgressDrawer";
import { TaskUnitControls } from "./TaskUnitControls";
import { settledTask, TaskControlMemory } from "./taskControlRecovery";
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

function TaskName({
  row,
  onProgress,
  controls,
}: {
  row: ResearchJobItem;
  onProgress?: (row: ResearchJobItem, trigger: HTMLButtonElement) => void;
  controls?: ReactNode;
}) {
  return (
    <div className="tasks-name-cell">
      <div className="tasks-name-head">
        <Tip content={`任务编号 ${row.job_id}`}>
          <strong className="tasks-name">{row.strategy_name}</strong>
        </Tip>
        {onProgress ? (
          <Button
            size="sm"
            variant="ghost"
            className="tasks-progress-link"
            aria-label={`查看${row.strategy_name}的进展`}
            onClick={(event) => onProgress(row, event.currentTarget)}
          >
            进展
          </Button>
        ) : null}
      </div>
      <span className="tasks-meta">
        {row.job_type_label} · {row.resource_label}
      </span>
      {controls}
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
  const [failedRefresh, setFailedRefresh] = useState<{
    jobId: string;
    commandId: string;
    viewer: string;
    refreshKey: number;
    grantState: "waiting" | "checking" | "allowed" | "denied";
  } | null>(null);
  const [, setClockPulse] = useState(0);
  const [selected, setSelected] = useState<SelectedTask | null>(null);
  const [selectedLog, setSelectedLog] = useState<SelectedServiceLog | null>(null);
  const [logNotice, setLogNotice] = useState<string | null>(null);
  const [blockedLogUnit, setBlockedLogUnit] = useState<{
    unit: string;
    viewer: string;
    generationId: string;
  } | null>(null);
  const [progressNotice, setProgressNotice] = useState<string | null>(null);
  const [blockedGeneration, setBlockedGeneration] = useState<string | null>(null);
  const returnFocus = useRef<HTMLButtonElement | null>(null);
  const logReturnFocus = useRef<HTMLButtonElement | null>(null);
  const refreshAction = useRef<HTMLSpanElement | null>(null);
  const taskMemory = useRef(new TaskControlMemory());
  const invalidateLogCapabilities = useInvalidateServiceLogCapabilities();
  const meta = useCurrentMeta();
  const viewer = meta.isError ? null : (meta.data?.data.viewer ?? null);
  const [identity, setIdentity] = useState({ viewer, failed: meta.isError });
  const identityChanged = identity.viewer !== viewer || identity.failed !== meta.isError;
  const trustedViewer = !identityChanged && !meta.isError ? viewer : null;
  taskMemory.current.activate(trustedViewer);
  const capabilities = useServiceLogCapabilities(trustedViewer);
  const pageIndex = cursors.length - 1;
  const result = useTaskOverview(
    cursors[pageIndex] ?? null,
    refreshKey,
    meta.data !== undefined && !meta.isError && !identityChanged,
  );
  const currentGeneration = meta.data?.data.generation?.generation_id;
  const snapshot = result.data;
  const data = snapshot?.overview;
  const labCapabilities = useLabControlCapabilities(
    data?.can_control_research_jobs === true ? trustedViewer : null,
  );
  const generationId = result.serving?.generation_id ?? null;
  const outdated = currentGeneration !== undefined && currentGeneration !== generationId;
  const changed = result.error instanceof ApiError && result.error.status === 409;
  const taskCapabilities = useTaskControlCapabilities(
    trustedViewer,
    currentGeneration ?? generationId,
    refreshKey,
  );
  const taskGrants = !taskCapabilities.isError ? taskCapabilities.data : undefined;
  const recoveryGeneration = currentGeneration ?? generationId;
  const revokeTaskControls = useCallback(() => {
    if (trustedViewer !== null) taskMemory.current.clear(trustedViewer);
    void taskCapabilities.refetch();
  }, [trustedViewer, taskCapabilities.refetch]);
  useEffect(() => {
    if (
      trustedViewer !== null &&
      taskCapabilities.error instanceof ApiError &&
      [401, 403].includes(taskCapabilities.error.status)
    ) {
      taskMemory.current.clear(trustedViewer);
    }
  }, [trustedViewer, taskCapabilities.error]);
  const canViewProgress =
    !outdated &&
    trustedViewer !== null &&
    result.error === null &&
    generationId !== null &&
    generationId !== blockedGeneration &&
    data?.can_view_research_logs === true &&
    data.research.source_state === "ready" &&
    data.research.items.length > 0;
  const canControlJobs =
    !outdated &&
    trustedViewer !== null &&
    result.error === null &&
    generationId !== null &&
    data?.can_control_research_jobs === true &&
    !labCapabilities.isError &&
    labCapabilities.data?.can_control === true &&
    data.research.source_state === "ready";
  useEffect(() => {
    if (
      failedRefresh?.grantState !== "waiting" ||
      failedRefresh.refreshKey !== refreshKey ||
      failedRefresh.viewer !== trustedViewer ||
      result.isFetching ||
      result.error !== null ||
      data?.can_control_research_jobs !== true
    ) {
      return;
    }
    const requested = failedRefresh;
    setFailedRefresh({ ...requested, grantState: "checking" });
    void labCapabilities.refetch().then((grant) => {
      setFailedRefresh((current) =>
        current?.commandId === requested.commandId && current.grantState === "checking"
          ? {
              ...current,
              grantState: grant.isSuccess && grant.data.can_control ? "allowed" : "denied",
            }
          : current,
      );
    });
  }, [
    failedRefresh,
    refreshKey,
    trustedViewer,
    result.isFetching,
    result.error,
    data?.can_control_research_jobs,
    labCapabilities.refetch,
  ]);
  const staleSelection =
    selected !== null &&
    (selected.generationId !== generationId ||
      selected.viewer !== trustedViewer ||
      outdated ||
      result.error !== null ||
      data?.can_view_research_logs !== true ||
      data.research.source_state !== "ready" ||
      !data.research.items.some((row) => row.job_id === selected.jobId));
  const activeSelection = staleSelection ? null : selected;
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
  const missingUnitRequests =
    trustedViewer === null
      ? []
      : taskMemory.current
          .entries(trustedViewer)
          .filter(
            ([unit, pending]) =>
              unit !== "scheduling" &&
              !settledTask(pending) &&
              (outdated ||
                !scheduledFresh ||
                result.error !== null ||
                !data?.scheduled.items.some((row) => row.service_unit === unit)),
          );
  const logUnits = useMemo(() => {
    if (trustedViewer === null || capabilities.isError || outdated || result.error !== null) {
      return new Set<string>();
    }
    return new Set(capabilities.data?.units ?? []);
  }, [trustedViewer, capabilities.data, capabilities.isError, outdated, result.error]);
  const allowedLogUnits = useMemo(() => {
    if (
      blockedLogUnit === null ||
      blockedLogUnit.viewer !== trustedViewer ||
      blockedLogUnit.generationId !== generationId
    ) {
      return logUnits;
    }
    const allowed = new Set(logUnits);
    allowed.delete(blockedLogUnit.unit);
    return allowed;
  }, [logUnits, blockedLogUnit, trustedViewer, generationId]);
  const staleLogSelection =
    selectedLog !== null &&
    (selectedLog.viewer !== trustedViewer ||
      selectedLog.generationId !== generationId ||
      outdated ||
      result.error !== null ||
      !allowedLogUnits.has(selectedLog.unit) ||
      (selectedLog.invocationId != null &&
        !data?.scheduled.items.some(
          (row) =>
            row.service_unit === selectedLog.unit && row.invocation_id === selectedLog.invocationId,
        )) ||
      !(
        data?.scheduled.items.some((row) => row.service_unit === selectedLog.unit) ||
        data?.services.items.some((row) => row.service_id === selectedLog.unit)
      ));
  const activeLogSelection = staleLogSelection ? null : selectedLog;

  const openProgress = useCallback(
    (row: ResearchJobItem, trigger: HTMLButtonElement) => {
      if (!canViewProgress || generationId === null || trustedViewer === null) return;
      returnFocus.current = trigger;
      setProgressNotice(null);
      setSelected({
        jobId: row.job_id,
        name: row.strategy_name,
        generationId,
        viewer: trustedViewer,
      });
    },
    [canViewProgress, generationId, trustedViewer],
  );
  const refreshFailedControl = useCallback(
    (jobId: string, commandId: string) => {
      if (trustedViewer === null) return;
      const next = refreshKey + 1;
      setFailedRefresh({
        jobId,
        commandId,
        viewer: trustedViewer,
        refreshKey: next,
        grantState: "waiting",
      });
      setRefreshKey(next);
    },
    [refreshKey, trustedViewer],
  );
  const columns = useMemo(
    () =>
      COLUMNS.map((column) =>
        column.id === "task"
          ? {
              ...column,
              cell: (row: ResearchJobItem) => (
                <TaskName
                  row={row}
                  onProgress={canViewProgress ? openProgress : undefined}
                  controls={
                    canControlJobs &&
                    trustedViewer !== null &&
                    row.job_version != null &&
                    row.available_actions != null ? (
                      <LabJobControls
                        row={row}
                        viewer={trustedViewer}
                        onRefresh={() => {
                          setCursors([null]);
                          setRefreshKey((value) => value + 1);
                        }}
                        onFailedRefresh={(commandId) => refreshFailedControl(row.job_id, commandId)}
                        rearmReadyForCommand={
                          failedRefresh?.jobId === row.job_id &&
                          failedRefresh.viewer === trustedViewer &&
                          failedRefresh.refreshKey === refreshKey &&
                          failedRefresh.grantState === "allowed"
                            ? failedRefresh.commandId
                            : null
                        }
                        onRevoked={() => void labCapabilities.refetch()}
                      />
                    ) : undefined
                  }
                />
              ),
            }
          : column,
      ),
    [
      canViewProgress,
      openProgress,
      canControlJobs,
      trustedViewer,
      labCapabilities.refetch,
      refreshFailedControl,
      failedRefresh,
      refreshKey,
    ],
  );
  const closeProgress = useCallback(() => setSelected(null), []);
  const openLog = useCallback(
    (unit: string, name: string, trigger: HTMLButtonElement, invocationId?: string | null) => {
      if (trustedViewer === null || generationId === null || !allowedLogUnits.has(unit)) return;
      logReturnFocus.current = trigger;
      setLogNotice(null);
      setSelectedLog({
        unit,
        name,
        viewer: trustedViewer,
        generationId,
        openedAt: Date.now(),
        invocationId,
      });
    },
    [trustedViewer, generationId, allowedLogUnits],
  );
  const closeLog = useCallback(() => setSelectedLog(null), []);
  const revokeLog = useCallback(() => {
    if (selectedLog !== null) {
      setBlockedLogUnit({
        unit: selectedLog.unit,
        viewer: selectedLog.viewer,
        generationId: selectedLog.generationId,
      });
    }
    setSelectedLog(null);
    setLogNotice("当前账号无法查看运行日志。");
    void invalidateLogCapabilities();
  }, [selectedLog, invalidateLogCapabilities]);
  const invalidateProgress = useCallback(() => {
    if (selected !== null) setBlockedGeneration(selected.generationId);
    setSelected(null);
    setCursors([null]);
    setRefreshKey((value) => value + 1);
    setProgressNotice("数据已更新。新数据发布后点「刷新」，再打开任务进展。");
  }, [selected]);

  useEffect(() => {
    if (!identityChanged) return;
    setIdentity({ viewer, failed: meta.isError });
    setCursors([null]);
    setRefreshKey((value) => value + 1);
    if (selected !== null) {
      setSelected(null);
      setProgressNotice(
        meta.isError ? "当前身份暂无法确认，任务进展已关闭。" : "当前身份已变化，任务进展已关闭。",
      );
    }
    if (selectedLog !== null) {
      setSelectedLog(null);
      setLogNotice("当前身份已变化，运行日志已关闭。");
    }
  }, [identityChanged, viewer, meta.isError, selected, selectedLog]);

  useEffect(() => {
    if (!staleSelection || identityChanged || selected?.viewer !== viewer || meta.isError) return;
    setSelected(null);
    setProgressNotice(
      selected?.generationId !== generationId || outdated || changed
        ? "数据已更新，请重新打开任务进展。"
        : "当前无法查看任务进展。",
    );
  }, [
    staleSelection,
    identityChanged,
    selected,
    viewer,
    meta.isError,
    generationId,
    outdated,
    changed,
  ]);

  useEffect(() => {
    if (selected !== null || returnFocus.current === null) return;
    const trigger = returnFocus.current;
    returnFocus.current = null;
    const frame = window.requestAnimationFrame(() => {
      if (trigger.isConnected && !trigger.disabled) {
        trigger.focus();
      } else {
        const refresh = refreshAction.current?.querySelector("button");
        if (refresh && !refresh.disabled) refresh.focus();
        else refreshAction.current?.focus();
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [selected]);

  useEffect(() => {
    if (!staleLogSelection || identityChanged || selectedLog?.viewer !== viewer || meta.isError)
      return;
    setSelectedLog(null);
    setLogNotice("运行日志权限或数据已变化，请刷新后重新查看。");
  }, [staleLogSelection, identityChanged, selectedLog, viewer, meta.isError]);

  useEffect(() => {
    if (selectedLog !== null || logReturnFocus.current === null) return;
    const trigger = logReturnFocus.current;
    logReturnFocus.current = null;
    const frame = window.requestAnimationFrame(() => {
      if (trigger.isConnected && !trigger.disabled) trigger.focus();
      else {
        const refresh = refreshAction.current?.querySelector("button");
        if (refresh && !refresh.disabled) refresh.focus();
        else refreshAction.current?.focus();
      }
    });
    return () => window.cancelAnimationFrame(frame);
  }, [selectedLog]);

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
    if (meta.isError || meta.data === undefined) void meta.refetch();
    setCursors([null]);
    setRefreshKey((value) => value + 1);
    if (trustedViewer !== null) void capabilities.refetch();
  }

  return (
    <>
      <PageHeader
        eyebrow="运维"
        title="任务与调度"
        note="定时任务、运行服务、资源和研究队列"
        actions={
          // biome-ignore lint/a11y/useSemanticElements: this is a focus fallback around one action, not a form fieldset.
          <span ref={refreshAction} role="group" tabIndex={-1} aria-label="任务总览刷新">
            <Button size="sm" variant="ghost" onClick={refresh} disabled={result.isFetching}>
              刷新
            </Button>
          </span>
        }
      />
      {progressNotice ? (
        <p className="tasks-notice tasks-progress-notice" role="status">
          {progressNotice}
        </p>
      ) : null}
      {logNotice ? (
        <p className="tasks-notice tasks-progress-notice" role="status">
          {logNotice}
        </p>
      ) : null}
      {trustedViewer !== null && recoveryGeneration !== null && missingUnitRequests.length > 0 ? (
        <Panel title="待确认的原请求">
          <p className="hint">当前任务状态不可用。可继续核验原请求。</p>
          {missingUnitRequests.map(([unit, pending]) => (
            <TaskUnitControls
              key={unit}
              unit={unit}
              name={pending.unitName ?? "任务"}
              viewer={trustedViewer}
              generationId={recoveryGeneration}
              choice={undefined}
              canRecover={taskGrants?.can_recover_units === true}
              memory={taskMemory.current}
              onRefresh={refresh}
              onRevoked={revokeTaskControls}
            />
          ))}
        </Panel>
      ) : null}
      {meta.isError ? (
        <Panel title="任务总览">
          <EmptyState title="当前身份暂无法确认" hint="请稍后点「刷新」重试。" />
        </Panel>
      ) : result.isLoading ||
        identityChanged ||
        (outdated && !result.error) ||
        (changed && pageIndex > 0) ? (
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
            logUnits={allowedLogUnits}
            onLog={openLog}
            unitControls={
              trustedViewer !== null && generationId !== null
                ? (row) => (
                    <TaskUnitControls
                      key={row.service_unit}
                      unit={row.service_unit}
                      name={row.name}
                      viewer={trustedViewer}
                      generationId={generationId}
                      choice={
                        scheduledFresh
                          ? taskGrants?.units.find((choice) => choice.unit === row.service_unit)
                          : undefined
                      }
                      canRecover={taskGrants?.can_recover_units === true}
                      memory={taskMemory.current}
                      onRefresh={refresh}
                      onRevoked={revokeTaskControls}
                    />
                  )
                : undefined
            }
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
            {trustedViewer !== null && generationId !== null ? (
              <LabSchedulingControls
                state={data.scheduling ?? { available: false, note: "调度状态尚未发布。" }}
                viewer={trustedViewer}
                generationId={generationId}
                canControl={taskGrants?.can_control_scheduling === true}
                canRecover={taskGrants?.can_recover_scheduling === true}
                memory={taskMemory.current}
                onRefresh={refresh}
                onRevoked={revokeTaskControls}
              />
            ) : null}
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
                  columns={columns}
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
      <TaskProgressDrawer
        selected={activeSelection}
        onClose={closeProgress}
        onInvalidated={invalidateProgress}
      />
      <ServiceLogDrawer selected={activeLogSelection} onClose={closeLog} onRevoked={revokeLog} />
    </>
  );
}
