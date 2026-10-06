import { useEffect, useState, useSyncExternalStore } from "react";
import {
  type BackfillPlanDetail,
  type DataCenterCommand,
  type DataCenterConfirmation,
  submitDataCenterCommand,
  useDataCenterExecutions,
  useDataCollection,
  useFinancialSources,
} from "@/api/endpoints";
import { useCurrentMeta } from "@/api/useMeta";
import { formatCount } from "@/format/number";
import { Button, ConfirmDialog, EmptyState, PageSkeleton, Panel, RelativeTime, Tip } from "@/ui";
import {
  createDataCenterCommandSession,
  type DataCenterCommandSession,
} from "./dataCenterCommandSession";

type Props = (
  | { mode: "backfill"; plan: BackfillPlanDetail; planTaskId: string | null }
  | { mode: "financial" }
) & { executionSession?: DataCenterCommandSession };

function commandIdentity() {
  return { command_id: `web-${crypto.randomUUID()}`, requested_at: new Date().toISOString() };
}

function selectedReportPeriods(first: string, last: string): string[] {
  const ends = ["03-31", "06-30", "09-30", "12-31"];
  const parse = (value: string) => {
    const year = Number(value.slice(0, 4));
    const quarter = ends.indexOf(value.slice(5));
    return /^\d{4}-\d{2}-\d{2}$/.test(value) && year > 0 && quarter >= 0
      ? year * 4 + quarter
      : null;
  };
  const start = parse(first);
  const end = parse(last || first);
  if (start === null || end === null || end < start || end - start > 40) return [];
  return Array.from({ length: end - start + 1 }, (_, index) => {
    const value = start + index;
    return `${String(Math.floor(value / 4)).padStart(4, "0")}-${ends[value % 4]}`;
  });
}

function confirmationText(value: DataCenterConfirmation) {
  return (
    <dl className="dc-execution-facts">
      <div className="dc-execution-fact">
        <dt className="dc-execution-term">日期</dt>
        <dd className="mono dc-execution-value">
          {value.start_date} — {value.end_date}
        </dd>
      </div>
      {value.kind === "backfill" ? (
        <div className="dc-execution-fact">
          <dt className="dc-execution-term">整日缺失</dt>
          <dd className="num dc-execution-value">{formatCount(value.missing_date_count)} 天</dd>
        </div>
      ) : (
        <>
          <div className="dc-execution-fact">
            <dt className="dc-execution-term">股票</dt>
            <dd className="num dc-execution-value">{formatCount(value.security_count)} 只</dd>
          </div>
          <div className="dc-execution-fact">
            <dt className="dc-execution-term">预计请求</dt>
            <dd className="num dc-execution-value">{formatCount(value.query_count)} 次</dd>
          </div>
          <div className="dc-execution-fact">
            <dt className="dc-execution-term">报告期</dt>
            <dd className="mono dc-execution-value">
              {(value.report_periods?.length ?? 0) > 4 ? (
                <Tip content={value.report_periods?.join("、")}>
                  {value.report_periods?.[0]} — {value.report_periods?.at(-1)}（
                  {formatCount(value.report_periods?.length)} 期）
                </Tip>
              ) : (
                value.report_periods?.join("、")
              )}
            </dd>
          </div>
        </>
      )}
    </dl>
  );
}

export function BackfillExecutionPanel(props: Props) {
  const index = useDataCenterExecutions();
  const collection = useDataCollection();
  const sources = useFinancialSources();
  const meta = useCurrentMeta();
  const viewer = meta.data?.data.viewer ?? null;
  const generation = meta.data?.data.generation?.generation_id ?? null;
  const indexCurrent = Boolean(generation && index.serving?.generation_id === generation);
  const [localSession] = useState(
    () => props.executionSession ?? createDataCenterCommandSession(submitDataCenterCommand),
  );
  const session = props.executionSession ?? localSession;
  const command = useSyncExternalStore(session.subscribe, session.snapshot, session.snapshot);
  const contextCurrent = session.matchesContext(viewer, generation);
  const receipt = contextCurrent ? command.receipt : null;
  const uncertain = contextCurrent && command.uncertain;
  const message = contextCurrent || !command.storageAvailable ? command.message : null;
  useEffect(() => session.sync(viewer, generation), [viewer, generation, session]);
  const name = props.mode === "backfill" ? "日线回补" : "财务采集";
  const [dialogOpen, setDialogOpen] = useState(false);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [period, setPeriod] = useState("");
  const [lastPeriod, setLastPeriod] = useState("");
  const [symbols, setSymbols] = useState("");
  const [selectAll, setSelectAll] = useState(false);
  const confirmation = receipt?.status === "prepared" ? receipt.confirmation : null;
  useEffect(() => {
    if (confirmation) setDialogOpen(true);
  }, [confirmation]);
  const published = indexCurrent
    ? index.data?.executions.find((value) => value.kind === props.mode)
    : undefined;
  const recordExecutionIds = new Set(
    indexCurrent
      ? index.data?.executions
          .filter((value) => value.kind === props.mode)
          .map((value) => value.execution_id)
      : [],
  );
  const records = indexCurrent
    ? (index.data?.events ?? [])
        .filter((value) => recordExecutionIds.has(value.execution_id))
        .slice()
        .reverse()
    : [];
  const acknowledged = receipt?.execution;
  const current =
    acknowledged?.kind === props.mode &&
    (!published ||
      (acknowledged.execution_id === published.execution_id &&
        acknowledged.control_sequence >= published.control_sequence))
      ? acknowledged
      : published;
  const active =
    indexCurrent &&
    index.data?.executions.some((value) =>
      ["queued", "running", "verifying"].includes(value.status),
    );
  const queuedWaiting =
    receipt?.status === "queued" &&
    !index.data?.executions.some((value) => value.execution_id === receipt.execution_id);
  const enabled =
    props.mode === "backfill" ? index.data?.backfill_enabled : index.data?.financial_enabled;
  const controlCurrent = Boolean(
    viewer &&
      contextCurrent &&
      indexCurrent &&
      !index.isFetching &&
      !meta.isFetching &&
      published &&
      current?.execution_id === published.execution_id,
  );
  const controlReason = !controlCurrent
    ? index.isFetching || meta.isFetching
      ? "正在刷新任务状态。"
      : "等待任务状态更新。"
    : uncertain
      ? "请先核对上次请求。"
      : undefined;
  const sourceMismatch =
    collection.serving?.generation_id !== generation ||
    sources.serving?.generation_id !== generation;
  const selectedSymbols = symbols
    .split(/[\s,，;；]+/)
    .filter(Boolean)
    .map((value) => value.toUpperCase())
    .sort();
  const reportPeriods = selectedReportPeriods(period, lastPeriod);
  const invalidScope =
    !startDate ||
    !endDate ||
    startDate > endDate ||
    !reportPeriods.length ||
    (reportPeriods.at(-1) ?? "") > endDate ||
    (!selectAll &&
      (!selectedSymbols.length ||
        selectedSymbols.length > 250 ||
        new Set(selectedSymbols).size !== selectedSymbols.length));
  const reason = !viewer
    ? "请先登录。"
    : !indexCurrent || !contextCurrent
      ? "等待数据更新。"
      : !command.storageAvailable
        ? "浏览器存储不可用。"
        : uncertain
          ? "请先核对上次请求。"
          : !enabled
            ? "执行条件尚未核验。"
            : active || queuedWaiting
              ? "请等待当前任务结束，或先暂停。"
              : props.mode === "backfill" && (!props.planTaskId || !props.plan.missing_day_count)
                ? "请先生成一份当前可用的回补计划。"
                : props.mode === "financial" &&
                    (invalidScope || sourceMismatch || !collection.data?.report_hash)
                  ? "请选择范围并等待来源核验。"
                  : props.mode === "financial" &&
                      (sources.data?.sources.length !== 7 ||
                        sources.data.sources.some(
                          (value) => value.permission_status !== "verified",
                        ))
                    ? "接口权益尚未核验。"
                    : undefined;

  async function prepare() {
    if (!session.matchesContext(viewer, generation)) return;
    const identity = commandIdentity();
    if (props.mode === "backfill" && props.planTaskId) {
      await session.start({
        ...identity,
        kind: "prepare_backfill_execution",
        plan_task_id: props.planTaskId,
        plan_hash: props.plan.plan_hash,
      });
    } else if (props.mode === "financial" && collection.data?.report_hash) {
      await session.start({
        ...identity,
        kind: "prepare_financial_collection",
        audit_report_hash: collection.data.report_hash,
        security_scope: selectAll ? "available_securities" : "selected_securities",
        selected_securities: selectAll ? [] : selectedSymbols,
        start_date: startDate,
        end_date: endDate,
        report_periods: reportPeriods,
      });
    }
    index.refetch();
  }

  async function execute() {
    if (
      !session.matchesContext(viewer, generation) ||
      !confirmation ||
      confirmation.kind !== props.mode
    )
      return;
    const common = {
      ...commandIdentity(),
      execution_id: confirmation.execution_id,
      intent_id: confirmation.intent_id,
      prepare_command_id: confirmation.prepare_command_id,
      plan_hash: confirmation.plan_hash,
      confirmed: true as const,
    };
    let body: DataCenterCommand;
    if (
      confirmation.kind === "backfill" &&
      confirmation.plan_task_id &&
      confirmation.exact_dates_sha256
    ) {
      body = {
        ...common,
        kind: "execute_backfill_plan",
        plan_task_id: confirmation.plan_task_id,
        exact_dates_sha256: confirmation.exact_dates_sha256,
      };
    } else if (confirmation.kind === "financial")
      body = { ...common, kind: "execute_financial_collection" };
    else return;
    await session.start(body);
    setDialogOpen(false);
    index.refetch();
  }

  async function control(kind: "pause_data_center_execution" | "resume_data_center_execution") {
    const { meta: liveMeta, metaFetchStatus, index: liveIndex } = index.readCurrent();
    const livePublished = liveIndex?.data?.data.executions.find(
      (value) => value.kind === props.mode,
    );
    const liveCurrent =
      acknowledged?.kind === props.mode &&
      acknowledged.execution_id === livePublished?.execution_id &&
      acknowledged &&
      livePublished &&
      acknowledged.control_sequence >= livePublished.control_sequence
        ? acknowledged
        : livePublished;
    if (
      !controlCurrent ||
      liveMeta?.data.viewer !== viewer ||
      liveMeta.data.generation?.generation_id !== generation ||
      metaFetchStatus !== "idle" ||
      liveIndex?.fetchStatus !== "idle" ||
      liveIndex.status !== "success" ||
      liveIndex.data?.serving.generation_id !== generation ||
      !liveCurrent ||
      current?.execution_id !== liveCurrent.execution_id ||
      current.control_sequence !== liveCurrent.control_sequence ||
      !session.matchesContext(viewer, generation) ||
      !current ||
      uncertain ||
      (kind === "pause_data_center_execution" && !current.can_pause) ||
      (kind === "resume_data_center_execution" && (!current.can_resume || !enabled))
    )
      return;
    await session.start({
      ...commandIdentity(),
      kind,
      execution_id: current.execution_id,
      expected_sequence: current.control_sequence,
    });
    index.refetch();
  }

  if (index.isLoading) return <PageSkeleton label={`${name}状态加载中`} />;
  return (
    <Panel
      title={props.mode === "backfill" ? "执行回补" : "采集财务"}
      actions={
        <Button
          size="sm"
          variant="ghost"
          onClick={() => {
            void meta.refetch();
            index.refetch();
            sources.refetch();
            collection.refetch();
          }}
        >
          刷新状态
        </Button>
      }
    >
      {index.error ? (
        <EmptyState title="暂时读不到任务状态" hint="刷新后再试" />
      ) : (
        <>
          {props.mode === "financial" ? (
            <>
              <section className="dc-financial-sources" aria-label="财务接口权益">
                {(sources.data?.sources ?? []).map((source) => (
                  <div className="dc-financial-source" key={source.api_name}>
                    <Tip
                      content={`可用日期：${source.scope_start ?? "未确认"} — ${source.scope_end ?? "未确认"}；凭据到期：${source.expires_at ?? "未确认"}${source.evidence_source === "offline_fixture" ? "；本地验证材料" : ""}`}
                    >
                      <strong>{source.name}</strong>
                    </Tip>
                    <span>{source.permission_label}</span>
                  </div>
                ))}
              </section>
              <p className="dc-execution-quota">
                <Tip content="按当前数据账户的原额度记录显示。每次实际请求会再次核验。">
                  当前剩余额度
                </Tip>
                <strong className="num">
                  {formatCount(sources.data?.sources[0]?.remaining_units)}
                </strong>
              </p>
              <div className="dc-execution-fields">
                <label className="field">
                  <span className="lbl">开始日期</span>
                  <input
                    className="inp"
                    aria-label="财务开始日期"
                    type="date"
                    value={startDate}
                    onChange={(event) => setStartDate(event.target.value)}
                    disabled={command.busy || uncertain}
                  />
                </label>
                <label className="field">
                  <span className="lbl">结束日期</span>
                  <input
                    className="inp"
                    aria-label="财务结束日期"
                    type="date"
                    value={endDate}
                    onChange={(event) => setEndDate(event.target.value)}
                    disabled={command.busy || uncertain}
                  />
                </label>
                <label className="field">
                  <span className="lbl">
                    <Tip content="选择季度末。只采集一期时，最后报告期留空。一次最多 41 期。">
                      首个报告期
                    </Tip>
                  </span>
                  <input
                    className="inp"
                    aria-label="财务报告期"
                    type="date"
                    value={period}
                    onChange={(event) => setPeriod(event.target.value)}
                    disabled={command.busy || uncertain}
                  />
                </label>
                <label className="field">
                  <span className="lbl">最后报告期（可选）</span>
                  <input
                    className="inp"
                    aria-label="财务最后报告期"
                    type="date"
                    value={lastPeriod}
                    onChange={(event) => setLastPeriod(event.target.value)}
                    disabled={command.busy || uncertain}
                  />
                </label>
                <label className="field">
                  <span className="lbl">
                    <Tip content="多个代码用逗号分隔，如 600000.SH、000001.SZ。一次最多选择 250 只。">
                      股票
                    </Tip>
                  </span>
                  <input
                    className="inp mono"
                    aria-label="财务股票范围"
                    value={symbols}
                    placeholder="600000.SH"
                    onChange={(event) => setSymbols(event.target.value)}
                    disabled={selectAll || command.busy || uncertain}
                  />
                </label>
              </div>
              <label className="dc-execution-all">
                <input
                  type="checkbox"
                  checked={selectAll}
                  disabled={command.busy || uncertain}
                  onChange={(event) => setSelectAll(event.target.checked)}
                />
                <Tip content="使用当前已核验来源中的股票清单；确认时会显示实际数量和请求数。">
                  选择可用股票
                </Tip>
              </label>
            </>
          ) : null}
          {current ? (
            <section className="dc-execution-current" aria-label={`${name}任务进度`}>
              <div className="dc-execution-current-head">
                <strong>{current.status_label}</strong>
                <RelativeTime at={current.updated_at} />
              </div>
              <progress
                aria-label="数据任务进度"
                max={current.total_tasks || 1}
                value={current.completed_tasks}
              />
              <div className="dc-execution-current-foot">
                <span className="num">
                  {formatCount(current.completed_tasks)} / {formatCount(current.total_tasks)}
                </span>
                {current.current_date ? <span className="mono">{current.current_date}</span> : null}
              </div>
              <div className="dc-execution-actions">
                {current.can_pause ? (
                  <Button
                    disabledReason={controlReason}
                    disabled={command.busy}
                    onClick={() => void control("pause_data_center_execution")}
                  >
                    暂停
                  </Button>
                ) : null}
                {current.can_resume ? (
                  <Button
                    disabledReason={!enabled ? "执行条件尚未核验。" : controlReason}
                    disabled={command.busy}
                    onClick={() => void control("resume_data_center_execution")}
                  >
                    继续
                  </Button>
                ) : null}
                {current.completion_verified ? (
                  <Tip content="原采集和装配事实、固定副本及审计报告均已核对。只确认本次选定范围。">
                    <span>所选范围已完成</span>
                  </Tip>
                ) : null}
              </div>
            </section>
          ) : null}
          <div className="dc-execution-actions">
            <Button
              variant="primary"
              disabledReason={reason}
              disabled={command.busy}
              onClick={() => void prepare()}
            >
              确认{name}范围
            </Button>
            <Tip content="工作日 17:50 后和周末执行。08:20 停止接新批次，08:30 前释放写入占用。">
              <span className="dc-execution-window">
                {index.data?.may_start ? "可执行时段" : "等待执行时段"}
              </span>
            </Tip>
          </div>
          {message ? (
            <p role="status" className="dc-execution-message">
              {message}
            </p>
          ) : null}
          {uncertain ? (
            <Button
              disabled={command.busy}
              onClick={() => {
                void session.retry().then(() => index.refetch());
              }}
            >
              核对上次请求
            </Button>
          ) : null}
          <section className="dc-execution-records" aria-label={`${name}最近运行记录`}>
            <h3>
              <Tip content="显示最近任务状态和操作记录，最多 20 条。时间来自原任务和操作回执。">
                最近运行记录
              </Tip>
            </h3>
            {records.length ? (
              <ul className="dc-execution-record-list" aria-label="最近运行记录">
                {records.map((record) => (
                  <li key={record.event_id}>
                    <div className="dc-execution-record-label">
                      <strong>{record.name}</strong>
                      {record.detail ? (
                        <Tip content={record.detail}>
                          <span>{record.status_label}</span>
                        </Tip>
                      ) : (
                        <span>{record.status_label}</span>
                      )}
                    </div>
                    <RelativeTime at={record.occurred_at} />
                  </li>
                ))}
              </ul>
            ) : (
              <EmptyState title="还没有运行记录" hint="任务开始后显示" />
            )}
          </section>
        </>
      )}
      {confirmation?.kind === props.mode ? (
        <ConfirmDialog
          open={dialogOpen && confirmation?.kind === props.mode}
          level="high"
          title={`开始${name}`}
          confirmName={name}
          description={confirmation ? confirmationText(confirmation) : null}
          expiresAt={confirmation ? new Date(confirmation.expires_at) : undefined}
          confirmLabel="开始执行"
          busy={command.busy}
          disabled={
            !indexCurrent ||
            !index.data?.may_start ||
            (sourceMismatch && props.mode === "financial")
          }
          onConfirm={() => void execute()}
          onCancel={() => {
            setDialogOpen(false);
            session.clear();
          }}
        />
      ) : null}
    </Panel>
  );
}
