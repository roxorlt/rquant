import { type ReactNode, useEffect, useRef, useState } from "react";
import { ApiError, type Schemas } from "@/api/client";
import {
  type BackfillPlanDetail,
  type BackfillPlanItem,
  useBackfillPlanDetail,
  useBackfillPlans,
} from "@/api/endpoints";
import { formatCount } from "@/format/number";
import { shanghaiDateOf } from "@/format/time";
import { Button, EmptyState, PageSkeleton, Panel, RelativeTime, StatusBadge, Tip } from "@/ui";
import { BackfillPlanCommandForm } from "./BackfillPlanCommandForm";
import type {
  BackfillPlanCommandSession,
  BackfillPlanCommandSnapshot,
} from "./backfillPlanCommandSession";

interface PagePosition {
  cursors: (string | null)[];
  index: number;
  generation: string | null;
}

const FIRST_PAGE: PagePosition = { cursors: [null], index: 0, generation: null };

function estimatedTime(value: string): string {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds === 0) return "0 分钟";
  const minutes = Math.ceil(seconds / 60);
  if (minutes < 60) return `${minutes} 分钟`;
  const hours = Math.floor(minutes / 60);
  return `${hours} 小时${minutes % 60 ? ` ${minutes % 60} 分钟` : ""}`;
}

function PlanListItem({
  plan,
  active,
  onSelect,
}: {
  plan: BackfillPlanItem;
  active: boolean;
  onSelect: () => void;
}) {
  return (
    <li>
      <button
        className="dc-plan-item"
        type="button"
        aria-label={`第 ${plan.rank + 1} 份计划`}
        aria-current={active ? "true" : undefined}
        onClick={onSelect}
      >
        <span className="dc-plan-item-top">
          <strong>股票日线</strong>
          <span>{plan.rank === 0 ? "最近" : "历史"}</span>
        </span>
        <span className="dc-plan-item-range mono">
          {plan.audit_start} — {plan.completed_through}
        </span>
        <span className="dc-plan-item-foot">
          <span className="num">缺 {formatCount(plan.missing_day_count)} 天</span>
          <span className="mono">{shanghaiDateOf(plan.published_at).slice(5)} 发布</span>
        </span>
      </button>
    </li>
  );
}

function MissingDays({ plan }: { plan: BackfillPlanDetail }) {
  if (plan.missing_day_count === 0) {
    return (
      <EmptyState title="这段时间没有整日缺失" hint="只代表每个交易日有记录，不代表逐股齐全" />
    );
  }
  const byMonth = new Map<string, string[]>();
  for (const date of plan.missing_dates) {
    const month = date.slice(0, 7);
    const dates = byMonth.get(month) ?? [];
    dates.push(date);
    byMonth.set(month, dates);
  }
  return (
    <div className="dc-plan-months">
      {[...byMonth].map(([month, dates]) => {
        const published = plan.monthly.find((item) => item.month.startsWith(month));
        return (
          <section className="dc-plan-month" key={month} aria-label={`${month} 缺失日期`}>
            <div className="dc-plan-month-head">
              <h4>{month}</h4>
              {published ? (
                <span className="num">
                  缺 {formatCount(published.missing_open_days)} /{" "}
                  {formatCount(published.expected_open_days)} 天
                </span>
              ) : null}
            </div>
            <ul className="dc-plan-dates">
              {dates.map((date) => (
                <li className="mono" key={date}>
                  {date}
                </li>
              ))}
            </ul>
          </section>
        );
      })}
    </div>
  );
}

function PlanDetail({ plan }: { plan: BackfillPlanDetail }) {
  const operations = plan.estimate.logical_operations;
  return (
    <div className="dc-plan-detail">
      <div className="dc-plan-detail-head">
        <div>
          <p className="dc-plan-kicker">计划详情</p>
          <h3>股票日线回补</h3>
        </div>
        <Tip
          content={
            <span>
              计划编号：{plan.plan_hash}
              <br />
              来源标记：{plan.snapshot_label}
            </span>
          }
        >
          <span className="dc-plan-tech">核对信息</span>
        </Tip>
      </div>
      <dl className="dc-plan-facts">
        <div>
          <dt>核对区间</dt>
          <dd className="mono">
            {plan.audit_start} — {plan.completed_through}
          </dd>
        </div>
        <div>
          <dt>来源时间</dt>
          <dd>
            <RelativeTime at={plan.cutoff_observed_at} />
          </dd>
        </div>
        <div>
          <dt>缺失交易日</dt>
          <dd className="num">{formatCount(plan.missing_day_count)} 天</dd>
        </div>
        <div>
          <dt>预计耗时</dt>
          <dd className="num">
            <Tip
              content={
                <span>
                  按日线 {formatCount(operations.daily)}、每日指标{" "}
                  {formatCount(operations.daily_basic)}、复权因子{" "}
                  {formatCount(operations.adj_factor)}、名称变更窗口{" "}
                  {formatCount(operations.namechange_windows)}、特殊股票状态最多{" "}
                  {formatCount(operations.stock_st_upper_bound)} 项估算，共{" "}
                  {formatCount(operations.total)} 次逻辑操作。实际请求次数与配额扣额待确认。
                </span>
              }
            >
              <span className="dc-plan-estimate">预计 {estimatedTime(plan.estimated_seconds)}</span>
            </Tip>
          </dd>
        </div>
      </dl>
      <div className="dc-plan-checks">
        <span>
          <StatusBadge state="warn" label="注意" reason="来源身份与采集完成情况尚未核验" />{" "}
          来源待核验
        </span>
        <Tip content="当前只能估算适配器的逻辑操作，无法据此确认真实请求次数和扣额">
          <span>配额待确认</span>
        </Tip>
      </div>
      <p className="dc-plan-scope">
        <Tip content="只核对整个交易日有无日线记录。某日有记录，不代表每只股票的数据都齐全。">
          <span>仅核对整日空洞</span>
        </Tip>
      </p>
      <section className="dc-plan-section" aria-label="缺失交易日">
        <div className="dc-plan-section-head">
          <h3>缺失交易日</h3>
          <span className="num">{formatCount(plan.gap_count)} 段</span>
        </div>
        <MissingDays plan={plan} />
      </section>
    </div>
  );
}

type Progress = Schemas["BackfillPlanProgress"];
const EVENT_LABEL: Record<Schemas["BackfillPlanProgressLog"]["event_type"], string> = {
  queued: "进入队列",
  started: "开始核对",
  resumed: "继续核对",
  source_check: "核对来源",
  succeeded: "生成完成",
  failed: "生成失败",
  retried: "重新尝试",
};

function commandLabel(command: BackfillPlanCommandSnapshot, progress: Progress | null | undefined) {
  const journal = command.journal;
  if (!journal) return null;
  if (journal.status === "failed") return "本次请求未通过，请调整后重试";
  if (["ambiguous", "unknown"].includes(journal.status)) return "本次提交状态待确认";
  if (journal.status !== "queued") return "本次请求正在处理";
  if (journal.taskId === progress?.task_id && progress?.availability === "ready") {
    if (progress.status === "succeeded") return "本次计划已生成，正在更新列表";
    if (progress.status === "failed") return "本次计划生成失败";
  }
  return "本次请求已排队，等待生成";
}

function ProgressPanel({
  progress,
  taskId,
}: {
  progress: Progress | null | undefined;
  taskId: string | null;
}) {
  const matching = taskId !== null && progress?.task_id === taskId;
  let label = "任务进度暂不可用";
  if (progress?.availability === "empty") label = "还没有生成任务";
  if (progress?.availability === "ready") {
    const prefix = matching ? "本次" : "最新";
    label =
      progress.status === "queued"
        ? `${prefix}任务等待生成`
        : progress.status === "running"
          ? `${prefix}任务正在生成`
          : progress.status === "succeeded"
            ? matching
              ? "计划已生成"
              : "最新计划已生成"
            : progress.status === "failed"
              ? `${prefix}任务生成失败`
              : "任务进度暂不可用";
  }
  const logs = progress?.event_history === "available" ? (progress.logs ?? []).slice(0, 20) : [];
  return (
    <Panel title="任务进度" label="任务进度">
      <div className="dc-plan-progress-head">
        <strong>{label}</strong>
        {progress?.updated_at ? <RelativeTime at={progress.updated_at} /> : null}
        {progress?.task_id ? (
          <Tip content={`任务编号：${progress.task_id}`}>
            <span className="dc-plan-tech">核对信息</span>
          </Tip>
        ) : null}
      </div>
      {progress?.availability === "ready" ? (
        progress.event_history === "unavailable" ? (
          <p className="dc-plan-progress-note">近期记录暂不可查看</p>
        ) : logs.length ? (
          <ol className="dc-plan-progress-logs">
            {logs.map((log) => (
              <li key={log.event_id}>
                <span>{EVENT_LABEL[log.event_type]}</span>
                <RelativeTime at={log.occurred_at} />
              </li>
            ))}
          </ol>
        ) : (
          <p className="dc-plan-progress-note">暂无近期记录</p>
        )
      ) : null}
    </Panel>
  );
}

export function BackfillPlanPanel({
  commandSession,
  command,
  canSubmit,
  requestOpen,
  onCloseRequest,
}: {
  commandSession: BackfillPlanCommandSession;
  command: BackfillPlanCommandSnapshot;
  canSubmit: boolean;
  requestOpen: boolean;
  onCloseRequest: () => void;
}) {
  const [page, setPage] = useState<PagePosition>(FIRST_PAGE);
  const [selectedHash, setSelectedHash] = useState<string | null>(null);
  const refreshedSuccess = useRef<string | null>(null);
  const cursor = page.cursors[page.index] ?? null;
  const plans = useBackfillPlans(cursor, page.generation);
  const generation = page.generation ?? plans.serving?.generation_id ?? null;
  const items = plans.data?.source_state === "ready" ? plans.data.items : [];
  const activeHash = items.some((item) => item.plan_hash === selectedHash)
    ? selectedHash
    : (items[0]?.plan_hash ?? null);
  const selected = items.find((item) => item.plan_hash === activeHash);
  const detail = useBackfillPlanDetail(activeHash, generation);
  const progress = plans.data?.progress;
  const matchingTask =
    command.journal?.taskId && progress?.task_id === command.journal.taskId
      ? command.journal.taskId
      : null;

  useEffect(() => {
    if (
      progress?.availability !== "ready" ||
      !["queued", "running"].includes(progress.status ?? "")
    )
      return;
    const timer = window.setInterval(() => void plans.refetch(), 10_000);
    return () => window.clearInterval(timer);
  }, [progress?.availability, progress?.status, plans.refetch]);

  useEffect(() => {
    if (
      !matchingTask ||
      progress?.status !== "succeeded" ||
      refreshedSuccess.current === matchingTask
    )
      return;
    refreshedSuccess.current = matchingTask;
    setPage(FIRST_PAGE);
    setSelectedHash(null);
    void plans.refetch();
    if (activeHash) void detail.refetch();
  }, [matchingTask, progress?.status, plans.refetch, detail.refetch, activeHash]);

  function reload() {
    setPage(FIRST_PAGE);
    setSelectedHash(null);
    if (page.index === 0 && page.generation === null) plans.refetch();
  }

  let catalog: ReactNode;
  if (plans.isLoading) catalog = <PageSkeleton label="回补计划加载中" />;
  else if (plans.error) {
    const changed = plans.error instanceof ApiError && plans.error.status === 409;
    catalog = (
      <Panel
        title="回补计划"
        actions={
          <Button size="sm" onClick={reload}>
            {changed ? "重新加载计划" : "刷新计划"}
          </Button>
        }
      >
        <EmptyState
          title={changed ? "计划列表已更新" : "暂时读不到回补计划"}
          hint={changed ? "重新加载后查看最新计划" : "稍后重试"}
        />
      </Panel>
    );
  } else if (plans.data?.source_state !== "ready") {
    const state = plans.data?.source_state;
    const title =
      state === "empty"
        ? "还没有回补计划"
        : state === "not_published"
          ? "回补计划尚未发布"
          : "回补计划暂时不可用";
    catalog = (
      <Panel
        title="回补计划"
        actions={
          <Button size="sm" onClick={reload}>
            刷新计划
          </Button>
        }
      >
        <EmptyState
          title={title}
          hint={
            state === "empty"
              ? "生成并发布计划后会在这里显示"
              : state === "not_published"
                ? "发布后会在这里显示"
                : "稍后刷新页面再试"
          }
        />
      </Panel>
    );
  } else
    catalog = (
      <Panel
        title="回补计划"
        sub={`共 ${formatCount(plans.data.total ?? items.length)} 份`}
        actions={
          <Button size="sm" onClick={reload}>
            刷新计划
          </Button>
        }
        flush
      >
        <div className="dc-plan-layout">
          <section className="dc-plan-list" aria-label="计划列表">
            <ul>
              {items.map((item) => (
                <PlanListItem
                  key={item.plan_hash}
                  plan={item}
                  active={item.plan_hash === activeHash}
                  onSelect={() => setSelectedHash(item.plan_hash)}
                />
              ))}
            </ul>
            <div className="dc-plan-pager">
              <Button
                size="sm"
                variant="ghost"
                disabled={page.index === 0}
                onClick={() => setPage((previous) => ({ ...previous, index: previous.index - 1 }))}
              >
                上一页
              </Button>
              <span className="num">第 {page.index + 1} 页</span>
              <Button
                size="sm"
                variant="ghost"
                disabled={!plans.data.next_cursor}
                onClick={() => {
                  const next = plans.data?.next_cursor;
                  if (!next || !generation) return;
                  setPage((previous) => ({
                    cursors: [...previous.cursors.slice(0, previous.index + 1), next],
                    index: previous.index + 1,
                    generation,
                  }));
                }}
              >
                下一页
              </Button>
            </div>
          </section>
          <section className="dc-plan-preview" aria-label="计划详情">
            {activeHash === null ? (
              <EmptyState title="这页没有计划" hint="刷新计划后再试" />
            ) : detail.isLoading ? (
              <PageSkeleton label="计划详情加载中" />
            ) : detail.error ? (
              <EmptyState
                title={
                  detail.error instanceof ApiError && detail.error.status === 404
                    ? "这份计划已不在当前列表"
                    : detail.error instanceof ApiError && detail.error.status === 409
                      ? "计划详情已更新"
                      : "暂时读不到计划详情"
                }
                hint="刷新计划后重试"
              />
            ) : detail.data?.source_state === "ready" && detail.data.plan ? (
              <>
                {selected && selected.rank > 0 ? <p className="dc-plan-older">历史计划</p> : null}
                <PlanDetail plan={detail.data.plan} />
              </>
            ) : (
              <EmptyState title="计划详情暂时不可用" hint="刷新计划后重试" />
            )}
          </section>
        </div>
      </Panel>
    );

  const status = commandLabel(command, progress);
  return (
    <div className="dc-plan-stack">
      {requestOpen ? (
        <BackfillPlanCommandForm
          session={commandSession}
          snapshot={command}
          canSubmit={canSubmit}
          onClose={onCloseRequest}
        />
      ) : null}
      {status || command.message || !command.storageAvailable ? (
        <div className="dc-plan-command-status" role="status">
          {status ? <strong>{status}</strong> : null}
          {command.message ? <span>{command.message}</span> : null}
          {!command.storageAvailable && !command.message ? (
            <span>浏览器存储不可用，无法安全提交。</span>
          ) : null}
          {command.journal?.taskId ? (
            <Tip
              content={`任务编号：${command.journal.taskId}; 请求编号：${command.journal.body.command_id}`}
            >
              <span className="dc-plan-tech">核对信息</span>
            </Tip>
          ) : null}
          {command.journal &&
          ["pending", "processing", "ambiguous", "unknown"].includes(command.journal.status) ? (
            <Button
              size="sm"
              disabledReason={
                !canSubmit
                  ? "请先登录，才能继续核对。"
                  : command.busy
                    ? "正在核对，请稍候。"
                    : undefined
              }
              onClick={() => void commandSession.advance()}
            >
              继续核对
            </Button>
          ) : null}
        </div>
      ) : null}
      {plans.isLoading || plans.error ? null : (
        <ProgressPanel progress={progress} taskId={command.journal?.taskId ?? null} />
      )}
      {catalog}
    </div>
  );
}
