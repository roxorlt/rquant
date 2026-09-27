import { useState } from "react";
import type { Schemas } from "@/api/client";
import type { AuditReportCalendarData, DataAuditReportData } from "@/api/endpoints";
import { Button, ConfirmDialog, RelativeTime, StatusBadge, Tip } from "@/ui";
import {
  type AuditReportCommandSession,
  type AuditReportCommandSnapshot,
  type AuditReportDateEvidence,
  validateAuditReportRange,
} from "./auditReportCommandSession";

type Market = Schemas["MarketInfo"];
type Progress = NonNullable<DataAuditReportData["progress"]>;
type Overview = NonNullable<DataAuditReportData["overview"]>;

function taskLabel(status: Progress["latest_status"]): string {
  switch (status) {
    case "queued":
      return "最近任务等待运行";
    case "running":
      return "最近任务正在核对";
    case "succeeded":
      return "最近任务已完成";
    case "failed":
      return "最近任务未完成";
    default:
      return "还没有运行记录";
  }
}

function taskTone(status: Progress["latest_status"]): "waiting" | "ok" | "crit" {
  if (status === "failed") return "crit";
  if (status === "succeeded") return "ok";
  return "waiting";
}

function currentRequestLabel(
  command: AuditReportCommandSnapshot,
  progress: Progress | null,
  ownPublished: boolean,
): string | null {
  const journal = command.journal;
  if (!journal) return null;
  if (journal.status === "failed") return "本次请求未通过";
  if (["ambiguous", "unknown", "pending", "processing"].includes(journal.status))
    return "本次提交状态待确认";
  if (ownPublished) return "本次报告已发布";
  if (journal.taskId !== progress?.latest_task_id) return "本次请求已排队";
  if (progress.latest_status === "queued") return "本次任务等待运行";
  if (progress.latest_status === "running") return "本次任务正在核对";
  if (progress.latest_status === "failed") return "本次审计未完成";
  if (progress.latest_status === "succeeded") {
    return "本次任务已结束，等待报告发布";
  }
  return "本次请求已排队";
}

/** Operation and progress stay visible even before the first report is published. */
export function AuditReportRun({
  calendar,
  calendarReady,
  market,
  viewer,
  overview,
  progress,
  ownPublished,
  readBlock,
  commandSession,
  command,
  onRefresh,
}: {
  calendar: AuditReportCalendarData | undefined;
  calendarReady: boolean;
  market: Market | null | undefined;
  viewer: string | null | undefined;
  overview: Overview | null;
  progress: Progress | null;
  ownPublished: boolean;
  readBlock: string | null;
  commandSession: AuditReportCommandSession;
  command: AuditReportCommandSnapshot;
  onRefresh: () => void;
}) {
  const [editedStart, setEditedStart] = useState<string | null>(null);
  const [editedEnd, setEditedEnd] = useState<string | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const earliest = calendarReady ? calendar?.earliest_selectable_date : null;
  const latest = calendarReady ? calendar?.latest_closed_date : null;
  const defaultStart = overview?.audit_start ?? "2024-09-01";
  const start = editedStart ?? (earliest && defaultStart < earliest ? earliest : defaultStart);
  const end = editedEnd ?? latest ?? "";
  const evidence: AuditReportDateEvidence | null =
    calendarReady && latest && calendar?.open_dates.includes(latest)
      ? {
          market,
          verifiedOpenDates: calendar.open_dates,
          verifiedClosedThrough: latest,
          verifiedEarliestSelectable: earliest ?? undefined,
        }
      : null;
  const invalid = validateAuditReportRange(start, end, evidence);
  const recent = progress?.availability === "ready" ? progress : null;
  const sameTask =
    command.journal?.taskId !== null &&
    command.journal?.taskId !== undefined &&
    command.journal.taskId === recent?.latest_task_id;
  const currentTaskActive =
    command.journal?.status === "queued" &&
    !ownPublished &&
    (!sameTask || !["succeeded", "failed"].includes(recent?.latest_status ?? ""));
  const requestUncertain =
    command.journal !== null && !["queued", "failed"].includes(command.journal.status);
  const disabledReason =
    readBlock ??
    (viewer === undefined
      ? "正在确认登录状态。"
      : viewer === null
        ? "请先登录，才能运行审计。"
        : !command.storageAvailable
          ? "浏览器记录不可用，无法安全提交。"
          : requestUncertain
            ? "请先核对上一次请求。"
            : currentTaskActive
              ? "当前审计尚未结束。"
              : !calendarReady || !evidence
                ? "交易日历暂不可用，请刷新后重试。"
                : (invalid ?? undefined));
  const requestLabel = currentRequestLabel(command, recent, ownPublished);
  const events = recent?.events?.slice(0, 20) ?? [];

  return (
    <div className="dc-report-operation">
      <section className="dc-report-run" aria-label="运行数据审计">
        <div className="dc-report-run-head">
          <div>
            <span className="dc-report-eyebrow">数据核对</span>
            <h3>运行数据审计</h3>
          </div>
          <Tip content="只读检查交易日覆盖和日线质量；报告发布后在下方查看。">
            <span className="dc-audit-help">说明</span>
          </Tip>
        </div>
        <div className="dc-report-run-fields">
          <label className="field">
            <span className="lbl">开始日期</span>
            <input
              className="inp mono"
              type="date"
              value={start}
              min={earliest ?? undefined}
              max={end || latest || undefined}
              onChange={(event) => setEditedStart(event.target.value)}
            />
          </label>
          <label className="field">
            <span className="lbl">结束日期</span>
            <input
              className="inp mono"
              type="date"
              value={end}
              min={earliest ?? undefined}
              max={latest ?? undefined}
              onChange={(event) => setEditedEnd(event.target.value)}
            />
          </label>
          <Button
            variant="primary"
            disabledReason={disabledReason}
            onClick={() => setConfirmOpen(true)}
          >
            运行数据审计
          </Button>
        </div>
        {readBlock ? (
          <p className="dc-report-run-note" role="status">
            {readBlock}
          </p>
        ) : invalid && calendarReady ? (
          <p className="dc-report-run-note" role="status">
            {invalid}
          </p>
        ) : !calendarReady ? (
          <p className="dc-report-run-note" role="status">
            交易日历暂不可用，刷新后再选择日期。
          </p>
        ) : null}
        {command.message ? (
          <p className="dc-report-run-note" role="status">
            {command.message}
          </p>
        ) : null}
        <ConfirmDialog
          open={confirmOpen}
          level="heavy"
          title="运行数据审计"
          confirmLabel="确认排队"
          busy={command.busy}
          description={
            <p>
              将只读核对 {start} 至 {end} 的交易日和日线质量，可能耗时较长；不会执行回补或写入日线。
            </p>
          }
          onCancel={() => setConfirmOpen(false)}
          onConfirm={() => {
            if (disabledReason || !evidence) return;
            setConfirmOpen(false);
            void commandSession.start(start, end, evidence);
          }}
        />
      </section>

      {requestLabel ? (
        <div className="dc-report-command" role="status">
          <StatusBadge
            state={
              command.journal?.status === "failed" ||
              (sameTask && recent?.latest_status === "failed")
                ? "crit"
                : requestLabel === "本次报告已发布"
                  ? "ok"
                  : "waiting"
            }
            label={requestLabel}
            reason="受理、任务完成与报告发布会分别核对"
          />
          {requestUncertain ? (
            <Button size="sm" onClick={() => void commandSession.advance()} disabled={command.busy}>
              {command.busy ? "正在核对" : "继续核对"}
            </Button>
          ) : null}
          {sameTask && recent?.latest_hint ? <span>{recent.latest_hint}</span> : null}
          {command.journal?.status === "queued" && !sameTask ? <span>任务进度尚未更新</span> : null}
        </div>
      ) : null}

      <section className="dc-report-progress" aria-label="最近审计任务">
        <div className="dc-report-progress-head">
          <h3>最近任务</h3>
          <Button size="sm" variant="ghost" onClick={onRefresh}>
            刷新进度
          </Button>
        </div>
        {recent?.latest_task_id ? (
          <>
            <div className="dc-report-progress-summary">
              <StatusBadge
                state={taskTone(recent.latest_status)}
                label={taskLabel(recent.latest_status)}
                reason={recent.latest_hint ?? "来自最近发布的任务记录"}
              />
              <span>
                <RelativeTime at={recent.latest_updated_at ?? recent.latest_created_at} />
              </span>
              {recent.latest_attempts && recent.latest_attempts > 1 ? (
                <span>已尝试 {recent.latest_attempts} 次</span>
              ) : null}
            </div>
            {recent.latest_hint ? (
              <p className="dc-report-progress-hint">{recent.latest_hint}</p>
            ) : null}
            {events.length ? (
              <ol className="dc-report-events" aria-label="最近任务记录">
                {events.map((event) => (
                  <li key={`${event.occurred_at}-${event.event_type}-${event.label}`}>
                    <span>{event.label}</span>
                    <RelativeTime at={event.occurred_at} />
                  </li>
                ))}
              </ol>
            ) : (
              <p className="dc-report-progress-hint">暂无近期记录</p>
            )}
          </>
        ) : (
          <p className="dc-report-progress-hint">
            {progress?.availability === "empty"
              ? "还没有审计任务，选择日期后运行。"
              : "近期任务暂不可查看，稍后刷新进度。"}
          </p>
        )}
      </section>
    </div>
  );
}
