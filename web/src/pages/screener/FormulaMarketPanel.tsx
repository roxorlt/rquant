import { useEffect, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type FormulaMarketCommandRequest,
  type FormulaMarketJobItem,
  submitFormulaMarketCommand,
  useFormulaMarketJob,
  useFormulaMarketJobs,
  useFormulaMarketMatches,
} from "@/api/formulaMarket";
import { useCurrentGeneration, useCurrentMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import { formatCount } from "@/format/number";
import { formatAge, formatShanghaiDateTime, formatShanghaiTime, shanghaiDate } from "@/format/time";
import { Button, ConfirmDialog, EmptyState, RelativeTime, Tip, useNow } from "@/ui";
import { FormulaPoolSave, readFormulaPoolSaveTaskId } from "./FormulaPoolSave";

const JOURNAL_KEY = "rquant-formula-market-command-v1";

type Journal = {
  request: FormulaMarketCommandRequest;
  state: "pending" | "queued";
  taskId: string | null;
};
type RunDraft = { formula: string; tradeDate: string };
type PageState = { key: string; index: number; cursors: (string | null)[] };

function readJournal(): Journal | null {
  try {
    const saved = window.sessionStorage.getItem(JOURNAL_KEY);
    if (!saved) return null;
    const value: unknown = JSON.parse(saved);
    if (typeof value !== "object" || value === null || !("request" in value)) return null;
    const request = value.request;
    if (typeof request !== "object" || request === null) return null;
    if (
      !("command_id" in request) ||
      typeof request.command_id !== "string" ||
      !("requested_at" in request) ||
      typeof request.requested_at !== "string" ||
      !("formula" in request) ||
      typeof request.formula !== "string" ||
      !("trade_date" in request) ||
      typeof request.trade_date !== "string" ||
      !("state" in value) ||
      (value.state !== "pending" && value.state !== "queued") ||
      !("taskId" in value) ||
      (value.taskId !== null && typeof value.taskId !== "string")
    ) {
      return null;
    }
    return {
      request: {
        command_id: request.command_id,
        requested_at: request.requested_at,
        formula: request.formula,
        trade_date: request.trade_date,
      },
      state: value.state,
      taskId: value.taskId,
    };
  } catch {
    return null;
  }
}

function saveJournal(value: Journal | null): void {
  try {
    if (value === null) window.sessionStorage.removeItem(JOURNAL_KEY);
    else window.sessionStorage.setItem(JOURNAL_KEY, JSON.stringify(value));
  } catch {
    // The current drawer still retains the request if browser storage is disabled.
  }
}

function latestClosedDate(): string {
  const now = new Date();
  const day = shanghaiDate(now).date;
  if (formatShanghaiTime(now) >= "15:00") return day;
  const previous = new Date(`${day}T00:00:00Z`);
  previous.setUTCDate(previous.getUTCDate() - 1);
  return previous.toISOString().slice(0, 10);
}

function jobTone(job: FormulaMarketJobItem): string {
  if (job.status === "failed" || (job.status === "succeeded" && !job.result_available)) {
    return "warn";
  }
  return job.status === "succeeded" ? "ok" : "active";
}

export function FormulaMarketPanel({ formula, checked }: { formula: string; checked: boolean }) {
  const [tradeDate, setTradeDate] = useState("");
  const [journal, setJournal] = useState<Journal | null>(readJournal);
  const [selectedId, setSelectedId] = useState<string | null>(() => readJournal()?.taskId ?? null);
  const [confirmDraft, setConfirmDraft] = useState<RunDraft | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [pollJobs, setPollJobs] = useState(journal !== null);
  const [page, setPage] = useState<PageState>({ key: "", index: 0, cursors: [null] });
  const [readEpoch, setReadEpoch] = useState(0);
  const [stockCode, setStockCode] = useState<string | null>(null);
  const now = useNow();
  const generation = useCurrentGeneration();
  const viewer = useCurrentMeta().data?.data.viewer ?? null;
  const jobs = useFormulaMarketJobs(pollJobs);
  const jobList =
    generation === undefined || jobs.serving?.generation_id === generation ? jobs.data : undefined;
  const selectedListJob = jobList?.jobs.find((job) => job.task_id === selectedId);
  const unreadableTaskId =
    selectedListJob?.status === "succeeded" && !selectedListJob.result_available
      ? selectedListJob.task_id
      : null;
  const resultUnavailable = unreadableTaskId !== null;
  const detailTaskId = resultUnavailable ? null : (selectedListJob?.task_id ?? null);
  const detail = useFormulaMarketJob(
    detailTaskId,
    selectedListJob?.updated_at ?? null,
    readEpoch,
    selectedListJob?.status === "queued" || selectedListJob?.status === "running",
  );
  const currentDetail =
    detailTaskId !== null &&
    detail.data?.job.task_id === selectedId &&
    (generation === undefined || detail.serving?.generation_id === generation)
      ? detail.data
      : null;
  const job = currentDetail?.job ?? null;
  const pageKey = JSON.stringify([selectedId, formula, tradeDate, generation]);
  const currentPage = page.key === pageKey ? page : { key: pageKey, index: 0, cursors: [null] };
  const cursor = currentPage.cursors[currentPage.index] ?? null;
  const canReadMatches =
    !resultUnavailable &&
    job?.status === "succeeded" &&
    job.result_available &&
    currentDetail?.summary !== null;
  const matches = useFormulaMarketMatches(
    selectedId,
    cursor,
    generation,
    readEpoch,
    canReadMatches === true,
  );
  const resultReady =
    canReadMatches &&
    !matches.error &&
    matches.data?.task_id === selectedId &&
    matches.serving?.generation_id === detail.serving?.generation_id;
  const activeJob = jobList?.jobs.some(
    (item) => item.status === "queued" || item.status === "running",
  );
  const busy = submitting || journal !== null || activeJob === true;
  const maxDate = latestClosedDate();
  const validDate = tradeDate !== "" && tradeDate <= maxDate;

  useEffect(() => {
    setPollJobs(journal !== null || activeJob === true);
  }, [journal, activeJob]);

  useEffect(() => {
    if (selectedId !== null) return;
    const savedTask = readFormulaPoolSaveTaskId(viewer);
    if (savedTask !== null) setSelectedId(savedTask);
  }, [selectedId, viewer]);

  useEffect(() => {
    if (journal?.state !== "queued" || journal.taskId === null) return;
    if (!jobList?.jobs.some((item) => item.task_id === journal.taskId)) return;
    setJournal(null);
    saveJournal(null);
  }, [journal, jobList]);

  useEffect(() => {
    if (unreadableTaskId !== null) setReadEpoch((value) => value + 1);
  }, [unreadableTaskId]);

  function remember(value: Journal | null): void {
    setJournal(value);
    saveJournal(value);
  }

  async function submit(request: FormulaMarketCommandRequest): Promise<void> {
    setSubmitting(true);
    setNotice(null);
    remember({ request, state: "pending", taskId: null });
    try {
      const receipt = await submitFormulaMarketCommand(request);
      if (receipt.status === "queued" && receipt.task_id) {
        remember({ request, state: "queued", taskId: receipt.task_id });
        setSelectedId(receipt.task_id);
        jobs.refetch();
        return;
      }
      if (
        receipt.status === "pending" ||
        receipt.status === "processing" ||
        receipt.status === "ambiguous"
      ) {
        setNotice("状态待确认，请重试原请求。");
        return;
      }
      remember(null);
      setNotice(receipt.message);
      jobs.refetch();
    } catch (caught) {
      if (
        caught instanceof ApiError &&
        caught.status >= 400 &&
        caught.status < 500 &&
        caught.status !== 408
      ) {
        remember(null);
        setNotice(caught.message);
      } else {
        setNotice("提交状态待确认，请重试原请求。");
      }
    } finally {
      setSubmitting(false);
    }
  }

  function confirmRun(): void {
    if (confirmDraft === null || submitting) return;
    const request: FormulaMarketCommandRequest = {
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      formula: confirmDraft.formula,
      trade_date: confirmDraft.tradeDate,
    };
    setConfirmDraft(null);
    void submit(request);
  }

  function selectJob(taskId: string): void {
    setSelectedId(taskId);
    setPage({ key: "", index: 0, cursors: [null] });
  }

  function restartMatches(): void {
    setReadEpoch((value) => value + 1);
    setPage({ key: pageKey, index: 0, cursors: [null] });
    jobs.refetch();
  }

  return (
    <section className="formula-market" aria-label="全市场公式选股">
      <div className="formula-market-heading">
        <div>
          <span className="formula-market-eyebrow">全市场</span>
          <h3>批量运行</h3>
        </div>
        <Tip content="按所选日期已归档的全部 A 股计算。日期是否可用由提交时核对。">
          <button type="button" className="formula-market-help" aria-label="批量运行说明">
            ?
          </button>
        </Tip>
      </div>
      <div className="formula-market-compose">
        <label className="field">
          <span className="lbl">运行日期</span>
          <input
            className="inp num"
            type="date"
            value={tradeDate}
            max={maxDate}
            onChange={(event) => setTradeDate(event.target.value)}
          />
        </label>
        <Button
          variant="primary"
          disabledReason={
            !checked
              ? "先检查当前公式"
              : !validDate
                ? "选择已收盘日期"
                : busy
                  ? "当前有任务或请求待确认"
                  : undefined
          }
          onClick={() => setConfirmDraft({ formula, tradeDate })}
        >
          运行全市场
        </Button>
      </div>
      {journal ? (
        <div className="formula-market-submission" role="status" aria-live="polite">
          <strong className="formula-market-submission-title">
            {journal.state === "queued" ? "已提交，等待任务出现" : "提交状态待确认"}
          </strong>
          <span className="formula-market-submission-date num">{journal.request.trade_date}</span>
          <span className="formula-market-submission-formula mono">{journal.request.formula}</span>
          {journal.state === "pending" ? (
            <Button size="sm" onClick={() => void submit(journal.request)} disabled={submitting}>
              {submitting ? "正在核对…" : "重试原请求"}
            </Button>
          ) : (
            <Button size="sm" onClick={() => jobs.refetch()} disabled={jobs.isFetching}>
              刷新任务
            </Button>
          )}
        </div>
      ) : notice ? (
        <p className="formula-market-notice" role="status">
          {notice}
        </p>
      ) : null}

      <div className="formula-market-recent-heading">
        <h3>最近运行</h3>
        <Button size="sm" variant="ghost" onClick={() => jobs.refetch()} disabled={jobs.isFetching}>
          刷新
        </Button>
      </div>
      {jobs.error ? (
        <EmptyState title="最近运行暂不可用" hint="稍后刷新再试。" />
      ) : jobs.isLoading || jobList === undefined ? (
        <p className="hint" role="status">
          正在读取最近运行…
        </p>
      ) : jobList.availability !== "ready" ? (
        <EmptyState
          title={jobList.message}
          hint={jobList.availability === "empty" ? "运行一次后会显示在这里。" : "稍后刷新再试。"}
        />
      ) : (
        <>
          <section className="formula-market-job-list" aria-label="最近运行记录">
            {jobList.jobs.map((item) => (
              <div
                key={item.task_id}
                className={`formula-market-job ${selectedId === item.task_id ? "selected" : ""}`}
              >
                <button
                  type="button"
                  className="formula-market-job-select"
                  onClick={() => selectJob(item.task_id)}
                  aria-pressed={selectedId === item.task_id}
                >
                  <span className={`formula-market-job-state ${jobTone(item)}`}>
                    {item.status_label}
                  </span>
                  <span className="formula-market-job-date num">{item.trade_date}</span>
                  <span className="formula-market-job-formula mono">{item.formula}</span>
                </button>
                <Tip content={formatShanghaiDateTime(item.updated_at)}>
                  <span className="formula-market-job-time rel-time">
                    {formatAge(Math.max(0, (now - new Date(item.updated_at).getTime()) / 1000))}
                  </span>
                </Tip>
              </div>
            ))}
          </section>
          {jobList.has_older_tasks ? (
            <p className="hint">仅显示最近 {formatCount(jobList.jobs.length)} 次。</p>
          ) : null}
        </>
      )}

      {selectedListJob !== undefined ? (
        <section className="formula-market-detail" aria-label="运行详情">
          <div className="formula-market-recent-heading">
            <h3>运行详情</h3>
            <Button
              size="sm"
              variant="ghost"
              onClick={() => (resultUnavailable ? jobs.refetch() : detail.refetch())}
              disabled={resultUnavailable ? jobs.isFetching : detail.isFetching}
            >
              刷新
            </Button>
          </div>
          {resultUnavailable ? (
            <EmptyState title="结果暂时无法读取" hint="稍后刷新再试。" />
          ) : detail.error ? (
            <EmptyState
              title={journal?.taskId === selectedId ? "已提交，等待任务出现" : "任务暂时无法读取"}
              hint="稍后刷新再试。"
            />
          ) : job === null ? (
            <p className="hint" role="status">
              正在读取运行详情…
            </p>
          ) : (
            <>
              <div className="formula-market-detail-head">
                <span className={`formula-market-job-state ${jobTone(job)}`}>
                  {job.status_label}
                </span>
                <span className="num">{job.trade_date}</span>
                <RelativeTime at={job.updated_at} suffix="更新" />
              </div>
              <div className="formula-market-saved-formula">
                <span>
                  {job.formula === formula && job.trade_date === tradeDate
                    ? "本次公式"
                    : "历史公式与日期"}
                </span>
                <code>{job.formula}</code>
              </div>
              {job.status === "failed" ? (
                <div className="formula-market-detail-state warn">
                  <p>{job.hint}</p>
                  <Button
                    size="sm"
                    disabledReason={busy ? "当前有任务或请求待确认" : undefined}
                    onClick={() =>
                      setConfirmDraft({ formula: job.formula, tradeDate: job.trade_date })
                    }
                  >
                    重新运行此公式
                  </Button>
                </div>
              ) : job.status === "queued" || job.status === "running" ? (
                <p className="formula-market-detail-state" role="status">
                  {job.hint}
                </p>
              ) : !job.result_available || currentDetail?.summary == null ? (
                <EmptyState title="结果暂时无法读取" hint="稍后刷新再试。" />
              ) : matches.error ? (
                <div className="formula-market-result-recovery">
                  <EmptyState
                    title={
                      matches.error instanceof ApiError && matches.error.status === 409
                        ? "结果已更新"
                        : "结果暂时无法读取"
                    }
                    hint={
                      matches.error instanceof ApiError && matches.error.status === 409
                        ? "请从第一页重新查看。"
                        : "稍后刷新详情再试。"
                    }
                  />
                  {matches.error instanceof ApiError && matches.error.status === 409 ? (
                    <Button size="sm" onClick={restartMatches}>
                      从第一页重看
                    </Button>
                  ) : null}
                </div>
              ) : !resultReady ? (
                <p className="hint" role="status">
                  正在读取结果…
                </p>
              ) : (
                <>
                  <section className="formula-market-summary" aria-label="市场结果">
                    <div className="formula-market-summary-lead">
                      <span>命中</span>
                      <strong className="num formula-market-summary-number">
                        {formatCount(currentDetail.summary.match_count)}
                      </strong>
                      <span>只</span>
                    </div>
                    <dl>
                      <div>
                        <dt>全部</dt>
                        <dd className="num">{formatCount(currentDetail.summary.market_total)}</dd>
                      </div>
                      <div>
                        <dt>未命中</dt>
                        <dd className="num">{formatCount(currentDetail.summary.no_match_count)}</dd>
                      </div>
                      <div>
                        <dt>未能判断</dt>
                        <dd className="num">{formatCount(currentDetail.summary.unknown_count)}</dd>
                      </div>
                    </dl>
                  </section>
                  {currentDetail.summary.unknown_reasons.length > 0 ? (
                    <div className="formula-market-unknown">
                      <h4>未能判断的原因</h4>
                      <ul>
                        {currentDetail.summary.unknown_reasons.map((reason) => (
                          <li key={reason.reason}>
                            <span>{reason.label}</span>
                            <strong className="num">{formatCount(reason.count)}</strong>
                          </li>
                        ))}
                      </ul>
                    </div>
                  ) : null}
                  {matches.data?.match_codes.length === 0 ? (
                    <EmptyState title="没有命中股票" hint="调整公式或日期后重新运行。" />
                  ) : (
                    <>
                      <div className="formula-market-matches-heading">
                        <h4>命中股票</h4>
                        <span>第 {formatCount(currentPage.index + 1)} 页</span>
                      </div>
                      <ul className="formula-market-matches" aria-label="命中股票">
                        {matches.data?.match_codes.map((code) => (
                          <li key={code}>
                            <Button size="sm" variant="ghost" onClick={() => setStockCode(code)}>
                              <span className="mono">{code}</span>
                              <span aria-hidden="true">↗</span>
                            </Button>
                          </li>
                        ))}
                      </ul>
                      <div className="formula-market-pagination">
                        <Button
                          size="sm"
                          disabled={currentPage.index === 0}
                          onClick={() => setPage({ ...currentPage, index: currentPage.index - 1 })}
                        >
                          上一页
                        </Button>
                        <Button
                          size="sm"
                          disabled={matches.data?.next_cursor === null}
                          onClick={() => {
                            if (!matches.data?.next_cursor) return;
                            setPage({
                              key: pageKey,
                              index: currentPage.index + 1,
                              cursors: [
                                ...currentPage.cursors.slice(0, currentPage.index + 1),
                                matches.data.next_cursor,
                              ],
                            });
                          }}
                        >
                          下一页
                        </Button>
                      </div>
                    </>
                  )}
                </>
              )}
            </>
          )}
        </section>
      ) : null}
      <FormulaPoolSave
        candidate={
          job?.status === "succeeded" &&
          job.result_available &&
          currentDetail?.summary &&
          resultReady === true
            ? {
                taskId: job.task_id,
                formula: job.formula,
                tradeDate: job.trade_date,
                matchCount: currentDetail.summary.match_count,
                unknownCount: currentDetail.summary.unknown_count,
              }
            : null
        }
      />
      <ConfirmDialog
        open={confirmDraft !== null}
        level="heavy"
        title="运行全市场公式选股"
        description={
          <>将用 {confirmDraft?.tradeDate} 的已归档 A 股计算；完成后可在最近运行中查看。</>
        }
        confirmLabel="确认运行"
        busy={submitting}
        onConfirm={confirmRun}
        onCancel={() => setConfirmDraft(null)}
      />
      <StockDrawer tsCode={stockCode} onClose={() => setStockCode(null)} />
    </section>
  );
}
