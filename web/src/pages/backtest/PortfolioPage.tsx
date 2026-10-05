import { useCallback, useEffect, useRef, useState } from "react";
import {
  type PortfolioConfig,
  type PortfolioCreateRequest,
  type PortfolioExportRequest,
  type PortfolioJob,
  type PortfolioReceipt,
  type PortfolioSummary,
  type PortfolioView,
  submitPortfolioExport,
  submitPortfolioRun,
  usePortfolioCapabilities,
  usePortfolioJobs,
  usePortfolioNav,
  usePortfolioRows,
  usePortfolioSummary,
} from "@/api/backtests";
import { ApiError, apiBaseUrl } from "@/api/client";
import { type LabControlRequest, submitLabControl } from "@/api/endpoints";
import { useCurrentMeta } from "@/api/useMeta";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ChangeText,
  ConfirmDialog,
  EmptyState,
  KpiStrip,
  PageHeader,
  PageSkeleton,
  Panel,
  RelativeTime,
  SideDrawer,
  Tabs,
  Tip,
} from "@/ui";
import { PortfolioCharts, type PortfolioRange } from "./PortfolioCharts";
import { PortfolioConfiguration } from "./PortfolioConfig";
import { PortfolioMetrics } from "./PortfolioMetrics";
import {
  type PortfolioDetail,
  PortfolioDetailDrawer,
  PortfolioTable,
  portfolioViews,
} from "./PortfolioTables";
import { portfolioPercent, portfolioRatio } from "./portfolioFormat";
import "./portfolio.css";

const jobColumns: DataColumn<PortfolioJob>[] = [
  {
    id: "range",
    header: "回测区间",
    value: (job) => job.start_date,
    cell: (job) => (
      <span className="pb-job-range">
        {job.start_date}
        <small>至 {job.end_date}</small>
      </span>
    ),
  },
  {
    id: "status",
    header: "状态",
    value: (job) => job.label,
    cell: (job) => (
      <span>
        {job.label}
        {job.status === "running" && job.progress !== null ? (
          <small className="num"> {Math.round(job.progress * 100)}%</small>
        ) : null}
      </span>
    ),
  },
];
const pendingStatus = (status: PortfolioReceipt["status"]) =>
  ["pending", "processing", "unknown"].includes(status);
const activeStatus = (job: PortfolioJob | undefined) =>
  job !== undefined && ["queued", "running", "sealing"].includes(job.status);

export default function PortfolioPage() {
  const meta = useCurrentMeta();
  const viewer = meta.data?.data.viewer;
  const [sessionOwner, setSessionOwner] = useState<{ viewer: string | null } | null>(null);
  const viewerChanged =
    sessionOwner !== null && viewer !== undefined && sessionOwner.viewer !== viewer;
  const capabilities = usePortfolioCapabilities();
  const [cursor, setCursor] = useState<string | null>(null);
  const [previousCursors, setPreviousCursors] = useState<(string | null)[]>([]);
  const [refresh, setRefresh] = useState(0);
  const jobs = usePortfolioJobs(cursor, refresh);
  const [jobId, setJobId] = useState<string | null>(null);
  const [poll, setPoll] = useState(true);
  const selection = useRef({ revision: 0, jobId: null as string | null });
  const summary = usePortfolioSummary(jobId, poll);
  const current = summary.data?.job.job_id === jobId ? summary.data : undefined;
  const verifiedResult =
    !viewerChanged &&
    current?.available &&
    current.result_hash !== null &&
    current.result_hash === current.job.result_hash &&
    current.result_hash === summary.serving?.generation_id
      ? current
      : undefined;
  const successfulResult =
    verifiedResult?.job.status === "completed" && verifiedResult.result_status === "complete"
      ? verifiedResult
      : undefined;
  const [lastSuccessful, setLastSuccessful] = useState<{
    value: PortfolioSummary;
    generationId: string;
    viewer: string;
  } | null>(null);
  const previousResult =
    !viewerChanged && lastSuccessful !== null && lastSuccessful.viewer === viewer
      ? lastSuccessful.value
      : undefined;
  const displayedResult = verifiedResult ?? previousResult;
  const showingPrevious =
    displayedResult !== undefined &&
    displayedResult === previousResult &&
    successfulResult === undefined;
  const resultJobId = displayedResult?.job.job_id ?? null;
  const resultHash = displayedResult?.result_hash ?? null;
  const previousDisplayed = useRef<{ jobId: string | null; hash: string | null } | null>(null);
  const nav = usePortfolioNav(resultJobId, resultHash);
  const [view, setView] = useState<PortfolioView>("trades");
  const [offset, setOffset] = useState(0);
  const rows = usePortfolioRows(resultJobId, resultHash, view, offset);
  const actualRows =
    rows.data?.view === view &&
    rows.data.result_hash === resultHash &&
    rows.serving?.generation_id === resultHash
      ? rows.data
      : undefined;
  const [range, setRange] = useState<PortfolioRange>("all");
  const [draft, setDraft] = useState<PortfolioConfig | null>(null);
  const [configOpen, setConfigOpen] = useState(false);
  const [metricsOpen, setMetricsOpen] = useState(false);
  const [detail, setDetail] = useState<PortfolioDetail | null>(null);
  const [busyRun, setBusyRun] = useState(false);
  const [runMessage, setRunMessage] = useState<string | null>(null);
  const [pendingRun, setPendingRun] = useState<{
    body: PortfolioCreateRequest;
    revision: number;
  } | null>(null);
  const [exportRequest, setExportRequest] = useState<PortfolioExportRequest | null>(null);
  const [exportReceipt, setExportReceipt] = useState<PortfolioReceipt | null>(null);
  const [exportMessage, setExportMessage] = useState<string | null>(null);
  const [busyExport, setBusyExport] = useState(false);
  const [controlRequest, setControlRequest] = useState<LabControlRequest | null>(null);
  const [controlMessage, setControlMessage] = useState<string | null>(null);
  const [busyControl, setBusyControl] = useState(false);
  const [cancelOpen, setCancelOpen] = useState(false);

  const selectJob = useCallback((id: string) => {
    selection.current = { revision: selection.current.revision + 1, jobId: id };
    setJobId(id);
    setPoll(true);
    setOffset(0);
    setDetail(null);
    setMetricsOpen(false);
  }, []);
  useEffect(() => {
    if (sessionOwner === null && viewer !== undefined) setSessionOwner({ viewer });
    if (viewerChanged) setLastSuccessful(null);
  }, [sessionOwner, viewer, viewerChanged]);
  useEffect(() => {
    const generationId = successfulResult?.result_hash;
    if (successfulResult === undefined || generationId == null || typeof viewer !== "string")
      return;
    setLastSuccessful((previous) =>
      previous?.viewer === viewer &&
      previous.value.job.job_id === successfulResult.job.job_id &&
      previous.generationId === generationId
        ? previous
        : { value: successfulResult, generationId, viewer },
    );
  }, [successfulResult, viewer]);
  useEffect(() => {
    if (
      previousDisplayed.current?.jobId === resultJobId &&
      previousDisplayed.current.hash === resultHash
    )
      return;
    previousDisplayed.current = { jobId: resultJobId, hash: resultHash };
    setOffset(0);
    setDetail(null);
    setMetricsOpen(false);
  }, [resultJobId, resultHash]);
  useEffect(() => {
    if (!viewerChanged && jobId === null && pendingRun === null && jobs.data?.jobs[0])
      selectJob(jobs.data.jobs[0].job_id);
  }, [jobId, jobs.data, pendingRun, selectJob, viewerChanged]);
  useEffect(() => {
    if (current?.job) setPoll(activeStatus(current.job));
  }, [current?.job]);

  function configure() {
    setRunMessage(null);
    if (pendingRun === null) setDraft(current?.config ?? capabilities.data?.default_config ?? null);
    setConfigOpen(true);
  }

  async function run(retry = false) {
    const original = retry
      ? pendingRun
      : draft === null
        ? null
        : {
            body: {
              command_id: crypto.randomUUID(),
              requested_at: new Date().toISOString(),
              config: draft,
            },
            revision: selection.current.revision,
          };
    if (original === null || busyRun) return;
    setPendingRun(original);
    setBusyRun(true);
    setRunMessage(null);
    try {
      const receipt = await submitPortfolioRun(original.body);
      setRunMessage(receipt.message);
      if (!pendingStatus(receipt.status)) setPendingRun(null);
      if (receipt.status === "submitted" && receipt.job_id) {
        setRefresh((value) => value + 1);
        setConfigOpen(false);
        if (selection.current.revision === original.revision) selectJob(receipt.job_id);
      }
    } catch (error) {
      setRunMessage(error instanceof Error ? error.message : "提交状态待确认，请重试原请求。");
      if (error instanceof ApiError && [401, 403, 413, 422].includes(error.status))
        setPendingRun(null);
    } finally {
      setBusyRun(false);
    }
  }

  async function exportZip() {
    if (resultJobId === null || resultHash === null || busyExport) return;
    const body = exportRequest ?? {
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      job_id: resultJobId,
      result_hash: resultHash,
    };
    setExportRequest(body);
    setBusyExport(true);
    setExportMessage(null);
    try {
      const receipt = await submitPortfolioExport(body);
      setExportMessage(receipt.message);
      if (receipt.status === "exported") {
        setExportReceipt(receipt);
        setExportRequest(null);
      } else if (!pendingStatus(receipt.status)) setExportRequest(null);
    } catch (error) {
      setExportMessage(error instanceof Error ? error.message : "导出状态待确认，请重试原请求。");
    } finally {
      setBusyExport(false);
    }
  }

  async function control(action: LabControlRequest["action"] | null) {
    if (busyControl || (action !== null && current === undefined)) return;
    const body =
      action === null
        ? controlRequest
        : current === undefined
          ? null
          : {
              action,
              command_id: crypto.randomUUID(),
              requested_at: new Date().toISOString(),
              job_id: current.job.job_id,
              expected_version: current.job.version,
            };
    if (body === null) return;
    setControlRequest(body);
    setBusyControl(true);
    setControlMessage(null);
    setCancelOpen(false);
    try {
      const receipt = await submitLabControl(body);
      setControlMessage(receipt.message);
      if (["submitted", "failed", "conflict"].includes(receipt.status)) setControlRequest(null);
      if (selection.current.jobId === body.job_id) {
        summary.refetch();
        setPoll(true);
      }
      setRefresh((value) => value + 1);
    } catch (error) {
      setControlMessage(error instanceof Error ? error.message : "操作状态待确认，请重试原请求。");
    } finally {
      setBusyControl(false);
    }
  }

  const perf = displayedResult?.performance;
  const htmlUrl =
    resultJobId === null || resultHash === null
      ? null
      : `${apiBaseUrl()}/api/v1/backtests/portfolio/runs/${encodeURIComponent(resultJobId)}/report.html?result_hash=${resultHash}`;
  const zipUrl =
    exportReceipt?.status === "exported" &&
    exportReceipt.job_id === resultJobId &&
    exportReceipt.result_hash === resultHash &&
    exportReceipt.zip_request_id
      ? `${apiBaseUrl()}/api/v1/backtests/portfolio/runs/${encodeURIComponent(resultJobId ?? "")}/exports/${exportReceipt.zip_request_id}.zip?result_hash=${resultHash}`
      : null;
  const source = capabilities.data?.sources.find(
    (item) =>
      item.key === current?.config?.source_key && item.version === current.config.source_version,
  );

  if (viewerChanged)
    return (
      <div className="pb-page">
        <PageHeader eyebrow="RESEARCH / BACKTEST" title="回测" />
        <Panel>
          <EmptyState
            title="访问身份已变，请刷新页面。"
            hint={<Button onClick={() => window.location.reload()}>刷新页面</Button>}
          />
        </Panel>
      </div>
    );

  return (
    <div className="pb-page">
      <PageHeader
        eyebrow="RESEARCH / BACKTEST"
        title="回测"
        note="组合净值 · 交易成本 · 基准比较"
        actions={
          <>
            <Button
              onClick={() => {
                setRefresh((value) => value + 1);
                summary.refetch();
                capabilities.refetch();
              }}
            >
              刷新
            </Button>
            <Button
              variant="primary"
              onClick={configure}
              disabledReason={
                capabilities.data?.can_run
                  ? undefined
                  : (capabilities.error?.message ??
                    capabilities.data?.message ??
                    "正在读取回测来源。")
              }
            >
              新建回测
            </Button>
          </>
        }
      />
      {capabilities.data?.message ? <p className="pb-notice">{capabilities.data.message}</p> : null}
      {capabilities.error ? (
        <p role="alert" className="pb-notice">
          {capabilities.error.message}
        </p>
      ) : null}
      {runMessage && !configOpen ? (
        <div className="pb-command" role="status">
          <span>{runMessage}</span>
          {pendingRun ? (
            <Button size="sm" disabled={busyRun} onClick={() => void run(true)}>
              重试原请求
            </Button>
          ) : null}
        </div>
      ) : null}
      <div className="pb-workspace">
        <Panel
          title="回测记录"
          flush
          actions={
            <Button
              size="sm"
              onClick={() => {
                setDraft(capabilities.data?.default_config ?? null);
                configure();
              }}
              disabled={!capabilities.data?.can_run}
            >
              新建
            </Button>
          }
        >
          {jobs.isLoading ? (
            <PageSkeleton />
          ) : jobs.error ? (
            <EmptyState
              title={jobs.error.message}
              hint={<Button onClick={jobs.refetch}>重新加载</Button>}
            />
          ) : jobs.data?.jobs.length ? (
            <DataTable
              label="回测记录"
              rows={jobs.data.jobs}
              columns={jobColumns}
              rowKey={(job) => job.job_id}
              selectedKey={jobId ?? undefined}
              onSelect={(job) => selectJob(job.job_id)}
            />
          ) : (
            <EmptyState title="还没有组合回测" hint="准备好候选与行情后，新建第一份回测。" />
          )}
          {previousCursors.length > 0 || jobs.data?.next_cursor ? (
            <div className="pb-pagination">
              <Button
                size="sm"
                disabled={previousCursors.length === 0}
                onClick={() => {
                  setCursor(previousCursors.at(-1) ?? null);
                  setPreviousCursors((value) => value.slice(0, -1));
                }}
              >
                上一页
              </Button>
              <Button
                size="sm"
                disabled={!jobs.data?.next_cursor}
                onClick={() => {
                  setPreviousCursors((value) => [...value, cursor]);
                  setCursor(jobs.data?.next_cursor ?? null);
                }}
              >
                下一页
              </Button>
            </div>
          ) : null}
        </Panel>
        <div className="pb-result">
          {jobId === null ? (
            <Panel>
              <EmptyState title="选择回测记录" hint="在这里查看净值、成交、持仓和报告。" />
            </Panel>
          ) : summary.isLoading ? (
            <PageSkeleton />
          ) : summary.error ? (
            <Panel>
              <EmptyState
                title={summary.error.message}
                hint={<Button onClick={summary.refetch}>重新加载</Button>}
              />
            </Panel>
          ) : current ? (
            <Panel
              title={`${current.job.start_date} — ${current.job.end_date}`}
              sub={current.job.label}
              actions={
                <>
                  <Tip content="候选与历史参考价按已确认的回顾假设使用；不声称候选在当时已经发布，也不把未知开盘条件当成可成交。">
                    <span className="pb-assumption">回顾假设</span>
                  </Tip>
                  <Button size="sm" onClick={configure}>
                    查看配置
                  </Button>
                  {current.job.can_pause ? (
                    <Button
                      size="sm"
                      disabled={busyControl || controlRequest !== null}
                      onClick={() => void control("pause")}
                    >
                      暂停
                    </Button>
                  ) : null}
                  {current.job.can_resume ? (
                    <Button
                      size="sm"
                      disabled={busyControl || controlRequest !== null}
                      onClick={() => void control("resume")}
                    >
                      继续
                    </Button>
                  ) : null}
                  {current.job.can_retry ? (
                    <Button
                      size="sm"
                      disabled={busyControl || controlRequest !== null}
                      onClick={() => void control("retry")}
                    >
                      重跑失败任务
                    </Button>
                  ) : null}
                  {current.job.can_cancel ? (
                    <Button
                      size="sm"
                      disabled={busyControl || controlRequest !== null}
                      onClick={() => setCancelOpen(true)}
                    >
                      取消
                    </Button>
                  ) : null}
                </>
              }
            >
              <div className="pb-context">
                <span>{source?.label ?? "来源待确认"}</span>
                <span>
                  已完成 <b className="num">{current.completed_days}</b> 个交易日
                </span>
                {current.source_updated_at ? (
                  <span>
                    数据 <RelativeTime at={current.source_updated_at} />
                  </span>
                ) : null}
              </div>
              {current.job.progress !== null && activeStatus(current.job) ? (
                <progress aria-label="回测进度" value={current.job.progress} max={1} />
              ) : null}
              {current.message ? <p className="pb-notice">{current.message}</p> : null}
              {controlMessage ? (
                <div className="pb-command" role="status">
                  <span>{controlMessage}</span>
                  {controlRequest ? (
                    <Button size="sm" disabled={busyControl} onClick={() => void control(null)}>
                      重试原操作
                    </Button>
                  ) : null}
                </div>
              ) : null}
            </Panel>
          ) : null}
          {showingPrevious && displayedResult ? (
            <section className="pb-context" aria-label="上一份结果区间">
              <strong>上一份结果</strong>
              <span>
                {displayedResult.job.start_date} — {displayedResult.job.end_date}
              </span>
            </section>
          ) : null}
          {displayedResult ? (
            <>
              {perf ? (
                <>
                  <div className="pb-kpis">
                    <KpiStrip
                      label="组合绩效"
                      compact
                      items={[
                        {
                          key: "return",
                          label: "累计收益",
                          value: (
                            <ChangeText
                              value={
                                perf.summary.total_return === null
                                  ? null
                                  : perf.summary.total_return * 100
                              }
                            />
                          ),
                        },
                        {
                          key: "annual",
                          label: "年化收益",
                          value: (
                            <ChangeText
                              value={
                                perf.summary.annualized_return === null
                                  ? null
                                  : perf.summary.annualized_return * 100
                              }
                            />
                          ),
                          tip: "按完整净值序列计算；很短的样本会放大年化值。",
                        },
                        {
                          key: "excess",
                          label: "超额收益",
                          value: (
                            <ChangeText
                              value={
                                perf.relative?.excess_total_return == null
                                  ? null
                                  : perf.relative.excess_total_return * 100
                              }
                            />
                          ),
                        },
                        {
                          key: "dd",
                          label: "最大回撤",
                          value: portfolioPercent(perf.summary.max_drawdown),
                        },
                        {
                          key: "sharpe",
                          label: "夏普比率",
                          value: portfolioRatio(perf.summary.sharpe),
                        },
                        {
                          key: "win",
                          label: "交易胜率",
                          value: portfolioPercent(perf.round_trip_analysis.overall.win_rate),
                          tip: "只统计完成买入与卖出的交易。",
                        },
                      ]}
                    />
                  </div>
                  <div className="pb-report-actions">
                    <Button size="sm" onClick={() => setMetricsOpen(true)}>
                      绩效详情
                    </Button>
                    {displayedResult.can_report && htmlUrl ? (
                      <a className="btn sm" href={htmlUrl} download>
                        HTML 报告
                      </a>
                    ) : null}
                    <Button
                      size="sm"
                      disabled={busyExport}
                      disabledReason={
                        !displayedResult.can_report
                          ? "完整结果生成后才能导出报告。"
                          : !capabilities.data?.can_export
                            ? "报告导出暂不可用。"
                            : undefined
                      }
                      onClick={() => void exportZip()}
                    >
                      {exportRequest ? "重试原导出" : busyExport ? "正在准备报告" : "导出 ZIP"}
                    </Button>
                    {zipUrl ? (
                      <a className="btn sm primary" href={zipUrl} download>
                        下载 ZIP
                      </a>
                    ) : null}
                  </div>
                </>
              ) : null}
              {exportMessage ? (
                <p className="pb-notice" role="status">
                  {exportMessage}
                </p>
              ) : null}
              {displayedResult.benchmark_message && displayedResult.available ? (
                <p className="pb-notice">{displayedResult.benchmark_message}</p>
              ) : null}
              {resultHash !== null ? (
                <>
                  {nav.error ? (
                    <Panel>
                      <EmptyState
                        title={nav.error.message}
                        hint={<Button onClick={nav.refetch}>重新加载净值</Button>}
                      />
                    </Panel>
                  ) : nav.isLoading ? (
                    <PageSkeleton />
                  ) : (
                    <PortfolioCharts
                      rows={
                        nav.data?.result_hash === resultHash &&
                        nav.serving?.generation_id === resultHash
                          ? nav.data.rows
                          : []
                      }
                      range={range}
                      onRange={setRange}
                    />
                  )}
                  <Panel flush>
                    <Tabs
                      activeKey={view}
                      onChange={(key) => {
                        const match = portfolioViews.find((item) => item.key === key);
                        if (match) {
                          setView(match.key);
                          setOffset(0);
                          setDetail(null);
                        }
                      }}
                      items={portfolioViews.map((item) => ({ key: item.key, label: item.label }))}
                    />
                    {rows.isLoading ? (
                      <PageSkeleton />
                    ) : rows.error ? (
                      <EmptyState
                        title={rows.error.message}
                        hint={<Button onClick={rows.refetch}>重新加载明细</Button>}
                      />
                    ) : actualRows ? (
                      <>
                        <PortfolioTable rows={actualRows} onDetail={setDetail} />
                        <div className="pb-pagination">
                          <span className="hint">
                            共 <b className="num">{actualRows.total.toLocaleString("zh-CN")}</b> 条
                          </span>
                          <Button
                            size="sm"
                            disabled={offset === 0}
                            onClick={() => {
                              setOffset((value) => Math.max(0, value - 50));
                              setDetail(null);
                            }}
                          >
                            上一页
                          </Button>
                          <Button
                            size="sm"
                            disabled={actualRows.next_offset === null}
                            onClick={() => {
                              setOffset(actualRows.next_offset ?? 0);
                              setDetail(null);
                            }}
                          >
                            下一页
                          </Button>
                        </div>
                      </>
                    ) : null}
                  </Panel>
                </>
              ) : null}
            </>
          ) : null}
        </div>
      </div>
      <SideDrawer
        wide
        open={configOpen}
        onClose={() => setConfigOpen(false)}
        title="回测配置"
        footer={
          <div className="pb-config-footer">
            {runMessage ? <p role="status">{runMessage}</p> : null}
            <Button onClick={() => setConfigOpen(false)}>关闭</Button>
            <Button
              variant="primary"
              type={pendingRun ? "button" : "submit"}
              form="portfolio-config-form"
              disabled={busyRun}
              disabledReason={
                !capabilities.data?.can_run
                  ? (capabilities.data?.message ?? "当前不能提交回测。")
                  : undefined
              }
              onClick={pendingRun ? () => void run(true) : undefined}
            >
              {pendingRun ? "重试原请求" : busyRun ? "正在提交" : "运行回测"}
            </Button>
          </div>
        }
      >
        {draft ? (
          <form
            id="portfolio-config-form"
            onSubmit={(event) => {
              event.preventDefault();
              void run();
            }}
          >
            <PortfolioConfiguration
              value={draft}
              sources={capabilities.data?.sources ?? []}
              locked={busyRun || pendingRun !== null}
              onChange={setDraft}
            />
          </form>
        ) : (
          <EmptyState title="暂无可用配置" hint="完整来源准备好后，可在这里设置回测。" />
        )}
      </SideDrawer>
      <PortfolioMetrics
        performance={perf ?? null}
        open={metricsOpen}
        onClose={() => setMetricsOpen(false)}
      />
      <PortfolioDetailDrawer detail={detail} onClose={() => setDetail(null)} />
      <ConfirmDialog
        open={cancelOpen}
        level="heavy"
        title="取消这次回测？"
        description="停止后续计算。已封存的历史结果会保留。"
        confirmLabel="取消回测"
        busy={busyControl}
        onCancel={() => setCancelOpen(false)}
        onConfirm={() => void control("cancel")}
      />
    </div>
  );
}
