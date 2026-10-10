import { type FormEvent, useCallback, useEffect, useRef, useState } from "react";
import {
  type BacktestGroup,
  type BacktestGroupKey,
  type BacktestTrade,
  useBacktestDetail,
  useBacktestRuns,
} from "@/api/backtests";
import { ApiError, type Schemas } from "@/api/client";
import {
  type MinuteCreate,
  type MinuteExport,
  type MinuteNav,
  type MinuteParameterSource,
  type MinuteParameters,
  type MinuteReceipt,
  type MinuteRows,
  type MinuteSource,
  type MinuteTable,
  minuteHtmlUrl,
  minuteParameterCreateRecipe,
  minuteZipUrl,
  restoreMinuteExportRequest,
  restoreMinuteRequest,
  submitMinuteExport,
  submitMinuteRun,
  useMinuteCapabilities,
  useMinuteJobs,
  useMinuteNav,
  useMinuteParameterSources,
  useMinuteRows,
  useMinuteSources,
  useMinuteSummary,
} from "@/api/minuteBacktests";
import { useCurrentMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import type { ChartColors } from "@/charts/tokens";
import { toneOf } from "@/format/color";
import { formatCount, formatPercent, formatPrice } from "@/format/number";
import { formatShanghaiDateTime } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ChangeText,
  EmptyState,
  KpiStrip,
  PageHeader,
  PageSkeleton,
  Panel,
  SideDrawer,
  Tip,
} from "@/ui";
import { MinuteParameterControls } from "./MinuteParameterControls";
import { MinuteStudyWorkspace } from "./MinuteStudyWorkspace";
import { PortfolioMetrics } from "./PortfolioMetrics";
import { portfolioPercent, portfolioRatio } from "./portfolioFormat";
import "./backtest.css";
import "./portfolioMonthlyHeatmap.css";

const PAGE_SIZE = 20;
type RuntimeMinuteJob = Schemas["MinuteJobsData"]["jobs"][number];

const minuteStatus: Record<RuntimeMinuteJob["status"], string> = {
  queued: "等待运行",
  running: "正在回放",
  paused: "已暂停",
  cancelled: "已取消",
  failed: "运行失败",
  sealing: "正在保存",
  completed: "已完成",
};

function isParameterJob(job: RuntimeMinuteJob): job is Schemas["MinuteParameterJob"] {
  return "kind" in job && job.kind === "minute_parameter_replay";
}

function minuteJobTitle(job: RuntimeMinuteJob): string {
  return `${job.native_name} · ${isParameterJob(job) ? "参数回测 · " : ""}版本 ${job.native_version}`;
}

const minuteTables: { value: MinuteTable; label: string }[] = [
  { value: "fills", label: "成交与费用" },
  { value: "signals", label: "策略信号" },
  { value: "orders", label: "委托" },
  { value: "paper_queue", label: "执行记录" },
  { value: "account", label: "最终账户" },
  { value: "daily_valuations", label: "逐日估值依据" },
  { value: "execution_profile", label: "执行与费用口径" },
  { value: "replay_summary", label: "完整结果依据" },
];
const signalActions: Record<string, string> = {
  watch: "观察",
  b_intent: "买入意向",
  s_intent: "卖出意向",
  reduce: "减仓",
  cancel: "取消",
};
const sourceChoice = (source: MinuteSource) =>
  `${source.native_id}:${source.native_version}:${source.source_key}:${source.source_version}:${source.full_input_hash}`;

function recordLabel(table: MinuteTable, payload: MinuteRows["rows"][number]["payload"]): string {
  if (payload === null || typeof payload !== "object" || Array.isArray(payload)) return "查看记录";
  const object = payload as Record<string, unknown>;
  const text = (key: string) =>
    typeof object[key] === "string" || typeof object[key] === "number" ? String(object[key]) : "—";
  const amount = (key: string) => {
    const value = Number(text(key));
    return Number.isFinite(value) ? formatPrice(value) : "—";
  };
  if (table === "fills")
    return `${text("executed_at").slice(0, 10)} · 数量 ${text("quantity")} · 成交价 ${amount("price")} · 费用 ${amount("total_fees")}`;
  if (table === "signals")
    return `${signalActions[text("action")] ?? "查看信号"} · ${formatShanghaiDateTime(text("event_time"))}`;
  if (table === "orders")
    return `${text("ts_code")} · ${text("side") === "BUY" ? "买入" : "卖出"} · 数量 ${text("quantity")}`;
  if (table === "daily_valuations")
    return `${text("trade_date")} · ${text("status") === "complete" ? "估值完整" : "估值不可用"}`;
  if (table === "account") return `最终净值 ${amount("nav")} · 现金 ${amount("cash")}`;
  if (table === "execution_profile") return "查看执行约束与完整费用口径";
  if (table === "replay_summary") return "查看完整策略、成交与来源依据";
  return "查看执行状态与报价依据";
}

function MinuteNavChart({ points }: { points: MinuteNav["points"] }) {
  const build = useCallback(
    (colors: ChartColors): EChartOption => ({
      animation: false,
      tooltip: { trigger: "axis" },
      grid: { top: 16, right: 24, bottom: 32, left: 65 },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: points.map((point) => point.trade_date),
        axisLine: { lineStyle: { color: colors.rule } },
        axisLabel: { color: colors.muted },
      },
      yAxis: {
        type: "value",
        scale: true,
        axisLabel: { color: colors.muted },
        splitLine: { lineStyle: { color: colors.grid } },
      },
      series: [
        {
          type: "line",
          name: "每日净值",
          showSymbol: points.length < 3,
          connectNulls: false,
          lineStyle: { color: colors.accent, width: 2 },
          itemStyle: { color: colors.accent },
          data: points.map((point) =>
            point.status === "complete" && point.nav !== null ? Number(point.nav) : null,
          ),
        },
      ],
    }),
    [points],
  );
  return <EChart label="分钟策略每日净值" build={build} />;
}

function MinutePerformanceChart({ daily }: { daily: Schemas["MinuteReplayPerformance"]["daily"] }) {
  const build = useCallback(
    (colors: ChartColors): EChartOption => ({
      animation: false,
      tooltip: { trigger: "axis" },
      grid: { top: 32, right: 64, bottom: 32, left: 60 },
      legend: { data: ["净值（起始为1）", "回撤"], textStyle: { color: colors.muted } },
      xAxis: {
        type: "category",
        boundaryGap: false,
        data: daily.map((day) => day.trade_date),
        axisLine: { lineStyle: { color: colors.rule } },
        axisLabel: { color: colors.muted },
      },
      yAxis: [
        {
          type: "value",
          scale: true,
          axisLabel: { color: colors.muted },
          splitLine: { lineStyle: { color: colors.grid } },
        },
        {
          type: "value",
          axisLabel: { color: colors.muted, formatter: (value: number) => portfolioPercent(value) },
          splitLine: { show: false },
        },
      ],
      series: [
        {
          name: "净值（起始为1）",
          type: "line",
          connectNulls: false,
          showSymbol: daily.length < 3,
          lineStyle: { color: colors.accent, width: 2 },
          itemStyle: { color: colors.accent },
          data: daily.map((day) => day.normalized_nav),
        },
        {
          name: "回撤",
          type: "line",
          yAxisIndex: 1,
          connectNulls: false,
          showSymbol: daily.length < 3,
          lineStyle: { color: colors.down, width: 1 },
          itemStyle: { color: colors.down },
          data: daily.map((day) => day.drawdown),
        },
      ],
    }),
    [daily],
  );
  return <EChart label="分钟绩效净值与回撤" build={build} />;
}

function MinuteMonthlyHeatmap({
  monthly,
  start,
  end,
}: {
  monthly: Schemas["MinuteReplayPerformance"]["monthly"];
  start: string;
  end: string;
}) {
  const years = [...new Set(monthly.map((row) => row.year))].sort((a, b) => b - a);
  const first = Number(start.slice(0, 4)) * 12 + Number(start.slice(5, 7));
  const last = Number(end.slice(0, 4)) * 12 + Number(end.slice(5, 7));
  const values = new Map(monthly.map((row) => [`${row.year}-${row.month}`, row]));
  return (
    <fieldset className="pbm bt-minute-monthly" aria-label="分钟月度收益">
      <div className="pbm-heading">
        <h3>月度收益</h3>
        <div className="pbm-legend">
          <span className="up">正收益</span>
          <span className="down">负收益</span>
        </div>
      </div>
      {monthly.length === 0 ? (
        <p className="hint">暂无月度收益</p>
      ) : (
        <>
          <div className="pbm-columns" aria-hidden="true">
            <span />
            <div className="pbm-months">
              {Array.from({ length: 12 }, (_, i) => i + 1).map((month) => (
                <span key={month}>{month}月</span>
              ))}
            </div>
          </div>
          {years.map((year) => (
            <div className="pbm-year" key={year}>
              <h4 className="num">{year}</h4>
              <div className="pbm-months">
                {Array.from({ length: 12 }, (_, i) => {
                  const month = i + 1;
                  const row = values.get(`${year}-${month}`);
                  const inPeriod = year * 12 + month >= first && year * 12 + month <= last;
                  const value = inPeriod ? (row?.return_value ?? null) : null;
                  const detail = !inPeriod
                    ? "不在本次区间"
                    : value === null
                      ? "暂无月收益"
                      : `月收益 ${portfolioPercent(value)}`;
                  return (
                    <Tip
                      interactive
                      key={month}
                      content={`${year}年${month}月 · ${detail}${inPeriod && row ? ` · ${row.daily_observations} 个完整交易日` : ""}`}
                    >
                      <button
                        type="button"
                        className="pbm-cell"
                        data-tone={value === null ? "unknown" : toneOf(value)}
                        aria-label={`${year}年${month}月，${detail}`}
                      >
                        <span className="pbm-cell-month" aria-hidden="true">
                          {month}月
                        </span>
                        <span className="num">{portfolioPercent(value)}</span>
                      </button>
                    </Tip>
                  );
                })}
              </div>
            </div>
          ))}
        </>
      )}
    </fieldset>
  );
}

const minuteDailyColumns: DataColumn<Schemas["MinutePerformanceDay"]>[] = [
  { id: "date", header: "日期", value: (row) => row.trade_date },
  {
    id: "nav",
    header: "净值（元）",
    value: (row) => row.nav,
    numeric: true,
    secondary: true,
    cell: (row) => (row.nav === null ? "—" : formatPrice(Number(row.nav))),
  },
  {
    id: "return",
    header: "日收益",
    value: (row) => row.daily_return,
    numeric: true,
    cell: (row) => (
      <span className={toneOf(row.daily_return === null ? null : Number(row.daily_return))}>
        {portfolioPercent(row.daily_return)}
      </span>
    ),
  },
  {
    id: "drawdown",
    header: "回撤",
    value: (row) => row.drawdown,
    numeric: true,
    cell: (row) => <span className={toneOf(row.drawdown)}>{portfolioPercent(row.drawdown)}</span>,
  },
];
const minuteTradeColumns: DataColumn<Schemas["RoundTrip"] & { displayIndex: number }>[] = [
  { id: "code", header: "股票", value: (row) => row.ts_code },
  { id: "entry", header: "买入日", value: (row) => row.entry_date, secondary: true },
  { id: "exit", header: "卖出日", value: (row) => row.exit_date, secondary: true },
  { id: "quantity", header: "数量", value: (row) => row.quantity, numeric: true, secondary: true },
  {
    id: "fees",
    header: "费用（元）",
    value: (row) => row.entry_fee,
    cell: (row) => (
      <span className="bt-minute-fees">
        <span>
          买入 <b className="num">{formatPrice(row.entry_fee)}</b>
        </span>
        <span>
          卖出 <b className="num">{formatPrice(row.exit_fee)}</b>
        </span>
      </span>
    ),
  },
  {
    id: "pnl",
    header: "净收益（元）",
    value: (row) => row.net_pnl,
    numeric: true,
    cell: (row) => <span className={`num ${toneOf(row.net_pnl)}`}>{formatPrice(row.net_pnl)}</span>,
  },
];

function MinutePerformance({ data }: { data: Schemas["MinuteSummaryData"] }) {
  const [metricsOpen, setMetricsOpen] = useState(false);
  const [trade, setTrade] = useState<Schemas["RoundTrip"] | null>(null);
  const value = data.performance;
  const bound =
    value?.input_hash === data.source.core_input_hash &&
    value?.profile_hash === data.source.profile_hash;
  const performance = bound ? value : null;
  const metrics = performance?.status === "complete" ? performance.metrics : null;
  const drawerMetrics: Schemas["PortfolioPerformanceData"] | null = metrics
    ? { ...metrics, overfit_state: metrics.overfit_state ?? "not_evaluated" }
    : null;
  return (
    <Panel
      label="分钟绩效"
      title="分钟绩效"
      actions={
        metrics ? (
          <Button size="sm" onClick={() => setMetricsOpen(true)}>
            查看完整绩效
          </Button>
        ) : null
      }
    >
      {performance ? (
        <>
          {metrics ? (
            <KpiStrip
              label="分钟绩效概览"
              items={[
                {
                  key: "return",
                  label: "累计收益",
                  value: (
                    <span className={toneOf(metrics.summary.total_return)}>
                      {portfolioPercent(metrics.summary.total_return)}
                    </span>
                  ),
                },
                {
                  key: "risk",
                  label: "最大回撤",
                  value: (
                    <span className={toneOf(metrics.summary.max_drawdown)}>
                      {portfolioPercent(metrics.summary.max_drawdown)}
                    </span>
                  ),
                },
                { key: "sharpe", label: "夏普比率", value: portfolioRatio(metrics.summary.sharpe) },
                {
                  key: "trades",
                  label: "闭环交易",
                  value: formatCount(metrics.round_trip_analysis.overall.count),
                  tip: "按已确认买卖成交及原费用配对；未平仓不计入闭环交易。",
                },
              ]}
            />
          ) : (
            <p role="status">
              绩效资料有缺口，完整指标不可用。{" "}
              <Tip content={performance.unavailable_reasons.join("；")}>
                <span className="bt-context-tip">查看原因</span>
              </Tip>
            </p>
          )}
          <MinutePerformanceChart daily={performance.daily} />
          <MinuteMonthlyHeatmap
            monthly={performance.monthly}
            start={data.job.start_date}
            end={data.job.end_date}
          />
          <DataTable
            label="每日收益与回撤"
            rows={performance.daily}
            columns={minuteDailyColumns}
            rowKey={(row) => row.trade_date}
          />
          {metrics ? (
            <div className="bt-minute-trades">
              <h3>闭环交易</h3>
              <DataTable
                label="闭环交易"
                rows={metrics.round_trips.map((row, displayIndex) => ({ ...row, displayIndex }))}
                columns={minuteTradeColumns}
                rowKey={(row) => String(row.displayIndex)}
                onSelect={setTrade}
              />
            </div>
          ) : null}
        </>
      ) : (
        <EmptyState title="绩效资料暂不可用" hint="完整结果通过核验后显示。" />
      )}
      <p className="bt-runtime-note">未提供基准，超额指标不可用。</p>
      <PortfolioMetrics
        performance={drawerMetrics}
        open={metricsOpen}
        onClose={() => setMetricsOpen(false)}
      />
      <SideDrawer title="闭环交易详情" open={trade !== null} onClose={() => setTrade(null)}>
        {trade ? (
          <dl className="bt-minute-trade-detail">
            {[
              ["股票", trade.ts_code],
              ["行业", trade.industry],
              ["买入日", trade.entry_date],
              ["卖出日", trade.exit_date],
              ["数量", formatCount(trade.quantity)],
              ["买入金额（元）", formatPrice(trade.entry_notional)],
              ["卖出金额（元）", formatPrice(trade.exit_notional)],
              ["买入费用（元）", formatPrice(trade.entry_fee)],
              ["卖出费用（元）", formatPrice(trade.exit_fee)],
              ["净收益（元）", formatPrice(trade.net_pnl)],
              ["收益率", portfolioPercent(trade.return_rate)],
              ["持有日", formatCount(trade.holding_days)],
            ].map(([label, text]) => (
              <div key={label}>
                <dt>{label}</dt>
                <dd className="num">{text}</dd>
              </div>
            ))}
          </dl>
        ) : null}
      </SideDrawer>
    </Panel>
  );
}

function MinuteReportActions({
  viewer,
  jobId,
  resultHash,
  canReport,
  canExport,
}: {
  viewer: string | null;
  jobId: string | null;
  resultHash: string | null;
  canReport: boolean;
  canExport: boolean;
}) {
  const [pending, setPending] = useState<MinuteExport | null>(null);
  const [published, setPublished] = useState<{ body: MinuteExport; receipt: MinuteReceipt } | null>(
    null,
  );
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [storageError, setStorageError] = useState(false);
  const storageKey = viewer === null ? null : `rquant.minute.export:${viewer}`;
  useEffect(() => {
    setPublished(null);
    setMessage(null);
    setPending(null);
    setStorageError(false);
    if (storageKey === null) return;
    try {
      const encoded = sessionStorage.getItem(storageKey);
      const saved = restoreMinuteExportRequest(encoded);
      if (encoded !== null && saved === null) {
        setStorageError(true);
        setMessage("保存的导出资料不可用，请检查浏览器存储。");
      }
      if (saved !== null) {
        setPending(saved);
        setMessage("导出状态待确认，请重试原导出。");
      }
    } catch {
      setStorageError(true);
      setMessage("导出请求无法保存，请检查浏览器存储。");
    }
  }, [storageKey]);
  const html =
    canReport && jobId !== null && resultHash !== null ? minuteHtmlUrl(jobId, resultHash) : null;
  const isCurrentExport =
    published !== null &&
    published.body.job_id === jobId &&
    published.body.result_hash === resultHash;
  const zip =
    canReport && published !== null ? minuteZipUrl(published.body, published.receipt) : null;
  async function submit(body: MinuteExport) {
    if (busy || !canExport || storageKey === null) return;
    setBusy(true);
    try {
      // Persist before admission. A failed write must not create an unrecoverable POST.
      sessionStorage.setItem(storageKey, JSON.stringify(body));
    } catch {
      setStorageError(true);
      setMessage("导出请求无法保存，请检查浏览器存储。");
      setBusy(false);
      return;
    }
    setStorageError(false);
    setPending(body);
    setPublished(null);
    setMessage("正在提交导出请求。");
    try {
      const receipt = await submitMinuteExport(body);
      setMessage(receipt.message);
      if (
        receipt.status === "exported" ||
        receipt.status === "failed" ||
        receipt.status === "conflict"
      ) {
        if (receipt.status === "exported") setPublished({ body, receipt });
        setPending(null);
        try {
          sessionStorage.removeItem(storageKey);
        } catch {
          /* Same UUID remains recoverable after remount. */
        }
      }
    } catch (error) {
      setMessage(error instanceof Error ? error.message : "导出状态待确认，请重试原导出。");
    } finally {
      setBusy(false);
    }
  }
  if (resultHash === null && pending === null && message === null) return null;
  return (
    <Panel label="报告导出" title="报告导出">
      <div className="bt-runtime-actions bt-minute-report-actions">
        {html === null ? null : (
          <a className="btn" download href={html}>
            HTML 报告
          </a>
        )}
        <Button
          disabled={
            busy ||
            pending !== null ||
            (zip !== null && isCurrentExport) ||
            storageError ||
            !canExport ||
            !canReport ||
            jobId === null ||
            resultHash === null
          }
          disabledReason={
            !canExport
              ? "当前身份不能准备报告。"
              : !canReport
                ? "完整结果尚未通过核验。"
                : undefined
          }
          onClick={() => {
            if (jobId !== null && resultHash !== null)
              void submit({
                command_id: crypto.randomUUID(),
                requested_at: new Date().toISOString(),
                job_id: jobId,
                result_hash: resultHash,
              });
          }}
        >
          准备完整 ZIP
        </Button>
        {pending === null ? null : (
          <Tip
            interactive
            content={`恢复原请求：${pending.command_id}；结果 ${pending.job_id} / ${pending.result_hash}；提交于 ${formatShanghaiDateTime(pending.requested_at)}`}
          >
            <Button disabled={busy || !canExport} onClick={() => void submit(pending)}>
              重试原导出
            </Button>
          </Tip>
        )}
        {zip === null ? null : (
          <Tip
            interactive
            content={
              published === null
                ? null
                : `报告 ${published.receipt.zip_request_id}；校验 ${published.receipt.sha256}；${formatCount(published.receipt.byte_size)} 字节`
            }
          >
            <a className="btn primary" download href={zip}>
              {isCurrentExport ? "下载 ZIP" : "下载原结果 ZIP"}
            </a>
          </Tip>
        )}
      </div>
      {message === null ? null : (
        <p role="status" className="bt-runtime-note">
          {message}
        </p>
      )}
    </Panel>
  );
}

function parameterSourceChoice(source: MinuteParameterSource) {
  return `${source.source_key}:${source.source_version}:${source.full_input_hash}`;
}

const parameterNature: Record<MinuteParameterSource["source_nature"], string> = {
  real_retained: "真实留存",
  historical_reconstruction: "历史重建",
  synthetic_validation: "合成验证",
};

function RuntimeMinuteReplay({ onHasResult }: { onHasResult: (value: boolean) => void }) {
  const meta = useCurrentMeta();
  const viewer = meta.error === null ? (meta.data?.data.viewer ?? null) : null;
  const capabilities = useMinuteCapabilities();
  const input = useMinuteSources();
  const [sourceKey, setSourceKey] = useState("");
  const [formMode, setFormMode] = useState<"fixed" | "parameters">("fixed");
  const [parameterSourceKey, setParameterSourceKey] = useState("");
  const [parameterFamily, setParameterFamily] =
    useState<Schemas["MinuteParameterCapability"]["family"]>("n_shape");
  const [parameterDraft, setParameterDraft] = useState<{
    identity: string;
    value: MinuteParameters;
  } | null>(null);
  const [parametersValid, setParametersValid] = useState(true);
  const [seed, setSeed] = useState("0");
  const viewerIdentity =
    viewer === null ? null : `${viewer}:${meta.data?.data.generation?.generation_id ?? ""}`;
  const identityRef = useRef(viewerIdentity);
  identityRef.current = viewerIdentity;
  const [dates, setDates] = useState({
    trainStart: "",
    trainEnd: "",
    validationStart: "",
    validationEnd: "",
    outerStart: "",
    outerEnd: "",
  });
  const [pending, setPending] = useState<{ owner: string; body: MinuteCreate } | null>(null);
  const [busy, setBusy] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [jobId, setJobId] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [refresh, setRefresh] = useState(0);
  const [table, setTable] = useState<MinuteTable>("fills");
  const [offset, setOffset] = useState(0);
  const [record, setRecord] = useState<MinuteRows["rows"][number] | null>(null);
  const [recipeSelection, setRecipeSelection] = useState<{
    viewer: string | null;
    jobId: string;
    parameterHash: string;
  } | null>(null);
  const restoredOwner = useRef<string | null>(null);
  const currentPending = pending?.owner === viewer ? pending.body : null;
  const parameterPending =
    currentPending !== null && "parameters" in currentPending.config ? currentPending.config : null;
  const fixedPending =
    currentPending !== null && !("parameters" in currentPending.config)
      ? currentPending.config
      : null;
  const mode =
    currentPending === null ? formMode : parameterPending === null ? "fixed" : "parameters";
  const parameterInput = useMinuteParameterSources(mode === "parameters");
  const sources = input.data?.sources ?? [];
  const source =
    fixedPending !== null
      ? sources.find(
          (item) =>
            item.full_input_hash === fixedPending.full_input_hash &&
            item.source_key === fixedPending.source_key &&
            item.source_version === fixedPending.source_version &&
            item.native_id === fixedPending.native_id &&
            item.native_version === fixedPending.native_version,
        )
      : sourceKey === ""
        ? sources[0]
        : sources.find((item) => sourceChoice(item) === sourceKey);
  const parameterSources = parameterInput.data?.available ? parameterInput.data.sources : [];
  const parameterSource =
    parameterPending !== null
      ? parameterSources.find(
          (item) =>
            item.full_input_hash === parameterPending.full_input_hash &&
            item.source_key === parameterPending.source_key &&
            item.source_version === parameterPending.source_version,
        )
      : parameterSourceKey === ""
        ? parameterSources[0]
        : parameterSources.find((item) => parameterSourceChoice(item) === parameterSourceKey);
  const family = parameterPending?.parameters.parameters.family ?? parameterFamily;
  const parameterCapability =
    parameterSource?.capabilities.find((item) => item.family === family) ??
    (parameterPending === null ? parameterSource?.capabilities[0] : undefined);
  const draftIdentity = `${viewerIdentity}:${parameterSource === undefined ? "" : parameterSourceChoice(parameterSource)}:${parameterCapability?.family ?? ""}`;
  const parameters =
    parameterPending?.parameters ??
    (parameterDraft?.identity === draftIdentity
      ? parameterDraft.value
      : parameterCapability?.default_parameters);
  const unavailable =
    (parameterSource?.unavailable_reasons.length ?? 0) > 0 ||
    (parameterCapability?.unavailable_reasons.length ?? 0) > 0;
  const parameterReady =
    parameters !== undefined &&
    parameterSource !== undefined &&
    parameterCapability !== undefined &&
    parameterInput.data?.available === true &&
    !unavailable &&
    parameters.parameters.family === parameterCapability.family &&
    parameters.parameters.freq === parameterSource.frequency &&
    parameters.parameters.paper !== undefined &&
    (parameters.parameters.family !== "n_shape" ||
      parameters.parameters.volume_profile !== undefined);
  const storageKey = viewer === null ? null : `rquant.minute.pending:${viewer}`;
  const locked = busy || currentPending !== null;
  const jobs = useMinuteJobs(cursor, refresh);
  const selectedId = jobId ?? jobs.data?.jobs[0]?.job_id ?? null;
  const knownJob = jobs.data?.jobs.find((item) => item.job_id === selectedId);
  const summary = useMinuteSummary(
    selectedId,
    knownJob === undefined || ["queued", "running", "sealing"].includes(knownJob.status),
  );
  const resultHash = summary.data?.result_hash ?? null;
  const parameterJob =
    summary.data?.job && isParameterJob(summary.data.job) ? summary.data.job : null;
  const recipeOpen =
    viewerIdentity !== null &&
    recipeSelection?.viewer === viewerIdentity &&
    recipeSelection.jobId === selectedId &&
    parameterJob?.job_id === selectedId &&
    recipeSelection.parameterHash === parameterJob?.parameter_hash &&
    summary.error === null;
  const selectedParameterHash = parameterJob?.parameter_hash ?? null;
  useEffect(() => {
    setRecipeSelection((previous) =>
      previous?.jobId === selectedId &&
      previous?.viewer === viewerIdentity &&
      previous?.parameterHash === selectedParameterHash
        ? previous
        : null,
    );
  }, [selectedId, viewerIdentity, selectedParameterHash]);
  useEffect(() => {
    onHasResult(resultHash !== null);
  }, [onHasResult, resultHash]);
  const nav = useMinuteNav(selectedId, resultHash);
  const rows = useMinuteRows(selectedId, resultHash, table, offset);
  const rangesOrdered =
    dates.trainStart !== "" &&
    dates.validationStart !== "" &&
    dates.trainStart <= dates.trainEnd &&
    dates.trainEnd < dates.validationStart &&
    dates.validationStart <= dates.validationEnd;
  const validDates =
    rangesOrdered &&
    (mode === "fixed"
      ? source !== undefined && dates.validationEnd < source.start_date
      : parameterSource !== undefined &&
        dates.outerStart !== "" &&
        dates.validationEnd < dates.outerStart &&
        dates.outerStart <= dates.outerEnd &&
        parameterSource.start_date <= dates.trainStart &&
        dates.outerEnd <= parameterSource.end_date);
  const numericSeed = Number(seed);
  const seedValid = seed.trim() !== "" && Number.isSafeInteger(numericSeed) && numericSeed >= 0;
  const canSubmit =
    validDates && (mode === "fixed" || (parameterReady && parametersValid && seedValid));

  useEffect(() => {
    setRecord(null);
    setOffset(0);
    setCursor(null);
    setJobId(null);
    setBusy(false);
    setMessage(null);
    setParameterDraft(null);
    setParameterSourceKey("");
    setParameterFamily("n_shape");
    setParametersValid(true);
    setSeed("0");
    setSourceKey("");
    if (viewer === null || viewerIdentity === null) {
      restoredOwner.current = null;
      return;
    }
    if (restoredOwner.current === viewerIdentity) return;
    restoredOwner.current = viewerIdentity;
    setFormMode("fixed");
    try {
      const saved = restoreMinuteRequest(sessionStorage.getItem(`rquant.minute.pending:${viewer}`));
      setPending(saved === null ? null : { owner: viewer, body: saved });
      if (saved !== null) {
        setMessage("提交状态待确认，请重试原请求。");
        if ("parameters" in saved.config) {
          setFormMode("parameters");
          setParameterFamily(saved.config.parameters.parameters.family);
        }
        setSeed(String(saved.config.random_seed));
        setDates({
          trainStart: saved.config.protocol.train_range.start_date,
          trainEnd: saved.config.protocol.train_range.end_date,
          validationStart: saved.config.protocol.validation_range.start_date,
          validationEnd: saved.config.protocol.validation_range.end_date,
          outerStart: saved.config.protocol.frozen_outer_test_range.start_date,
          outerEnd: saved.config.protocol.frozen_outer_test_range.end_date,
        });
      } else
        setDates({
          trainStart: "",
          trainEnd: "",
          validationStart: "",
          validationEnd: "",
          outerStart: "",
          outerEnd: "",
        });
    } catch {
      setPending(null);
    }
  }, [viewer, viewerIdentity]);

  function savePending(body: MinuteCreate | null): boolean {
    if (viewer === null) return false;
    setPending(body === null ? null : { owner: viewer, body });
    if (storageKey === null) return false;
    try {
      if (body === null) sessionStorage.removeItem(storageKey);
      else {
        const encoded = JSON.stringify(body);
        if ("parameters" in body.config && new TextEncoder().encode(encoded).length > 32 * 1024)
          return false;
        sessionStorage.setItem(storageKey, encoded);
      }
      return true;
    } catch {
      return false;
    }
  }

  async function submit(body: MinuteCreate) {
    if (busy || viewer === null) return;
    setBusy(true);
    const persisted = savePending(body);
    if ("parameters" in body.config && !persisted) {
      setMessage("原请求无法保存，请保留页面后重试。");
      setBusy(false);
      return;
    }
    try {
      const receipt = await submitMinuteRun(body);
      if (identityRef.current !== viewerIdentity) return;
      setMessage(receipt.message);
      if (["submitted", "failed", "conflict"].includes(receipt.status)) savePending(null);
      if (receipt.status === "submitted" && typeof receipt.job_id === "string") {
        setJobId(receipt.job_id);
        setOffset(0);
        setRecord(null);
        setCursor(null);
        setRefresh((value) => value + 1);
      }
    } catch (error) {
      if (identityRef.current !== viewerIdentity) return;
      setMessage(error instanceof Error ? error.message : "提交状态待确认，请重试原请求。");
      if (error instanceof ApiError && error.status === 422) savePending(null);
    } finally {
      if (identityRef.current === viewerIdentity) setBusy(false);
    }
  }

  function run(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (
      !canSubmit ||
      locked ||
      viewer === null ||
      !capabilities.data?.can_run ||
      !event.currentTarget.reportValidity()
    )
      return;
    const now = new Date();
    const common = {
      random_seed: mode === "parameters" ? numericSeed : 0,
      deadline: new Date(now.getTime() + 24 * 60 * 60 * 1000).toISOString(),
      protocol: {
        train_range: { start_date: dates.trainStart, end_date: dates.trainEnd },
        validation_range: { start_date: dates.validationStart, end_date: dates.validationEnd },
        frozen_outer_test_range: { start_date: dates.outerStart, end_date: dates.outerEnd },
      },
    };
    if (mode === "parameters" && parameterSource !== undefined && parameters !== undefined) {
      void submit({
        command_id: crypto.randomUUID(),
        requested_at: now.toISOString(),
        config: {
          ...common,
          kind: "minute_parameter_replay",
          source_key: parameterSource.source_key,
          source_version: parameterSource.source_version,
          full_input_hash: parameterSource.full_input_hash,
          parameters: minuteParameterCreateRecipe(parameters),
        },
      });
    } else if (source !== undefined) {
      void submit({
        command_id: crypto.randomUUID(),
        requested_at: now.toISOString(),
        config: {
          ...common,
          source_key: source.source_key,
          source_version: source.source_version,
          full_input_hash: source.full_input_hash,
          native_id: source.native_id,
          native_version: source.native_version,
          protocol: {
            ...common.protocol,
            frozen_outer_test_range: { start_date: source.start_date, end_date: source.end_date },
          },
        },
      });
    }
  }

  const jobColumns: DataColumn<RuntimeMinuteJob>[] = [
    {
      id: "strategy",
      header: "策略版本",
      value: (item) => item.native_name,
      cell: minuteJobTitle,
      wrap: true,
    },
    {
      id: "range",
      header: "输入区间",
      value: (item) => item.start_date,
      cell: (item) => `${item.start_date} 至 ${item.end_date}`,
      secondary: true,
    },
    { id: "status", header: "状态", value: (item) => minuteStatus[item.status] },
  ];
  const rowColumns: DataColumn<MinuteRows["rows"][number]>[] = [
    { id: "number", header: "序号", value: (item) => item.sequence + 1 },
    {
      id: "record",
      header: "内容",
      value: (item) => recordLabel(table, item.payload),
      cell: (item) => <span className="bt-runtime-row">{recordLabel(table, item.payload)}</span>,
    },
  ];
  return (
    <div className="bt-runtime">
      <Panel title="运行分钟回测" sub="使用与模拟盘一致的策略版本、执行约束与费用">
        <form onSubmit={run}>
          <fieldset disabled={locked || viewer === null} className="bt-runtime-fields">
            <legend className="sr-only">分钟策略与输入</legend>
            <label className="bt-runtime-source">
              回测配置
              <select
                aria-label="回测配置"
                value={mode}
                onChange={(event) => {
                  setFormMode(event.target.value === "parameters" ? "parameters" : "fixed");
                  setMessage(null);
                  setParameterDraft(null);
                  setParametersValid(true);
                }}
              >
                <option value="fixed">已发布策略版本</option>
                <option value="parameters">完整参数</option>
              </select>
            </label>
            {mode === "fixed" ? (
              <label className="bt-runtime-source">
                策略版本与输入
                <select
                  aria-label="策略版本与输入"
                  value={source === undefined ? "" : sourceChoice(source)}
                  onChange={(event) => setSourceKey(event.target.value)}
                >
                  {source === undefined ? (
                    <option value="">
                      {currentPending === null ? "请选择可用来源" : "原请求来源（当前不可用）"}
                    </option>
                  ) : null}
                  {sources.map((item) => (
                    <option key={sourceChoice(item)} value={sourceChoice(item)}>
                      {item.native_name} · 版本 {item.native_version} · {item.start_date} 至{" "}
                      {item.end_date} · 输入版本 {item.source_version}
                    </option>
                  ))}
                </select>
              </label>
            ) : (
              <>
                <label className="bt-runtime-source">
                  分钟资料
                  <select
                    aria-label="分钟资料"
                    value={
                      parameterSource === undefined ? "" : parameterSourceChoice(parameterSource)
                    }
                    onChange={(event) => {
                      setParameterSourceKey(event.target.value);
                      setParameterDraft(null);
                      setParametersValid(true);
                      setMessage(null);
                    }}
                  >
                    {parameterSource === undefined ? (
                      <option value="">
                        {currentPending === null ? "请选择可用资料" : "原请求资料（当前不可用）"}
                      </option>
                    ) : null}
                    {parameterSources.map((item) => (
                      <option key={parameterSourceChoice(item)} value={parameterSourceChoice(item)}>
                        {item.display_name} · {item.start_date} 至 {item.end_date} ·{" "}
                        {parameterNature[item.source_nature]} ·{" "}
                        {item.frequency.replace("min", " 分钟")}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="bt-runtime-source">
                  策略族
                  <select
                    aria-label="策略族"
                    value={parameters?.parameters.family ?? ""}
                    onChange={(event) => {
                      const next = parameterSource?.capabilities.find(
                        (item) => item.family === event.target.value,
                      );
                      if (next) {
                        setParameterFamily(next.family);
                        setParameterDraft(null);
                        setParametersValid(true);
                        setMessage(null);
                      }
                    }}
                  >
                    {parameters === undefined ? <option value="">请选择可用策略</option> : null}
                    {parameterSource?.capabilities.map((item) => (
                      <option key={item.family} value={item.family}>
                        {item.display_name}
                      </option>
                    ))}
                    {parameterPending !== null && parameterCapability === undefined ? (
                      <option value={parameterPending.parameters.parameters.family}>
                        原请求策略（当前不可用）
                      </option>
                    ) : null}
                  </select>
                </label>
              </>
            )}
            {(
              [
                ["trainStart", "训练开始"],
                ["trainEnd", "训练结束"],
                ["validationStart", "验证开始"],
                ["validationEnd", "验证结束"],
                ...(mode === "parameters"
                  ? ([
                      ["outerStart", "样本外开始"],
                      ["outerEnd", "样本外结束"],
                    ] as const)
                  : []),
              ] as const
            ).map(([key, label]) => (
              <label key={key}>
                {label}
                <input
                  type="date"
                  required
                  value={dates[key]}
                  min={mode === "parameters" ? parameterSource?.start_date : undefined}
                  max={mode === "parameters" ? parameterSource?.end_date : source?.start_date}
                  onChange={(event) =>
                    setDates((value) => ({ ...value, [key]: event.target.value }))
                  }
                />
              </label>
            ))}
            {mode === "parameters" ? (
              <label>
                随机种子
                <Tip content="保留同一随机种子，便于复现；它不代表策略表现。">
                  <span className="bt-context-tip">ⓘ</span>
                </Tip>
                <input
                  aria-label="随机种子"
                  type="number"
                  inputMode="numeric"
                  min={0}
                  max={Number.MAX_SAFE_INTEGER}
                  step={1}
                  value={seed}
                  required
                  onChange={(event) => setSeed(event.target.value)}
                />
              </label>
            ) : null}
          </fieldset>
          {mode === "parameters" ? (
            <>
              {parameterInput.isLoading ? (
                <p role="status" className="bt-runtime-note">
                  正在加载完整参数来源…
                </p>
              ) : parameterSource === undefined ? (
                <p className="bt-runtime-note">
                  尚无可用完整参数资料。
                  <Tip content={parameterInput.data?.message ?? "需要完整发布资料和当前写入权限。"}>
                    <span className="bt-context-tip">查看原因</span>
                  </Tip>
                </p>
              ) : (
                <p className="bt-runtime-note">
                  {parameterNature[parameterSource.source_nature]} · 发布于{" "}
                  {formatShanghaiDateTime(parameterSource.provenance.published_at)}。
                  <Tip
                    content={
                      parameterSource.provenance.visibility_limitations ??
                      "按所选完整资料的原可见时刻回放。"
                    }
                  >
                    <span className="bt-context-tip">查看来源说明</span>
                  </Tip>
                  <Tip
                    interactive
                    content={`资料版本：${parameterSource.source_version}\n资料指纹：${parameterSource.full_input_hash}`}
                  >
                    <button className="bt-context-tip bt-minute-tip" type="button">
                      查看资料依据
                    </button>
                  </Tip>
                </p>
              )}
              {parameters !== undefined ? (
                <MinuteParameterControls
                  key={draftIdentity}
                  value={parameters}
                  frequency={parameterSource?.frequency ?? parameters.parameters.freq}
                  supported={parameterCapability?.supported_parameter_names ?? []}
                  disabled={
                    locked || viewer === null || unavailable || parameterSource === undefined
                  }
                  onValidityChange={setParametersValid}
                  onChange={(value) => setParameterDraft({ identity: draftIdentity, value })}
                />
              ) : null}
              {parameterSource !== undefined && !parameterReady ? (
                <p className="bt-runtime-note">
                  当前来源尚不支持此配置。
                  <Tip
                    content={
                      [
                        ...parameterSource.unavailable_reasons,
                        ...(parameterCapability?.unavailable_reasons ?? []),
                      ].join("\n") || "完整参数、采样频率或必要配置资料尚未就绪。"
                    }
                  >
                    <span className="bt-context-tip">查看原因</span>
                  </Tip>
                </p>
              ) : null}
              <p className="bt-runtime-note">
                训练、验证、样本外区间须依次分开，且都在所选资料区间内。
              </p>
            </>
          ) : (
            <>
              {viewer !== null && source === undefined && !input.isLoading ? (
                <p className="bt-runtime-note">尚无可用分钟来源，请先准备完整发布资料。</p>
              ) : null}
              {source ? (
                <p className="bt-runtime-note">
                  {source.provenance.source_kind === "captured" ? "原始捕获" : "历史重建"} · 发布于{" "}
                  {formatShanghaiDateTime(source.provenance.published_at)}。
                  {source.provenance.source_kind === "reconstructed" ? (
                    <Tip
                      content={
                        source.provenance.visibility_limitations ??
                        "历史可见时刻按已登记的研究假设重建。"
                      }
                    >
                      <span className="bt-context-tip">查看重建假设</span>
                    </Tip>
                  ) : null}
                  <Tip interactive content="评估区间为所选输入区间；训练与验证区间用于正式登记。">
                    <button className="bt-context-tip bt-minute-tip" type="button">
                      区间说明
                    </button>
                  </Tip>
                </p>
              ) : null}
            </>
          )}
          {capabilities.error || input.error || (mode === "parameters" && parameterInput.error) ? (
            <p role="alert">来源暂时无法加载，请稍后刷新。</p>
          ) : null}
          {viewer !== null && !capabilities.isLoading && capabilities.data?.can_run === false ? (
            <p className="bt-runtime-note">
              当前无法提交回测。
              <Tip content={capabilities.data.message ?? "当前权限或完整运行资料未就绪。"}>
                <span className="bt-context-tip">查看原因</span>
              </Tip>
            </p>
          ) : null}
          <div className="bt-runtime-actions">
            <Button
              type="submit"
              disabled={locked || !canSubmit || viewer === null || !capabilities.data?.can_run}
            >
              运行分钟回测
            </Button>
            {currentPending === null ? null : (
              <Button
                disabled={busy || !capabilities.data?.can_run}
                onClick={() => void submit(currentPending)}
              >
                重试原请求
              </Button>
            )}
            <Button
              size="sm"
              disabled={busy}
              onClick={() => {
                input.refetch();
                if (mode === "parameters") parameterInput.refetch();
                capabilities.refetch();
                jobs.refetch();
              }}
            >
              刷新来源与任务
            </Button>
          </div>
          {message === null ? null : (
            <p role="status" className="bt-runtime-note">
              {message}
            </p>
          )}
        </form>
      </Panel>
      {jobs.error ? (
        <RunError message="分钟任务暂时无法加载。" onRetry={jobs.refetch} />
      ) : jobs.data?.jobs.length ? (
        <Panel title="分钟策略任务">
          <DataTable
            label="分钟策略任务"
            rows={jobs.data.jobs}
            columns={jobColumns}
            rowKey={(item) => item.job_id}
            selectedKey={selectedId}
            onSelect={(item) => {
              setJobId(item.job_id);
              setOffset(0);
              setRecord(null);
            }}
          />
          {jobs.data.next_cursor ? (
            <Button onClick={() => setCursor(jobs.data?.next_cursor ?? null)}>下一批任务</Button>
          ) : null}
          {cursor === null ? null : <Button onClick={() => setCursor(null)}>回到首批任务</Button>}
        </Panel>
      ) : null}
      {summary.error && selectedId !== null ? (
        <RunError
          message={
            summary.error instanceof ApiError && summary.error.status === 404
              ? "任务正在登记，请稍后刷新。"
              : "分钟结果暂时无法加载。"
          }
          onRetry={summary.refetch}
        />
      ) : null}
      {summary.data ? (
        <Panel
          title={minuteJobTitle(summary.data.job)}
          label={minuteJobTitle(summary.data.job)}
          sub={minuteStatus[summary.data.job.status]}
          actions={
            parameterJob === null ? null : (
              <Button
                size="sm"
                onClick={() =>
                  setRecipeSelection({
                    viewer: viewerIdentity,
                    jobId: parameterJob.job_id,
                    parameterHash: parameterJob.parameter_hash,
                  })
                }
              >
                参数与来源
              </Button>
            )
          }
        >
          <p className="bt-runtime-note">
            {summary.data.job.start_date} 至 {summary.data.job.end_date} ·{" "}
            {"source_nature" in summary.data.source
              ? parameterNature[summary.data.source.source_nature]
              : summary.data.source.provenance.source_kind === "captured"
                ? "原始捕获"
                : "历史重建"}
            {"source_nature" in summary.data.source ? (
              <Tip
                content={
                  summary.data.source.provenance.visibility_limitations ??
                  "按原资料记录的可见时刻回放。"
                }
                interactive
              >
                <button type="button" className="bt-context-tip bt-minute-tip">
                  查看来源说明
                </button>
              </Tip>
            ) : null}
          </p>
          {summary.data.result_hash === null ? (
            <p>{summary.data.message ?? "结果尚未保存完成。"}</p>
          ) : (
            <KpiStrip
              label="分钟执行统计"
              items={[
                { key: "signals", label: "信号", value: formatCount(summary.data.signal_count) },
                { key: "orders", label: "委托", value: formatCount(summary.data.order_count) },
                { key: "fills", label: "成交", value: formatCount(summary.data.fill_count) },
                { key: "queue", label: "执行记录", value: formatCount(summary.data.queue_count) },
              ]}
            />
          )}
        </Panel>
      ) : null}
      {summary.data && resultHash !== null ? (
        <MinutePerformance key={`${selectedId}:${resultHash}`} data={summary.data} />
      ) : null}
      <MinuteReportActions
        viewer={viewer}
        jobId={summary.data?.job.job_id ?? null}
        resultHash={resultHash}
        canReport={viewer !== null && summary.error === null && summary.data?.can_report === true}
        canExport={
          viewer !== null && capabilities.error === null && capabilities.data?.can_export === true
        }
      />
      {nav.data ? (
        <Panel title="每日净值" sub="按15:00已确认的行情估值">
          {nav.data.daily_status === "unavailable" ? (
            <p role="status">部分交易日缺少有效报价，净值有缺口。</p>
          ) : null}
          <MinuteNavChart points={nav.data.points} />
          <div className="bt-runtime-days">
            {nav.data.points.map((point) => (
              <details key={point.trade_date}>
                <summary>
                  {point.trade_date} ·{" "}
                  {point.status === "complete" ? formatPrice(Number(point.nav)) : "估值不可用"}
                </summary>
                <p>
                  {point.status === "complete"
                    ? "按当日15:00已确认的行情估值。"
                    : point.unavailable_reasons.join("；")}
                </p>
                {point.price_times.map((price) => (
                  <p key={price.code}>
                    {price.code} · 报价 {formatShanghaiDateTime(price.event_time)} · 可见于{" "}
                    {formatShanghaiDateTime(price.available_at)}
                  </p>
                ))}
              </details>
            ))}
          </div>
        </Panel>
      ) : null}
      {nav.error ? (
        <RunError message="每日净值尚未通过完整校验，请刷新后重试。" onRetry={nav.refetch} />
      ) : null}
      {resultHash !== null ? (
        <Panel title="分钟结果明细">
          <label>
            结果内容{" "}
            <select
              aria-label="分钟结果内容"
              value={table}
              onChange={(event) => {
                setTable(event.target.value as MinuteTable);
                setOffset(0);
                setRecord(null);
              }}
            >
              {minuteTables.map((item) => (
                <option key={item.value} value={item.value}>
                  {item.label}
                </option>
              ))}
            </select>
          </label>
          {rows.error ? (
            <RunError message="明细暂时无法加载。" onRetry={rows.refetch} />
          ) : rows.data ? (
            <>
              <DataTable
                label="分钟结果明细"
                rows={rows.data.rows}
                columns={rowColumns}
                rowKey={(item) => `${table}:${item.sequence}`}
                onSelect={setRecord}
              />
              <div className="bt-pages">
                <span className="hint">共 {formatCount(rows.data.total)} 条</span>
                <Button
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                >
                  上一页明细
                </Button>
                <Button
                  disabled={rows.data.next_offset === null}
                  onClick={() => setOffset(rows.data?.next_offset ?? offset)}
                >
                  下一页明细
                </Button>
              </div>
            </>
          ) : (
            <PageSkeleton label="正在加载分钟明细" />
          )}
        </Panel>
      ) : null}
      <SideDrawer open={record !== null} onClose={() => setRecord(null)} title="结果依据">
        <pre className="bt-runtime-proof">
          {record === null ? "" : JSON.stringify(record.payload, null, 2)}
        </pre>
      </SideDrawer>
      <SideDrawer
        open={recipeOpen}
        onClose={() => setRecipeSelection(null)}
        title="参数与来源"
        wide
      >
        {recipeOpen && parameterJob !== null ? (
          <>
            <p className="bt-runtime-note">本次任务的原参数，只读查看。</p>
            <MinuteParameterControls
              value={parameterJob.parameters}
              frequency={parameterJob.parameters.parameters.freq}
              supported={[]}
              disabled
              onChange={() => undefined}
              onValidityChange={() => undefined}
            />
            <details className="bt-parameter-section">
              <summary>完整参数与来源依据</summary>
              <pre className="bt-runtime-proof">
                {JSON.stringify({ job: parameterJob, source: summary.data?.source }, null, 2)}
              </pre>
            </details>
          </>
        ) : null}
      </SideDrawer>
    </div>
  );
}

function groupKey(group: BacktestGroup): string {
  return `${group.entry_mode}\0${group.profile_variant}`;
}

function weighted(groups: BacktestGroup[], field: "mean_ret_pct" | "win_rate_pct"): number | null {
  const valid = groups.filter((group) => group.trades > 0 && group[field] !== null);
  const count = valid.reduce((sum, group) => sum + group.trades, 0);
  if (count === 0) return null;
  return valid.reduce((sum, group) => sum + (group[field] ?? 0) * group.trades, 0) / count;
}

function RunError({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <Panel>
      <div className="bt-error" role="alert">
        <p>{message}</p>
        <Button size="sm" onClick={onRetry}>
          重新加载
        </Button>
      </div>
    </Panel>
  );
}

function TradeDrawer({
  trade,
  onClose,
  onStock,
}: {
  trade: BacktestTrade | null;
  onClose: () => void;
  onStock: (code: string) => void;
}) {
  const at = (value: string | null) => (value ? formatShanghaiDateTime(value).slice(0, 16) : "—");
  return (
    <SideDrawer
      open={trade !== null}
      onClose={onClose}
      title={`${trade?.name ?? "个股"} · 交易详情`}
    >
      {trade === null ? null : (
        <div className="bt-trade-detail">
          <div className="bt-trade-return">
            <span>单笔收益</span>
            <ChangeText value={trade.ret_pct} />
          </div>
          <dl>
            <div>
              <dt>买入时间</dt>
              <dd className="mono">{at(trade.entry_time)}</dd>
            </div>
            <div>
              <dt>买入价</dt>
              <dd className="num">{formatPrice(trade.entry_price)}</dd>
            </div>
            <div>
              <dt>卖出时间</dt>
              <dd className="mono">{at(trade.exit_time)}</dd>
            </div>
            <div>
              <dt>卖出价</dt>
              <dd className="num">{formatPrice(trade.exit_price)}</dd>
            </div>
            <div>
              <dt>退出原因</dt>
              <dd>{trade.exit_reason_label}</dd>
            </div>
            <div>
              <dt>入场方式</dt>
              <dd>{trade.entry_mode_label}</dd>
            </div>
            <div>
              <dt>风控</dt>
              <dd>{trade.profile_variant_label}</dd>
            </div>
          </dl>
          <Button onClick={() => onStock(trade.ts_code)}>查看个股</Button>
        </div>
      )}
    </SideDrawer>
  );
}

const groupColumns: DataColumn<BacktestGroup>[] = [
  { id: "mode", header: "入场方式", value: (row) => row.entry_mode_label },
  { id: "variant", header: "风控", value: (row) => row.profile_variant_label },
  {
    id: "candidates",
    header: "候选",
    value: (row) => row.candidates,
    numeric: true,
    secondary: true,
    cell: (row) => formatCount(row.candidates),
  },
  {
    id: "trades",
    header: "交易",
    value: (row) => row.trades,
    numeric: true,
    cell: (row) => formatCount(row.trades),
  },
  {
    id: "trigger",
    header: "触发率",
    value: (row) => row.trigger_rate_pct,
    numeric: true,
    secondary: true,
    cell: (row) => formatPercent(row.trigger_rate_pct),
  },
  {
    id: "mean",
    header: "平均收益",
    value: (row) => row.mean_ret_pct,
    numeric: true,
    cell: (row) => <ChangeText value={row.mean_ret_pct} />,
  },
  {
    id: "win",
    header: "胜率",
    value: (row) => row.win_rate_pct,
    numeric: true,
    cell: (row) => formatPercent(row.win_rate_pct),
  },
  {
    id: "best",
    header: "最好",
    value: (row) => row.best_ret_pct,
    numeric: true,
    secondary: true,
    cell: (row) => <ChangeText value={row.best_ret_pct} />,
  },
  {
    id: "worst",
    header: "最差",
    value: (row) => row.worst_ret_pct,
    numeric: true,
    secondary: true,
    cell: (row) => <ChangeText value={row.worst_ret_pct} />,
  },
];

const tradeColumns: DataColumn<BacktestTrade>[] = [
  {
    id: "date",
    header: "信号日",
    value: (row) => row.signal_date,
    cell: (row) => <span className="mono">{row.signal_date.slice(5)}</span>,
  },
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.ts_code,
    cell: (row) => <span className="bt-stock">{row.name ?? "个股"}</span>,
  },
  { id: "mode", header: "入场", value: (row) => row.entry_mode_label, secondary: true },
  {
    id: "entry",
    header: "买入",
    value: (row) => row.entry_time,
    secondary: true,
    cell: (row) => (row.entry_time ? formatShanghaiDateTime(row.entry_time).slice(5, 16) : "—"),
  },
  {
    id: "entryPrice",
    header: "买价",
    value: (row) => row.entry_price,
    numeric: true,
    secondary: true,
    cell: (row) => formatPrice(row.entry_price),
  },
  {
    id: "exit",
    header: "卖出",
    value: (row) => row.exit_time,
    secondary: true,
    cell: (row) => (row.exit_time ? formatShanghaiDateTime(row.exit_time).slice(5, 16) : "—"),
  },
  {
    id: "exitPrice",
    header: "卖价",
    value: (row) => row.exit_price,
    numeric: true,
    secondary: true,
    cell: (row) => formatPrice(row.exit_price),
  },
  {
    id: "return",
    header: "单笔收益",
    value: (row) => row.ret_pct,
    numeric: true,
    cell: (row) => <ChangeText value={row.ret_pct} />,
  },
  { id: "reason", header: "退出", value: (row) => row.exit_reason_label, secondary: true },
];

function MinutePlayback({
  viewer,
  onHasResult,
}: {
  viewer: string | null;
  onHasResult: (value: boolean) => void;
}) {
  const [open, setOpen] = useState(() => {
    if (viewer === null) return false;
    try {
      return (
        restoreMinuteRequest(sessionStorage.getItem(`rquant.minute.pending:${viewer}`)) !== null ||
        sessionStorage.getItem(`rquant.minute.export:${viewer}`) !== null
      );
    } catch {
      return true;
    }
  });
  return (
    <Panel
      title="分钟回放"
      actions={
        <Button
          disabled={viewer === null}
          aria-expanded={open}
          aria-controls="minute-playback"
          onClick={() => {
            if (open) onHasResult(false);
            setOpen((previous) => !previous);
          }}
        >
          {open ? "收起分钟回放" : "打开分钟回放"}
        </Button>
      }
    >
      {open ? (
        <div id="minute-playback">
          <RuntimeMinuteReplay onHasResult={onHasResult} />
        </div>
      ) : (
        <span>分钟任务与回放结果</span>
      )}
    </Panel>
  );
}

export default function MinuteReplay() {
  const privateMeta = useCurrentMeta();
  const [nativeHasResult, setNativeHasResult] = useState(false);
  const [studyOpen, setStudyOpen] = useState(false);
  const privateKey = `${privateMeta.error === null ? (privateMeta.data?.data.viewer ?? "") : ""}:${privateMeta.data?.data.generation?.generation_id ?? ""}`;
  const [runOffset, setRunOffset] = useState(0);
  const [runGeneration, setRunGeneration] = useState<string | null>(null);
  const [refreshKey, setRefreshKey] = useState(0);
  const [runId, setRunId] = useState<string | null>(null);
  const [group, setGroup] = useState<BacktestGroupKey | null>(null);
  const [tradeOffset, setTradeOffset] = useState(0);
  const [selectedTrade, setSelectedTrade] = useState<BacktestTrade | null>(null);
  const [selectedStock, setSelectedStock] = useState<string | null>(null);
  const seenGeneration = useRef<string | null>(null);
  const recoveredGeneration = useRef<string | null>(null);
  const listing = useBacktestRuns(runOffset, runGeneration, refreshKey);
  const currentGeneration = listing.serving?.generation_id ?? null;
  const listChanged =
    currentGeneration !== null &&
    seenGeneration.current !== null &&
    currentGeneration !== seenGeneration.current;
  const visibleGroup = listChanged ? null : group;
  const visibleTradeOffset = listChanged ? 0 : tradeOffset;
  const runs = listing.data?.runs ?? [];
  const selectedRun =
    runs.find((item) => item.run_id === (listChanged ? null : runId)) ?? runs[0] ?? null;
  const detail = useBacktestDetail(
    selectedRun?.run_id ?? null,
    currentGeneration,
    visibleTradeOffset,
    visibleGroup,
  );
  const resetPage = useCallback(() => {
    setRunOffset(0);
    setRunGeneration(null);
    setRunId(null);
    setGroup(null);
    setTradeOffset(0);
    setSelectedTrade(null);
    setSelectedStock(null);
    setRefreshKey((value) => value + 1);
  }, []);

  useEffect(() => {
    if (currentGeneration === null) return;
    if (seenGeneration.current !== null && seenGeneration.current !== currentGeneration) {
      setRunId(null);
      setGroup(null);
      setTradeOffset(0);
      setSelectedTrade(null);
      setSelectedStock(null);
      if (runOffset > 0) resetPage();
    }
    seenGeneration.current = currentGeneration;
  }, [currentGeneration, runOffset, resetPage]);

  useEffect(() => {
    const conflict =
      (listing.error instanceof ApiError && listing.error.status === 409) ||
      (detail.error instanceof ApiError && detail.error.status === 409);
    if (!conflict) return;
    const generation = runGeneration ?? currentGeneration;
    if (generation === null || recoveredGeneration.current === generation) return;
    recoveredGeneration.current = generation;
    seenGeneration.current = null;
    resetPage();
  }, [listing.error, detail.error, runGeneration, currentGeneration, resetPage]);
  const groups = detail.data?.groups ?? [];
  const chosen =
    visibleGroup === null
      ? null
      : (groups.find(
          (item) =>
            item.entry_mode === visibleGroup.entryMode &&
            item.profile_variant === visibleGroup.profileVariant,
        ) ?? null);
  const tradesCount = chosen?.trades ?? selectedRun?.trades ?? 0;
  const candidates = chosen?.candidates ?? selectedRun?.candidates ?? 0;
  const mean = chosen === null ? weighted(groups, "mean_ret_pct") : chosen.mean_ret_pct;
  const win = chosen === null ? weighted(groups, "win_rate_pct") : chosen.win_rate_pct;

  function selectRun(next: string) {
    setRunId(next);
    setGroup(null);
    setTradeOffset(0);
    setSelectedTrade(null);
  }

  function selectGroup(row: BacktestGroup) {
    setGroup({ entryMode: row.entry_mode, profileVariant: row.profile_variant });
    setTradeOffset(0);
  }

  function reload() {
    recoveredGeneration.current = null;
    seenGeneration.current = null;
    resetPage();
  }

  function goToRunPage(offset: number) {
    if (currentGeneration === null) return;
    setRunGeneration(currentGeneration);
    setRunOffset(offset);
    selectRun("");
  }

  return (
    <>
      <PageHeader eyebrow="策略与验证" title="回测" note="查看已完成的分钟回放" />
      <MinutePlayback
        key={privateKey}
        viewer={privateMeta.error === null ? (privateMeta.data?.data.viewer ?? null) : null}
        onHasResult={setNativeHasResult}
      />
      <Panel
        title="参数研究"
        actions={
          <Button
            aria-expanded={studyOpen}
            aria-controls="minute-study-workspace"
            onClick={() => setStudyOpen((previous) => !previous)}
          >
            {studyOpen ? "收起研究" : "打开参数研究"}
          </Button>
        }
      >
        {studyOpen ? (
          <div id="minute-study-workspace">
            <MinuteStudyWorkspace key={privateKey} />
          </div>
        ) : (
          <span>参数对照 · 五组消融 · 滚动分窗</span>
        )}
      </Panel>
      <section aria-label="历史归档" className="bt-history">
        {listing.data?.available && selectedRun !== null ? <h2>历史归档</h2> : null}
        {listing.isLoading ? (
          <PageSkeleton label="正在加载回放记录" />
        ) : listing.error ? (
          <RunError
            message={
              listing.error instanceof ApiError
                ? listing.error.message
                : "回放记录暂时无法加载，请稍后重试。"
            }
            onRetry={reload}
          />
        ) : !listing.data?.available || selectedRun === null ? (
          nativeHasResult ? null : (
            <Panel>
              <EmptyState
                title={
                  runOffset > 0 && listing.data?.available
                    ? "这一批没有回放记录"
                    : "还没有可查看的回放结果"
                }
                hint={
                  runOffset > 0 && listing.data?.available
                    ? "可返回上一批继续查看。"
                    : listing.data?.available
                      ? "新的回放完成后会出现在这里。"
                      : "回放结果发布后会出现在这里。"
                }
              />
              {runOffset > 0 ? (
                <div className="bt-empty-actions">
                  <Button size="sm" onClick={() => goToRunPage(Math.max(0, runOffset - PAGE_SIZE))}>
                    上一批
                  </Button>
                </div>
              ) : null}
            </Panel>
          )
        ) : (
          <>
            <section className="bt-selector" aria-label="回放选择">
              <div className="bt-selector-main">
                <label htmlFor="bt-run-select">回放记录</label>
                <select
                  id="bt-run-select"
                  className="inp"
                  value={selectedRun.run_id}
                  onChange={(event) => selectRun(event.target.value)}
                >
                  {runs.map((run) => (
                    <option key={run.run_id} value={run.run_id}>
                      {formatShanghaiDateTime(run.computed_at).slice(0, 16)} ·{" "}
                      {run.start_date.slice(5)}—{run.end_date.slice(5)}
                    </option>
                  ))}
                </select>
                <Tip content={`记录 ${selectedRun.run_id}`}>
                  <span className="bt-selector-info">
                    {formatCount(selectedRun.configurations)} 组配置
                  </span>
                </Tip>
              </div>
              {listing.data.total > PAGE_SIZE ? (
                <div className="bt-selector-pages">
                  <Button
                    size="sm"
                    disabled={runOffset === 0}
                    onClick={() => goToRunPage(Math.max(0, runOffset - PAGE_SIZE))}
                  >
                    上一批
                  </Button>
                  <Button
                    size="sm"
                    disabled={listing.data.next_offset === null}
                    onClick={() => goToRunPage(listing.data?.next_offset ?? runOffset)}
                  >
                    下一批
                  </Button>
                </div>
              ) : null}
            </section>
            <div className="bt-context">
              <span>
                区间{" "}
                <b className="mono">
                  {selectedRun.start_date} 至 {selectedRun.end_date}
                </b>
              </span>
              <span>
                最长持有 <b className="num">{formatCount(selectedRun.max_hold_days)} 天</b>
              </span>
              <Tip content="这一页展示分钟回放的逐笔统计；逐笔收益不能推导组合净值。">
                <span className="bt-context-tip">分钟回放</span>
              </Tip>
            </div>
            {detail.isLoading ? (
              <PageSkeleton label="正在加载回放详情" />
            ) : detail.error ? (
              <RunError
                message={
                  detail.error instanceof ApiError
                    ? detail.error.message
                    : "回放详情暂时无法加载，请稍后重试。"
                }
                onRetry={
                  detail.error instanceof ApiError && detail.error.status === 409
                    ? reload
                    : detail.refetch
                }
              />
            ) : !detail.data?.summary_available ? (
              <Panel>
                <EmptyState title="回放汇总暂时不可用" hint="数据发布后再查看。" />
              </Panel>
            ) : (
              <>
                <div className="bt-kpis">
                  <KpiStrip
                    label="回放概览"
                    compact
                    items={[
                      {
                        key: "candidates",
                        label: "候选",
                        value: <span className="num">{formatCount(candidates)}</span>,
                      },
                      {
                        key: "trades",
                        label: "触发交易",
                        value: <span className="num">{formatCount(tradesCount)}</span>,
                      },
                      {
                        key: "mean",
                        label: "平均单笔收益",
                        value: <ChangeText value={mean} />,
                        tip: "逐笔交易按配置加权，未计算组合净值。",
                      },
                      {
                        key: "win",
                        label: "胜率",
                        value: <span className="num">{formatPercent(win)}</span>,
                        tip: "按已发布交易笔数统计。",
                      },
                    ]}
                  />
                </div>
                <Panel title="净值与回撤" label="净值与回撤">
                  <EmptyState title="这次回放未产出逐日净值" hint="回撤、持仓与基准也尚未发布。" />
                </Panel>
                <Panel
                  title="配置统计"
                  sub="点选一组，查看对应交易"
                  actions={
                    visibleGroup === null ? undefined : (
                      <Button
                        size="sm"
                        onClick={() => {
                          setGroup(null);
                          setTradeOffset(0);
                        }}
                      >
                        全部交易
                      </Button>
                    )
                  }
                  flush
                >
                  <DataTable
                    rows={groups}
                    columns={groupColumns}
                    rowKey={groupKey}
                    selectedKey={chosen === null ? null : groupKey(chosen)}
                    onSelect={selectGroup}
                    label="配置统计"
                    emptyText="没有配置统计"
                  />
                </Panel>
                <Panel
                  title="交易明细"
                  sub={
                    detail.data.trades_available
                      ? `共 ${formatCount(detail.data.total_trades)} 笔`
                      : "交易记录尚未发布"
                  }
                  flush
                >
                  {!detail.data.trades_available ? (
                    <EmptyState title="交易记录暂时不可用" hint="记录发布后会显示在这里。" />
                  ) : detail.data.total_trades === 0 ? (
                    <EmptyState title="这次回放没有触发交易" hint="可选择其他配置或回放记录。" />
                  ) : (
                    <>
                      <DataTable
                        rows={detail.data.trades}
                        columns={tradeColumns}
                        rowKey={(trade) => trade.trade_id}
                        onSelect={setSelectedTrade}
                        label="交易明细"
                        emptyText="这一页没有交易"
                      />
                      <div className="bt-pages">
                        <span className="hint">
                          第 {Math.floor(visibleTradeOffset / PAGE_SIZE) + 1} 页 · 每页 {PAGE_SIZE}{" "}
                          笔
                        </span>
                        <Button
                          size="sm"
                          disabled={visibleTradeOffset === 0}
                          onClick={() =>
                            setTradeOffset(Math.max(0, visibleTradeOffset - PAGE_SIZE))
                          }
                        >
                          上一页
                        </Button>
                        <Button
                          size="sm"
                          disabled={detail.data.next_offset === null}
                          onClick={() =>
                            setTradeOffset(detail.data?.next_offset ?? visibleTradeOffset)
                          }
                        >
                          下一页
                        </Button>
                      </div>
                    </>
                  )}
                </Panel>
              </>
            )}
          </>
        )}
      </section>
      <TradeDrawer
        trade={listChanged ? null : selectedTrade}
        onClose={() => setSelectedTrade(null)}
        onStock={(code) => {
          setSelectedTrade(null);
          setSelectedStock(code);
        }}
      />
      <StockDrawer
        tsCode={listChanged ? null : selectedStock}
        onClose={() => setSelectedStock(null)}
      />
    </>
  );
}
