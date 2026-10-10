import { useEffect, useRef, useState } from "react";
import {
  type AIBacktestConfirmRequest,
  type AIBacktestPreparation,
  type AIBacktestPrepareRequest,
  confirmAiBacktest,
  prepareAiBacktest,
  useAiCapabilities,
} from "@/api/aiAssistance";
import type { PortfolioConfig } from "@/api/backtests";
import { ApiError } from "@/api/client";
import {
  fetchScreenExecutionResults,
  type ScreenExecutionView,
  type ScreenOriginalAction,
  type ScreenQueryDefinition,
  type ScreenQueryReadData,
  type ScreenRow,
  screenQueryTransport,
  useScreenCatalog,
} from "@/api/screen";
import { formatCount, formatNumber, formatPrice } from "@/format/number";
import { PortfolioConfiguration } from "@/pages/backtest/PortfolioConfig";
import { type RankingDraft, RankingEditor } from "@/pages/screener/RankingEditor";
import { ScreenNaturalLanguage } from "@/pages/screener/ScreenNaturalLanguage";
import {
  type ScreenConditionDraft,
  ScreenConditionEditor,
} from "@/pages/shared/ScreenConditionEditor";
import { StockCell } from "@/pages/shared/StockCell";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, ConfirmDialog, EmptyState, Panel, SideDrawer, SkeletonRows, Tip } from "@/ui";
import { StockDrawer } from "./StockDrawer";
import "@/pages/screener/screener.css";
import "@/pages/backtest/portfolio.css";

function stored<T>(key: string): T | null {
  try {
    const raw = sessionStorage.getItem(key);
    return raw && raw.length <= 65536 ? (JSON.parse(raw) as T) : null;
  } catch {
    return null;
  }
}
function original<T>(key: string, body: T): T {
  const old = stored<T>(key);
  if (old) return old;
  sessionStorage.setItem(key, JSON.stringify(body));
  return body;
}
const text = (error: unknown) =>
  error instanceof Error ? error.message : "结果待确认，请继续查看原请求。";

export function AiScreenBacktest({
  viewer,
  execution,
  blocked = false,
}: {
  viewer: string | null;
  execution: ScreenExecutionView | null;
  blocked?: boolean;
}) {
  return (
    <AiScreenBacktestBody
      key={`${viewer}:${execution?.execution_id}`}
      viewer={viewer}
      execution={execution}
      blocked={blocked}
    />
  );
}
function AiScreenBacktestBody({
  viewer,
  execution,
  blocked,
}: {
  viewer: string | null;
  execution: ScreenExecutionView | null;
  blocked: boolean;
}) {
  const capability = useAiCapabilities(viewer);
  const [start, setStart] = useState("");
  const [end, setEnd] = useState(execution?.definition.trade_date ?? "");
  const [prepared, setPrepared] = useState<AIBacktestPreparation | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [confirm, setConfirm] = useState(false);
  const [job, setJob] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  const key = `rquant.ai.backtest:${viewer}:${execution?.execution_id}`;
  const [saved, setSaved] = useState(() => stored<AIBacktestPrepareRequest>(key));
  const [absent, setAbsent] = useState(false);
  useEffect(() => () => controller.current?.abort(), []);
  async function prepare() {
    if (!viewer || !execution || busy) return;
    setBusy(true);
    setError(null);
    const abort = new AbortController();
    controller.current = abort;
    try {
      const body = original(key, {
        request_id: crypto.randomUUID(),
        execution_id: execution.execution_id,
        start_date: start,
        end_date: end,
      });
      setSaved(body);
      const value = await prepareAiBacktest(body, saved !== null && !absent, abort.signal);
      if (
        !abort.signal.aborted &&
        value.execution_id === execution.execution_id &&
        value.request_id === body.request_id
      )
        setPrepared(value);
    } catch (caught) {
      if (!abort.signal.aborted) {
        setAbsent(caught instanceof ApiError && caught.status === 404);
        setError(
          caught instanceof ApiError && caught.status === 404
            ? "区间尚未准备，可以继续原请求。"
            : text(caught),
        );
      }
    } finally {
      if (!abort.signal.aborted) setBusy(false);
    }
  }
  async function submit() {
    if (!prepared || busy) return;
    const abort = new AbortController();
    controller.current = abort;
    setBusy(true);
    setError(null);
    try {
      const body = original<AIBacktestConfirmRequest>(`${key}:confirm`, {
        command_id: crypto.randomUUID(),
        requested_at: new Date().toISOString(),
        prepared_request_id: prepared.request_id,
        config_sha256: prepared.config_sha256,
        proof_sha256: prepared.proof_sha256,
      });
      const value = await confirmAiBacktest(body, abort.signal);
      if (!abort.signal.aborted && value.receipt.command_id === body.command_id) {
        setJob(value.job_id);
        setConfirm(false);
      } else if (!abort.signal.aborted) {
        setError("回测回执不匹配，请继续原请求。");
      }
    } catch (caught) {
      if (!abort.signal.aborted) setError(text(caught));
    } finally {
      if (!abort.signal.aborted) setBusy(false);
    }
  }
  const ready =
    !blocked &&
    execution?.status === "succeeded" &&
    execution.artifact_sha256 &&
    execution.unknown_count === 0;
  return (
    <Panel title="3 · 验证历史表现">
      {!ready ? (
        <EmptyState title="先完成筛选" hint="核对当前条件，再执行筛选。" />
      ) : (
        <>
          <div className="row">
            <label className="field">
              <span className="lbl">开始日期</span>
              <input
                className="inp num"
                type="date"
                value={saved?.start_date ?? start}
                onChange={(event) => setStart(event.target.value)}
                disabled={saved !== null}
              />
            </label>
            <label className="field">
              <span className="lbl">结束日期</span>
              <input
                className="inp num"
                type="date"
                value={saved?.end_date ?? end}
                onChange={(event) => setEnd(event.target.value)}
                disabled={saved !== null}
              />
            </label>
          </div>
          <div className="row">
            <Button
              disabled={busy || (!saved && (!start || start > end))}
              disabledReason={
                (saved && !absent) || capability.data?.can_prepare_backtest
                  ? undefined
                  : "完整历史来源尚未配置。"
              }
              onClick={() => void prepare()}
            >
              {saved ? (absent ? "继续准备原区间" : "继续查看原区间") : "准备完整区间"}
            </Button>
            <Tip content="逐日使用当前条件筛选，并使用原组合回测的默认规则。缺少任何一天的候选、行情或基准时，暂不能确认。">
              <span className="screen-help" role="img" aria-label="完整区间说明">
                ⓘ
              </span>
            </Tip>
          </div>
          {prepared ? (
            <>
              <dl className="kv">
                <dt>完整交易日</dt>
                <dd className="num">{prepared.trading_days.toLocaleString("zh-CN")}</dd>
                <dt>候选股票</dt>
                <dd className="num">{prepared.candidate_count.toLocaleString("zh-CN")}</dd>
                <dt>初始资金</dt>
                <dd className="num">
                  {Number(prepared.config.initial_cash).toLocaleString("zh-CN")}
                </dd>
                <dt>最多持仓</dt>
                <dd className="num">{prepared.config.weight_rule.max_positions}</dd>
                <dt>比较基准</dt>
                <dd className="mono">{prepared.config.benchmark_code}</dd>
              </dl>
              <Button onClick={() => setConfirm(true)} disabled={busy || !prepared.complete}>
                核对并确认回测
              </Button>
            </>
          ) : null}
          {job ? (
            <p role="status">
              回测已提交。
              <a href={`#/backtest?tab=portfolio&job=${encodeURIComponent(job)}`}>查看运行与结果</a>
            </p>
          ) : null}
          {error ? (
            <p role="alert" className="hint">
              {error}
            </p>
          ) : null}
          {busy ? (
            <p role="status" className="hint">
              正在处理原请求…
            </p>
          ) : null}
          <ConfirmDialog
            open={confirm}
            level="heavy"
            title="确认组合回测"
            description={
              prepared ? (
                <>
                  <p>使用已核对的完整历史区间和默认策略。</p>
                  <PortfolioConfiguration
                    value={prepared.config as PortfolioConfig}
                    sources={[]}
                    locked
                    onChange={() => {}}
                  />
                </>
              ) : null
            }
            busy={busy}
            disabled={!prepared?.complete}
            onConfirm={() => void submit()}
            onCancel={() => setConfirm(false)}
            confirmLabel="确认回测"
          />
        </>
      )}
    </Panel>
  );
}

function AssistantWorkflow({ viewer }: { viewer: string }) {
  const catalog = useScreenCatalog();
  const data = catalog.data;
  const [description, setDescription] = useState("");
  const [date, setDate] = useState("");
  const [conditions, setConditions] = useState<ScreenConditionDraft[]>([]);
  const [ranking, setRanking] = useState<RankingDraft[]>([]);
  const [topN, setTopN] = useState("20");
  const [revision, setRevision] = useState(0);
  const [execution, setExecution] = useState<ScreenExecutionView | null>(null);
  const [readResult, setReadResult] = useState<ScreenQueryReadData | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(() =>
    stored<ScreenOriginalAction>(`rquant.ai.execute:${viewer}`),
  );
  const abort = useRef<AbortController | null>(null);
  const next = useRef(1);
  const undo = useRef<{
    conditions: ScreenConditionDraft[];
    ranking: RankingDraft[];
    topN: string;
  } | null>(null);
  const activeDate = date || data?.dates[0] || "";
  const blocks = data?.blocks ?? [];
  const metrics = data?.ranking_metrics ?? [];
  const weights = ranking.map((row) => Number(row.weight));
  const total = weights.reduce((sum, value) => sum + value, 0);
  const rankError =
    ranking.length &&
    (ranking.length > 4 ||
      !Number.isFinite(total) ||
      total <= 0 ||
      weights.some((value) => !Number.isFinite(value) || value < 0 || value > 100) ||
      !Number.isInteger(Number(topN)) ||
      Number(topN) < 1 ||
      Number(topN) > 100)
      ? "请核对排名权重和股票数量。"
      : null;
  function changed(invalidateSuggestion = true) {
    if (invalidateSuggestion) setRevision((value) => value + 1);
    setExecution(null);
    setReadResult(null);
  }
  useEffect(() => () => abort.current?.abort(), []);
  async function run() {
    if (busy) return;
    let current = saved;
    if (!current) {
      if (
        !data?.source ||
        !activeDate ||
        rankError ||
        !conditions.length ||
        data.source_kind === "intraday"
      )
        return;
      const definition: ScreenQueryDefinition = {
        schema_version: 1,
        mode: "daily",
        description,
        trade_date: activeDate,
        source_kind: data.source_kind,
        source_identity: data.source.identity,
        conditions: conditions.map((condition) => ({ name: condition.key, args: condition.args })),
        ranking: ranking.length
          ? {
              top_n: Number(topN),
              conditions: ranking.map((row) => ({
                metric: row.metric,
                ascending: row.ascending,
                weight: Number(row.weight),
              })),
            }
          : null,
      };
      current = {
        action: "execute",
        command: {
          kind: "execute_screen_query",
          page_size: 20,
          command_id: crypto.randomUUID(),
          requested_at: new Date().toISOString(),
          definition,
        },
      };
    }
    const controller = new AbortController();
    abort.current = controller;
    setBusy(true);
    setError(null);
    try {
      const body = original<ScreenOriginalAction>(`rquant.ai.execute:${viewer}`, current);
      setSaved(body);
      const reply = await screenQueryTransport(
        body,
        saved ? "lookup" : "submit",
        controller.signal,
      );
      if (!controller.signal.aborted) {
        setExecution(reply.execution ?? null);
        setReadResult(reply);
        if (reply.receipt?.status !== "succeeded") setError("筛选结果待确认，请继续查看原请求。");
      }
    } catch (caught) {
      if (!controller.signal.aborted) setError(text(caught));
    } finally {
      if (!controller.signal.aborted) setBusy(false);
    }
  }
  if (catalog.isLoading && !saved) return <SkeletonRows rows={3} />;
  const available = data?.available && data.source;
  return (
    <div className="screen-nl">
      <h2>1 · 把想法变成条件</h2>
      {!available ? (
        <EmptyState
          title="筛选数据暂不可用"
          hint={<Button onClick={catalog.refetch}>刷新数据</Button>}
        />
      ) : (
        <>
          <label className="field">
            <span className="lbl">筛选日期</span>
            <select
              className="inp num"
              value={activeDate}
              onChange={(event) => {
                setDate(event.target.value);
                changed();
              }}
            >
              {data.dates.map((value) => (
                <option key={value}>{value}</option>
              ))}
            </select>
          </label>
          <ScreenNaturalLanguage
            ownerScope={viewer}
            description={description}
            onDescriptionChange={setDescription}
            available={data.nl_generate_available}
            sourceKind={data.source_kind === "intraday" ? null : data.source_kind}
            sourceIdentity={data.source?.identity ?? null}
            tradeDate={activeDate}
            conditionRevision={revision}
            successfulRunRevision={0}
            blocks={blocks}
            onApply={(draft, plan) => {
              undo.current = { conditions, ranking, topN };
              setConditions(draft.map((row) => ({ ...row, id: next.current++ })));
              setRanking(
                (plan?.conditions ?? []).map((row) => ({
                  id: next.current++,
                  metric: row.metric,
                  ascending: row.ascending,
                  weight: String(row.weight),
                })),
              );
              setTopN(String(plan?.top_n ?? 20));
              changed(false);
            }}
            onUndo={() => {
              if (undo.current) {
                setConditions(undo.current.conditions);
                setRanking(undo.current.ranking);
                setTopN(undo.current.topN);
                undo.current = null;
                changed();
              }
            }}
            onConflict={catalog.refetch}
          />
          <Panel title="2 · 核对并执行筛选">
            <ScreenConditionEditor
              conditions={conditions}
              blocks={blocks}
              allowRsi={blocks.some(
                (block) =>
                  (block.key === "rsi_oversold" || block.key === "rsi_overbought") &&
                  block.parameters.some(
                    (parameter) => parameter.key === "period" && parameter.input === "integer",
                  ),
              )}
              onUpdate={(id, key, value) => {
                setConditions((rows) =>
                  rows.map((row) =>
                    row.id === id ? { ...row, args: { ...row.args, [key]: value } } : row,
                  ),
                );
                changed();
              }}
              onRemove={(id) => {
                setConditions((rows) => rows.filter((row) => row.id !== id));
                changed();
              }}
            />
            <label className="field">
              <span className="lbl">添加条件</span>
              <select
                className="inp"
                value=""
                onChange={(event) => {
                  const block = blocks.find((item) => item.key === event.target.value);
                  if (block) {
                    setConditions((rows) => [
                      ...rows,
                      {
                        id: next.current++,
                        key: block.key,
                        args: Object.fromEntries(
                          block.parameters.map((parameter) => [parameter.key, parameter.initial]),
                        ),
                      },
                    ]);
                    changed();
                  }
                }}
                disabled={conditions.length >= 26}
              >
                <option value="">选择条件</option>
                {blocks.map((block) => (
                  <option value={block.key} key={block.key}>
                    {block.label}
                  </option>
                ))}
              </select>
            </label>
          </Panel>
          <RankingEditor
            metrics={metrics}
            rows={ranking}
            topN={topN}
            totalWeight={total}
            error={rankError}
            onAdd={() => {
              const metric = metrics.find(
                (item) => !ranking.some((row) => row.metric === item.value),
              );
              if (metric && ranking.length < 4) {
                setRanking((rows) => [
                  ...rows,
                  {
                    id: next.current++,
                    metric: metric.value,
                    ascending: metric.value === "CIRC_MV[0]",
                    weight: "100",
                  },
                ]);
                changed();
              }
            }}
            onUpdate={(id, change) => {
              setRanking((rows) =>
                rows.map((row) => (row.id === id ? { ...row, ...change } : row)),
              );
              changed();
            }}
            onRemove={(id) => {
              setRanking((rows) => rows.filter((row) => row.id !== id));
              changed();
            }}
            onTopN={(value) => {
              setTopN(value);
              changed();
            }}
          />
        </>
      )}
      <div className="row">
        <Button
          onClick={() => void run()}
          disabled={busy || (!saved && (!conditions.length || !!rankError))}
        >
          {saved ? "继续查看筛选" : "执行筛选"}
        </Button>
        {saved && execution?.status === "succeeded" ? (
          <Button
            variant="ghost"
            onClick={() => {
              sessionStorage.removeItem(`rquant.ai.execute:${viewer}`);
              setSaved(null);
              setExecution(null);
            }}
          >
            新建筛选
          </Button>
        ) : null}
        <a href="#/screener">打开条件筛选</a>
      </div>
      {busy ? <p role="status">正在查看筛选…</p> : null}
      {error ? <p role="alert">{error}</p> : null}
      {execution ? (
        <p role="status">
          {execution.status === "succeeded"
            ? `筛选已完成，共 ${execution.ranked_count ?? execution.total ?? "—"} 只股票。`
            : "筛选结果待确认。"}
        </p>
      ) : null}
      {execution?.status === "succeeded" && readResult?.results ? (
        <OriginalScreenResults
          key={execution.execution_id}
          execution={execution}
          original={readResult}
        />
      ) : null}
      <AiScreenBacktest viewer={viewer} execution={execution} />
    </div>
  );
}

const resultColumns: DataColumn<ScreenRow>[] = [
  {
    id: "stock",
    header: "股票",
    value: (row) => row.name ?? row.ts_code,
    cell: (row) => <StockCell code={row.ts_code} name={row.name} />,
  },
  {
    id: "close",
    header: "收盘价",
    value: (row) => row.close,
    cell: (row) => <span className="num">{formatPrice(row.close)}</span>,
    numeric: true,
  },
  {
    id: "rank",
    header: "名次",
    value: (row) => row.rank_position ?? null,
    cell: (row) => <span className="num">{formatCount(row.rank_position)}</span>,
    numeric: true,
  },
  {
    id: "score",
    header: "排名分",
    value: (row) => row.ranking_score ?? null,
    cell: (row) => <span className="num">{formatNumber(row.ranking_score, 1)}</span>,
    numeric: true,
  },
];
function OriginalScreenResults({
  execution,
  original,
}: {
  execution: ScreenExecutionView;
  original: ScreenQueryReadData;
}) {
  const [result, setResult] = useState(original.results);
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [page, setPage] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [stock, setStock] = useState<string | null>(null);
  const controller = useRef<AbortController | null>(null);
  useEffect(() => () => controller.current?.abort(), []);
  async function move(index: number) {
    const cursor = index > page ? result?.next_cursor : cursors[index];
    if (busy || cursor === undefined || (index > page && !cursor)) return;
    const abort = new AbortController();
    controller.current = abort;
    setBusy(true);
    setError(null);
    try {
      const reply = await fetchScreenExecutionResults(
        execution.execution_id,
        cursor ?? null,
        abort.signal,
      );
      if (abort.signal.aborted) return;
      if (
        reply.owner_scope_tag !== original.owner_scope_tag ||
        reply.results?.execution_id !== execution.execution_id ||
        reply.results.artifact_sha256 !== execution.artifact_sha256
      )
        throw new Error("原结果不匹配，请继续查看原请求。");
      setResult(reply.results);
      setCursors((old) => (index > page ? [...old.slice(0, index), cursor ?? null] : old));
      setPage(index);
    } catch (caught) {
      if (!abort.signal.aborted) setError(text(caught));
    } finally {
      if (!abort.signal.aborted) setBusy(false);
    }
  }
  const rows =
    result?.execution_id === execution.execution_id &&
    result.artifact_sha256 === execution.artifact_sha256
      ? result.rows
      : [];
  return (
    <>
      <Panel title="原筛选结果" sub={execution.definition.trade_date}>
        {execution.unknown_count ? (
          <p className="hint">未判定 {formatCount(execution.unknown_count)} 只</p>
        ) : null}
        <DataTable
          rows={rows}
          columns={execution.ranked_count === null ? resultColumns.slice(0, 2) : resultColumns}
          rowKey={(row) => row.ts_code}
          label="助手筛选结果"
          onSelect={(row) => setStock(row.ts_code)}
        />
        <div className="row">
          <Button size="sm" disabled={busy || page === 0} onClick={() => void move(page - 1)}>
            上一页
          </Button>
          <span className="hint">第 {page + 1} 页</span>
          <Button
            size="sm"
            disabled={busy || !result?.next_cursor}
            onClick={() => void move(page + 1)}
          >
            下一页
          </Button>
        </div>
        {error ? <p role="alert">{error}</p> : null}
      </Panel>
      <StockDrawer tsCode={stock} onClose={() => setStock(null)} />
    </>
  );
}

export function AiAssistantDrawer({
  open,
  viewer,
  onClose,
  onClosed,
}: {
  open: boolean;
  viewer: string | null;
  onClose: () => void;
  onClosed?: () => void;
}) {
  const capability = useAiCapabilities(viewer, open);
  return (
    <SideDrawer
      open={open}
      onClose={onClose}
      afterOpenChange={(visible) => {
        if (!visible) onClosed?.();
      }}
      title="AI 助手"
      wide
    >
      {!viewer ? (
        <EmptyState title="请先登录" hint="登录后可查看本人的建议和用量。" />
      ) : capability.isLoading ? (
        <SkeletonRows rows={3} />
      ) : capability.data?.available ? (
        <AssistantWorkflow key={viewer} viewer={viewer} />
      ) : (
        <EmptyState
          title={capability.error?.message ?? capability.data?.message ?? "助手暂不可用。"}
          hint={<a href="#/screener">打开条件筛选</a>}
        />
      )}
    </SideDrawer>
  );
}
