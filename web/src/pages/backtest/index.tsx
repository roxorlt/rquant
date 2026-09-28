import { useCallback, useEffect, useRef, useState } from "react";
import {
  type BacktestGroup,
  type BacktestGroupKey,
  type BacktestTrade,
  useBacktestDetail,
  useBacktestRuns,
} from "@/api/backtests";
import { ApiError } from "@/api/client";
import { StockDrawer } from "@/app/StockDrawer";
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
import "./backtest.css";

const PAGE_SIZE = 20;

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

export default function BacktestPage() {
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
                        第 {Math.floor(visibleTradeOffset / PAGE_SIZE) + 1} 页 · 每页 {PAGE_SIZE} 笔
                      </span>
                      <Button
                        size="sm"
                        disabled={visibleTradeOffset === 0}
                        onClick={() => setTradeOffset(Math.max(0, visibleTradeOffset - PAGE_SIZE))}
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
