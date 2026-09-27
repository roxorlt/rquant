import { useState } from "react";
import { type PoolMember, type PublishedPool, usePools } from "@/api/endpoints";
import { useMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import { FlowGraph } from "@/charts/FlowGraph";
import { formatCount, formatPrice } from "@/format/number";
import { formatTradeDate } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { ChangeText, EmptyState, PageHeader, PageSkeleton, Panel, Tip } from "@/ui";
import "./pools.css";

const MEMBER_COLUMNS: DataColumn<PoolMember>[] = [
  {
    id: "name",
    header: "股票",
    value: (row) => row.name ?? row.code,
    cell: (row) => (
      <span>
        {row.name ?? row.code} <span className="mono pool-code">{row.code}</span>
      </span>
    ),
    sortable: true,
  },
  {
    id: "close",
    header: "收盘价",
    value: (row) => row.close,
    cell: (row) => <span className="num">{formatPrice(row.close)}</span>,
    numeric: true,
    secondary: true,
  },
  {
    id: "change",
    header: "涨跌幅",
    value: (row) => row.pct_chg,
    cell: (row) => <ChangeText value={row.pct_chg} />,
    numeric: true,
    secondary: true,
  },
];

function poolStatus(pool: PublishedPool): string {
  if (pool.state === "missing") return "引用的池子已失效";
  if (pool.state === "older") return "不是最新交易日的结果";
  if (pool.state === "unavailable") return "池子结果暂不可用";
  if (pool.state === "no_data") return "这只池子暂无可确认的最新结果";
  return `${formatCount(pool.member_count)} 只`;
}

export default function PoolsPage() {
  const query = usePools();
  const meta = useMeta();
  const [canvasName, setCanvasName] = useState<string | null>(null);
  const [poolKey, setPoolKey] = useState<string | null>(null);
  const [stockCode, setStockCode] = useState<string | null>(null);
  const data = query.data;
  const newest = meta.data?.serving.generation_id;
  const visibleGeneration = query.serving?.generation_id;
  const changing =
    meta.isError ||
    (newest !== undefined && visibleGeneration !== undefined && newest !== visibleGeneration);
  const canvas =
    canvasName === ""
      ? undefined
      : (data?.canvases.find((item) => item.name === canvasName) ?? data?.canvases[0]);
  const shown = canvas
    ? canvas.pool_keys.flatMap((key) => data?.pools.find((pool) => pool.key === key) ?? [])
    : (data?.pools ?? []);
  const selected = shown.find((pool) => pool.key === poolKey) ?? shown[0];
  const graphPools = shown.filter((pool) => pool.state !== "missing");

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="池子画布"
        note={
          !changing && data?.state === "ready" && data.latest_trade_date
            ? `${formatTradeDate(data.latest_trade_date)} · 最近发布`
            : undefined
        }
      />
      {query.isLoading ? (
        <PageSkeleton />
      ) : query.error || changing ? (
        <EmptyState title="池子数据正在更新" hint="稍后刷新页面再查看。" />
      ) : data?.state === "unavailable" ? (
        <EmptyState title="池子结果暂不可用" hint="发布后会在这里显示，请稍后刷新。" />
      ) : data?.state === "no_data" ? (
        <EmptyState title="还没有已发布的池子" hint="首次筛选并发布后会在这里显示。" />
      ) : data ? (
        <div className="pools-page">
          <div className="pools-toolbar">
            {data.canvases.length > 0 ? (
              <label className="pools-canvas-picker">
                <span>画布</span>
                <select
                  aria-label="选择画布"
                  value={canvas?.name ?? ""}
                  onChange={(event) => {
                    setCanvasName(event.target.value);
                    setPoolKey(null);
                  }}
                >
                  <option value="">全部已发布池子</option>
                  {data.canvases.map((item) => (
                    <option key={item.name} value={item.name}>
                      {item.name}
                    </option>
                  ))}
                </select>
              </label>
            ) : (
              <span className="hint">已发布池子</span>
            )}
            {canvas?.description ? (
              <Tip content={canvas.description}>
                <span className="pools-info">画布说明</span>
              </Tip>
            ) : null}
          </div>
          {!data.definitions_available ? (
            <p className="pools-note">保存的画布暂不可用，显示已发布池子。</p>
          ) : null}
          {data.canvases_truncated || data.pools_truncated || canvas?.refs_truncated ? (
            <p className="pools-note" role="status">
              内容较多，仅显示前一部分池子或画布。
            </p>
          ) : null}
          {canvas && shown.length === 0 ? (
            <EmptyState title="这张画布暂无可查看的池子" hint="池子发布后会显示。" />
          ) : (
            <div className="pools-layout">
              <Panel title="池子分布" sub="选择池子查看成员" label="池子分布">
                {graphPools.length ? (
                  <FlowGraph
                    nodes={graphPools.map((pool) => ({ id: pool.key, label: pool.name }))}
                    edges={[]}
                    label="已发布池子；关系尚未发布"
                    onSelect={setPoolKey}
                  />
                ) : (
                  <EmptyState title="没有可显示的池子节点" />
                )}
                <Tip content="当前只发布画布名称和池子引用，没有规则或依赖关系；节点之间暂无线条。">
                  <span className="pools-info">关于连线</span>
                </Tip>
                <fieldset className="pools-list" aria-label="池子列表">
                  {shown.map((pool) => (
                    <Tip key={pool.key} content={pool.key} interactive>
                      <button
                        type="button"
                        className="pools-list-item"
                        aria-pressed={selected?.key === pool.key}
                        onClick={() => setPoolKey(pool.key)}
                      >
                        <span>{pool.name}</span>
                        <small>{poolStatus(pool)}</small>
                      </button>
                    </Tip>
                  ))}
                </fieldset>
              </Panel>
              <div className="pools-detail">
                {selected ? (
                  <>
                    <Panel
                      title={selected.name}
                      sub={selected.trade_date ? formatTradeDate(selected.trade_date) : "—"}
                      label="池子详情"
                    >
                      {selected.state === "current" ? (
                        <div className="pools-count">
                          <span>最新成员</span>
                          <strong className="num">{formatCount(selected.member_count)} 只</strong>
                        </div>
                      ) : (
                        <EmptyState
                          title={poolStatus(selected)}
                          hint={
                            selected.state === "older"
                              ? "最近交易日没有这只池子的成员，不能沿用旧日人数。"
                              : selected.state === "missing"
                                ? "保存画布中的引用找不到对应的已发布池子。"
                                : "这只池子的最新成员尚未发布。"
                          }
                        />
                      )}
                      {selected.state === "current" ? (
                        <div className="pools-steps">
                          <h3>命中步骤</h3>
                          {selected.steps.length ? (
                            selected.steps.map((step) => (
                              <div className="pools-step" key={step.step_index}>
                                <span>{step.label}</span>
                                <strong className="num">{formatCount(step.count)} 只</strong>
                              </div>
                            ))
                          ) : (
                            <span className="hint">暂无已发布的命中步骤</span>
                          )}
                          {selected.steps_truncated ? (
                            <p className="pools-note">仅显示前一部分命中步骤。</p>
                          ) : null}
                        </div>
                      ) : null}
                    </Panel>
                    {selected.state === "current" ? (
                      <Panel
                        title="成员"
                        sub={
                          selected.members_truncated
                            ? `仅显示前 ${formatCount(selected.members.length)} 只`
                            : undefined
                        }
                        label="池子成员"
                        flush
                      >
                        <DataTable
                          rows={selected.members}
                          columns={MEMBER_COLUMNS}
                          rowKey={(row) => row.code}
                          label="池子成员"
                          onSelect={(row) => setStockCode(row.code)}
                          emptyText="本次没有成员"
                        />
                      </Panel>
                    ) : null}
                  </>
                ) : (
                  <EmptyState title="暂无可查看的池子" />
                )}
              </div>
            </div>
          )}
        </div>
      ) : (
        <EmptyState title="池子结果暂不可用" />
      )}
      <StockDrawer tsCode={stockCode} onClose={() => setStockCode(null)} />
    </>
  );
}
