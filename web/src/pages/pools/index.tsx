import { useState } from "react";
import { type PoolMember, type PublishedPool, usePools } from "@/api/endpoints";
import { useMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import { FlowGraph, type FlowGraphEdge, type FlowGraphNode } from "@/charts/FlowGraph";
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
  if (pool.state === "unpublished") return "尚无已发布结果";
  if (pool.state === "older") return "不是最新交易日的结果";
  if (pool.state === "unavailable") return "结果暂不可用";
  if (pool.state === "no_data") return "暂无可确认的结果";
  return `${formatCount(pool.member_count)} 只`;
}

function parentStatus(
  pool: PublishedPool,
  shown: readonly PublishedPool[],
  all: readonly PublishedPool[],
  truncated: boolean,
): string | null {
  const parentKey = pool.definition?.depends_on;
  if (!parentKey) return null;
  const visible = shown.find((item) => item.key === parentKey);
  if (visible?.definition?.state === "available") return visible.name;
  if (visible) return "父池规则暂不可查看，未绘制连线";
  if (all.some((item) => item.key === parentKey)) return "父池不在当前画布，未绘制连线";
  return truncated ? "父池未显示，列表已达上限" : "父池暂不可查看，未绘制连线";
}

function RulesDetail({
  pool,
  shown,
  all,
  truncated,
  rulesAvailable,
}: {
  pool: PublishedPool;
  shown: readonly PublishedPool[];
  all: readonly PublishedPool[];
  truncated: boolean;
  rulesAvailable: boolean;
}) {
  const definition = pool.definition;
  const parent = parentStatus(pool, shown, all, truncated);
  return (
    <Panel title="规则" sub={definition?.status_label ?? "暂不可查看"} label="规则详情">
      {!definition || !rulesAvailable ? (
        <EmptyState
          title={rulesAvailable ? "这只池子的规则尚未发布" : "规则暂不可查看"}
          hint="规则发布后会在这里显示；上次成员仍可单独查看。"
        />
      ) : definition.state !== "available" ? (
        <EmptyState
          title={definition.status_label}
          hint={definition.reason_label ?? "规则暂不可查看。"}
        />
      ) : (
        <div className="pools-rule-body">
          <div className="pools-rule-source">
            <span>{definition.source_label}</span>
            {definition.description ? (
              <Tip content={definition.description}>
                <span className="pools-info">规则说明</span>
              </Tip>
            ) : null}
          </div>
          {definition.depends_on ? (
            <div className="pools-rule-parent">
              <span>筛选来源</span>
              <strong>{parent}</strong>
              {definition.delay_label ? <small>{definition.delay_label}</small> : null}
            </div>
          ) : (
            <p className="pools-rule-root">独立筛选</p>
          )}
          {definition.rules.length ? (
            <ol className="pools-rules" aria-label="已发布条件">
              {definition.rules.map((rule, index) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: Published rule order is fixed, and repeated conditions are valid.
                <li className="pools-rule" key={`${rule.label}-${index}`}>
                  <span className="pools-rule-index num">{String(index + 1).padStart(2, "0")}</span>
                  <div>
                    <h3>{rule.label}</h3>
                    {rule.parameters.length ? (
                      <dl>
                        {rule.parameters.map((parameter) => (
                          <div key={parameter.label}>
                            <dt>{parameter.label}</dt>
                            <dd>{parameter.value}</dd>
                          </div>
                        ))}
                      </dl>
                    ) : null}
                  </div>
                </li>
              ))}
            </ol>
          ) : (
            <p className="pools-note">这只池子没有附加筛选条件。</p>
          )}
        </div>
      )}
    </Panel>
  );
}

function ResultsDetail({
  pool,
  onSelectStock,
}: {
  pool: PublishedPool;
  onSelectStock: (code: string) => void;
}) {
  return (
    <>
      <Panel
        title="上次选股结果"
        sub={pool.trade_date ? formatTradeDate(pool.trade_date) : "—"}
        label="上次选股结果"
      >
        {pool.state === "current" ? (
          <>
            <div className="pools-count">
              <span>成员</span>
              <strong className="num">{formatCount(pool.member_count)} 只</strong>
            </div>
            <div className="pools-steps">
              <h3>上次命中步骤</h3>
              {pool.steps.length ? (
                pool.steps.map((step) => (
                  <div className="pools-step" key={step.step_index}>
                    <span>{step.label}</span>
                    <strong className="num">{formatCount(step.count)} 只</strong>
                  </div>
                ))
              ) : (
                <span className="hint">暂无已发布的命中步骤</span>
              )}
              {pool.steps_truncated ? <p className="pools-note">仅显示前一部分命中步骤。</p> : null}
            </div>
          </>
        ) : (
          <EmptyState
            title={poolStatus(pool)}
            hint={
              pool.state === "older"
                ? "最近交易日没有这只池子的成员，不能沿用旧日人数。"
                : pool.state === "unpublished"
                  ? "这只池子还没有可确认的选股结果。"
                  : "成员发布后会在这里显示。"
            }
          />
        )}
      </Panel>
      {pool.state === "current" ? (
        <Panel
          title="成员"
          sub={
            pool.members_truncated ? `仅显示前 ${formatCount(pool.members.length)} 只` : undefined
          }
          label="池子成员"
          flush
        >
          <DataTable
            rows={pool.members}
            columns={MEMBER_COLUMNS}
            rowKey={(row) => row.code}
            label="池子成员"
            onSelect={(row) => onSelectStock(row.code)}
            emptyText="本次没有成员"
          />
        </Panel>
      ) : null}
    </>
  );
}

export default function PoolsPage() {
  const query = usePools();
  const meta = useMeta();
  const [canvasName, setCanvasName] = useState<string | null>(null);
  const [selectionId, setSelectionId] = useState<string | null>(null);
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
  const selectedKey = selectionId?.startsWith("condition:")
    ? selectionId.slice("condition:".length)
    : selectionId;
  const selected = shown.find((pool) => pool.key === selectedKey) ?? shown[0];
  const selectedKind =
    selectionId?.startsWith("condition:") && selected?.key === selectedKey ? "condition" : "pool";
  const graphPools = shown.filter(
    (pool) =>
      pool.definition?.state === "available" ||
      (pool.state !== "unpublished" && pool.state !== "unavailable"),
  );
  const graphNodes: FlowGraphNode[] = graphPools.flatMap((pool) => [
    ...(pool.definition?.state === "available"
      ? [
          {
            id: `condition:${pool.key}`,
            label: `${pool.name} · ${pool.definition.rules.length} 条条件`,
          },
        ]
      : []),
    { id: pool.key, label: pool.name },
  ]);
  const graphEdges: FlowGraphEdge[] = graphPools.flatMap((pool) => {
    if (pool.definition?.state !== "available") return [];
    const edges: FlowGraphEdge[] = [{ source: `condition:${pool.key}`, target: pool.key }];
    const parent = shown.find((item) => item.key === pool.definition?.depends_on);
    if (parent && parent.key !== pool.key && parent.definition?.state === "available") {
      edges.push({ source: parent.key, target: `condition:${pool.key}` });
    }
    return edges;
  });
  const hasRules = graphPools.some((pool) => pool.definition?.state === "available");

  return (
    <>
      <PageHeader
        eyebrow="研究"
        title="池子画布"
        note={
          !changing && data?.latest_trade_date
            ? `上次选股 · ${formatTradeDate(data.latest_trade_date)}`
            : undefined
        }
      />
      {query.isLoading ? (
        <PageSkeleton />
      ) : query.error || changing ? (
        <EmptyState title="池子数据正在更新" hint="稍后刷新页面再查看。" />
      ) : data?.pools.length ? (
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
                    setSelectionId(null);
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
          <p className="pools-note">上次结果与当前规则的对应关系尚未确认。</p>
          {data.canvases_truncated || data.pools_truncated || canvas?.refs_truncated ? (
            <p className="pools-note" role="status">
              内容较多，仅显示前一部分池子或画布。
            </p>
          ) : null}
          {canvas && shown.length === 0 ? (
            <EmptyState title="这张画布暂无可查看的池子" hint="池子发布后会显示。" />
          ) : (
            <div className="pools-layout">
              <Panel title="池子分布" sub="选择条件或池子" label="池子分布">
                {graphNodes.length ? (
                  <FlowGraph
                    nodes={graphNodes}
                    edges={graphEdges}
                    label={hasRules ? "已发布规则与池子" : "已发布池子；关系尚未发布"}
                    onSelect={setSelectionId}
                  />
                ) : (
                  <EmptyState title="暂无可查看的池子节点" />
                )}
                <Tip
                  content={
                    hasRules
                      ? "画布只决定显示哪些池子；连线仅来自已发布的规则依赖。"
                      : "规则发布后，条件和池子依赖会在这里显示。"
                  }
                >
                  <span className="pools-info">连线说明</span>
                </Tip>
                <fieldset className="pools-list" aria-label="池子列表">
                  {shown.map((pool) => (
                    <div className="pools-list-entry" key={pool.key}>
                      <button
                        type="button"
                        className="pools-list-item"
                        aria-label={`查看 ${pool.name}成员`}
                        aria-pressed={selected?.key === pool.key && selectedKind === "pool"}
                        onClick={() => setSelectionId(pool.key)}
                      >
                        <span>{pool.name}</span>
                        <small>上次结果 · {poolStatus(pool)}</small>
                      </button>
                      {pool.definition?.state === "available" ? (
                        <button
                          type="button"
                          className="pools-rule-button"
                          aria-label={`查看 ${pool.name}条件`}
                          aria-pressed={selected?.key === pool.key && selectedKind === "condition"}
                          onClick={() => setSelectionId(`condition:${pool.key}`)}
                        >
                          已发布条件 <span className="num">{pool.definition.rules.length}</span>
                        </button>
                      ) : (
                        <span className="pools-rule-unavailable">
                          规则 · {pool.definition?.status_label ?? "暂不可查看"}
                        </span>
                      )}
                    </div>
                  ))}
                </fieldset>
              </Panel>
              <section className="pools-detail" aria-label="池子详情">
                {selected ? (
                  selectedKind === "condition" ? (
                    <>
                      <RulesDetail
                        pool={selected}
                        shown={shown}
                        all={data.pools}
                        truncated={data.pools_truncated}
                        rulesAvailable={data.rules_available}
                      />
                      <ResultsDetail pool={selected} onSelectStock={setStockCode} />
                    </>
                  ) : (
                    <>
                      <ResultsDetail pool={selected} onSelectStock={setStockCode} />
                      <RulesDetail
                        pool={selected}
                        shown={shown}
                        all={data.pools}
                        truncated={data.pools_truncated}
                        rulesAvailable={data.rules_available}
                      />
                    </>
                  )
                ) : (
                  <EmptyState title="暂无可查看的池子" />
                )}
              </section>
            </div>
          )}
        </div>
      ) : data?.state === "unavailable" ? (
        <EmptyState title="池子结果暂不可用" hint="结果或规则发布后会在这里显示。" />
      ) : (
        <EmptyState title="还没有已发布的池子" hint="规则或选股结果发布后会在这里显示。" />
      )}
      <StockDrawer tsCode={stockCode} onClose={() => setStockCode(null)} />
    </>
  );
}
