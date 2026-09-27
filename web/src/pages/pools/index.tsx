import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import { type PoolMember, type PublishedPool, usePools } from "@/api/endpoints";
import {
  type BuiltinPoolCopySource,
  type EditablePool,
  submitPoolEditorCommand,
  usePoolEditor,
} from "@/api/poolEditor";
import { useMeta } from "@/api/useMeta";
import { StockDrawer } from "@/app/StockDrawer";
import { FlowGraph, type FlowGraphEdge, type FlowGraphNode } from "@/charts/FlowGraph";
import { formatCount, formatPrice } from "@/format/number";
import { formatTradeDate, weekdayOf } from "@/format/time";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { Button, ChangeText, EmptyState, PageHeader, PageSkeleton, Panel, Tip } from "@/ui";
import { CanvasCreateForm, canvasCreateLabel } from "./CanvasCreateForm";
import { CanvasCreateSession } from "./canvasCreateSession";
import { canvasPublicationStage } from "./canvasPublication";
import { publicationStage } from "./editorPublication";
import { PoolEditorSession } from "./editorSession";
import { FirstPoolAction } from "./FirstPoolAction";
import { PoolEditorForm } from "./PoolEditorForm";
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
    id: "entry_date",
    header: "入池日",
    value: (row) => row.entry_trade_date,
    cell: (row) => (
      <span className="pool-entry-cell">
        <time className="num" dateTime={row.entry_trade_date ?? undefined}>
          {row.entry_trade_date ?? "—"}
        </time>
        <span className="pool-entry-phone">
          入池收盘价 <span className="num">{formatPrice(row.entry_close)}</span>
        </span>
      </span>
    ),
    sortable: true,
  },
  {
    id: "entry_close",
    header: "入池日收盘价",
    value: (row) => row.entry_close,
    cell: (row) => <span className="num">{formatPrice(row.entry_close)}</span>,
    numeric: true,
    secondary: true,
  },
  {
    id: "gain",
    header: "入池后复权涨幅",
    value: (row) => row.gain_pct,
    cell: (row) => (
      <span className="pool-gain-cell">
        <ChangeText value={row.gain_pct} />
        {row.gain_through_date ? (
          <time dateTime={row.gain_through_date}>截至 {row.gain_through_date.slice(5)}</time>
        ) : null}
      </span>
    ),
    numeric: true,
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
  awaitingNewRules = false,
}: {
  pool: PublishedPool;
  shown: readonly PublishedPool[];
  all: readonly PublishedPool[];
  truncated: boolean;
  rulesAvailable: boolean;
  awaitingNewRules?: boolean;
}) {
  const definition = pool.definition;
  const parent = parentStatus(pool, shown, all, truncated);
  return (
    <Panel title="规则" sub={definition?.status_label ?? "暂不可查看"} label="规则详情">
      {awaitingNewRules ? (
        <EmptyState title="新规则等待发布" hint="保存的条件发布后会在这里显示。" />
      ) : !definition || !rulesAvailable ? (
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
  awaitingNewResult = false,
}: {
  pool: PublishedPool;
  onSelectStock: (member: PoolMember, poolKey: string) => void;
  awaitingNewResult?: boolean;
}) {
  const resultDate = pool.result.trade_date ?? pool.trade_date;
  return (
    <>
      <Panel title="上次选股结果" sub={pool.result.status_label} label="上次选股结果">
        <p className="pools-result-date">
          选股日期 · {resultDate ? `${resultDate} ${weekdayOf(resultDate)}` : "—"}
        </p>
        {awaitingNewResult ? (
          <EmptyState title="等待新规则选股" hint="保存的规则生效后，成员会随下次选股结果更新。" />
        ) : pool.state === "current" ? (
          <>
            {pool.result.zero_hit_label ? (
              <p className="pools-zero" role="status">
                {pool.result.zero_hit_label}
              </p>
            ) : null}
            <div className="pools-count">
              <span>成员</span>
              <strong className="num">{formatCount(pool.member_count)} 只</strong>
            </div>
            {pool.member_count ? (
              <div className="pools-return-summary">
                <span>涨幅已核验</span>
                <strong className="num">
                  {formatCount(pool.gain_verified_count)}/{formatCount(pool.member_count)} 只
                </strong>
                {pool.gain_sample_avg_pct !== null ? (
                  <>
                    <span>已核验样本平均</span>
                    <strong>
                      <ChangeText value={pool.gain_sample_avg_pct} />
                    </strong>
                  </>
                ) : null}
              </div>
            ) : null}
            {pool.steps.length ? (
              <div className="pools-steps">
                <h3>上次命中步骤</h3>
                {pool.steps.map((step) => (
                  <div className="pools-step" key={step.step_index}>
                    <span>{step.label}</span>
                    <strong className="num">{formatCount(step.count)} 只</strong>
                  </div>
                ))}
                {pool.steps_truncated ? (
                  <p className="pools-note">仅显示前一部分命中步骤。</p>
                ) : null}
              </div>
            ) : null}
          </>
        ) : pool.result.state === "older_rules" ? (
          <div className="pools-older-result">
            <div className="pools-count">
              <span>上次命中</span>
              <strong className="num">{formatCount(pool.result.hit_count)} 只</strong>
            </div>
            <p className="pools-note">最近交易日未运行，暂不显示旧日成员。</p>
          </div>
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
      {!awaitingNewResult && pool.state === "current" && pool.members.length > 0 ? (
        <Panel
          title="成员"
          sub={
            pool.members_truncated ? `仅显示前 ${formatCount(pool.members.length)} 只` : undefined
          }
          actions={
            <Tip content="入池价取入池日收盘价；涨幅使用复权价格，按已核验样本统计。暂无可靠记录时显示—。">
              <span className="pools-info">入池价说明</span>
            </Tip>
          }
          label="池子成员"
          flush
        >
          <DataTable
            rows={pool.members}
            columns={MEMBER_COLUMNS}
            rowKey={(row) => row.code}
            label="池子成员"
            onSelect={(row) => onSelectStock(row, pool.key)}
            emptyText="本次没有成员"
          />
        </Panel>
      ) : null}
    </>
  );
}

function browserStorage(): Storage {
  try {
    return window.sessionStorage;
  } catch {
    return {
      getItem: () => {
        throw new Error("storage unavailable");
      },
      setItem: () => {
        throw new Error("storage unavailable");
      },
      removeItem: () => {
        throw new Error("storage unavailable");
      },
    } as unknown as Storage;
  }
}

export default function PoolsPage() {
  const query = usePools();
  const editorQuery = usePoolEditor();
  const meta = useMeta();
  const [editorSession] = useState(
    () =>
      new PoolEditorSession(
        browserStorage(),
        submitPoolEditorCommand,
        () =>
          `web-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (item) => item.toString(16).padStart(2, "0")).join("")}`,
        () => new Date().toISOString(),
      ),
  );
  const editorSnapshot = useSyncExternalStore(
    editorSession.subscribe,
    editorSession.snapshot,
    editorSession.snapshot,
  );
  const [canvasCreateSession] = useState(
    () =>
      new CanvasCreateSession(
        browserStorage(),
        submitPoolEditorCommand,
        () =>
          `web-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (item) => item.toString(16).padStart(2, "0")).join("")}`,
        () => new Date().toISOString(),
      ),
  );
  const canvasCreateSnapshot = useSyncExternalStore(
    canvasCreateSession.subscribe,
    canvasCreateSession.snapshot,
    canvasCreateSession.snapshot,
  );
  const canvasAutoRetry = useRef({ commandId: "", attempts: 0 });
  const [canvasDrawerOpen, setCanvasDrawerOpen] = useState(false);
  const autoRetry = useRef({ commandId: "", attempts: 0 });
  const [editorMode, setEditorMode] = useState<
    | { kind: "create"; parentKey: string | null }
    | { kind: "edit"; pool: EditablePool }
    | { kind: "copy"; source: BuiltinPoolCopySource }
    | null
  >(null);
  const [canvasName, setCanvasName] = useState<string | null>(null);
  const [selectionId, setSelectionId] = useState<string | null>(null);
  const [stockSelection, setStockSelection] = useState<{
    code: string;
    poolKey: string;
    generationId: string | null;
  } | null>(null);
  const data = query.data;
  const newest = meta.data?.serving.generation_id;
  const visibleGeneration = query.serving?.generation_id;
  const changing =
    meta.isError ||
    (newest !== undefined && visibleGeneration !== undefined && newest !== visibleGeneration);
  const editorReady =
    !changing &&
    !editorQuery.isLoading &&
    !editorQuery.error &&
    editorQuery.data?.state === "ready" &&
    newest != null &&
    editorQuery.serving?.generation_id === newest &&
    visibleGeneration === newest;
  const editorUnavailable = editorQuery.data?.state === "unavailable";
  const editorNotice = editorUnavailable
    ? "编辑资料暂不可用"
    : editorQuery.error
      ? "编辑资料暂无法加载"
      : "池子数据正在更新";
  const stage = publicationStage(
    editorSnapshot.journal,
    editorQuery.data,
    data,
    editorQuery.serving?.generation_id,
    visibleGeneration,
    newest,
  );
  const canvasStage = canvasPublicationStage(
    canvasCreateSnapshot.journal,
    editorQuery.data,
    data,
    editorQuery.serving?.generation_id,
    visibleGeneration,
    newest,
  );
  const canCreateCanvas =
    editorReady && editorQuery.data?.canvas_create_available === true && !!meta.data?.data.viewer;
  const canvasCreateBlockedReason = !meta.data
    ? "正在加载用户信息。"
    : !meta.data.data.viewer
      ? "请先登录，才能新建画布。"
      : "画布资料正在更新，暂时无法创建。";
  const canvasStatus = canvasCreateLabel(canvasCreateSnapshot, canvasStage === "available");
  const savedKey =
    editorSnapshot.journal?.saveStatus === "succeeded"
      ? `user/${editorSnapshot.journal.save.base_name}`
      : null;

  useEffect(() => {
    const journal = editorSnapshot.journal;
    if (!editorReady || !journal || editorSnapshot.busy) return;
    const savePhase = journal.saveStatus !== "succeeded";
    const status = savePhase ? journal.saveStatus : journal.attachStatus;
    if (!["pending", "processing", "unknown"].includes(status)) return;
    const commandId = savePhase ? journal.save.command_id : journal.attach?.command_id;
    if (!commandId) return;
    if (autoRetry.current.commandId !== commandId) autoRetry.current = { commandId, attempts: 0 };
    if (autoRetry.current.attempts >= 3) return;
    const timer = window.setTimeout(() => {
      autoRetry.current.attempts += 1;
      void editorSession.advance();
    }, 1800);
    return () => window.clearTimeout(timer);
  }, [editorReady, editorSession, editorSnapshot]);

  useEffect(() => {
    const journal = canvasCreateSnapshot.journal;
    if (!canCreateCanvas || !journal || canvasCreateSnapshot.busy) return;
    if (!["pending", "processing", "unknown"].includes(journal.status)) return;
    if (canvasAutoRetry.current.commandId !== journal.body.command_id)
      canvasAutoRetry.current = { commandId: journal.body.command_id, attempts: 0 };
    if (canvasAutoRetry.current.attempts >= 3) return;
    const timer = window.setTimeout(() => {
      canvasAutoRetry.current.attempts += 1;
      void canvasCreateSession.advance();
    }, 1800);
    return () => window.clearTimeout(timer);
  }, [canCreateCanvas, canvasCreateSession, canvasCreateSnapshot]);

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
  const selectedEditable = editorQuery.data?.pools.find((item) => item.key === selected?.key);
  const selectedCopySource = editorQuery.data?.copy_sources.find(
    (item) => item.key === selected?.key,
  );
  const activeMode = editorMode;
  const editorDrawerOpen = activeMode !== null;
  const activePoolKey =
    activeMode?.kind === "edit"
      ? activeMode.pool.key
      : activeMode?.kind === "copy"
        ? activeMode.source.key
        : null;
  const verifiedVersion =
    editorReady && activePoolKey
      ? activeMode?.kind === "edit"
        ? (editorQuery.data?.pools.find((item) => item.key === activePoolKey)?.version ?? null)
        : (editorQuery.data?.copy_sources.find(
            (item) => item.key === activePoolKey && item.copyable,
          )?.version ?? null)
      : null;
  const attachmentVersion =
    editorReady && savedKey
      ? (editorQuery.data?.pools.find((item) => item.key === savedKey)?.version ?? null)
      : null;
  const stockPool = data?.pools.find((pool) => pool.key === stockSelection?.poolKey);
  const stockMember = stockPool?.members.find((member) => member.code === stockSelection?.code);
  const entryMark =
    !changing &&
    stockSelection?.generationId &&
    stockSelection.generationId === visibleGeneration &&
    stockPool?.result.state === "current_rules" &&
    (stockPool.key !== savedKey || stage === "result") &&
    stockMember?.entry_trade_date
      ? {
          date: stockMember.entry_trade_date,
          generationId: stockSelection.generationId,
          linePrice: stockMember.entry_line_price,
          factorChanged: stockMember.gain_pct !== null && stockMember.entry_line_price === null,
        }
      : null;
  const selectStock = (member: PoolMember, poolKey: string) => {
    setStockSelection({ code: member.code, poolKey, generationId: visibleGeneration ?? null });
  };
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
            label:
              pool.key === savedKey && stage !== "published" && stage !== "result"
                ? `${pool.name}\n新规则等待发布`
                : `${pool.name}\n规则已发布 · ${pool.definition.rules.length} 条条件`,
            width: 190,
            height: 82,
          },
        ]
      : []),
    {
      id: pool.key,
      label: `${pool.name}\n${pool.key === savedKey && stage !== "result" ? "等待新规则选股" : pool.result.status_label}`,
      width: 190,
      height: 82,
    },
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
            ? `最近选股记录 · ${formatTradeDate(data.latest_trade_date)}`
            : undefined
        }
      />
      {!canvasDrawerOpen && canvasCreateSnapshot.journal && canvasStatus ? (
        <div className="pools-editor-evidence" role="status" aria-label="画布创建状态">
          <span>
            {canvasCreateSnapshot.journal.body.name} · {canvasStatus}
          </span>
          {canvasStage === "available" ? (
            <Button
              size="sm"
              onClick={() => {
                setCanvasName(canvasCreateSnapshot.journal?.body.name ?? null);
                setSelectionId(null);
              }}
            >
              打开画布
            </Button>
          ) : ["pending", "processing", "unknown", "ambiguous"].includes(
              canvasCreateSnapshot.journal.status,
            ) ? (
            <Button
              size="sm"
              disabledReason={
                !canCreateCanvas
                  ? canvasCreateBlockedReason
                  : canvasCreateSnapshot.busy
                    ? "正在核对，请稍候。"
                    : undefined
              }
              onClick={() => void canvasCreateSession.advance()}
            >
              继续核对
            </Button>
          ) : canvasCreateSnapshot.journal.status === "succeeded" ? (
            <Button
              size="sm"
              onClick={() => {
                void meta.refetch();
                editorQuery.refetch();
                query.refetch();
              }}
            >
              检查发布
            </Button>
          ) : null}
        </div>
      ) : null}
      {!editorDrawerOpen && editorSnapshot.journal?.saveStatus === "succeeded" ? (
        <div className="pools-editor-evidence" role="status">
          <span>池子已保存</span>
          {editorSnapshot.journal.canvasName ? (
            <span>
              {editorSnapshot.journal.attachStatus === "succeeded"
                ? stage === "published" || stage === "result"
                  ? "已加入当前画布"
                  : "加入请求已完成，等待画布更新"
                : editorSnapshot.journal.attachStatus === "failed"
                  ? "画布挂接失败"
                  : "尚未加入当前画布"}
            </span>
          ) : null}
          <span>{stage === "published" || stage === "result" ? "规则已发布" : "等待规则发布"}</span>
          <span>{stage === "result" ? "结果已按当前规则更新" : "等待新规则选股"}</span>
          {editorSnapshot.journal.attachStatus === "failed" ? (
            <>
              <Button
                size="sm"
                disabledReason={
                  !editorReady ||
                  editorSnapshot.journal.attachConflict ||
                  attachmentVersion !== editorSnapshot.journal.saveVersion
                    ? "本次保存的规则已变化，请结束此次挂接。"
                    : undefined
                }
                onClick={() =>
                  void editorSession.retryAttachment(
                    editorQuery.data?.pools.find((item) => item.key === savedKey)?.version ?? null,
                  )
                }
              >
                按本次保存规则重试
              </Button>
              <Button size="sm" onClick={() => editorSession.discardFailedAttachment()}>
                结束本次挂接
              </Button>
            </>
          ) : null}
          {["pending", "processing", "ambiguous", "unknown"].includes(
            editorSnapshot.journal.attachStatus,
          ) ? (
            <Button
              size="sm"
              disabled={editorSnapshot.busy}
              onClick={() => void editorSession.advance()}
            >
              继续核对画布
            </Button>
          ) : null}
          {["ambiguous", "unknown"].includes(editorSnapshot.journal.attachStatus) ? (
            <Button
              size="sm"
              disabled={editorSnapshot.busy}
              onClick={() => editorSession.deferAttachment()}
            >
              留待核对，继续编辑
            </Button>
          ) : null}
        </div>
      ) : !editorDrawerOpen &&
        editorSnapshot.journal &&
        ["ambiguous", "unknown"].includes(editorSnapshot.journal.saveStatus) ? (
        <div className="pools-editor-evidence" role="status">
          <span>保存状态待确认</span>
          <Button size="sm" onClick={() => void editorSession.advance()}>
            继续核对
          </Button>
        </div>
      ) : !editorDrawerOpen && editorSnapshot.journal?.saveStatus === "failed" ? (
        <div className="pools-editor-evidence" role="status">
          <span>{editorSnapshot.journal.saveConflict ? "上次保存未完成" : "上次保存失败"}</span>
          <Button size="sm" onClick={() => editorSession.discardFailedSave()}>
            结束本次编辑
          </Button>
        </div>
      ) : null}
      {editorSnapshot.deferred.map((item) =>
        item.attach ? (
          <div className="pools-editor-evidence" role="status" key={item.attach.command_id}>
            <span>
              {item.save.display_name} ·{" "}
              {item.attachStatus === "succeeded"
                ? ["published", "result"].includes(
                    publicationStage(
                      item,
                      editorQuery.data,
                      data,
                      editorQuery.serving?.generation_id,
                      visibleGeneration,
                      newest,
                    ),
                  )
                  ? "已加入画布"
                  : "加入请求已完成，等待画布更新"
                : item.attachStatus === "failed"
                  ? "画布挂接失败"
                  : "画布状态待确认"}
            </span>
            {["pending", "processing", "ambiguous", "unknown"].includes(item.attachStatus) ? (
              <Button
                size="sm"
                disabled={editorSnapshot.busy}
                onClick={() => void editorSession.advanceDeferred(item.attach?.command_id ?? "")}
              >
                继续核对这次挂接
              </Button>
            ) : null}
            {["failed", "succeeded"].includes(item.attachStatus) ? (
              <Button
                size="sm"
                onClick={() => editorSession.dismissDeferred(item.attach?.command_id ?? "")}
              >
                关闭记录
              </Button>
            ) : null}
          </div>
        ) : null,
      )}
      {query.isLoading ? (
        <PageSkeleton />
      ) : query.error || changing ? (
        <EmptyState title="池子数据正在更新" hint="稍后刷新页面再查看。" />
      ) : data &&
        (data.state === "ready" ||
          data.pools.length > 0 ||
          (data.state !== "unavailable" && data.canvases.length > 0) ||
          editorReady) ? (
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
            <Button
              className="pools-create-canvas-button"
              size="sm"
              disabledReason={canCreateCanvas ? undefined : canvasCreateBlockedReason}
              onClick={() => {
                if (canvasStage === "available") canvasCreateSession.clearTerminal();
                setCanvasDrawerOpen(true);
              }}
            >
              新建画布
            </Button>
            {data.pools.length === 0 ? (
              <FirstPoolAction
                canvas={canvas}
                editor={editorQuery.data}
                editorReady={editorReady}
                editorNotice={editorNotice}
                generationId={visibleGeneration}
                viewer={meta.data?.data.viewer}
                definitionsAvailable={data.definitions_available}
                poolsTruncated={data.pools_truncated}
                storageAvailable={editorSnapshot.storageAvailable}
                onCreate={() => setEditorMode({ kind: "create", parentKey: null })}
              />
            ) : (
              <Button
                className="pools-add-button"
                variant="primary"
                size="sm"
                disabledReason={editorReady ? undefined : `${editorNotice}，暂时无法添加条件。`}
                onClick={() =>
                  setEditorMode({
                    kind: "create",
                    parentKey: selected?.key ?? data.pools[0]?.key ?? null,
                  })
                }
              >
                添加条件节点
              </Button>
            )}
          </div>
          {!editorReady && !editorQuery.isLoading ? (
            <p className="pools-note" role="status">
              {editorNotice}
            </p>
          ) : null}
          {!data.definitions_available ? (
            <p className="pools-note">保存的画布暂不可用，显示已发布池子。</p>
          ) : null}
          {data.canvases_truncated || data.pools_truncated || canvas?.refs_truncated ? (
            <p className="pools-note" role="status">
              内容较多，仅显示前一部分池子或画布。
            </p>
          ) : null}
          {canvas && shown.length === 0 ? (
            <EmptyState
              title="这张画布还是空的"
              hint={
                data.pools.length
                  ? "添加条件节点后，池子会显示在这里。"
                  : "创建首只池子后，这里会显示条件和结果。"
              }
            />
          ) : !canvas && data.pools.length === 0 ? (
            <EmptyState
              title={data.state === "unavailable" ? "池子结果暂不可用" : "还没有已发布的池子"}
              hint={
                data.state === "unavailable"
                  ? "结果或规则发布后会在这里显示。"
                  : "规则或选股结果发布后会在这里显示。"
              }
            />
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
                  {shown.map((pool, index) => (
                    <div className="pools-list-entry" key={pool.key}>
                      <button
                        type="button"
                        className="pools-list-item"
                        aria-label={`查看 ${pool.name}成员`}
                        aria-describedby={`pool-result-${index}`}
                        aria-pressed={selected?.key === pool.key && selectedKind === "pool"}
                        onClick={() => setSelectionId(pool.key)}
                      >
                        <span>{pool.name}</span>
                        <small id={`pool-result-${index}`}>
                          {pool.key === savedKey && stage !== "result"
                            ? "等待新规则选股"
                            : pool.result.status_label}
                        </small>
                        <small>
                          {pool.key === savedKey && stage !== "result"
                            ? "成员待更新"
                            : poolStatus(pool)}
                        </small>
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
                      <div className="pools-detail-actions">
                        {selected.key.startsWith("user/") ? (
                          <Button
                            size="sm"
                            disabledReason={
                              editorReady && selectedEditable
                                ? undefined
                                : "这只池子的可编辑规则暂不可用。"
                            }
                            onClick={() =>
                              selectedEditable &&
                              setEditorMode({ kind: "edit", pool: selectedEditable })
                            }
                          >
                            编辑规则
                          </Button>
                        ) : (
                          <Button
                            size="sm"
                            disabledReason={
                              editorReady &&
                              selectedCopySource?.copyable &&
                              selectedCopySource.delay_mode !== "legacy_window"
                                ? undefined
                                : (selectedCopySource?.copy_block_reason ??
                                  "这只内置池的复制资料暂不可用。")
                            }
                            onClick={() =>
                              selectedCopySource &&
                              setEditorMode({ kind: "copy", source: selectedCopySource })
                            }
                          >
                            复制为自建池
                          </Button>
                        )}
                      </div>
                      <RulesDetail
                        pool={selected}
                        shown={shown}
                        all={data.pools}
                        truncated={data.pools_truncated}
                        rulesAvailable={data.rules_available}
                        awaitingNewRules={
                          selected.key === savedKey && stage !== "published" && stage !== "result"
                        }
                      />
                      <ResultsDetail
                        pool={selected}
                        onSelectStock={selectStock}
                        awaitingNewResult={selected.key === savedKey && stage !== "result"}
                      />
                    </>
                  ) : (
                    <>
                      <ResultsDetail
                        pool={selected}
                        onSelectStock={selectStock}
                        awaitingNewResult={selected.key === savedKey && stage !== "result"}
                      />
                      <RulesDetail
                        pool={selected}
                        shown={shown}
                        all={data.pools}
                        truncated={data.pools_truncated}
                        rulesAvailable={data.rules_available}
                        awaitingNewRules={
                          selected.key === savedKey && stage !== "published" && stage !== "result"
                        }
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
      <StockDrawer
        tsCode={stockSelection?.code ?? null}
        entryMark={entryMark}
        onClose={() => setStockSelection(null)}
      />
      {canvasDrawerOpen ? (
        <CanvasCreateForm
          session={canvasCreateSession}
          snapshot={canvasCreateSnapshot}
          available={canvasStage === "available"}
          canCreate={canCreateCanvas}
          unavailableReason={canvasCreateBlockedReason}
          existingNames={editorQuery.data?.canvases.map((item) => item.name) ?? []}
          onClose={() => setCanvasDrawerOpen(false)}
          onOpen={() => {
            setCanvasName(canvasCreateSnapshot.journal?.body.name ?? null);
            setSelectionId(null);
            setCanvasDrawerOpen(false);
          }}
          onRefresh={() => {
            void meta.refetch();
            editorQuery.refetch();
            query.refetch();
          }}
        />
      ) : null}
      {editorDrawerOpen && activeMode ? (
        <PoolEditorForm
          key={
            activeMode.kind === "edit"
              ? `edit:${activeMode.pool.key}:${activeMode.pool.version}`
              : activeMode.kind === "copy"
                ? `copy:${activeMode.source.key}:${activeMode.source.version}`
                : `create:${activeMode.parentKey ?? ""}`
          }
          mode={activeMode}
          publishedPools={data?.pools ?? []}
          canvases={editorQuery.data?.canvases ?? []}
          currentCanvas={canvas?.name ?? null}
          generationId={editorReady ? (visibleGeneration ?? null) : null}
          verifiedVersion={verifiedVersion}
          attachmentVersion={attachmentVersion}
          session={editorSession}
          snapshot={editorSnapshot}
          publicationStage={stage}
          onClose={() => setEditorMode(null)}
          onRestart={() => {
            if (!editorReady) return;
            if (activeMode.kind === "edit") {
              const latest = editorQuery.data?.pools.find(
                (item) => item.key === activeMode.pool.key,
              );
              if (latest) setEditorMode({ kind: "edit", pool: latest });
            } else if (activeMode.kind === "copy") {
              const latest = editorQuery.data?.copy_sources.find(
                (item) => item.key === activeMode.source.key && item.copyable,
              );
              if (latest) setEditorMode({ kind: "copy", source: latest });
            }
          }}
        />
      ) : null}
    </>
  );
}
