import { useEffect, useRef, useState } from "react";
import {
  fetchScreenExecution,
  type ScreenExecutionView,
  type ScreenOriginalAction,
  type ScreenQueryDefinition,
  useScreenQueryHistory,
} from "@/api/screen";
import { Button, EmptyState, RelativeTime, SideDrawer, SkeletonRows, Tip } from "@/ui";

export function ScreenQueryHistory({
  viewer,
  open,
  onClose,
  afterOpenChange,
  onRestore,
  onInspect,
  onRecover,
}: {
  viewer: string | null;
  open: boolean;
  onClose: () => void;
  afterOpenChange?: (open: boolean) => void;
  onRestore: (definition: ScreenQueryDefinition) => void;
  onInspect: (execution: ScreenExecutionView) => void;
  onRecover: (original: ScreenOriginalAction) => void;
}) {
  const [cursors, setCursors] = useState<(string | null)[]>([null]);
  const [page, setPage] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const query = useScreenQueryHistory(viewer, cursors[page] ?? null, open);
  const history = query.data?.history;
  const ownerScope = query.data?.owner_scope_tag;
  const pending = useRef<AbortController | null>(null);
  useEffect(() => {
    if (viewer === null || !open || ownerScope == null) {
      pending.current?.abort();
      pending.current = null;
    }
    setBusy(false);
    return () => {
      pending.current?.abort();
      pending.current = null;
    };
  }, [viewer, open, ownerScope]);
  async function detail(entry: ScreenExecutionView, operation: "restore" | "inspect" | "recover") {
    if (busy) return;
    const controller = new AbortController();
    pending.current = controller;
    setBusy(true);
    setError(null);
    try {
      const data = await fetchScreenExecution(entry.execution_id, controller.signal);
      if (controller.signal.aborted || pending.current !== controller) return;
      if (data.owner_scope_tag !== query.data?.owner_scope_tag || !data.execution)
        throw new Error("这次选股暂不可用，请重试。");
      if (operation === "restore") onRestore(data.execution.definition);
      else if (operation === "inspect") onInspect(data.execution);
      else onRecover({ action: "execute", command: data.execution.original_command });
    } catch (caught) {
      if (!controller.signal.aborted && pending.current === controller)
        setError(caught instanceof Error ? caught.message : "这次选股暂不可用，请重试。");
    } finally {
      if (!controller.signal.aborted && pending.current === controller) {
        pending.current = null;
        setBusy(false);
      }
    }
  }
  return (
    <SideDrawer
      open={open}
      onClose={onClose}
      afterOpenChange={afterOpenChange}
      title="选股历史"
      wide
    >
      {query.isLoading ? (
        <SkeletonRows rows={4} />
      ) : query.error ? (
        <div role="alert">
          <p>历史暂不可用，请重试。</p>
          <Button
            onClick={() => {
              void query.refetch();
            }}
          >
            重试
          </Button>
        </div>
      ) : history?.items.length === 0 ? (
        <EmptyState title="还没有选股记录" hint="运行后会保留完整条件和结果。" />
      ) : null}
      {error ? <p role="alert">{error}</p> : null}
      <ol className="screen-query-list">
        {(history?.items ?? []).map((entry) => (
          <li key={entry.execution_id}>
            <div className="screen-query-head">
              <strong>{entry.definition.description || "条件选股"}</strong>
              <span>{entry.definition.mode === "intraday" ? "盘中" : "日线"}</span>
            </div>
            <div className="screen-query-facts">
              {entry.started_at ? <RelativeTime at={entry.started_at} /> : <span>尚未运行</span>}
              <Tip
                content={`数据日期 ${entry.definition.trade_date}；${entry.definition.conditions.length} 条条件${entry.definition.ranking ? `；取前 ${entry.definition.ranking.top_n} 只` : ""}`}
              >
                <span>{entry.definition.conditions.length} 条条件</span>
              </Tip>
              <span>
                {entry.status === "succeeded" && entry.total != null
                  ? `命中 ${entry.total.toLocaleString("zh-CN")} 只`
                  : entry.status === "failed" || entry.status === "source_expired"
                    ? "本次未完成"
                    : "结果待确认"}
              </span>
            </div>
            <div className="screen-query-actions">
              <Button
                size="sm"
                onClick={() => {
                  void detail(entry, "restore");
                }}
                disabled={busy}
              >
                回填条件
              </Button>
              {entry.status === "succeeded" ? (
                <Button
                  size="sm"
                  onClick={() => {
                    void detail(entry, "inspect");
                  }}
                  disabled={busy}
                >
                  查看原结果
                </Button>
              ) : ["pending", "processing", "unknown"].includes(entry.status) ? (
                <Button
                  size="sm"
                  onClick={() => {
                    void detail(entry, "recover");
                  }}
                  disabled={busy}
                >
                  恢复原请求
                </Button>
              ) : null}
            </div>
          </li>
        ))}
      </ol>
      {history ? (
        <div className="screen-query-actions">
          <Button
            size="sm"
            onClick={() => setPage((current) => current - 1)}
            disabled={page === 0 || query.isFetching}
          >
            较新记录
          </Button>
          <Button
            size="sm"
            disabled={!history.next_cursor || query.isFetching}
            onClick={() => {
              setCursors((current) => [...current.slice(0, page + 1), history.next_cursor ?? null]);
              setPage((current) => current + 1);
            }}
          >
            更早记录
          </Button>
        </div>
      ) : null}
    </SideDrawer>
  );
}
