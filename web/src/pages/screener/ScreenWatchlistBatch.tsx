import { useEffect, useState } from "react";
import { useManualWatchlist } from "@/api/manualWatchlist";
import {
  type BatchCandidate,
  type BatchItemStatus,
  batchCounts,
  freezeScreenWatchlistPage,
  useManualWatchlistBatch,
} from "@/api/manualWatchlistBatch";
import type { ScreenRunData } from "@/api/screen";
import { Button, ConfirmDialog } from "@/ui";

const STATUS_LABEL: Record<BatchItemStatus, string> = {
  added: "已加入",
  already: "已在名单",
  syncing: "已保存，正在同步",
  processing: "正在处理",
  uncertain: "状态待核对",
  conflict: "冲突",
  capacity: "名单已满",
  failed: "失败",
  unavailable: "暂不可用",
  queued: "未处理",
};

export function ScreenWatchlistBatch({
  data,
  pageIndex,
  revision,
  stale,
  running,
}: {
  data: ScreenRunData | null;
  pageIndex: number;
  revision: string;
  stale: boolean;
  running: boolean;
}) {
  const watchlist = useManualWatchlist();
  const candidate = freezeScreenWatchlistPage(data, pageIndex, revision);
  const ready = candidate !== null && !stale && !running && watchlist.state === "ready";
  const batch = useManualWatchlistBatch(watchlist.viewer, watchlist.generationId, candidate, ready);
  const [confirmation, setConfirmation] = useState<{
    candidate: BatchCandidate;
    viewer: string;
    generationId: string;
  } | null>(null);
  const confirmedScope =
    confirmation !== null &&
    ready &&
    confirmation.candidate.key === candidate?.key &&
    confirmation.viewer === watchlist.viewer &&
    confirmation.generationId === watchlist.generationId;
  useEffect(() => {
    if (confirmation && !confirmedScope) setConfirmation(null);
  }, [confirmation, confirmedScope]);

  const manifest = batch.state?.manifest ?? null;
  const counts = manifest ? batchCounts(manifest) : null;
  const outstanding = counts !== null && counts.processing + counts.syncing + counts.uncertain > 0;
  const canBegin =
    ready &&
    watchlist.viewer !== null &&
    watchlist.generationId !== null &&
    batch.session !== null &&
    batch.state?.storageAvailable === true &&
    !batch.state.busy &&
    !outstanding;
  if (candidate === null && manifest === null) return null;

  return (
    <div className="screen-watchlist-batch">
      {candidate ? (
        <div className="screen-watchlist-batch-actions">
          <Button
            size="sm"
            disabled={!canBegin}
            onClick={() => {
              if (canBegin && watchlist.viewer && watchlist.generationId)
                setConfirmation({
                  candidate,
                  viewer: watchlist.viewer,
                  generationId: watchlist.generationId,
                });
            }}
          >
            加入本页 {candidate.codes.length} 只
          </Button>
          <span className="hint">
            {candidate.tradeDate} · 第 {candidate.pageIndex + 1} 页
          </span>
          {!ready ? (
            <span className="hint">
              {stale || running
                ? "请先确认当前结果"
                : watchlist.state === "loading"
                  ? "正在核对名单"
                  : "名单暂不可用，请稍后重试"}
            </span>
          ) : outstanding ? (
            <span className="hint">先核对上次操作</span>
          ) : null}
          {watchlist.state === "unavailable" ? (
            <Button size="sm" variant="ghost" onClick={watchlist.retry}>
              重试名单
            </Button>
          ) : null}
        </div>
      ) : null}
      {batch.state?.message ? (
        <p className="screen-watchlist-message" role="status">
          {batch.state.message}
        </p>
      ) : null}
      {manifest && counts ? (
        <div className="screen-watchlist-summary" role="status">
          <p>
            {manifest.candidate.key === candidate?.key ? "本页" : "上次操作"}{" "}
            {manifest.items.length} 只 · {manifest.candidate.tradeDate} · 第{" "}
            {manifest.candidate.pageIndex + 1} 页
          </p>
          <div className="screen-watchlist-counts">
            {(Object.entries(counts) as [BatchItemStatus, number][])
              .filter(([, count]) => count > 0)
              .map(([status, count]) => (
                <span key={status}>
                  {STATUS_LABEL[status]} <strong className="num">{count}</strong>
                </span>
              ))}
          </div>
          <div className="screen-watchlist-batch-actions">
            <Button
              size="sm"
              variant="ghost"
              disabled={batch.state?.busy}
              onClick={() => {
                watchlist.refreshMeta();
                void batch.session?.reconcile(watchlist.generationId);
              }}
            >
              核对进度
            </Button>
            {counts.conflict +
              counts.capacity +
              counts.failed +
              counts.unavailable +
              counts.queued >
            0 ? (
              <span className="hint">未完成的股票可在结果更新后重新确认。</span>
            ) : null}
          </div>
          {manifest.items.some((item) => !["added", "already"].includes(item.status)) ? (
            <ul className="screen-watchlist-items" aria-label="本页处理明细">
              {manifest.items
                .filter((item) => !["added", "already"].includes(item.status))
                .map((item) => (
                  <li key={item.code}>
                    <span className="mono">{item.code}</span>
                    <span>{STATUS_LABEL[item.status]}</span>
                  </li>
                ))}
            </ul>
          ) : null}
        </div>
      ) : null}
      <ConfirmDialog
        open={confirmedScope}
        level="heavy"
        title={`加入本页 ${confirmation?.candidate.codes.length ?? candidate?.codes.length ?? 0} 只`}
        description={
          confirmation ? (
            <span>
              {confirmation.candidate.tradeDate} · 第 {confirmation.candidate.pageIndex + 1}{" "}
              页。仅处理当前页 {confirmation.candidate.codes.length} 只，逐只核对名单后加入。
            </span>
          ) : null
        }
        confirmLabel={`确认加入本页 ${confirmation?.candidate.codes.length ?? 0} 只`}
        onCancel={() => setConfirmation(null)}
        onConfirm={() => {
          if (
            confirmedScope &&
            confirmation &&
            batch.session !== null &&
            watchlist.generationId !== null
          ) {
            void batch.session.begin(confirmation.candidate, watchlist.generationId);
            setConfirmation(null);
          }
        }}
      />
    </div>
  );
}
