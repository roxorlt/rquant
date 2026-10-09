import { useMemo } from "react";
import { useDaily, useStockSummary } from "@/api/endpoints";
import { useManualWatchlistExact } from "@/api/manualWatchlist";
import {
  publishedMatches,
  publishedSuperseded,
  useManualWatchlistCommand,
} from "@/api/manualWatchlistCommand";
import { PriceChart } from "@/charts/PriceChart";
import { formatPrice } from "@/format/number";
import { Button, EmptyState, Pill, SideDrawer, SkeletonRows, Tip } from "@/ui";
import { StockNewsDigest } from "./StockNewsDigest";

export function StockDrawer({
  tsCode,
  onClose,
  entryMark,
}: {
  tsCode: string | null;
  onClose: () => void;
  entryMark?: {
    date: string;
    generationId: string;
    linePrice?: number | null;
    factorChanged?: boolean;
  } | null;
}) {
  const summary = useStockSummary(tsCode);
  const daily = useDaily(tsCode);
  const watchlist = useManualWatchlistExact(tsCode);
  const command = useManualWatchlistCommand(watchlist.viewer, tsCode, watchlist.generationId);
  const record = command.record;
  const confirmed = record !== null && publishedMatches(record, watchlist);
  const settledExpired =
    record?.status === "published" &&
    record.body.action === "add" &&
    watchlist.generationId !== null &&
    watchlist.generationId !== record.body.generation_id &&
    watchlist.status === "expired" &&
    watchlist.version === record.version;
  const settled =
    confirmed || settledExpired || (record !== null && publishedSuperseded(record, watchlist));
  const syncing =
    record !== null &&
    !settled &&
    (record.status === "saved_syncing" || record.status === "published");
  const processing =
    record !== null && !settled && (record.status === "pending" || record.status === "processing");
  const uncertain =
    record !== null && !settled && (record.status === "uncertain" || record.status === "unknown");
  const failure = record !== null && ["conflict", "capacity", "failed"].includes(record.status);
  const canStart =
    command.storageAvailable &&
    !command.busy &&
    !processing &&
    !uncertain &&
    !syncing &&
    (!failure || watchlist.generationId !== record?.body.generation_id) &&
    watchlist.viewer !== null &&
    watchlist.generationId !== null;
  const action =
    watchlist.status === "active"
      ? "remove"
      : ["absent", "expired", "deleted"].includes(watchlist.status)
        ? "add"
        : null;
  const name = summary.data?.name ?? tsCode ?? "个股";
  const bars = useMemo(
    () =>
      (daily.data?.bars ?? []).map((bar) => ({
        time: bar.date,
        open: bar.open,
        high: bar.high,
        low: bar.low,
        close: bar.close,
        volume: bar.volume,
        ma5: bar.ma5,
        ma10: bar.ma10,
        ma20: bar.ma20,
        provisional: bar.provisional,
      })),
    [daily.data],
  );
  const markedDay =
    !daily.isLoading &&
    !daily.error &&
    tsCode !== null &&
    entryMark !== null &&
    entryMark !== undefined &&
    daily.data?.ts_code === tsCode &&
    entryMark.generationId === daily.serving?.generation_id &&
    daily.data?.bars.some((bar) => bar.date === entryMark.date && !bar.provisional)
      ? entryMark.date
      : null;
  const entryLine =
    markedDay &&
    entryMark?.linePrice != null &&
    Number.isFinite(entryMark.linePrice) &&
    entryMark.linePrice > 0 &&
    daily.data?.bars.find((bar) => bar.date === markedDay)?.close === entryMark.linePrice &&
    daily.data?.bars.at(-1)?.provisional === false
      ? { price: entryMark.linePrice, label: "入池日收盘价" }
      : undefined;

  return (
    <SideDrawer
      open={tsCode !== null}
      onClose={onClose}
      title={
        <span>
          {name}{" "}
          {tsCode && name !== tsCode ? (
            <span className="mono stock-drawer-code">{tsCode}</span>
          ) : null}
        </span>
      }
    >
      <div className="stock-drawer-body">
        {summary.isLoading ? (
          <SkeletonRows rows={2} />
        ) : summary.error ? (
          <div className="empty-state" role="alert">
            <p className="empty-title">个股信息暂时无法加载</p>
            <Button size="sm" onClick={summary.refetch}>
              重试
            </Button>
          </div>
        ) : (
          <div className="stock-summary">
            <div>
              <span className="hint">最新价</span>
              <strong className="num stock-price">{formatPrice(summary.data?.price)}</strong>
              {summary.data?.price == null ? <span className="hint">暂无最新价</span> : null}
            </div>
            <div>
              <span className="hint">所在池子</span>
              <div className="stock-pools">
                {summary.data?.pools.length ? (
                  summary.data.pools.map((pool) => (
                    <Pill key={pool} kind="acc">
                      {pool}
                    </Pill>
                  ))
                ) : (
                  <span className="hint">暂无所在池子</span>
                )}
              </div>
            </div>
          </div>
        )}
        <StockNewsDigest viewer={watchlist.viewer} stockCode={tsCode} />
        <section className="stock-watchlist" aria-label="手动盯盘状态">
          <div className="stock-watchlist-head">
            <span className="hint">手动盯盘</span>
            {processing ? (
              <Pill kind="acc">正在处理</Pill>
            ) : uncertain ? (
              <Pill kind="warn">状态待核对</Pill>
            ) : syncing ? (
              <Pill kind="acc">已保存，正在同步</Pill>
            ) : watchlist.status === "active" ? (
              <Pill kind="ok">已加入盯盘</Pill>
            ) : watchlist.status === "expired" ? (
              <Pill kind="warn">已到期</Pill>
            ) : watchlist.status === "deleted" ? (
              <Pill kind="idle">已移出盯盘</Pill>
            ) : watchlist.status === "absent" ? (
              <Pill kind="idle">尚未加入</Pill>
            ) : (
              <span className="hint" role="status">
                {watchlist.status === "loading" ? "正在核对名单" : watchlist.message}
              </span>
            )}
          </div>
          {watchlist.status === "unavailable" ? (
            <Button size="sm" variant="ghost" onClick={watchlist.retry}>
              重试
            </Button>
          ) : null}
          {action && !processing && !uncertain && !syncing ? (
            <Button
              size="sm"
              disabled={!canStart}
              onClick={() => {
                if (watchlist.generationId)
                  void command.session.start({
                    action,
                    generationId: watchlist.generationId,
                    expectedVersion: watchlist.version,
                    observedStatus:
                      watchlist.status === "active"
                        ? "active"
                        : watchlist.status === "expired"
                          ? "expired"
                          : watchlist.status === "deleted"
                            ? "deleted"
                            : "absent",
                  });
              }}
            >
              {action === "add" ? "加入盯盘" : "移出盯盘"}
            </Button>
          ) : null}
          {processing || uncertain ? (
            <Button
              size="sm"
              variant="ghost"
              disabled={command.busy}
              onClick={() => void command.session.advance()}
            >
              继续核对
            </Button>
          ) : null}
          {syncing ? (
            <Button
              size="sm"
              variant="ghost"
              disabled={command.busy}
              onClick={() => {
                watchlist.refreshMeta();
                if (record?.status === "saved_syncing") void command.session.advance();
              }}
            >
              核对状态
            </Button>
          ) : null}
          {failure ? (
            <div className="stock-watchlist-feedback" role="status">
              {record?.status === "capacity"
                ? "名单已满，请先移出其他股票。"
                : record?.status === "conflict"
                  ? "名单已更新，请刷新后重试。"
                  : "操作未完成，请刷新后重试。"}
              <Button size="sm" variant="ghost" onClick={watchlist.refreshMeta}>
                刷新名单
              </Button>
            </div>
          ) : null}
          {command.message && watchlist.viewer ? (
            <p className="stock-watchlist-feedback" role="status">
              {command.message}
            </p>
          ) : null}
        </section>
        <section aria-label="日 K 走势">
          <div className="stock-chart-heading">
            <h3 className="stock-chart-title">日 K</h3>
            {markedDay ? <span className="stock-entry-mark">入池 · {markedDay}</span> : null}
            {markedDay && entryMark?.factorChanged ? (
              <Tip content="收益按复权价格计算；日 K 显示原始价，不能画入池价横线。">
                <span className="stock-entry-mark">价格口径不同</span>
              </Tip>
            ) : null}
          </div>
          {daily.isLoading ? (
            <SkeletonRows rows={6} />
          ) : daily.error ? (
            <div className="empty-state" role="alert">
              <p className="empty-title">日 K 暂时无法加载</p>
              <Button size="sm" onClick={daily.refetch}>
                重试
              </Button>
            </div>
          ) : bars.length ? (
            <PriceChart
              mode="daily"
              bars={bars}
              marks={markedDay ? [{ time: markedDay, label: "入池" }] : []}
              referenceLine={entryLine}
              label={`${name} 日 K`}
            />
          ) : (
            <EmptyState title="暂无日 K 数据" hint="目前仅覆盖候选股票" />
          )}
        </section>
      </div>
    </SideDrawer>
  );
}
