import { useMemo } from "react";
import { useDaily, useStockSummary } from "@/api/endpoints";
import { PriceChart } from "@/charts/PriceChart";
import { formatPrice } from "@/format/number";
import { Button, EmptyState, Pill, SideDrawer, SkeletonRows } from "@/ui";

export function StockDrawer({ tsCode, onClose }: { tsCode: string | null; onClose: () => void }) {
  const summary = useStockSummary(tsCode);
  const daily = useDaily(tsCode);
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
        <section aria-label="日 K 走势">
          <h3 className="stock-chart-title">日 K</h3>
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
            <PriceChart mode="daily" bars={bars} label={`${name} 日 K`} />
          ) : (
            <EmptyState title="暂无日 K 数据" hint="目前仅覆盖候选股票" />
          )}
        </section>
      </div>
    </SideDrawer>
  );
}
