import { useMemo, useState } from "react";
import { type DailyData, type IntradayData, useDaily, useIntraday } from "@/api/endpoints";
import { type DailyBar, PriceChart, type SessionBar, type SessionMark } from "@/charts/PriceChart";
import { EmptyState, Segmented, SkeletonRows, Tip } from "@/ui";
import { LoadError } from "./LoadError";

export type ChartPeriod = "intraday" | "five-day" | "daily";

const PERIODS = [
  { value: "intraday", label: "分时" },
  { value: "five-day", label: "5 日" },
  { value: "daily", label: "日 K" },
] as const;

export function sessionBars(data: IntradayData | undefined): SessionBar[] {
  return (data?.bars ?? []).map((bar) => ({
    day: bar.day,
    t: bar.t,
    slot: bar.slot,
    price: bar.price,
    avg: bar.avg_price,
    volume: bar.volume,
    direction: bar.direction,
  }));
}

export function sessionMarks(data: IntradayData | undefined): SessionMark[] {
  return (data?.marks ?? []).map((mark) => ({ day: mark.day, slot: mark.slot, label: mark.label }));
}

export function dailyBars(data: DailyData | undefined): DailyBar[] {
  return (data?.bars ?? []).map((bar) => ({
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
  }));
}

const LEGEND = {
  intraday:
    "蓝线为价格，虚线为均价（按分钟收盘价估算）；量柱红涨绿跌按分钟涨跌近似；橙点和竖线为爆量确认",
  "five-day": "最近 5 个交易日的分钟走势；橙点和竖线为每天第一次爆量确认",
  daily: "最近 120 个交易日；MA5 / MA10 / MA20；「盘中」为今天用实时快照临时补上的 K 线",
} as const;

/** One stock's minute session (or a given day) with its 爆量 marks. */
export function SessionChart({
  tsCode,
  days,
  date,
  label,
}: {
  tsCode: string;
  days: 1 | 5;
  date?: string;
  label: string;
}) {
  const query = useIntraday(tsCode, date ? { date } : { days });
  const bars = useMemo(() => sessionBars(query.data), [query.data]);
  const marks = useMemo(() => sessionMarks(query.data), [query.data]);
  if (query.isLoading) {
    return <SkeletonRows rows={6} />;
  }
  if (query.error) {
    return <LoadError label="分时走势" onRetry={query.refetch} />;
  }
  if (!bars.length) {
    return (
      <EmptyState
        title={date ? `${date} 这只股票没有分钟数据` : "这只股票暂时没有分钟数据"}
        hint="分钟数据只覆盖盘中关注的股票"
      />
    );
  }
  return (
    <PriceChart
      mode={days === 5 && !date ? "five-day" : "intraday"}
      bars={bars}
      days={query.data?.days ?? []}
      marks={marks}
      label={label}
    />
  );
}

function DailyChart({ tsCode, label }: { tsCode: string; label: string }) {
  const query = useDaily(tsCode);
  const bars = useMemo(() => dailyBars(query.data), [query.data]);
  if (query.isLoading) {
    return <SkeletonRows rows={6} />;
  }
  if (query.error) {
    return <LoadError label="日 K 走势" onRetry={query.refetch} />;
  }
  if (!bars.length) {
    return <EmptyState title="暂无日 K 数据" hint="日 K 只覆盖候选和持仓相关的股票" />;
  }
  return <PriceChart mode="daily" bars={bars} label={label} />;
}

export function StockChart({ tsCode, name }: { tsCode: string | null; name: string | null }) {
  const [period, setPeriod] = useState<ChartPeriod>("intraday");
  const title = tsCode ? `${name ?? tsCode}` : "个股走势";
  return (
    <section className="panel" aria-label="个股走势">
      <div className="panel-h">
        <h2>
          {title}
          {tsCode && name ? <span className="h-code mono"> {tsCode}</span> : null}
        </h2>
        <div className="row">
          <Tip content={LEGEND[period]}>
            <span className="hint legend-tip">图例</span>
          </Tip>
          <Segmented label="走势周期" options={PERIODS} value={period} onChange={setPeriod} />
        </div>
      </div>
      <div className="panel-b">
        {tsCode === null ? (
          <EmptyState title="选一只股票查看走势" />
        ) : period === "daily" ? (
          <DailyChart tsCode={tsCode} label={`${title} 日 K`} />
        ) : (
          <SessionChart
            tsCode={tsCode}
            days={period === "five-day" ? 5 : 1}
            label={`${title} ${period === "five-day" ? "5 日" : "分时"}`}
          />
        )}
      </div>
    </section>
  );
}
