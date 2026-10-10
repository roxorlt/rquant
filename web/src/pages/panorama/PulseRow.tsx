import { useEffect } from "react";
import type { PulseData } from "@/api/endpoints";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import { baseOption } from "@/charts/options";
import type { ChartColors } from "@/charts/tokens";
import { formatCount, formatPercent } from "@/format/number";
import {
  Button,
  type Kpi,
  KpiStrip,
  Popover,
  RelativeTime,
  StatusBadge,
  Tip,
  useToast,
} from "@/ui";

type Point = PulseData["history"][number];
type SeriesKey = "limit_up" | "broken" | "limit_down" | "up_ratio_pct";

const FACETS: readonly { key: SeriesKey; label: string; color: (c: ChartColors) => string }[] = [
  { key: "limit_up", label: "涨停", color: (c) => c.up },
  { key: "broken", label: "炸板", color: (c) => c.series[1] },
  { key: "limit_down", label: "跌停", color: (c) => c.down },
  { key: "up_ratio_pct", label: "上涨占比 %", color: (c) => c.accent },
];

function facetOption(points: readonly Point[], key: SeriesKey, color: string) {
  return (colors: ChartColors): EChartOption => ({
    ...baseOption(colors, false),
    grid: { left: 36, right: 8, top: 6, bottom: 18 },
    xAxis: {
      type: "category",
      data: points.map((point) => point.t),
      axisLabel: {
        color: colors.muted,
        fontSize: 10,
        interval: Math.max(Math.floor(points.length / 5), 1),
      },
      axisLine: { lineStyle: { color: colors.rule } },
      axisTick: { show: false },
    },
    yAxis: {
      type: "value",
      scale: true,
      splitNumber: 2,
      axisLabel: { color: colors.muted, fontSize: 10 },
      splitLine: { lineStyle: { color: colors.grid } },
    },
    series: [
      {
        type: "line",
        data: points.map((point) => point[key]),
        showSymbol: false,
        lineStyle: { color, width: 1.6 },
        itemStyle: { color },
      },
    ],
  });
}

function History({ points }: { points: readonly Point[] }) {
  if (points.length < 2) {
    return <p className="hint">脉搏曲线累积中，至少要两分钟的数据</p>;
  }
  return (
    <div className="pulse-facets">
      {FACETS.map((facet) => (
        <div key={facet.key}>
          <span className="facet-label">{facet.label}</span>
          <EChart
            build={(colors) => facetOption(points, facet.key, facet.color(colors))(colors)}
            label={`${facet.label}当日走势`}
            className="chart mini"
          />
        </div>
      ))}
    </div>
  );
}

const SEEN_KEY = "rq.panorama.alerts";
let seenWithoutStorage = "";

function readSeen(): string {
  try {
    return window.sessionStorage.getItem(SEEN_KEY) ?? seenWithoutStorage;
  } catch {
    return seenWithoutStorage;
  }
}

function writeSeen(value: string): void {
  seenWithoutStorage = value;
  try {
    window.sessionStorage.setItem(SEEN_KEY, value);
  } catch {
    // One page view still avoids duplicate toasts when browser storage is unavailable.
  }
}

/** The latest alert within 30 minutes stays on screen; a new one also pops once. */
function AlertLine({ pulse }: { pulse: PulseData }) {
  const toast = useToast();
  const alert = pulse.recent_alert;
  const key = alert ? `${pulse.trade_date}-${alert.t}-${alert.kind}` : null;
  useEffect(() => {
    if (key === null || alert === null) {
      return;
    }
    const seen = readSeen();
    if (!seen.split(",").includes(key)) {
      writeSeen([key, ...seen.split(",").slice(0, 20)].join(","));
      toast(`${alert.t} ${alert.kind_label}：${alert.message}`);
    }
  }, [key, alert, toast]);
  if (alert === null) {
    return null;
  }
  const all = pulse.alerts
    .map((item) => `${item.t} ${item.kind_label}：${item.message}`)
    .join("\n");
  return (
    <div className="banner warn pulse-alert" role="status">
      <span className="d" aria-hidden="true" />
      <span className="banner-text">
        {alert.t} {alert.kind_label}：{alert.message}
      </span>
      {pulse.alerts.length > 1 ? (
        <Tip content={<span className="tip-pre">{all}</span>} placement="bottom">
          <span className="banner-more">今天共 {pulse.alerts.length} 次</span>
        </Tip>
      ) : null}
    </div>
  );
}

function kpis(pulse: PulseData): Kpi[] {
  const counts = pulse.counts;
  if (counts === null) {
    return [];
  }
  return [
    { key: "limit_up", label: "涨停", value: formatCount(counts.limit_up), tone: "crit" },
    { key: "limit_down", label: "跌停", value: formatCount(counts.limit_down) },
    {
      key: "broken",
      label: "炸板",
      value: formatCount(counts.broken),
      tip: "盘中摸过涨停价、现在没封住",
    },
    {
      key: "ratio",
      label: "上涨占比",
      value: formatPercent(counts.up_ratio_pct, 1),
      tip: `有效样本 ${formatCount(counts.total)} 只，停牌不计`,
    },
    {
      key: "updown",
      label: "涨 / 跌家数",
      value: (
        <>
          <span className="up">{formatCount(counts.up)}</span>
          <small> / </small>
          <span className="down">{formatCount(counts.down)}</span>
        </>
      ),
    },
  ];
}

export function PulseRow({ pulse }: { pulse: PulseData }) {
  const items = kpis(pulse);
  return (
    <section className="pulse" aria-label="市场脉搏">
      {items.length ? (
        <KpiStrip label="涨跌停与涨跌家数" items={items} compact />
      ) : (
        <div className="panel">
          <p className="empty-state empty-title">暂时没有全市场行情，开盘后每分钟更新</p>
        </div>
      )}
      <div className="pulse-meta">
        <StatusBadge
          state={pulse.freshness.state}
          label={pulse.freshness.label}
          reason={pulse.freshness.reason}
        />
        {pulse.as_of ? <RelativeTime at={pulse.as_of} suffix="的快照" /> : null}
        {pulse.source === "history" ? <span className="hint">来自分钟脉搏记录</span> : null}
        <Popover title="今日脉搏" content={<History points={pulse.history} />}>
          <Button size="sm" variant="ghost" aria-label="查看今日脉搏走势">
            今日走势
          </Button>
        </Popover>
      </div>
      <AlertLine pulse={pulse} />
    </section>
  );
}

export const _test = { facetOption, SEEN_KEY };
