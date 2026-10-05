import { useCallback } from "react";
import type { Schemas } from "@/api/client";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import { type ChartColors, withAlpha } from "@/charts/tokens";
import { formatNumber, formatSignedPercent } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import { EmptyState, Panel, StatusBadge, Tip } from "@/ui";
import type { PaperDetail } from "./paperPortfolioApi";

type Point = Schemas["PaperDailyNavPoint"];
const COLUMNS: DataColumn<Point>[] = [
  {
    id: "date",
    header: "交易日",
    value: (row) => row.trade_date,
    cell: (row) => <span className="num">{row.trade_date}</span>,
  },
  {
    id: "nav",
    header: "总资产",
    value: (row) => row.nav ?? null,
    cell: (row) => formatNumber(row.nav == null ? null : Number(row.nav)),
    numeric: true,
  },
  {
    id: "normalized",
    header: "净值",
    value: (row) => row.normalized_nav ?? null,
    cell: (row) => formatNumber(row.normalized_nav == null ? null : Number(row.normalized_nav), 4),
    numeric: true,
  },
  {
    id: "return",
    header: "日收益",
    value: (row) => row.daily_return ?? null,
    cell: (row) =>
      formatSignedPercent(row.daily_return == null ? null : Number(row.daily_return) * 100),
    numeric: true,
    secondary: true,
  },
  {
    id: "state",
    header: "状态",
    value: (row) => row.status,
    cell: (row) => (
      <StatusBadge
        state={row.status === "complete" ? "ok" : "warn"}
        label={row.status === "complete" ? "正常" : "注意"}
        reason={row.reason}
      />
    ),
  },
];

export function navChartOption(detail: PaperDetail, colors: ChartColors): EChartOption {
  const dates = detail.nav.map((row) => row.trade_date);
  const band = new Map(
    detail.band?.dates.map((day, index) => [day, detail.band?.points[index]]) ?? [],
  );
  const lower = dates.map((day) => {
    const point = band.get(day);
    return point ? Number(point.lower) : null;
  });
  const width = dates.map((day) => {
    const point = band.get(day);
    return point ? Number(point.upper) - Number(point.lower) : null;
  });
  return {
    animation: false,
    textStyle: { color: colors.text, fontFamily: colors.fontSans },
    grid: { left: 48, right: 18, top: 18, bottom: 44 },
    tooltip: {
      trigger: "axis",
      renderMode: "richText",
      formatter: (values) => {
        const value = Array.isArray(values) ? values[0] : values;
        const index = value && "dataIndex" in value ? value.dataIndex : -1;
        const row = typeof index === "number" ? detail.nav[index] : undefined;
        if (!row) return "";
        const point = band.get(row.trade_date);
        return `${row.trade_date}\n模拟净值 ${formatNumber(row.normalized_nav == null ? null : Number(row.normalized_nav), 4)}${point ? `\n5%–95% ${formatNumber(Number(point.lower), 4)}–${formatNumber(Number(point.upper), 4)}` : ""}`;
      },
    },
    xAxis: {
      type: "category",
      data: dates,
      boundaryGap: false,
      axisLabel: { color: colors.muted, fontFamily: colors.fontMono },
      axisLine: { lineStyle: { color: colors.rule } },
    },
    yAxis: {
      type: "value",
      scale: true,
      axisLabel: { color: colors.muted, fontFamily: colors.fontMono },
      splitLine: { lineStyle: { color: colors.grid } },
    },
    series: [
      {
        type: "line",
        name: "区间下界",
        stack: "band",
        data: lower,
        symbol: "none",
        connectNulls: false,
        lineStyle: { opacity: 0 },
        areaStyle: { opacity: 0 },
      },
      {
        type: "line",
        name: "回测区间",
        stack: "band",
        data: width,
        symbol: "none",
        connectNulls: false,
        lineStyle: { opacity: 0 },
        areaStyle: { color: withAlpha(colors.accent, 0.14) },
      },
      {
        type: "line",
        name: "模拟净值",
        data: detail.nav.map((row) =>
          row.status === "complete" && row.normalized_nav != null
            ? Number(row.normalized_nav)
            : null,
        ),
        symbol: "circle",
        symbolSize: 5,
        connectNulls: false,
        lineStyle: { color: colors.accent, width: 2 },
        itemStyle: { color: colors.accent },
      },
    ],
  };
}

export function PaperNavView({ detail }: { detail: PaperDetail }) {
  const build = useCallback((colors: ChartColors) => navChartOption(detail, colors), [detail]);
  const gaps = detail.nav.filter((row) => row.status !== "complete").length;
  const position = detail.band_position;
  return (
    <Panel
      title="逐日净值"
      sub={
        detail.band ? (
          <StatusBadge
            state={position === "outside" ? "warn" : position === "inside" ? "ok" : "idle"}
            label={position === "outside" ? "注意" : position === "inside" ? "正常" : "未运行"}
            reason={
              position === "outside"
                ? "模拟净值在回测抽样区间外，需要关注。"
                : position === "inside"
                  ? "模拟净值在回测抽样区间内。"
                  : "模拟净值缺口，暂不能对照。"
            }
          />
        ) : undefined
      }
    >
      {detail.nav.length ? (
        <>
          <div className="paper-nav-caption">
            <span>
              {detail.band
                ? position === "inside"
                  ? "在区间内"
                  : position === "outside"
                    ? "需要关注"
                    : "暂不能对照"
                : "回测区间尚未计算"}
            </span>
            <Tip
              content="回测日收益有放回抽样，固定 2,048 条路径。阴影为逐日 5%–95% 区间，不能证明未来结果。"
              interactive
            >
              <button type="button" className="screen-help" aria-label="回测区间说明">
                ?
              </button>
            </Tip>
            {gaps ? <span className="paper-nav-gap">{gaps} 天缺数据</span> : null}
          </div>
          <EChart build={build} label="模拟净值与回测区间" />
          <details className="paper-nav-details">
            <summary>查看净值数据</summary>
            <DataTable
              rows={detail.nav}
              columns={COLUMNS}
              rowKey={(row) => row.trade_date}
              label="逐日净值数据"
            />
          </details>
        </>
      ) : (
        <EmptyState title="收盘净值尚未发布" hint="可核实的收盘估值发布后会显示。" />
      )}
    </Panel>
  );
}
