import { useCallback, useMemo } from "react";
import type { PortfolioNav } from "@/api/backtests";
import { EChart } from "@/charts/EChart";
import type { EChartOption } from "@/charts/echarts";
import type { ChartColors } from "@/charts/tokens";
import { EmptyState, Panel, Segmented } from "@/ui";

export type PortfolioRange = "3m" | "1y" | "all";

export function PortfolioCharts({
  rows,
  range,
  onRange,
}: {
  rows: PortfolioNav["rows"];
  range: PortfolioRange;
  onRange: (range: PortfolioRange) => void;
}) {
  const selected = useMemo(() => {
    const last = rows.at(-1)?.trade_date;
    if (range === "all" || last === undefined) return rows;
    const lower = new Date(`${last}T00:00:00Z`);
    lower.setUTCMonth(lower.getUTCMonth() - (range === "3m" ? 3 : 12));
    return rows.filter((row) => row.trade_date >= lower.toISOString().slice(0, 10));
  }, [rows, range]);
  const nav = useCallback(
    (colors: ChartColors): EChartOption => ({
      animation: false,
      tooltip: { trigger: "axis" },
      legend: {
        data: ["组合", "基准"],
        textStyle: { color: colors.muted, fontFamily: colors.fontSans },
      },
      grid: { top: 34, right: 24, bottom: 32, left: 55 },
      xAxis: {
        type: "category",
        data: selected.map((row) => row.trade_date),
        boundaryGap: false,
        axisLine: { lineStyle: { color: colors.rule } },
        axisLabel: { color: colors.muted, fontFamily: colors.fontMono },
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
          name: "组合",
          showSymbol: selected.length < 3,
          lineStyle: { width: 2, color: colors.accent },
          itemStyle: { color: colors.accent },
          data: selected.map((row) =>
            row.normalized_nav === null ? null : Number(row.normalized_nav),
          ),
          connectNulls: false,
        },
        {
          type: "line",
          name: "基准",
          showSymbol: false,
          lineStyle: { width: 1.5, color: colors.series[1], type: "dashed" },
          itemStyle: { color: colors.series[1] },
          data: selected.map((row) => row.benchmark_nav),
          connectNulls: false,
        },
      ],
    }),
    [selected],
  );
  const drawdown = useCallback(
    (colors: ChartColors): EChartOption => ({
      animation: false,
      tooltip: {
        trigger: "axis",
        valueFormatter: (value) => (typeof value === "number" ? `${value.toFixed(2)}%` : "—"),
      },
      grid: { top: 12, right: 24, bottom: 32, left: 55 },
      xAxis: {
        type: "category",
        data: selected.map((row) => row.trade_date),
        boundaryGap: false,
        axisLine: { lineStyle: { color: colors.rule } },
        axisLabel: { color: colors.muted, fontFamily: colors.fontMono },
      },
      yAxis: {
        type: "value",
        max: 0,
        axisLabel: { color: colors.muted, formatter: "{value}%", fontFamily: colors.fontMono },
        splitLine: { lineStyle: { color: colors.grid } },
      },
      series: [
        {
          type: "line",
          name: "回撤",
          showSymbol: false,
          lineStyle: { color: colors.down, width: 1.5 },
          areaStyle: { color: colors.down, opacity: 0.12 },
          itemStyle: { color: colors.down },
          data: selected.map((row) => (row.drawdown === null ? null : row.drawdown * 100)),
          connectNulls: false,
        },
      ],
    }),
    [selected],
  );
  return (
    <Panel
      title="净值与回撤"
      sub="起始净值 1"
      actions={
        <Segmented
          label="净值区间"
          value={range}
          onChange={onRange}
          options={[
            { value: "3m", label: "3 个月" },
            { value: "1y", label: "1 年" },
            { value: "all", label: "全部" },
          ]}
        />
      }
    >
      {rows.length === 0 ? (
        <EmptyState title="尚无完整净值" hint="完成交易日后在这里查看净值和回撤。" />
      ) : (
        <>
          <EChart label="组合与基准净值" build={nav} className="chart pb-nav-chart" />
          <EChart label="组合回撤" build={drawdown} className="chart pb-dd-chart" />
        </>
      )}
    </Panel>
  );
}
