import type { FactorExtendedStatistics, FactorResearchDisplay } from "@/api/factors";
import type { EChartOption } from "@/charts/echarts";
import { baseOption } from "@/charts/options";
import type { ChartColors } from "@/charts/tokens";
import { formatNumber, formatPercent } from "@/format/number";

export type IcMethod = "normal_ic" | "rank_ic";

export function industryIcOption(
  statistics: FactorExtendedStatistics,
  colors: ChartColors,
): EChartOption {
  const summaries = statistics.industry_summaries;
  const numericAxis = yAxis(colors);
  return {
    ...baseOption(colors),
    grid: { left: 88, right: summaries.length > 10 ? 36 : 16, top: 16, bottom: 32 },
    xAxis: { ...numericAxis, axisLabel: { ...numericAxis.axisLabel, hideOverlap: true } },
    yAxis: {
      ...xAxis(
        summaries.map((item) => item.l1_name),
        colors,
      ),
      inverse: true,
      axisLabel: { color: colors.muted, fontSize: 11, width: 78, overflow: "truncate" },
    },
    ...(summaries.length > 10
      ? {
          dataZoom: [
            {
              type: "slider" as const,
              yAxisIndex: 0,
              startValue: 0,
              endValue: 9,
              right: 4,
              width: 14,
              showDetail: false,
            },
          ],
        }
      : {}),
    series: [
      {
        name: statistics.ic_method === "rank" ? "RankIC 均值" : "NormalIC 均值",
        type: "bar",
        barMaxWidth: 20,
        data: summaries.map((item) =>
          item.ic_summary.mean === null
            ? null
            : {
                value: item.ic_summary.mean,
                itemStyle: { color: item.ic_summary.mean >= 0 ? colors.up : colors.down },
              },
        ),
      },
    ],
  };
}

export function autocorrelationOption(
  statistics: FactorExtendedStatistics,
  colors: ChartColors,
): EChartOption {
  return {
    ...baseOption(colors),
    grid: { left: 52, right: 16, top: 16, bottom: 32 },
    xAxis: xAxis(
      statistics.autocorrelation_points.map((point) => point.trade_date),
      colors,
    ),
    yAxis: yAxis(colors),
    series: [
      {
        name: "因子自相关",
        type: "line",
        connectNulls: false,
        showSymbol: true,
        symbolSize: 5,
        data: statistics.autocorrelation_points.map((point) => point.value),
        lineStyle: { color: colors.accent, width: 2 },
        itemStyle: { color: colors.accent },
      },
    ],
  };
}

export function icSeries(research: FactorResearchDisplay, method: IcMethod) {
  return research.ic_points.map((point) => ({
    date: point.decision_date,
    value: point[method]?.value ?? null,
    cumulative:
      method === "normal_ic" ? point.normal_ic_cumulative_sum : point.rank_ic_cumulative_sum,
    reason: point[method]?.status ?? "insufficient_samples",
  }));
}

export function decaySeries(research: FactorResearchDisplay, method: IcMethod) {
  return research.decay_periods.map((period) => ({
    lag: period.lag,
    value: period.ic_summary?.[method].mean ?? null,
    reason: period.status,
  }));
}

export function groupCounts(research: FactorResearchDisplay): number[] {
  return [
    ...new Set(
      research.portfolio_days.flatMap((day) =>
        day.groupings
          .filter((grouping) => grouping.status === "ok")
          .map((grouping) => grouping.group_count),
      ),
    ),
  ]
    .filter((count) => count === 3 || count === 5 || count === 10)
    .sort((a, b) => a - b);
}

export function groupSeries(research: FactorResearchDisplay, count: number) {
  const portfolioByDate = new Map(research.portfolio_days.map((day) => [day.decision_date, day]));
  return research.coverage_days.map((day) => ({
    date: day.decision_date,
    groups:
      portfolioByDate
        .get(day.decision_date)
        ?.groupings.find((grouping) => grouping.group_count === count && grouping.status === "ok")
        ?.groups ?? [],
  }));
}

function xAxis(dates: string[], colors: ChartColors, gridIndex = 0) {
  return {
    type: "category" as const,
    gridIndex,
    data: dates,
    boundaryGap: true,
    axisLine: { lineStyle: { color: colors.rule } },
    axisTick: { show: false },
    axisLabel: { color: colors.muted, fontSize: 11, hideOverlap: true },
  };
}

function yAxis(colors: ChartColors, gridIndex = 0, percent = false) {
  return {
    type: "value" as const,
    gridIndex,
    scale: true,
    axisLabel: {
      color: colors.muted,
      fontSize: 11,
      formatter: (value: number) =>
        percent ? formatPercent(value * 100, 0) : formatNumber(value, 4),
    },
    splitLine: { lineStyle: { color: colors.grid } },
  };
}

export function icOption(
  research: FactorResearchDisplay,
  method: IcMethod,
  colors: ChartColors,
): EChartOption {
  const points = icSeries(research, method);
  const dates = points.map((point) => point.date);
  return {
    ...baseOption(colors),
    axisPointer: { link: [{ xAxisIndex: "all" }] },
    grid: [
      { left: 52, right: 16, top: 20, height: "47%" },
      { left: 52, right: 16, top: "68%", bottom: 28 },
    ],
    xAxis: [{ ...xAxis(dates, colors), axisLabel: { show: false } }, xAxis(dates, colors, 1)],
    yAxis: [yAxis(colors), yAxis(colors, 1)],
    series: [
      {
        name: method === "normal_ic" ? "NormalIC" : "RankIC",
        type: "bar",
        barMaxWidth: 14,
        data: points.map((point) =>
          point.value === null
            ? null
            : {
                value: point.value,
                itemStyle: { color: point.value >= 0 ? colors.up : colors.down },
              },
        ),
      },
      {
        name: "累计 IC",
        type: "line",
        xAxisIndex: 1,
        yAxisIndex: 1,
        connectNulls: false,
        showSymbol: true,
        symbolSize: 5,
        data: points.map((point) => point.cumulative),
        lineStyle: { color: colors.accent, width: 2 },
        itemStyle: { color: colors.accent },
      },
    ],
  };
}

export function decayOption(
  research: FactorResearchDisplay,
  method: IcMethod,
  colors: ChartColors,
): EChartOption {
  const points = decaySeries(research, method);
  return {
    ...baseOption(colors),
    grid: { left: 48, right: 10, top: 18, bottom: 28 },
    xAxis: xAxis(
      points.map((point) => String(point.lag)),
      colors,
    ),
    yAxis: yAxis(colors),
    series: [
      {
        name: "IC 均值",
        type: "bar",
        barMaxWidth: 22,
        data: points.map((point) =>
          point.value === null
            ? null
            : {
                value: point.value,
                itemStyle: { color: point.value >= 0 ? colors.up : colors.down },
              },
        ),
      },
    ],
  };
}

function groupColor(index: number, colors: ChartColors): string {
  const palette = [
    colors.series[0],
    colors.series[1],
    colors.series[2],
    colors.accent,
    colors.up,
    colors.down,
  ];
  return palette[index % palette.length] ?? colors.accent;
}

export function groupOption(
  research: FactorResearchDisplay,
  count: number,
  colors: ChartColors,
): EChartOption {
  const points = groupSeries(research, count);
  return {
    ...baseOption(colors),
    legend: { top: 0, textStyle: { color: colors.muted }, type: "scroll" },
    grid: { left: 52, right: 12, top: 42, bottom: 28 },
    xAxis: xAxis(
      points.map((point) => point.date),
      colors,
    ),
    yAxis: yAxis(colors, 0, true),
    series: Array.from({ length: count }, (_, index) => ({
      name: `第 ${index + 1} 组`,
      type: "line" as const,
      connectNulls: false,
      showSymbol: true,
      symbolSize: 5,
      data: points.map(
        (point) =>
          point.groups.find((group) => group.group_number === index + 1)?.cumulative_return ?? null,
      ),
      lineStyle: {
        color: groupColor(index, colors),
        width: 2,
        type: index >= 6 ? ("dashed" as const) : ("solid" as const),
      },
      itemStyle: { color: groupColor(index, colors) },
    })),
  };
}

export function turnoverOption(
  research: FactorResearchDisplay,
  count: number,
  colors: ChartColors,
): EChartOption {
  const points = groupSeries(research, count);
  return {
    ...baseOption(colors),
    legend: { top: 0, textStyle: { color: colors.muted }, type: "scroll" },
    grid: { left: 52, right: 12, top: 42, bottom: 28 },
    xAxis: xAxis(
      points.map((point) => point.date),
      colors,
    ),
    yAxis: yAxis(colors, 0, true),
    series: Array.from({ length: count }, (_, index) => ({
      name: `第 ${index + 1} 组`,
      type: "bar" as const,
      barMaxWidth: 16,
      data: points.map(
        (point) =>
          point.groups.find((group) => group.group_number === index + 1)?.target_weight_turnover ??
          null,
      ),
      itemStyle: { color: groupColor(index, colors) },
    })),
  };
}
