import type { FactorResearchDisplay } from "@/api/factors";
import type { ChartColors } from "@/charts/tokens";
import {
  decayOption,
  decaySeries,
  groupCounts,
  groupOption,
  groupSeries,
  icOption,
  icSeries,
  turnoverOption,
} from "./factorCharts";

const research = {
  coverage_days: [{ decision_date: "2026-09-21" }, { decision_date: "2026-09-22" }],
  ic_points: [
    {
      decision_date: "2026-09-21",
      normal_ic: { status: "ok", value: 0.03 },
      normal_ic_cumulative_sum: 0.03,
      rank_ic: { status: "ok", value: -0.02 },
      rank_ic_cumulative_sum: -0.02,
    },
    {
      decision_date: "2026-09-22",
      normal_ic: { status: "insufficient_samples", value: null },
      normal_ic_cumulative_sum: null,
      rank_ic: null,
      rank_ic_cumulative_sum: null,
    },
  ],
  decay_periods: [
    {
      lag: 1,
      status: "evaluated",
      ic_summary: { normal_ic: { mean: 0.03 }, rank_ic: { mean: -0.02 } },
    },
    { lag: 2, status: "no_target_period", ic_summary: null },
  ],
  portfolio_days: [
    {
      decision_date: "2026-09-21",
      groupings: [
        {
          status: "ok",
          group_count: 3,
          groups: [{ group_number: 1, cumulative_return: 0.02, target_weight_turnover: null }],
        },
        {
          status: "ok",
          group_count: 5,
          groups: [{ group_number: 1, cumulative_return: 0.01, target_weight_turnover: 0.2 }],
        },
      ],
    },
    {
      decision_date: "2026-09-22",
      groupings: [{ status: "insufficient_samples", group_count: 5, groups: [] }],
    },
  ],
} as FactorResearchDisplay;

const colors = {
  surface: "#fff",
  text: "#111",
  muted: "#777",
  grid: "#ddd",
  rule: "#ccc",
  ruleStrong: "#aaa",
  accent: "#007",
  up: "#d00",
  down: "#080",
  warn: "#a80",
  series: ["#123", "#456", "#789"],
  fontSans: "sans-serif",
  fontMono: "monospace",
} as ChartColors;

it("只传已核验的 IC 和累计值，缺失日保留 null 断点", () => {
  expect(icSeries(research, "normal_ic")).toEqual([
    { date: "2026-09-21", value: 0.03, cumulative: 0.03, reason: "ok" },
    { date: "2026-09-22", value: null, cumulative: null, reason: "insufficient_samples" },
  ]);
  expect(icSeries(research, "rank_ic")[1]?.value).toBeNull();
  const option = icOption(research, "normal_ic", colors);
  const series = option.series as Array<{ data: unknown[]; connectNulls?: boolean }>;
  expect(series[0]?.data[1]).toBeNull();
  expect(series[1]?.data[1]).toBeNull();
  expect(series[1]?.connectNulls).toBe(false);
});

it("衰减和分组只呈现原始可用值，不补值或计算平均", () => {
  expect(decaySeries(research, "normal_ic")).toEqual([
    { lag: 1, value: 0.03, reason: "evaluated" },
    { lag: 2, value: null, reason: "no_target_period" },
  ]);
  expect(groupCounts(research)).toEqual([3, 5]);
  expect(groupSeries(research, 5)).toEqual([
    {
      date: "2026-09-21",
      groups: [{ group_number: 1, cumulative_return: 0.01, target_weight_turnover: 0.2 }],
    },
    { date: "2026-09-22", groups: [] },
  ]);
});

it("IC 双轴和衰减轴能区分相近的小数刻度", () => {
  type Axis = { axisLabel: { formatter: (value: number) => string } };
  const axes = icOption(research, "normal_ic", colors).yAxis as Axis[];
  const decayAxis = decayOption(research, "normal_ic", colors).yAxis as Axis;
  for (const axis of [...axes, decayAxis]) {
    expect(axis.axisLabel.formatter(0.0449)).toBe("0.0449");
    expect(axis.axisLabel.formatter(0.0451)).toBe("0.0451");
    expect(axis.axisLabel.formatter(-0.0142)).toBe("-0.0142");
  }
});

it("分组收益与换手沿完整评价日期轴保留中间无样本日的 null 断点", () => {
  const withGap = {
    ...research,
    coverage_days: [
      { decision_date: "2026-09-21" },
      { decision_date: "2026-09-22" },
      { decision_date: "2026-09-23" },
    ],
    portfolio_days: [
      {
        decision_date: "2026-09-21",
        groupings: [
          {
            status: "ok",
            group_count: 3,
            groups: [{ group_number: 1, cumulative_return: 0.01, target_weight_turnover: 0.2 }],
          },
        ],
      },
      {
        decision_date: "2026-09-23",
        groupings: [
          {
            status: "ok",
            group_count: 3,
            groups: [{ group_number: 1, cumulative_return: 0.03, target_weight_turnover: 0.3 }],
          },
        ],
      },
    ],
  } as FactorResearchDisplay;
  expect(groupSeries(withGap, 3).map((point) => point.date)).toEqual([
    "2026-09-21",
    "2026-09-22",
    "2026-09-23",
  ]);
  expect(groupSeries(withGap, 3)[1]?.groups).toEqual([]);
  const groups = groupOption(withGap, 3, colors);
  const turnover = turnoverOption(withGap, 3, colors);
  const groupXAxis = groups.xAxis as { data: string[] };
  const turnoverXAxis = turnover.xAxis as { data: string[] };
  const groupLines = groups.series as Array<{ data: (number | null)[]; connectNulls?: boolean }>;
  const turnoverBars = turnover.series as Array<{ data: (number | null)[] }>;
  expect(groupXAxis.data).toEqual(["2026-09-21", "2026-09-22", "2026-09-23"]);
  expect(turnoverXAxis.data).toEqual(groupXAxis.data);
  expect(groupLines[0]?.data).toEqual([0.01, null, 0.03]);
  expect(groupLines[0]?.connectNulls).toBe(false);
  expect(turnoverBars[0]?.data).toEqual([0.2, null, 0.3]);
});
