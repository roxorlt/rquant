import type { ChartColors } from "@/charts/tokens";
import { autocorrelationOption, industryIcOption } from "./factorCharts";
import { diagnosticStatistics, diagnosticSummary } from "./factorDiagnostics.fixture";

const colors: ChartColors = {
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
};

it("自相关图保留全部相邻评价期、服务端值与空点断线", () => {
  const option = autocorrelationOption(diagnosticStatistics, colors);
  expect((option.xAxis as { data: string[] }).data).toEqual(
    diagnosticStatistics.autocorrelation_points.map((point) => point.trade_date),
  );
  const line = (option.series as Array<{ data: (number | null)[]; connectNulls: boolean }>)[0];
  expect(line?.data).toEqual([null, 0.3, null, 0.5]);
  expect(line?.connectNulls).toBe(false);
});

it("行业图保留全部 31 个服务端中文名及原均值，null 不伪造为 0", () => {
  const statistics = {
    ...diagnosticStatistics,
    industry_summaries: Array.from({ length: 31 }, (_, index) => ({
      l1_code: `${801000 + index}.SI`,
      l1_name: `行业 ${index + 1}`,
      sample_count: index + 3,
      ic_summary: { ...diagnosticSummary, mean: index === 7 ? null : index / 1000 },
    })),
  };
  const option = industryIcOption(statistics, colors);
  expect((option.yAxis as { data: string[] }).data).toEqual(
    statistics.industry_summaries.map((item) => item.l1_name),
  );
  const bars = option.series as Array<{ data: (null | { value: number })[] }>;
  expect(bars[0]?.data).toHaveLength(31);
  expect(bars[0]?.data[7]).toBeNull();
  expect(bars[0]?.data[30]?.value).toBe(0.03);
});
