import type { Schemas } from "@/api/client";

// Synthetic display facts; none of these values are calculated by the page.
export const diagnosticFactor: Schemas["FactorDefinitionItem"] = {
  factor_id: "price_volume_factor",
  name_zh: "价量动量",
  category: "technical",
  category_label: "技术",
  direction: "higher_is_better",
  direction_label: "偏好高值",
  version: 2,
  content_sha256: "a".repeat(64),
  earliest_available_date: null,
  archived: false,
  expression: "ref(close, 2)",
  dependency_columns: ["close"],
  max_history_window: 2,
};

export const diagnosticAvailability: Schemas["FactorRunAvailability"] = {
  enabled: true,
  reason: null,
  start_date: "2026-09-17",
  end_date: "2026-09-22",
  pools: [{ selection: "all", label: "全市场（沪深非 ST）", available: true, reason: null }],
};

export const diagnosticSummary: Schemas["ICSeriesSummary"] = {
  status: "ok",
  mean: 0.0451,
  sample_std: 0.125,
  ir: 0.3608,
  positive_rate: 2 / 3,
  strong_signal_rate: 2 / 3,
  t_value: 0.7,
  p_value: 0.6,
  skewness: null,
  excess_kurtosis: null,
  source_day_count: 4,
  valid_day_count: 3,
  insufficient_day_count: 1,
  zero_variance_day_count: 0,
};

export const diagnosticStatistics: Schemas["FactorExtendedStatistics"] = {
  ic_method: "rank",
  industry_status: "available",
  industry_reason: "缺少明确行业标签的股票未参与行业 IC。",
  industry_summaries: [
    { l1_code: "801010.SI", l1_name: "农林牧渔", sample_count: 7, ic_summary: diagnosticSummary },
    {
      l1_code: "801030.SI",
      l1_name: "基础化工",
      sample_count: 9,
      ic_summary: { ...diagnosticSummary, mean: -0.0142, ir: -0.1136 },
    },
  ],
  industry_coverage_days: [
    { trade_date: "2026-09-17", panel_date: "2026-09-16" },
    { trade_date: "2026-09-18", panel_date: "2026-09-17" },
    { trade_date: "2026-09-21", panel_date: "2026-09-18" },
    { trade_date: "2026-09-22", panel_date: "2026-09-21" },
  ].map((day) => ({
    ...day,
    expected_count: 8,
    valid_label_count: 6,
    paired_count: 4,
    missing_by_reason: [
      { reason: "missing", count: 1 },
      { reason: "boundary_unverified", count: 1 },
    ],
  })),
  autocorrelation_points: [
    {
      trade_date: "2026-09-17",
      previous_trade_date: null,
      common_count: 0,
      status: "first_period",
      value: null,
    },
    {
      trade_date: "2026-09-18",
      previous_trade_date: "2026-09-17",
      common_count: 6,
      status: "ok",
      value: 0.3,
    },
    {
      trade_date: "2026-09-21",
      previous_trade_date: "2026-09-18",
      common_count: 0,
      status: "no_common_members",
      value: null,
    },
    {
      trade_date: "2026-09-22",
      previous_trade_date: "2026-09-21",
      common_count: 4,
      status: "ok",
      value: 0.5,
    },
  ],
};

export const diagnosticResearch: Schemas["FactorStreamResearchDisplay"] = {
  schema_version: 2,
  basis_label: "收盘价到下一次调仓收盘价",
  return_price_basis: "raw",
  pool_label: "全市场（沪深非 ST）",
  holding_sessions: 5,
  neutralization: "none",
  neutralization_label: "无",
  summary_status: "evaluated",
  ic_summary: { normal_ic: diagnosticSummary, rank_ic: diagnosticSummary },
  ic_points: [
    { decision_date: "2026-09-17", value: 0.02, cumulative: 0.02 },
    { decision_date: "2026-09-18", value: 0.0453, cumulative: 0.0653 },
    { decision_date: "2026-09-21", value: null, cumulative: null },
    { decision_date: "2026-09-22", value: 0.07, cumulative: 0.1353 },
  ].map((point) => ({
    decision_date: point.decision_date,
    normal_ic: {
      status: point.value === null ? "insufficient_samples" : "ok",
      value: point.value,
      source_sample_count: 8,
      effective_sample_count: point.value === null ? 0 : 6,
    },
    rank_ic: {
      status: point.value === null ? "insufficient_samples" : "ok",
      value: point.value,
      source_sample_count: 8,
      effective_sample_count: point.value === null ? 0 : 6,
    },
    normal_ic_cumulative_sum: point.cumulative,
    rank_ic_cumulative_sum: point.cumulative,
  })),
  decay_periods: [
    {
      lag: 1,
      status: "evaluated",
      source_day_count: 4,
      valid_pair_count: 18,
      ic_summary: { normal_ic: diagnosticSummary, rank_ic: diagnosticSummary },
    },
  ],
  portfolio_status: "insufficient_data",
  portfolio_days: [],
  coverage_days: diagnosticStatistics.autocorrelation_points.map((point) => ({
    decision_date: point.trade_date,
    status: point.status === "no_common_members" ? "no_samples" : "partial",
    coverage: {
      expected_count: 8,
      valid_count: point.status === "no_common_members" ? 0 : 6,
      factor_missing_count: point.status === "no_common_members" ? 8 : 2,
      return_missing_count: 0,
      factor_missing_by_reason: [
        { reason: "insufficient_history", count: point.status === "no_common_members" ? 8 : 2 },
      ],
      return_missing_by_reason: [],
    },
  })),
  mad_multiple: 2.5,
  extended_statistics: diagnosticStatistics,
};

export function diagnosticResult(
  overrides: Partial<Schemas["FactorResultItem"]> = {},
): Schemas["FactorResultItem"] {
  return {
    job_id: "b".repeat(32),
    spec_sha256: "c".repeat(64),
    definition_content_sha256: diagnosticFactor.content_sha256,
    factor_id: diagnosticFactor.factor_id,
    factor_version: diagnosticFactor.version,
    factor_name_zh: diagnosticFactor.name_zh,
    definition_status: "current",
    status: "succeeded",
    status_label: "已完成",
    failure_message: null,
    updated_at: "2026-09-24T07:31:00Z",
    as_of_time: "2026-09-23T07:00:00Z",
    display_status: "available",
    display_message: "结果已发布。",
    ...overrides,
  };
}
