import type { Schemas } from "@/api/client";
import { metaEnvelope } from "../../test/fixtures";

export const trackingGeneration = "f".repeat(64);
export const trackingNextGeneration = "e".repeat(64);
export const trackingKey = "rquant.factor.tracking-operation.v1";
export const trackedFactor: Schemas["FactorDefinitionItem"] = {
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
export const anotherFactor = { ...trackedFactor, factor_id: "flow_factor", name_zh: "成交变化" };
export const trackingSummary: Schemas["FactorTrackingSummary"] = {
  latest_trade_date: "2026-09-23",
  yesterday_ic: 0.0312,
  complete_day_count: 20,
  yesterday_long_short: 0.0123,
  week_long_short: -0.0256,
  cumulative_long_short: 0.0789,
  week_day_count: 5,
  week_complete_day_count: 5,
  invalidated: false,
  reason: null,
  ic_20: {
    status: "ok",
    mean: 0.0412,
    sample_std: 0.1,
    ir: 0.412,
    positive_rate: 0.7,
    strong_signal_rate: 0.7,
    t_value: 1.1,
    p_value: 0.2,
    skewness: null,
    excess_kurtosis: null,
    source_day_count: 20,
    valid_day_count: 20,
    insufficient_day_count: 0,
    zero_variance_day_count: 0,
  },
};
export function trackingPanel(
  patch: Partial<Schemas["FactorTrackingPanel"]> = {},
): Schemas["FactorTrackingPanel"] {
  return {
    factor_id: trackedFactor.factor_id,
    availability: "not_tracked",
    status: "not_tracked",
    tracked: false,
    tracking_generation: null,
    definition_head: {
      version: trackedFactor.version,
      content_sha256: trackedFactor.content_sha256,
    },
    actual_start_date: null,
    updated_at: null,
    summary: null,
    reason: null,
    can_set_tracked: true,
    policy_version: 1,
    policy_label: "全市场（剔除北交所、ST） · 每日 · 5组 · RankIC · 无运行后中性化",
    basis_label: "历史回顾研究诊断；累计从实际起日计算，并非实盘收益。",
    ...patch,
  };
}
export function trackingRequest(tracked = true): Schemas["FactorTrackingRequest"] {
  return {
    command_id: "tracking-original-1",
    requested_at: "2026-10-02T00:00:00Z",
    serving_generation_id: trackingGeneration,
    factor_id: trackedFactor.factor_id,
    tracked,
    expected_head: { version: 2, content_sha256: trackedFactor.content_sha256 },
    expected_tracking_generation: null,
  };
}
export function trackingResult(
  request: Schemas["FactorTrackingRequest"],
  status: Schemas["FactorTrackingOperationResult"]["status"] = "applied",
): Schemas["FactorTrackingOperationResult"] {
  return {
    original_request: request,
    status,
    reason: status === "rejected" ? "定义已变化，请刷新后重新确认。" : null,
    receipt:
      status === "applied"
        ? {
            command_id: request.command_id,
            factor_id: request.factor_id,
            tracked: request.tracked,
            tracking_generation: "b".repeat(32),
            segment_id: request.tracked ? "b".repeat(32) : null,
            definition_head: request.expected_head,
          }
        : null,
  };
}
export function trackingEnvelope(
  data: Schemas["FactorTrackingPanel"],
  generationId = trackingGeneration,
) {
  return { data, serving: metaEnvelope({ generationId }).serving };
}
