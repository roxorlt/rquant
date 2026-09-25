import type { MetaEnvelope } from "@/api/client";

/** A synthetic /api/v1/meta envelope (shape from the generated schema). */
export function metaEnvelope(
  overrides: {
    state?: MetaEnvelope["serving"]["state"];
    detail?: string;
    generationId?: string;
    viewer?: string | null;
    phase?: MetaEnvelope["data"]["market"]["phase"];
    phaseLabel?: string;
    isTradingDay?: boolean | null;
  } = {},
): MetaEnvelope {
  const generationId =
    overrides.generationId ?? "a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90";
  return {
    data: {
      server_time: "2026-09-24T07:31:30Z",
      viewer: overrides.viewer === undefined ? "tester" : overrides.viewer,
      generation: {
        generation_id: generationId,
        built_at: "2026-09-24T07:31:00Z",
        published_at: "2026-09-24T07:31:00Z",
        previous_generation_id: null,
        producer_commit: "0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b0e5b",
        schema_version: 3,
        age_seconds: 190,
      },
      datasets: [],
      projections: [],
      market: {
        trade_date: "2026-09-24",
        phase: overrides.phase ?? "continuous",
        phase_label: overrides.phaseLabel ?? "连续竞价",
        is_trading_day: overrides.isTradingDay === undefined ? true : overrides.isTradingDay,
      },
    },
    serving: {
      generation_id: generationId,
      built_at: "2026-09-24T07:31:00Z",
      state: overrides.state ?? "ready",
      detail: overrides.detail ?? "serving generation verified",
    },
  };
}
