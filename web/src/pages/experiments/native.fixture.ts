import type { Schemas } from "@/api/client";
import { experimentFixture } from "./formal.fixture";

// Synthetic display facts follow the original C5 native configuration and API04
// models. No market run, captured prices or sealed native execution is claimed.
const original = experimentFixture.family.data.items[0];
const costs = experimentFixture.capabilities.data.default_config?.execution_cost_spec;
if (!original || !costs) throw new Error("original experiment display fixture is missing");
const result = experimentFixture.results[original.experiment_id];
if (!result) throw new Error("original experiment result display fixture is missing");

const configuration: Schemas["NativeMinuteConfiguration"] = {
  kind: "native_minute",
  selection: {
    target: {
      source_kind: "builtin",
      owner_id: "alice",
      strategy_id: "n_shape",
      name: "N 形突破",
      head: {
        version: 1,
        record_hash: "1".repeat(64),
        registration_fingerprint: "2".repeat(64),
        spec_fingerprint: "3".repeat(64),
      },
      parameter_fingerprint: "4".repeat(64),
      cost_fingerprint: "5".repeat(64),
    },
    source_key: "original-minute",
    source_version: 1,
    profile_hash: "f".repeat(64),
  },
  start_date: "2026-01-01",
  end_date: "2026-01-02",
};

const attempt: Schemas["ExperimentAttemptRow"] = {
  ...original,
  family_name: "分钟策略实验",
  configuration,
  rules: null,
  strategy_name: configuration.selection.target.name,
  strategy_version: configuration.selection.target.head.version,
};

const native: Schemas["ExperimentNativeResultIdentity"] = {
  execution: "minute_runtime_replay@2",
  target: configuration.selection.target,
  profile_hash: configuration.selection.profile_hash,
  core_input_hash: "6".repeat(64),
  seed_hash: "7".repeat(64),
  content_hash: "8".repeat(64),
  source_kind: "captured",
  execution_costs: costs,
  parameters: [
    { name: "break_high_ratio", value: 1.002, label: "突破高点比例", display_value: "1.002 倍" },
    { name: "carry_low_ratio", value: 0.998, label: "回踩低点比例", display_value: "0.998 倍" },
    { name: "expires_seconds", value: 90, label: "信号有效期", display_value: "90 秒" },
  ],
  execution_profile: {
    key: "synthetic-minute-profile",
    version: 1,
    initial_cash: "100000",
    execution_costs: costs,
    paper_policy: {
      account_id: "synthetic-paper-alice",
      producer_commit: "a".repeat(40),
      action_quantities: { b_intent: 100, s_intent: 100 },
      execution_lag: "PT5S",
    },
    routing_policy_fingerprint: "9".repeat(64),
    quote_max_age_seconds: 90,
    candidate_max_age_seconds: 604800,
    max_visible_scan_batches: 120,
    max_finalize_scan_batches: 32,
    timestamp_semantics: "provider_snapshot",
  },
};

export const nativeExperimentFixture = {
  actual_market_source: false,
  actual_native_worker: false,
  generation_id: experimentFixture.generation_id,
  configuration,
  attempt,
  capabilities: {
    ...experimentFixture.capabilities,
    data: { ...experimentFixture.capabilities.data, can_search: false },
  } satisfies Schemas["Envelope_ExperimentCapabilities_"],
  mine: {
    ...experimentFixture.mine,
    data: {
      ...experimentFixture.mine.data,
      items: [attempt],
      retained_count: 1,
    },
  } satisfies Schemas["Envelope_ExperimentMineData_"],
  family: {
    ...experimentFixture.family,
    data: { ...experimentFixture.family.data, name: attempt.family_name, items: [attempt] },
  } satisfies Schemas["Envelope_ExperimentFamilyData_"],
  result: {
    ...result,
    data: { ...result.data, configuration, template: null, native },
  } satisfies Schemas["Envelope_ExperimentResultData_"],
};
