import { renderHook, waitFor } from "@testing-library/react";
import { HttpResponse, http } from "msw";
import { createElement, type PropsWithChildren } from "react";
import { AppProviders } from "@/app/App";
import {
  auctionParameters,
  auctionV2Recipe,
  growthParameters,
  nShapeParameters,
  parameterSet,
} from "@/pages/backtest/MinuteParameterControls.fixture";
import { metaEnvelope } from "@/test/fixtures";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import type { Schemas } from "./client";
import {
  type MinuteCreate,
  minuteParameterCreateRecipe,
  restoreMinuteRequest,
  restoreMinuteStudyRequest,
  useMinuteJobs,
  useMinuteSummary,
} from "./minuteBacktests";
import { META_QUERY_KEY } from "./useMeta";

function request(parameters: Schemas["MinuteParameterSet"]["parameters"]): MinuteCreate {
  return {
    command_id: "de7c4e75-6b7d-4153-858b-f4b321546766",
    requested_at: "2026-10-07T02:31:17.123Z",
    config: {
      kind: "minute_parameter_replay",
      source_key: "synthetic-ui-facts",
      source_version: 3,
      full_input_hash: "a".repeat(64),
      parameters: parameterSet(parameters),
      random_seed: 137,
      deadline: "2026-10-08T02:31:17.123Z",
      protocol: {
        train_range: { start_date: "2026-01-05", end_date: "2026-02-27" },
        validation_range: { start_date: "2026-03-02", end_date: "2026-04-30" },
        frozen_outer_test_range: { start_date: "2026-05-04", end_date: "2026-08-03" },
      },
    },
  };
}

it.each([
  { ...nShapeParameters, max_hold_days: 13, carry_low_ratio: 1.123456789 },
  { ...auctionParameters, max_hold_days: 7, factor_score_threshold: null },
  { ...growthParameters, lookback_days: 37, require_large_net_vol: true },
])(
  "restores the complete original parameter request, UUID, clocks and nondefault values",
  (params) => {
    const original = request(params);
    const encoded = JSON.stringify(original);
    expect(JSON.stringify(restoreMinuteRequest(encoded))).toBe(encoded);
  },
);

it("rejects a changed recipe variant, extra browser authority and incomplete numeric fields", () => {
  const original = request(nShapeParameters);
  expect(restoreMinuteRequest(JSON.stringify({ ...original, actor_id: "other" }))).toBeNull();
  expect(
    restoreMinuteRequest(
      JSON.stringify({ ...original, config: { ...original.config, trusted: true } }),
    ),
  ).toBeNull();
  if (!("parameters" in original.config)) throw Error("wrong fixture variant");
  for (const parameters of [
    { ...nShapeParameters, family: "unknown" },
    { ...nShapeParameters, max_hold_days: "5" },
    { ...nShapeParameters, paper: { ...nShapeParameters.paper, extra: true } },
  ]) {
    expect(
      restoreMinuteRequest(
        JSON.stringify({
          ...original,
          config: { ...original.config, parameters: { ...original.config.parameters, parameters } },
        }),
      ),
    ).toBeNull();
  }
});

it("preserves the original fixed native request branch", () => {
  const config = request(nShapeParameters).config;
  const original: MinuteCreate = {
    command_id: "de7c4e75-6b7d-4153-858b-f4b321546766",
    requested_at: "2026-10-07T02:31:17.123Z",
    config: {
      source_key: config.source_key,
      source_version: config.source_version,
      full_input_hash: config.full_input_hash,
      native_id: "n_shape",
      native_version: 1,
      protocol: config.protocol,
      random_seed: config.random_seed,
      deadline: config.deadline,
    },
  };
  expect(restoreMinuteRequest(JSON.stringify(original))).toEqual(original);
});

it("rejects an oversized saved parameter body by UTF-8 bytes without truncating or refreshing it", () => {
  const original = request({
    ...nShapeParameters,
    paper: { ...nShapeParameters.paper, candidate_id: "中".repeat(12_000) },
  });
  const encoded = JSON.stringify(original);
  expect(encoded.length).toBeLessThan(32 * 1024);
  expect(new TextEncoder().encode(encoded).length).toBeGreaterThan(32 * 1024);
  expect(restoreMinuteRequest(encoded)).toBeNull();
});

it("restores the complete fixed candidate-policy v2 auction body without replacing its identity", () => {
  const original = request({
    ...auctionParameters,
    factor_score_threshold: null,
    max_hold_days: 7,
  });
  if (!("parameters" in original.config)) throw Error("wrong fixture variant");
  const body = {
    ...original,
    config: {
      ...original.config,
      parameters: {
        ...original.config.parameters,
        schema_version: 2,
        parameters: {
          ...original.config.parameters.parameters,
          next_day_price_policy: "keep_candidate_mark_unavailable",
        },
      },
    },
  };
  const encoded = JSON.stringify(body);
  expect(JSON.stringify(restoreMinuteRequest(encoded))).toBe(encoded);
});

it("upgrades only a new auction recipe without changing the source default or nullable values", () => {
  const original = parameterSet({ ...auctionParameters, max_hold_days: 7 });
  const before = JSON.stringify(original);
  const created = minuteParameterCreateRecipe(original);
  expect(created).toEqual({
    ...auctionV2Recipe,
    parameters: { ...auctionV2Recipe.parameters, max_hold_days: 7 },
  });
  expect(created).not.toBe(original);
  expect(created.parameters).not.toBe(original.parameters);
  expect(JSON.stringify(original)).toBe(before);
  expect(created.parameters).toMatchObject({
    factor_score_threshold: null,
    entry_vwap_buffer_pct: 0,
  });
  for (const parameters of [nShapeParameters, growthParameters]) {
    const recipe = parameterSet(parameters);
    expect(minuteParameterCreateRecipe(recipe)).toBe(recipe);
    expect(recipe.schema_version).toBe(1);
  }
  expect(minuteParameterCreateRecipe(auctionV2Recipe)).toEqual(auctionV2Recipe);
});

it("rejects a v1 policy field, an incomplete v2 auction and v2 recipes from other families", () => {
  const original = request(auctionParameters);
  if (!("parameters" in original.config)) throw Error("wrong fixture variant");
  for (const recipe of [
    { ...auctionV2Recipe, schema_version: 1 },
    { ...auctionV2Recipe, schema_version: 3 },
    { ...auctionV2Recipe, parameters: auctionParameters },
    {
      ...auctionV2Recipe,
      parameters: { ...auctionV2Recipe.parameters, next_day_price_policy: null },
    },
    {
      ...auctionV2Recipe,
      parameters: { ...auctionV2Recipe.parameters, next_day_price_policy: "exclude_candidate" },
    },
    { ...parameterSet(nShapeParameters), schema_version: 2 },
    { ...parameterSet(growthParameters), schema_version: 2 },
  ]) {
    expect(
      restoreMinuteRequest(
        JSON.stringify({ ...original, config: { ...original.config, parameters: recipe } }),
      ),
    ).toBeNull();
  }
});

it.each([parameterSet(auctionParameters), auctionV2Recipe])(
  "restores an auction study and search base with their original wire, UUID and clocks",
  (recipe) => {
    const original = request(auctionParameters);
    const body = {
      command_id: original.command_id,
      requested_at: original.requested_at,
      source_key: original.config.source_key,
      source_version: original.config.source_version,
      full_input_hash: original.config.full_input_hash,
      parameters: recipe,
      protocol: original.config.protocol,
      settings: [{ score_profile: "synthetic-ui-score", top_n: 7, min_trades: 13 }],
      random_seed: original.config.random_seed,
      deadline: original.config.deadline,
      mode: "grid",
      search: {
        base: recipe,
        axes: [{ path: "max_hold_days", values: [1, 3, 7] }],
        mode: "grid",
        seed: original.config.random_seed,
        requested_trials: null,
      },
      walk_forward: null,
    } satisfies Schemas["MinuteStudyCreateRequest"];
    const encoded = JSON.stringify(body);
    expect(JSON.stringify(restoreMinuteStudyRequest(encoded))).toBe(encoded);
    expect(JSON.stringify(restoreMinuteRequest(JSON.stringify(original)))).toBe(
      JSON.stringify(original),
    );
  },
);

function parameterSummary(recipe: Schemas["MinuteParameterSet"]): Schemas["MinuteSummaryData"] {
  const semantic = recipe.schema_version === 2 ? "2.1.0" : "2.0.0";
  const job = {
    job_id: "64b75c4e-2f7c-4842-adeb-ab6f405da382",
    kind: "minute_parameter_replay",
    status: "completed",
    version: 1,
    created_at: "2026-10-07T00:00:00Z",
    updated_at: "2026-10-07T00:01:00Z",
    spec_hash: "3".repeat(64),
    source_key: "synthetic-ui-auction",
    source_version: 3,
    full_input_hash: "a".repeat(64),
    native_id: `np.${"r".repeat(52)}`,
    native_name: "竞价高开",
    native_version: 1,
    evaluator_semantic_version: semantic,
    start_date: auctionParameters.start_date,
    end_date: auctionParameters.end_date,
    result_hash: "4".repeat(64),
    family: "auction_gap",
    parameters: recipe,
    parameter_hash: "b".repeat(64),
  } satisfies Schemas["MinuteParameterJob"];
  return {
    job,
    source: {
      kind: "minute_parameter_replay",
      source_key: job.source_key,
      source_version: job.source_version,
      full_input_hash: job.full_input_hash,
      core_input_hash: "5".repeat(64),
      seed_hash: "6".repeat(64),
      native_id: job.native_id,
      native_name: job.native_name,
      native_version: 1,
      evaluator_semantic_version: semantic,
      native_registration_hash: "7".repeat(64),
      native_executable_fingerprint: "8".repeat(64),
      wrapper_registration_hash: "9".repeat(64),
      profile_hash: "c".repeat(64),
      dataset_snapshot_id: "d".repeat(64),
      start_date: job.start_date,
      end_date: job.end_date,
      work_units: 600,
      provenance: {
        source_kind: "reconstructed",
        extracted_at: "2026-10-07T00:00:00Z",
        published_at: "2026-10-07T00:00:00Z",
        replay_start: "2026-01-05T01:30:00Z",
        replay_end: "2026-08-03T07:00:00Z",
        acquisition_commits: [],
        real_capture_times: [],
        research_code_commit: "a".repeat(40),
        visibility_policy_id: "synthetic-ui",
        visibility_policy_version: 1,
        visibility_limitations: "合成界面资料。",
      },
      family: "auction_gap",
      parameters: recipe,
      parameter_hash: job.parameter_hash,
      baseline_source_key: "synthetic-ui-baseline",
      baseline_source_version: 1,
      baseline_full_input_hash: "e".repeat(64),
      source_nature: "synthetic_validation",
    },
    fill_count: 0,
    signal_count: 0,
    can_report: false,
    performance: null,
    daily_status: "unavailable",
    result_hash: job.result_hash,
    tables: [],
  };
}

function resultHookHarness() {
  const queryClient = testQueryClient();
  queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "researcher" }));
  function Wrapper({ children }: PropsWithChildren) {
    return createElement(AppProviders, { queryClient, children });
  }
  return { wrapper: Wrapper };
}

it.each([parameterSet(auctionParameters), auctionV2Recipe])(
  "decodes original auction result and job facts only with the matching recipe semantic version",
  async (recipe) => {
    const data = parameterSummary(recipe);
    server.use(
      http.get("*/api/v1/backtests/minute-runtime/runs", () =>
        HttpResponse.json({
          data: { available: true, jobs: [data.job], next_cursor: null },
          serving: metaEnvelope().serving,
        }),
      ),
      http.get(`*/api/v1/backtests/minute-runtime/runs/${data.job.job_id}`, () =>
        HttpResponse.json({ data, serving: metaEnvelope().serving }),
      ),
    );
    const { wrapper } = resultHookHarness();
    const current = renderHook(
      () => ({ jobs: useMinuteJobs(null, 0), summary: useMinuteSummary(data.job.job_id, false) }),
      { wrapper },
    );
    await waitFor(() => expect(current.result.current.summary.data).toEqual(data));
    await waitFor(() => expect(current.result.current.jobs.data?.jobs).toEqual([data.job]));
    expect(current.result.current.summary.error).toBeNull();
    expect(current.result.current.summary.data?.fill_count).toBe(0);
    expect(current.result.current.summary.data?.performance).toBeNull();
    expect(current.result.current.summary.data?.source.native_version).toBe(1);
  },
);

it.each(["job", "source"] as const)(
  "rejects a changed auction %s semantic version while keeping its complete original recipe",
  async (field) => {
    const original = parameterSummary(auctionV2Recipe);
    const data = {
      ...original,
      [field]: { ...original[field], evaluator_semantic_version: "2.0.0" },
    };
    server.use(
      http.get(`*/api/v1/backtests/minute-runtime/runs/${data.job.job_id}`, () =>
        HttpResponse.json({ data, serving: metaEnvelope().serving }),
      ),
    );
    const { wrapper } = resultHookHarness();
    const current = renderHook(() => useMinuteSummary(data.job.job_id, false), { wrapper });
    await waitFor(() => expect(current.result.current.error).toMatchObject({ status: 409 }));
    expect(current.result.current.data).toBeUndefined();
    expect(original[field]).toMatchObject({ evaluator_semantic_version: "2.1.0" });
  },
);
