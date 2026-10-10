import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { restoreMinuteExportRequest, restoreMinuteRequest } from "@/api/minuteBacktests";
import { META_QUERY_KEY } from "@/api/useMeta";
import type { EChartProps } from "@/charts/EChart";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import {
  auctionParameters,
  nShapeParameters,
  parameterSet,
} from "./MinuteParameterControls.fixture";

// JSDOM has no Canvas renderer; retain the real page-built chart values for the browser gate.
vi.mock("@/charts/EChart", async () => {
  const { chartColors } = await import("@/charts/tokens");
  return {
    EChart: ({ label, build }: EChartProps) => (
      <div role="img" aria-label={label} data-options={JSON.stringify(build(chartColors()))} />
    ),
  };
});

const base = "*/api/v1/backtests/minute-runtime";
const serving = metaEnvelope({ viewer: "researcher" }).serving;
const jobId = "6fd352e6-ccbd-4b86-8b41-73b0d90fde33";
// Browser projection fixture only; original installed/sealed acceptance is in Python.
const source: Schemas["MinuteSourceOption"] = {
  source_key: "synthetic-source",
  source_version: 1,
  full_input_hash: "a".repeat(64),
  core_input_hash: "8db84adc6f989fd88d0215b11ec5c43973d0db7b13d5b382252a0d19a38cda5c",
  seed_hash: "c".repeat(64),
  native_id: "n_shape",
  native_name: "N字形",
  native_version: 1,
  native_registration_hash: "d".repeat(64),
  native_executable_fingerprint: "e".repeat(64),
  wrapper_registration_hash: "f".repeat(64),
  profile_hash: "c9394077acc20be247d6df5851b7fc32aaf92e83e9c3b308bcd3130a6b56a96e",
  dataset_snapshot_id: "2".repeat(64),
  start_date: "2026-07-31",
  end_date: "2026-08-03",
  work_units: 600,
  provenance: {
    source_kind: "reconstructed",
    extracted_at: "2026-10-07T00:00:00Z",
    published_at: "2026-10-07T00:00:00Z",
    replay_start: "2026-07-31T01:30:00Z",
    replay_end: "2026-08-03T07:00:00Z",
    acquisition_commits: ["a".repeat(40)],
    real_capture_times: [],
    research_code_commit: "a".repeat(40),
    visibility_policy_id: "synthetic-fixture",
    visibility_policy_version: 1,
    visibility_limitations: "Synthetic browser projection only.",
  },
};
const job: Schemas["MinuteJob"] = {
  job_id: jobId,
  status: "queued",
  version: 1,
  created_at: "2026-10-07T00:00:00Z",
  updated_at: "2026-10-07T00:00:00Z",
  spec_hash: "3".repeat(64),
  source_key: source.source_key,
  source_version: source.source_version,
  full_input_hash: source.full_input_hash,
  native_id: source.native_id,
  native_name: source.native_name,
  native_version: source.native_version,
  start_date: source.start_date,
  end_date: source.end_date,
  result_hash: null,
};

beforeEach(() => {
  sessionStorage.clear();
  server.use(
    metaHandler(metaEnvelope({ viewer: "researcher" })),
    http.get("*/api/v1/backtests", () =>
      HttpResponse.json({
        data: { available: false, runs: [], total: 0, next_offset: null },
        serving,
      }),
    ),
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({
        data: {
          available: true,
          can_run: true,
          can_export: true,
          source_count: 1,
          source_unavailable_count: 0,
          valuation_basis: "pit_asof_15:00",
        },
        serving,
      }),
    ),
    http.get(`${base}/sources`, () =>
      HttpResponse.json({
        data: { available: true, sources: [source], unavailable_count: 0 },
        serving,
      }),
    ),
    http.get(`${base}/runs`, () =>
      HttpResponse.json({ data: { available: true, jobs: [], next_cursor: null }, serving }),
    ),
    http.get(`${base}/runs/${jobId}`, () =>
      HttpResponse.json({
        data: {
          job,
          source,
          result_hash: null,
          tables: [],
          message: "已排队。",
        },
        serving,
      }),
    ),
  );
});

async function openMinutePlayback() {
  const view = renderApp("/backtest?view=minute");
  await screen.findByRole("button", { name: /^(打开|收起)分钟回放$/ });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: /^(打开|收起)分钟回放$/ })).toBeEnabled(),
  );
  const current = screen.getByRole("button", { name: /^(打开|收起)分钟回放$/ });
  if (current.textContent === "打开分钟回放") await userEvent.click(current);
  return view;
}

it("uses the exact selected source and native version; unknown submission retries identical bytes", async () => {
  const sent: Record<string, unknown>[] = [];
  server.use(
    http.post(`${base}/runs`, async ({ request }) => {
      const body = (await request.json()) as Record<string, unknown>;
      sent.push(body);
      if (sent.length === 1) return HttpResponse.error();
      return HttpResponse.json({
        command_id: body.command_id,
        status: "submitted",
        job_id: jobId,
        message: "已提交分钟回测。",
      });
    }),
  );
  const user = userEvent.setup();
  await openMinutePlayback();
  expect(await screen.findByRole("option", { name: /N字形 · 版本 1/ })).toBeVisible();
  fireEvent.change(screen.getByLabelText("训练开始"), { target: { value: "2026-07-01" } });
  fireEvent.change(screen.getByLabelText("训练结束"), { target: { value: "2026-07-15" } });
  fireEvent.change(screen.getByLabelText("验证开始"), { target: { value: "2026-07-16" } });
  fireEvent.change(screen.getByLabelText("验证结束"), { target: { value: "2026-07-30" } });
  await user.click(screen.getByRole("button", { name: "运行分钟回测" }));
  await screen.findByText("提交状态待确认，请重试原请求。");
  expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeDisabled();
  expect(screen.getByLabelText("策略版本与输入")).toBeDisabled();
  await user.click(screen.getByRole("button", { name: "重试原请求" }));
  await screen.findByText("已提交分钟回测。");
  expect(sent).toHaveLength(2);
  expect(sent[1]).toEqual(sent[0]);
  expect(sent[0]?.config).toMatchObject({
    source_key: source.source_key,
    source_version: 1,
    full_input_hash: source.full_input_hash,
    native_id: "n_shape",
    native_version: 1,
  });
  expect(sent[0]).not.toHaveProperty("actor_id");
  expect(sent[0]?.config).not.toHaveProperty("hold_days");
});

it("empty installed sources disable submission and keep legacy results visible", async () => {
  server.use(
    http.get(`${base}/sources`, () =>
      HttpResponse.json({ data: { available: true, sources: [], unavailable_count: 1 }, serving }),
    ),
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({
        data: { available: true, can_run: false, source_count: 0, source_unavailable_count: 1 },
        serving,
      }),
    ),
  );
  await openMinutePlayback();
  await waitFor(() => expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeDisabled());
  expect(await screen.findByText("尚无可用分钟来源，请先准备完整发布资料。")).toBeVisible();
  expect(await screen.findByText("还没有可查看的回放结果")).toBeVisible();
});

it("restores the original uncertain request for the same owner even when the current source hash changes", async () => {
  const original = {
    command_id: "923020b8-a005-45ee-8daf-6bc2c714ed2c",
    requested_at: "2026-10-07T00:00:00Z",
    config: {
      source_key: source.source_key,
      source_version: source.source_version,
      full_input_hash: source.full_input_hash,
      native_id: source.native_id,
      native_version: source.native_version,
      random_seed: 0,
      deadline: "2026-10-08T00:00:00Z",
      protocol: {
        train_range: { start_date: "2026-07-01", end_date: "2026-07-15" },
        validation_range: { start_date: "2026-07-16", end_date: "2026-07-30" },
        frozen_outer_test_range: { start_date: source.start_date, end_date: source.end_date },
      },
    },
  };
  sessionStorage.setItem("rquant.minute.pending:researcher", JSON.stringify(original));
  const sent: unknown[] = [];
  server.use(
    http.get(`${base}/sources`, () =>
      HttpResponse.json({
        data: {
          available: true,
          sources: [{ ...source, full_input_hash: "9".repeat(64) }],
          unavailable_count: 0,
        },
        serving,
      }),
    ),
    http.post(`${base}/runs`, async ({ request }) => {
      const body = await request.json();
      sent.push(body);
      return HttpResponse.json({
        command_id: original.command_id,
        status: "failed",
        job_id: null,
        message: "原请求未完成。",
      });
    }),
  );
  const user = userEvent.setup();
  renderApp("/backtest?view=minute");
  await screen.findByText("提交状态待确认，请重试原请求。");
  expect(screen.getByLabelText("训练开始")).toHaveValue("2026-07-01");
  expect(screen.getByLabelText("策略版本与输入")).toBeDisabled();
  await waitFor(() => expect(screen.getByRole("button", { name: "重试原请求" })).toBeEnabled());
  await user.click(screen.getByRole("button", { name: "重试原请求" }));
  await screen.findByText("原请求未完成。");
  expect(sent).toEqual([original]);
  expect(sessionStorage.getItem("rquant.minute.pending:researcher")).toBeNull();
});

it("does not restore another owner's command", async () => {
  sessionStorage.setItem(
    "rquant.minute.pending:other-owner",
    JSON.stringify({ command_id: jobId }),
  );
  await openMinutePlayback();
  await screen.findByRole("option", { name: /N字形 · 版本 1/ });
  expect(screen.queryByRole("button", { name: "重试原请求" })).not.toBeInTheDocument();
  expect(screen.getByLabelText("训练开始")).toHaveValue("");
});

it.each(["new", "restored-v1"] as const)(
  "auction-wire ordinary %s persists and posts the same complete body without upgrading a recovered request",
  async (mode) => {
    const original = {
      command_id: "923020b8-a005-45ee-8daf-6bc2c714ed2c",
      requested_at: "2026-10-07T00:00:00Z",
      config: {
        kind: "minute_parameter_replay",
        source_key: source.source_key,
        source_version: source.source_version,
        full_input_hash: source.full_input_hash,
        parameters: parameterSet({ ...auctionParameters, max_hold_days: 7 }),
        random_seed: 137,
        deadline: "2026-10-08T00:00:00Z",
        protocol: {
          train_range: { start_date: "2026-01-05", end_date: "2026-02-27" },
          validation_range: { start_date: "2026-03-02", end_date: "2026-04-30" },
          frozen_outer_test_range: { start_date: "2026-05-04", end_date: "2026-08-03" },
        },
      },
    } satisfies Schemas["MinuteCreateRequest"];
    const sourceDefault = parameterSet(auctionParameters);
    const defaultBefore = JSON.stringify(sourceDefault);
    const sent: string[] = [];
    const saved: (string | null)[] = [];
    server.use(
      http.get(`${base}/parameter-sources`, () =>
        HttpResponse.json({
          data: {
            available: true,
            can_run: true,
            sources: [
              {
                source_key: source.source_key,
                source_version: source.source_version,
                full_input_hash: source.full_input_hash,
                display_name: "竞价参数资料",
                start_date: auctionParameters.start_date,
                end_date: auctionParameters.end_date,
                frequency: "1min",
                source_nature: "synthetic_validation",
                provenance: source.provenance,
                unavailable_reasons: [],
                capabilities: [
                  {
                    family: "auction_gap",
                    display_name: "竞价高开",
                    default_parameters: sourceDefault,
                    supported_parameter_names: [
                      ...Object.keys(auctionParameters),
                      ...Object.keys(auctionParameters.paper).map((key) => `paper.${key}`),
                    ],
                    unavailable_reasons: [],
                  },
                ],
              },
            ],
            unavailable_count: 0,
          },
          serving,
        }),
      ),
      http.post(`${base}/runs`, async ({ request }) => {
        sent.push(await request.text());
        saved.push(sessionStorage.getItem("rquant.minute.pending:researcher"));
        return HttpResponse.error();
      }),
    );
    if (mode === "restored-v1")
      sessionStorage.setItem("rquant.minute.pending:researcher", JSON.stringify(original));
    await openMinutePlayback();
    if (mode === "new") {
      fireEvent.change(screen.getByLabelText("回测配置"), { target: { value: "parameters" } });
      await screen.findByRole("option", { name: /竞价参数资料/ });
      for (const [label, value] of [
        ["训练开始", "2026-01-05"],
        ["训练结束", "2026-02-27"],
        ["验证开始", "2026-03-02"],
        ["验证结束", "2026-04-30"],
        ["样本外开始", "2026-05-04"],
        ["样本外结束", "2026-08-03"],
        ["最长持仓（交易日）", "7"],
        ["随机种子", "137"],
      ])
        fireEvent.change(screen.getByLabelText(label ?? ""), { target: { value } });
      await waitFor(() =>
        expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeEnabled(),
      );
      await userEvent.click(screen.getByRole("button", { name: "运行分钟回测" }));
    } else {
      await waitFor(() => expect(screen.getByRole("button", { name: "重试原请求" })).toBeEnabled());
      await userEvent.click(screen.getByRole("button", { name: "重试原请求" }));
    }
    await screen.findByText("提交状态待确认，请重试原请求。");
    await waitFor(() => expect(sent).toHaveLength(1));
    expect(saved).toEqual(sent);
    const body = restoreMinuteRequest(sent[0] ?? null);
    expect(body).not.toBeNull();
    expect(body?.config).toMatchObject({
      kind: original.config.kind,
      source_key: original.config.source_key,
      source_version: original.config.source_version,
      full_input_hash: original.config.full_input_hash,
      protocol: original.config.protocol,
      random_seed: original.config.random_seed,
      parameters: {
        ...original.config.parameters,
        schema_version: mode === "new" ? 2 : 1,
        parameters: {
          ...original.config.parameters.parameters,
          ...(mode === "new" ? { next_day_price_policy: "keep_candidate_mark_unavailable" } : {}),
        },
      },
    });
    if (mode === "restored-v1") expect(sent[0]).toBe(JSON.stringify(original));
    await userEvent.click(screen.getByRole("button", { name: "重试原请求" }));
    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toBe(sent[0]);
    expect(saved).toEqual(sent);
    expect(JSON.stringify(sourceDefault)).toBe(defaultBefore);
  },
);

it("shows PIT quote times and an unavailable day without filling its NAV with zero", async () => {
  const resultHash = "4".repeat(64);
  const completed = { ...job, status: "completed", result_hash: resultHash };
  server.use(
    http.get(`${base}/runs`, () =>
      HttpResponse.json({
        data: { available: true, jobs: [completed], next_cursor: null },
        serving,
      }),
    ),
    http.get(`${base}/runs/${jobId}`, () =>
      HttpResponse.json({
        data: {
          job: completed,
          source,
          result_hash: resultHash,
          daily_status: "unavailable",
          signal_count: 3,
          order_count: 2,
          fill_count: 2,
          queue_count: 3,
          tables: [
            "signals",
            "orders",
            "fills",
            "paper_queue",
            "account",
            "daily_valuations",
            "execution_profile",
            "replay_summary",
          ],
        },
        serving,
      }),
    ),
    http.get(`${base}/runs/${jobId}/nav`, () =>
      HttpResponse.json({
        data: {
          job_id: jobId,
          result_hash: resultHash,
          daily_status: "unavailable",
          basis: "pit_asof_15:00",
          points: [
            {
              trade_date: "2026-07-31",
              as_of: "2026-07-31T07:00:00Z",
              basis: "pit_asof_15:00",
              status: "complete",
              nav: "100092.9000",
              cash: "90000",
              account_snapshot_id: "5".repeat(64),
              profile_hash: source.profile_hash,
              price_times: [
                {
                  code: "000001.SZ",
                  event_time: "2026-07-31T06:59:00Z",
                  available_at: "2026-07-31T06:59:01Z",
                  quote_snapshot_id: "6".repeat(64),
                },
              ],
              unavailable_reasons: [],
            },
            {
              trade_date: "2026-08-03",
              as_of: "2026-08-03T07:00:00Z",
              basis: "pit_asof_15:00",
              status: "unavailable",
              nav: null,
              cash: null,
              account_snapshot_id: null,
              profile_hash: source.profile_hash,
              price_times: [],
              unavailable_reasons: ["持仓缺少当时可见报价。"],
            },
          ],
        },
        serving,
      }),
    ),
    http.get(`${base}/runs/${jobId}/rows`, ({ request }) =>
      HttpResponse.json({
        data: {
          job_id: jobId,
          result_hash: resultHash,
          table: new URL(request.url).searchParams.get("table"),
          rows: [
            {
              sequence: 0,
              payload: {
                executed_at: "2026-07-31T02:00:00Z",
                quantity: 100,
                price: "10.00",
                total_fees: "5.10",
              },
            },
          ],
          total: 1,
          next_offset: null,
        },
        serving,
      }),
    ),
  );
  const user = userEvent.setup();
  await openMinutePlayback();
  expect(await screen.findByText("部分交易日缺少有效报价，净值有缺口。")).toBeVisible();
  expect(await screen.findByText("2026-08-03 · 估值不可用")).toBeVisible();
  const options = JSON.parse(
    screen.getByRole("img", { name: "分钟策略每日净值" }).getAttribute("data-options") ?? "null",
  );
  expect(options.series[0].data).toEqual([100092.9, null]);
  expect(options.series[0].connectNulls).toBe(false);
  await user.click(screen.getByText("2026-07-31 · 100,092.90"));
  expect(await screen.findByText(/报价 2026-07-31 14:59:00/)).toBeVisible();
  expect(await screen.findByText(/成交价 10.00 · 费用 5.10/)).toBeVisible();
  await user.click(screen.getByText("2026-08-03 · 估值不可用"));
  expect(screen.getByText("持仓缺少当时可见报价。")).toBeVisible();
});

it("keeps opaque signal identity and raw hashes in details only", async () => {
  const resultHash = "4".repeat(64);
  const opaqueId = `min1:opaque:${"8".repeat(64)}`;
  const rawHash = "9".repeat(64);
  const completed = { ...job, status: "completed", result_hash: resultHash };
  server.use(
    http.get(`${base}/runs`, () =>
      HttpResponse.json({
        data: { available: true, jobs: [completed], next_cursor: null },
        serving,
      }),
    ),
    http.get(`${base}/runs/${jobId}`, () =>
      HttpResponse.json({
        data: {
          job: completed,
          source,
          result_hash: resultHash,
          daily_status: "complete",
          signal_count: 1,
          order_count: 0,
          fill_count: 0,
          queue_count: 0,
          tables: ["signals"],
        },
        serving,
      }),
    ),
    http.get(`${base}/runs/${jobId}/nav`, () =>
      HttpResponse.json({
        data: {
          job_id: jobId,
          result_hash: resultHash,
          daily_status: "complete",
          basis: "pit_asof_15:00",
          points: [],
        },
        serving,
      }),
    ),
    http.get(`${base}/runs/${jobId}/rows`, ({ request }) =>
      HttpResponse.json({
        data: {
          job_id: jobId,
          result_hash: resultHash,
          table: new URL(request.url).searchParams.get("table"),
          rows: [
            {
              sequence: 0,
              payload: {
                candidate_id: opaqueId,
                action: "b_intent",
                event_time: "2026-07-31T02:00:00Z",
                raw_input_hash: rawHash,
              },
            },
          ],
          total: 1,
          next_offset: null,
        },
        serving,
      }),
    ),
  );
  const user = userEvent.setup();
  await openMinutePlayback();
  await screen.findByRole("combobox", { name: "分钟结果内容" });
  await user.selectOptions(screen.getByRole("combobox", { name: "分钟结果内容" }), "signals");
  expect(screen.queryByText(opaqueId, { exact: false })).not.toBeInTheDocument();
  expect(screen.queryByText(rawHash, { exact: false })).not.toBeInTheDocument();
  const label = await screen.findByText("买入意向 · 2026-07-31 10:00:00");
  await user.click(label);
  expect(await screen.findByText(opaqueId, { exact: false })).toBeVisible();
  expect(screen.getByText(rawHash, { exact: false })).toBeVisible();
});

// Exact existing build_minute_performance output from frozen synthetic sealed result34.
const originalPerformance: Schemas["MinuteReplayPerformance"] = {
  contract: "minute-replay-performance/v1",
  input_hash: "8db84adc6f989fd88d0215b11ec5c43973d0db7b13d5b382252a0d19a38cda5c",
  profile_hash: "c9394077acc20be247d6df5851b7fc32aaf92e83e9c3b308bcd3130a6b56a96e",
  basis: "pit_asof_15:00",
  status: "complete",
  daily: [
    {
      trade_date: "2026-07-31",
      status: "complete",
      nav: "100092.899999999999000",
      daily_return: "0.00092899999999999",
      normalized_nav: 1.000929,
      drawdown: 0.0,
    },
    {
      trade_date: "2026-08-03",
      status: "complete",
      nav: "99256.5100",
      daily_return: "-0.0083561371485889509481305582",
      normalized_nav: 0.9925651,
      drawdown: -0.00835613714858896,
    },
  ],
  metrics: {
    summary: {
      observations: 2,
      total_return: -0.007434900000000022,
      annualized_return: -0.6094882637240191,
      annualized_volatility: 0.10422540599768085,
      sharpe: -8.978801970251212,
      sortino: -9.977027212613567,
      calmar: -72.93899715695056,
      max_drawdown: -0.00835613714858896,
      max_drawdown_duration: 1,
      win_rate: 0.5,
      payoff_ratio: 0.11117577218761475,
    },
    benchmark_summary: null,
    relative: null,
    annualized_turnover: 24.584335358332112,
    rolling: [
      {
        trade_date: "2026-07-31",
        volatility: null,
        sharpe: null,
      },
      {
        trade_date: "2026-08-03",
        volatility: null,
        sharpe: null,
      },
    ],
    round_trips: [
      {
        ts_code: "600000.SH",
        industry: "未分类",
        entry_date: "2026-07-31",
        exit_date: "2026-08-03",
        quantity: 1000,
        entry_notional: 10122.0,
        exit_notional: 9398.099999999999,
        entry_fee: 5.1,
        exit_fee: 14.49,
        net_pnl: -743.4900000000015,
        return_rate: -0.07341588411292486,
        holding_days: 3,
      },
    ],
    round_trip_analysis: {
      overall: {
        count: 1,
        net_pnl: -743.4900000000015,
        win_rate: 0.0,
        payoff_ratio: null,
        average_holding_days: 3.0,
      },
      by_symbol: {
        "600000.SH": {
          count: 1,
          net_pnl: -743.4900000000015,
          win_rate: 0.0,
          payoff_ratio: null,
          average_holding_days: 3.0,
        },
      },
      by_industry: {
        未分类: {
          count: 1,
          net_pnl: -743.4900000000015,
          win_rate: 0.0,
          payoff_ratio: null,
          average_holding_days: 3.0,
        },
      },
      by_holding_days: {
        "3": {
          count: 1,
          net_pnl: -743.4900000000015,
          win_rate: 0.0,
          payoff_ratio: null,
          average_holding_days: 3.0,
        },
      },
    },
    distribution: {
      count: 2,
      mean: -0.003713568574294481,
      median: -0.003713568574294481,
      p05: -0.007891880291159505,
      p95: 0.00046474314257054244,
      bins: [
        {
          lower: -1.0,
          upper: 0.0,
          count: 1,
        },
        {
          lower: 0.0,
          upper: 1.0,
          count: 1,
        },
      ],
    },
    streaks: {
      longest_win: 1,
      longest_loss: 1,
      current_win: 0,
      current_loss: 1,
    },
    overfit_state: "not_evaluated",
  },
  monthly: [
    {
      year: 2026,
      month: 1,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 2,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 3,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 4,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 5,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 6,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 7,
      daily_observations: 1,
      return_value: 0.0009289999999999576,
    },
    {
      year: 2026,
      month: 8,
      daily_observations: 1,
      return_value: -0.00835613714858896,
    },
    {
      year: 2026,
      month: 9,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 10,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 11,
      daily_observations: 0,
      return_value: null,
    },
    {
      year: 2026,
      month: 12,
      daily_observations: 0,
      return_value: null,
    },
  ],
  open_quantity: {},
  benchmark_unavailable: "minute_source_has_no_benchmark_series",
  unavailable_reasons: [],
};

const reportHash = "4".repeat(64);
const zipRequestId = "aaa4966f-17aa-530b-844b-1530b2d29310";

function installReport(
  performance: Schemas["MinuteReplayPerformance"] | null = originalPerformance,
  options: {
    canExport?: boolean;
    canReport?: boolean;
    name?: string;
    hash?: string;
    parameterSummary?: Schemas["MinuteSummaryData"];
  } = {},
) {
  const fixedCompleted: Schemas["MinuteJob"] = {
    ...job,
    native_name: options.name ?? "原来源策略名",
    status: "completed",
    result_hash: options.hash ?? reportHash,
  };
  const summary: Schemas["MinuteSummaryData"] = options.parameterSummary ?? {
    job: fixedCompleted,
    source: { ...source, native_name: fixedCompleted.native_name },
    result_hash: fixedCompleted.result_hash,
    performance,
    can_report: options.canReport ?? true,
    signal_count: 3,
    order_count: 2,
    fill_count: 2,
    queue_count: 3,
    tables: [
      "signals",
      "orders",
      "fills",
      "paper_queue",
      "account",
      "daily_valuations",
      "execution_profile",
      "replay_summary",
    ],
  };
  const completed = summary.job;
  server.use(
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({
        data: {
          available: true,
          can_run: false,
          can_export: options.canExport ?? true,
          source_count: 1,
          source_unavailable_count: 0,
          valuation_basis: "pit_asof_15:00",
        },
        serving,
      }),
    ),
    http.get(`${base}/runs`, () =>
      HttpResponse.json({
        data: { available: true, jobs: [completed], next_cursor: null },
        serving,
      }),
    ),
    http.get(`${base}/runs/${completed.job_id}`, () =>
      HttpResponse.json({ data: summary, serving }),
    ),
    http.get(`${base}/runs/${completed.job_id}/nav`, () =>
      HttpResponse.json({
        data: {
          job_id: completed.job_id,
          result_hash: completed.result_hash,
          basis: "pit_asof_15:00",
          daily_status: "complete",
          points: [],
        },
        serving,
      }),
    ),
    http.get(`${base}/runs/${completed.job_id}/rows`, ({ request }) =>
      HttpResponse.json({
        data: {
          job_id: completed.job_id,
          result_hash: completed.result_hash,
          table: new URL(request.url).searchParams.get("table"),
          total: 0,
          rows: [],
          next_offset: null,
        },
        serving,
      }),
    ),
  );
}

it("API08 parameter results reuse original owner performance and HTML/ZIP identities with same-body export recovery", async () => {
  // Synthetic union projection using the retained owner performance values above.
  const parameters = parameterSet({ ...nShapeParameters, max_hold_days: 13 });
  const completed: Schemas["MinuteParameterJob"] = {
    ...job,
    job_id: "64b75c4e-2f7c-4842-adeb-ab6f405da382",
    native_id: `np.${"r".repeat(52)}`,
    native_name: "N字形完整参数",
    kind: "minute_parameter_replay",
    family: "n_shape",
    parameter_hash: "b".repeat(64),
    evaluator_semantic_version: "2.0.0",
    parameters,
    status: "completed",
    result_hash: reportHash,
  };
  const parameterSource: Schemas["MinuteParameterResultSource"] = {
    ...source,
    native_id: completed.native_id,
    native_name: completed.native_name,
    kind: completed.kind,
    family: completed.family,
    parameter_hash: completed.parameter_hash,
    evaluator_semantic_version: completed.evaluator_semantic_version,
    parameters,
    baseline_source_key: "synthetic.baseline",
    baseline_source_version: 2,
    baseline_full_input_hash: "7".repeat(64),
    source_nature: "synthetic_validation",
  };
  const summary: Schemas["MinuteSummaryData"] = {
    job: completed,
    source: parameterSource,
    result_hash: reportHash,
    performance: originalPerformance,
    can_report: true,
    tables: [
      "signals",
      "orders",
      "fills",
      "paper_queue",
      "account",
      "daily_valuations",
      "execution_profile",
      "replay_summary",
    ],
  };
  installReport(originalPerformance, { parameterSummary: summary });
  const bodies: string[] = [];
  server.use(
    http.post(`${base}/exports`, async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      bodies.push(await request.text());
      if (bodies.length === 1) return HttpResponse.error();
      const body: unknown = JSON.parse(bodies.at(-1) ?? "null");
      if (
        body === null ||
        typeof body !== "object" ||
        !("command_id" in body) ||
        typeof body.command_id !== "string"
      )
        return new HttpResponse(null, { status: 422 });
      return HttpResponse.json({
        command_id: body.command_id,
        status: "exported",
        job_id: completed.job_id,
        zip_request_id: zipRequestId,
        result_hash: reportHash,
        sha256: "8".repeat(64),
        byte_size: 167577,
        message: "完整报告已准备。",
      } satisfies Schemas["MinuteCommandReceipt"]);
    }),
  );
  const user = userEvent.setup();
  const first = await openMinutePlayback();
  await screen.findByRole("region", { name: "N字形完整参数 · 参数回测 · 版本 1" });
  await screen.findByRole("region", { name: "分钟绩效" });
  expect(screen.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
    "href",
    `http://localhost:3000/api/v1/backtests/minute-runtime/runs/${completed.job_id}/report.html?result_hash=${reportHash}`,
  );
  expect(screen.queryByText("还没有可查看的回放结果")).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "2026年7月，月收益 0.09%" })).toBeVisible();
  expect(screen.getByRole("table", { name: "闭环交易" })).toHaveTextContent("-743.49");
  await user.click(screen.getByRole("button", { name: "准备完整 ZIP" }));
  await screen.findByRole("button", { name: "重试原导出" });
  const saved = sessionStorage.getItem("rquant.minute.export:researcher");
  expect(JSON.parse(saved ?? "null")).toMatchObject({
    job_id: completed.job_id,
    result_hash: reportHash,
  });
  first.unmount();
  await openMinutePlayback();
  await user.click(await screen.findByRole("button", { name: "重试原导出" }));
  const zip = await screen.findByRole("link", { name: "下载 ZIP" });
  expect(zip).toHaveAttribute(
    "href",
    `http://localhost:3000/api/v1/backtests/minute-runtime/runs/${completed.job_id}/exports/${zipRequestId}.zip?result_hash=${reportHash}`,
  );
  expect(bodies).toHaveLength(2);
  expect(JSON.parse(bodies[1] ?? "null")).toEqual(JSON.parse(bodies[0] ?? "null"));
  expect(JSON.parse(bodies[0] ?? "null")).toEqual(JSON.parse(saved ?? "null"));
});

it("R05 shows original performance, FIFO fees, months, daily gaps and owner name without a second empty state", async () => {
  installReport();
  const user = userEvent.setup();
  await openMinutePlayback();
  const panel = await screen.findByRole("region", { name: "分钟绩效" });
  expect(within(panel).getByText("-0.74%")).toBeVisible();
  expect(
    within(within(panel).getByRole("region", { name: "分钟绩效概览" })).getByText("-0.84%"),
  ).toBeVisible();
  const fifo = screen.getByRole("table", { name: "闭环交易" });
  expect(within(fifo).getByText("5.10")).toBeVisible();
  expect(within(fifo).getByText("14.49")).toBeVisible();
  expect(within(fifo).getByText("-743.49")).toBeVisible();
  expect(screen.getByRole("button", { name: "2026年7月，月收益 0.09%" })).toHaveAttribute(
    "data-tone",
    "up",
  );
  expect(screen.getByRole("button", { name: "2026年8月，月收益 -0.84%" })).toHaveAttribute(
    "data-tone",
    "down",
  );
  expect(screen.getByRole("button", { name: "2026年9月，不在本次区间" })).toHaveAttribute(
    "data-tone",
    "unknown",
  );
  expect(screen.queryByText("还没有可查看的回放结果")).not.toBeInTheDocument();
  expect(screen.getByRole("region", { name: "原来源策略名 · 版本 1" })).toBeVisible();
  expect(screen.queryByText(reportHash, { exact: false })).not.toBeInTheDocument();
  const chart = JSON.parse(
    screen.getByRole("img", { name: "分钟绩效净值与回撤" }).getAttribute("data-options") ?? "null",
  );
  expect(chart.series[0].data).toEqual([1.000929, 0.9925651]);
  expect(chart.series[1].data).toEqual([0, -0.00835613714858896]);
  expect(chart.series[0].connectNulls).toBe(false);
  await user.click(screen.getByRole("button", { name: "查看完整绩效" }));
  expect(await screen.findByRole("dialog", { name: "完整绩效" })).toBeVisible();
  expect(screen.getByRole("table", { name: "滚动指标" })).toBeVisible();
});

it("R05 unavailable original performance retains nulls and does not invent benchmark or zero returns", async () => {
  installReport({
    ...originalPerformance,
    status: "unavailable",
    metrics: null,
    monthly: [],
    unavailable_reasons: ["daily_nav_unavailable"],
    daily: originalPerformance.daily.map((day, index) => ({
      ...day,
      status: index === 0 ? "complete" : "unavailable",
      nav: index === 0 ? day.nav : null,
      daily_return: null,
      normalized_nav: null,
      drawdown: null,
    })),
  });
  await openMinutePlayback();
  expect(await screen.findByText("绩效资料有缺口，完整指标不可用。")).toBeVisible();
  const daily = screen.getByRole("table", { name: "每日收益与回撤" });
  expect(within(daily).queryByText("0.00%")).not.toBeInTheDocument();
  expect(screen.queryByRole("button", { name: "查看完整绩效" })).not.toBeInTheDocument();
  expect(screen.queryByRole("table", { name: "闭环交易" })).not.toBeInTheDocument();
  expect(screen.getByText("未提供基准，超额指标不可用。")).toBeVisible();
  const chart = JSON.parse(
    screen.getByRole("img", { name: "分钟绩效净值与回撤" }).getAttribute("data-options") ?? "null",
  );
  expect(chart.series[0].data).toEqual([null, null]);
  expect(chart.series[1].data).toEqual([null, null]);
  expect(chart.series[1].connectNulls).toBe(false);
});

it("R05 zero is a real monthly observation and missing monthly facts remain uncolored", async () => {
  installReport({
    ...originalPerformance,
    monthly: [
      { year: 2026, month: 7, daily_observations: 1, return_value: 0 },
      { year: 2026, month: 8, daily_observations: 0, return_value: null },
    ],
  });
  await openMinutePlayback();
  expect(await screen.findByRole("button", { name: "2026年7月，月收益 0.00%" })).toHaveAttribute(
    "data-tone",
    "flat",
  );
  const missing = screen.getByRole("button", { name: "2026年8月，暂无月收益" });
  expect(missing).toHaveAttribute("data-tone", "unknown");
  expect(within(missing).getByText("—")).toBeVisible();
});

it("R05 downloads exact HTML and published ZIP using the original receipt identity", async () => {
  installReport();
  server.use(
    http.post(`${base}/exports`, async ({ request }) => {
      expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
      const body: unknown = await request.json();
      if (body === null || typeof body !== "object" || !("command_id" in body))
        return new HttpResponse(null, { status: 422 });
      return HttpResponse.json({
        command_id: body.command_id,
        status: "exported",
        job_id: jobId,
        zip_request_id: zipRequestId,
        result_hash: reportHash,
        sha256: "8".repeat(64),
        byte_size: 167577,
        message: "完整报告已准备。",
      });
    }),
  );
  const user = userEvent.setup();
  await openMinutePlayback();
  const html = await screen.findByRole("link", { name: "HTML 报告" });
  expect(html).toHaveAttribute(
    "href",
    `http://localhost:3000/api/v1/backtests/minute-runtime/runs/${jobId}/report.html?result_hash=${reportHash}`,
  );
  expect(html).toHaveAttribute("download");
  await user.click(screen.getByRole("button", { name: "准备完整 ZIP" }));
  const zip = await screen.findByRole("link", { name: "下载 ZIP" });
  expect(zip).toHaveAttribute(
    "href",
    `http://localhost:3000/api/v1/backtests/minute-runtime/runs/${jobId}/exports/${zipRequestId}.zip?result_hash=${reportHash}`,
  );
  expect(zip).toHaveAttribute("download");
  expect(sessionStorage.getItem("rquant.minute.export:researcher")).toBeNull();
  expect(document.querySelector("iframe")).toBeNull();
});

it.each(["pending", "processing", "unknown"] as const)(
  "R05 %s export remount retries the same UUID, time and body",
  async (status) => {
    installReport();
    const sent: unknown[] = [];
    server.use(
      http.post(`${base}/exports`, async ({ request }) => {
        const body: unknown = await request.json();
        sent.push(body);
        if (body === null || typeof body !== "object" || !("command_id" in body))
          return new HttpResponse(null, { status: 422 });
        if (sent.length === 1)
          return status === "unknown"
            ? HttpResponse.error()
            : HttpResponse.json({ command_id: body.command_id, status, message: "正在准备报告。" });
        return HttpResponse.json({
          command_id: body.command_id,
          status: "exported",
          job_id: jobId,
          zip_request_id: zipRequestId,
          result_hash: reportHash,
          sha256: "8".repeat(64),
          byte_size: 167577,
          message: "完整报告已准备。",
        });
      }),
    );
    const user = userEvent.setup();
    const first = await openMinutePlayback();
    await user.click(await screen.findByRole("button", { name: "准备完整 ZIP" }));
    expect(await screen.findByRole("button", { name: "重试原导出" })).toBeEnabled();
    const saved = sessionStorage.getItem("rquant.minute.export:researcher");
    expect(saved).not.toBeNull();
    first.unmount();
    await openMinutePlayback();
    await user.click(await screen.findByRole("button", { name: "重试原导出" }));
    await screen.findByRole("link", { name: "下载 ZIP" });
    expect(sent).toHaveLength(2);
    expect(sent[1]).toEqual(sent[0]);
    expect(sent[0]).toEqual(JSON.parse(saved ?? "null"));
  },
);

it.each(["failed", "conflict"] as const)(
  "R05 terminal %s does not publish a ZIP and releases the original request",
  async (status) => {
    installReport();
    server.use(
      http.post(`${base}/exports`, async ({ request }) => {
        const body: unknown = await request.json();
        if (body === null || typeof body !== "object" || !("command_id" in body))
          return new HttpResponse(null, { status: 422 });
        return HttpResponse.json(
          { command_id: body.command_id, status, message: "原导出未完成，请检查结果。" },
          { status: status === "conflict" ? 409 : 200 },
        );
      }),
    );
    const user = userEvent.setup();
    await openMinutePlayback();
    await user.click(await screen.findByRole("button", { name: "准备完整 ZIP" }));
    expect(await screen.findByText("原导出未完成，请检查结果。")).toBeVisible();
    expect(screen.queryByRole("link", { name: "下载 ZIP" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "重试原导出" })).not.toBeInTheDocument();
    expect(sessionStorage.getItem("rquant.minute.export:researcher")).toBeNull();
  },
);

it("R05 mismatched publication keeps unknown request recoverable and permission forbids new export", async () => {
  installReport();
  server.use(
    http.post(`${base}/exports`, async ({ request }) => {
      const body: unknown = await request.json();
      if (body === null || typeof body !== "object" || !("command_id" in body))
        return new HttpResponse(null, { status: 422 });
      return HttpResponse.json({
        command_id: body.command_id,
        status: "exported",
        job_id: jobId,
        zip_request_id: zipRequestId,
        result_hash: "7".repeat(64),
        sha256: "8".repeat(64),
        byte_size: 10,
        message: "完整报告已准备。",
      });
    }),
  );
  const user = userEvent.setup();
  const app = await openMinutePlayback();
  await user.click(await screen.findByRole("button", { name: "准备完整 ZIP" }));
  expect(await screen.findByRole("button", { name: "重试原导出" })).toBeEnabled();
  expect(screen.queryByRole("link", { name: "下载 ZIP" })).not.toBeInTheDocument();
  app.unmount();
  installReport(originalPerformance, { canExport: false });
  await openMinutePlayback();
  expect(await screen.findByRole("button", { name: "重试原导出" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "准备完整 ZIP" })).toBeDisabled();
});

it("R05 cannot send a new export when the original body cannot be persisted", async () => {
  installReport();
  let posts = 0;
  server.use(
    http.post(`${base}/exports`, () => {
      posts += 1;
      return HttpResponse.error();
    }),
  );
  const user = userEvent.setup();
  await openMinutePlayback();
  const button = await screen.findByRole("button", { name: "准备完整 ZIP" });
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new Error("storage unavailable");
  });
  await user.click(button);
  expect(await screen.findByText("导出请求无法保存，请检查浏览器存储。")).toBeVisible();
  expect(button).toBeDisabled();
  expect(posts).toBe(0);
  expect(screen.queryByRole("link", { name: "下载 ZIP" })).not.toBeInTheDocument();
});

it("R05 recovery uses the saved result hash when a different result is now selected", async () => {
  const original: Schemas["MinuteExportRequest"] = {
    command_id: "923020b8-a005-45ee-8daf-6bc2c714ed2c",
    requested_at: "2026-10-07T00:00:00Z",
    job_id: jobId,
    result_hash: reportHash,
  };
  sessionStorage.setItem("rquant.minute.export:researcher", JSON.stringify(original));
  installReport(originalPerformance, { hash: "7".repeat(64) });
  const sent: unknown[] = [];
  server.use(
    http.post(`${base}/exports`, async ({ request }) => {
      sent.push(await request.json());
      return HttpResponse.json({
        command_id: original.command_id,
        job_id: original.job_id,
        result_hash: original.result_hash,
        status: "exported",
        zip_request_id: zipRequestId,
        sha256: "8".repeat(64),
        byte_size: 167577,
        message: "原结果报告已准备。",
      });
    }),
  );
  const user = userEvent.setup();
  await openMinutePlayback();
  const retry = await screen.findByRole("button", { name: "重试原导出" });
  await waitFor(() => expect(retry).toBeEnabled());
  await user.click(retry);
  expect(await screen.findByText("原结果报告已准备。")).toBeVisible();
  expect(sent).toEqual([original]);
  expect(screen.queryByRole("link", { name: "下载 ZIP" })).not.toBeInTheDocument();
  expect(screen.getByRole("link", { name: "下载原结果 ZIP" })).toHaveAttribute(
    "href",
    `http://localhost:3000/api/v1/backtests/minute-runtime/runs/${jobId}/exports/${zipRequestId}.zip?result_hash=${reportHash}`,
  );
});

it("R05 a different viewer cannot see or recover another owner's pending request", async () => {
  const original: Schemas["MinuteExportRequest"] = {
    command_id: "923020b8-a005-45ee-8daf-6bc2c714ed2c",
    requested_at: "2026-10-07T00:00:00Z",
    job_id: jobId,
    result_hash: reportHash,
  };
  sessionStorage.setItem("rquant.minute.export:researcher", JSON.stringify(original));
  installReport();
  const app = await openMinutePlayback();
  await screen.findByRole("button", { name: "重试原导出" });
  server.use(
    http.get(`${base}/runs`, () =>
      HttpResponse.json({ data: { available: true, jobs: [], next_cursor: null }, serving }),
    ),
  );
  await act(async () => {
    app.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "another-owner" }));
  });
  await waitFor(() =>
    expect(screen.queryByRole("button", { name: "重试原导出" })).not.toBeInTheDocument(),
  );
  expect(screen.queryByRole("link", { name: "HTML 报告" })).not.toBeInTheDocument();
  expect(screen.queryByRole("region", { name: "分钟绩效" })).not.toBeInTheDocument();
  expect(sessionStorage.getItem("rquant.minute.export:researcher")).toBe(JSON.stringify(original));
  expect(sessionStorage.getItem("rquant.minute.export:another-owner")).toBeNull();
});

it("R05 missing private permission hides HTML and disables preparing a ZIP", async () => {
  installReport(originalPerformance, { canExport: false, canReport: false });
  await openMinutePlayback();
  expect(await screen.findByRole("button", { name: "准备完整 ZIP" })).toBeDisabled();
  expect(screen.queryByRole("link", { name: "HTML 报告" })).not.toBeInTheDocument();
  expect(screen.queryByRole("link", { name: "下载 ZIP" })).not.toBeInTheDocument();
});

it("R05 stored recovery accepts the exact four fields and rejects malformed dates or extra identity", () => {
  const original: Schemas["MinuteExportRequest"] = {
    command_id: "923020b8-a005-45ee-8daf-6bc2c714ed2c",
    requested_at: "2026-10-07T00:00:00Z",
    job_id: jobId,
    result_hash: reportHash,
  };
  expect(restoreMinuteExportRequest(JSON.stringify(original))).toEqual(original);
  expect(
    restoreMinuteExportRequest(JSON.stringify({ ...original, actor_id: "researcher" })),
  ).toBeNull();
  expect(
    restoreMinuteExportRequest(JSON.stringify({ ...original, result_hash: "not-a-hash" })),
  ).toBeNull();
  expect(
    restoreMinuteExportRequest(
      JSON.stringify({ ...original, requested_at: "2026-02-30T00:00:00Z" }),
    ),
  ).toBeNull();
  expect(restoreMinuteExportRequest(" ".repeat(32 * 1024 + 1))).toBeNull();
});

it("R05 result without report eligibility cannot start a fresh export even with write permission", async () => {
  installReport(originalPerformance, { canExport: true, canReport: false });
  await openMinutePlayback();
  expect(await screen.findByRole("button", { name: "准备完整 ZIP" })).toBeDisabled();
  expect(screen.queryByRole("link", { name: "HTML 报告" })).not.toBeInTheDocument();
});

it("loads minute playback only after its own explicit entry", async () => {
  const reads: string[] = [];
  server.use(
    http.get(`${base}/capabilities`, () => {
      reads.push("capabilities");
      return HttpResponse.json({
        data: {
          available: true,
          can_run: true,
          can_export: false,
          source_count: 1,
          source_unavailable_count: 0,
          valuation_basis: "pit_asof_15:00",
        },
        serving,
      });
    }),
    http.get(`${base}/sources`, () => {
      reads.push("sources");
      return HttpResponse.json({
        data: { available: true, sources: [source], unavailable_count: 0 },
        serving,
      });
    }),
    http.get(`${base}/runs`, () => {
      reads.push("runs");
      return HttpResponse.json({ data: { available: true, jobs: [], next_cursor: null }, serving });
    }),
  );
  renderApp("/backtest?view=minute");
  await screen.findByText("还没有可查看的回放结果");
  expect(reads).toEqual([]);
  await userEvent.click(screen.getByRole("button", { name: "打开分钟回放" }));
  expect(await screen.findByRole("option", { name: /N字形 · 版本 1/ })).toBeVisible();
  await waitFor(() => expect(reads.sort()).toEqual(["capabilities", "runs", "sources"]));
});
