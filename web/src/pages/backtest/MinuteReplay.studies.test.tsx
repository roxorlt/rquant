import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY, useMeta } from "@/api/useMeta";
import { AppProviders } from "@/app/App";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { testQueryClient } from "@/test/queryClient";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import {
  auctionParameters,
  growthParameters,
  nShapeParameters,
  parameterSet,
} from "./MinuteParameterControls.fixture";
import { MinuteStudyWorkspace } from "./MinuteStudyWorkspace";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));
const base = "*/api/v1/backtests/minute-runtime/studies";
const serving = metaEnvelope({ viewer: "researcher" }).serving;
const source = {
  source_key: "synthetic.ui.study.facts",
  source_version: 7,
  full_input_hash: "7".repeat(64),
  display_name: "分钟研究验证资料",
  start_date: "2026-01-05",
  end_date: "2026-08-03",
  frequency: "1min",
  source_nature: "synthetic_validation",
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
    visibility_limitations: "合成界面资料，不是行情或任务证明。",
  },
  unavailable_reasons: [],
  capabilities: [
    {
      family: "n_shape",
      display_name: "N字形",
      default_parameters: parameterSet(nShapeParameters),
      supported_parameter_names: [
        "max_hold_days",
        "paper.stop_loss_pct",
        "volume_profile.enabled",
        "volume_profile.lookback_days",
      ],
      unavailable_reasons: [],
    },
    {
      family: "growth_board_surge",
      display_name: "成长板放量",
      default_parameters: parameterSet(growthParameters),
      supported_parameter_names: ["max_hold_days", "paper.stop_loss_pct", "require_fresh_surge"],
      unavailable_reasons: [],
    },
  ],
} satisfies Schemas["MinuteParameterFactSourceOption"];
const capability = {
  source,
  modes: ["grid", "random", "ablation", "walk_forward"],
  score_profiles: Array.from({ length: 11 }, (_, index) => ({
    name: `synthetic-score-${index + 1}`,
    label: `原评分 ${index + 1}`,
    available: true,
    missing_features: [],
  })),
  searchable_parameter_names: [
    "max_hold_days",
    "paper.stop_loss_pct",
    "volume_profile.enabled",
    "volume_profile.lookback_days",
    "require_fresh_surge",
  ],
  heatmap_parameter_names: ["max_hold_days", "paper.stop_loss_pct", "volume_profile.enabled"],
  unavailable_reasons: [],
} satisfies Schemas["MinuteStudySourceCapability"];
const originalRequest = {
  command_id: "00000000-0000-4000-8000-000000000137",
  requested_at: "2026-10-07T00:00:00Z",
  source_key: source.source_key,
  source_version: source.source_version,
  full_input_hash: source.full_input_hash,
  parameters: parameterSet({ ...nShapeParameters, max_hold_days: 13 }),
  protocol: {
    train_range: { start_date: "2026-01-05", end_date: "2026-02-27" },
    validation_range: { start_date: "2026-03-02", end_date: "2026-04-30" },
    frozen_outer_test_range: { start_date: "2026-05-04", end_date: "2026-08-03" },
  },
  settings: [{ score_profile: "synthetic-score-7", top_n: 7, min_trades: 13 }],
  random_seed: 137,
  deadline: "2026-10-08T00:00:00Z",
  mode: "grid",
  search: {
    base: parameterSet({ ...nShapeParameters, max_hold_days: 13 }),
    axes: [{ path: "max_hold_days", values: [3, 7, 13] }],
    mode: "grid",
    seed: 137,
    requested_trials: null,
  },
  walk_forward: null,
} satisfies Schemas["MinuteStudyCreateRequest"];

function windowData(window: Schemas["DateRange"], trades: number, mean: number | null) {
  return {
    window,
    status: "complete",
    summary: {
      trades,
      mean_ret_pct: mean,
      win_rate_pct: 0,
      worst_ret_pct: -3.25,
      gap_stop_rate_pct: null,
    },
    cross_window_trades: 0,
    unavailable_reasons: [],
    daily: [
      {
        trade_date: window.start_date,
        nav: "100000.00",
        normalized_nav: 1,
        drawdown: 0,
        daily_return: null,
        status: "complete",
      },
      {
        trade_date: window.end_date,
        nav: "99256.51",
        normalized_nav: 0.9925651,
        drawdown: -0.0074349,
        daily_return: "-0.0074349",
        status: "complete",
      },
    ],
  } satisfies Schemas["MinuteStudyWindowData"];
}
const trial = {
  index: 0,
  fold: 1,
  label: "第一窗口",
  job_id: "00000000-0000-4000-8000-000000000138",
  state: "sealed",
  parameters: originalRequest.parameters,
  settings: { score_profile: "synthetic-score-7", top_n: 7, min_trades: 13 },
  protocol: originalRequest.protocol,
  request_hash: "1".repeat(64),
  study_id: "study.synthetic.original.137",
  result_hash: "2".repeat(64),
  full_input_hash: "3".repeat(64),
  core_input_hash: "4".repeat(64),
  completed_at: "2026-10-07T00:01:00Z",
  training: windowData(originalRequest.protocol.train_range, 13, 1.25),
  validation: windowData(originalRequest.protocol.validation_range, 7, -2.5),
  out_of_sample: windowData(originalRequest.protocol.frozen_outer_test_range, 1, -0.74349),
  training_rank: {
    study_id: "study.synthetic.original.137",
    result_hash: "2".repeat(64),
    available_at: "2026-03-01T00:00:00Z",
    selection_cutoff: "2026-03-02T00:00:00Z",
    training_score: -0.125,
    rank: 2,
  },
  unavailable_reasons: [],
} satisfies Schemas["MinuteStudyTrialData"];
const completed = {
  command_id: originalRequest.command_id,
  request: {
    ...originalRequest,
    mode: "walk_forward",
    search: null,
    walk_forward: { fold_count: 4, min_training_dates: 31, validation_date_count: 7 },
  },
  status: "complete",
  plan_id: "5".repeat(64),
  trial_count: 1,
  trials: [trial],
  missing_trial_indices: [],
  unavailable_reasons: [],
} satisfies Schemas["MinuteStudyResultData"];

function installCapabilities(overrides: Partial<Schemas["MinuteStudyCapabilitiesData"]> = {}) {
  server.use(
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({
        data: {
          available: true,
          can_run: true,
          sources: [capability],
          source_unavailable_count: 0,
          ...overrides,
        },
        serving,
      }),
    ),
  );
}
function installResult(value: Schemas["MinuteStudyResultData"]) {
  server.use(
    http.get(base, () =>
      HttpResponse.json({
        data: {
          available: true,
          studies: [
            {
              command_id: value.command_id,
              mode: value.request.mode,
              family: value.request.parameters.parameters.family,
              display_name: "原研究",
              source_key: value.request.source_key,
              source_version: value.request.source_version,
              full_input_hash: value.request.full_input_hash,
              status: "submitted",
              requested_at: value.request.requested_at,
              completed_at: null,
              plan_id: value.plan_id,
              trial_count: value.trial_count,
              submitted_count: value.trials.length,
            },
          ],
          next_cursor: null,
        },
        serving,
      }),
    ),
    http.get(`${base}/${value.command_id}`, () => HttpResponse.json({ data: value, serving })),
  );
}
function observeMinuteRequests() {
  const paths: string[] = [];
  function observe({ request }: { request: Request }) {
    if (request.method === "GET") paths.push(new URL(request.url).pathname);
  }
  server.events.on("request:start", observe);
  return {
    paths,
    stop: () => server.events.removeListener("request:start", observe),
  };
}
function installClosedNativeEntries() {
  server.use(
    http.get("*/api/v1/backtests", () =>
      HttpResponse.json({
        data: { available: false, runs: [], total: 0, next_offset: null },
        serving,
      }),
    ),
    ...["sources", "capabilities", "runs"].map((entry) =>
      http.get(`*/api/v1/backtests/minute-runtime/${entry}`, () =>
        HttpResponse.json({ detail: "Minute playback has not been opened" }, { status: 503 }),
      ),
    ),
  );
}
function creationAndNativeReads(paths: string[]) {
  return paths.filter((path) =>
    ["sources", "capabilities", "runs", "studies/capabilities"].some((entry) =>
      path.endsWith(`/api/v1/backtests/minute-runtime/${entry}`),
    ),
  );
}
async function openStudyPage() {
  installClosedNativeEntries();
  const current = renderApp("/backtest?view=minute");
  await userEvent.click(await screen.findByRole("button", { name: "打开参数研究" }));
  return current;
}
function mount() {
  const queryClient = testQueryClient();
  function MetaWorkspace() {
    useMeta();
    return <MinuteStudyWorkspace />;
  }
  return {
    ...render(
      <AppProviders queryClient={queryClient}>
        <MetaWorkspace />
      </AppProviders>,
    ),
    queryClient,
  };
}
function fillDates() {
  for (const [label, value] of [
    ["研究训练开始", "2026-01-05"],
    ["研究训练结束", "2026-02-27"],
    ["研究验证开始", "2026-03-02"],
    ["研究验证结束", "2026-04-30"],
    ["研究最终测试开始", "2026-05-04"],
    ["研究最终测试结束", "2026-08-03"],
  ])
    fireEvent.change(screen.getByLabelText(label ?? ""), { target: { value } });
}
async function ready() {
  const entry = await screen.findByRole("button", { name: /新建研究|收起配置/ });
  if (entry.textContent === "新建研究") await userEvent.click(entry);
  await screen.findByRole("option", { name: /分钟研究验证资料/ });
}
beforeEach(() => {
  server.use(
    metaHandler(metaEnvelope({ viewer: "researcher" })),
    http.get(base, () =>
      HttpResponse.json({ data: { available: true, studies: [], next_cursor: null }, serving }),
    ),
    http.get(`${base}/:command`, ({ params }) =>
      HttpResponse.json({
        data: {
          command_id: params.command,
          request: { ...originalRequest, command_id: params.command },
          status: "pending",
          plan_id: null,
          trials: [],
          missing_trial_indices: [],
          unavailable_reasons: [],
        },
        serving,
      }),
    ),
    http.get(`${base}/:command/heatmap`, () =>
      HttpResponse.json({ detail: "no complete parameter comparison" }, { status: 409 }),
    ),
  );
  installCapabilities();
});

it("study request intent: browses and refreshes original records without native or creation capability GETs", async () => {
  installResult(completed);
  installClosedNativeEntries();
  const requests = observeMinuteRequests();
  try {
    renderApp("/backtest?view=minute");
    await userEvent.click(await screen.findByRole("button", { name: "打开参数研究" }));
    await screen.findByRole("button", { name: "查看原研究" });
    await userEvent.click(screen.getByRole("button", { name: "刷新研究" }));
    await waitFor(() =>
      expect(
        requests.paths.filter((path) => path.endsWith("/minute-runtime/studies")),
      ).toHaveLength(2),
    );
    await act(async () => undefined);
    expect(creationAndNativeReads(requests.paths)).toEqual([]);
    expect(screen.getByRole("button", { name: "查看原研究" })).toBeInTheDocument();
    expect(screen.queryByRole("form", { name: "研究配置" })).not.toBeInTheDocument();
  } finally {
    requests.stop();
  }
});

it("study request intent: requests creation capabilities only after opening new research and shows errors", async () => {
  installResult(completed);
  server.use(
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({ detail: "capability unavailable" }, { status: 503 }),
    ),
  );
  const requests = observeMinuteRequests();
  try {
    await openStudyPage();
    await screen.findByRole("button", { name: "查看原研究" });
    expect(creationAndNativeReads(requests.paths)).toEqual([]);
    await userEvent.click(screen.getByRole("button", { name: "新建研究" }));
    expect(await screen.findByText("研究暂不可用")).toBeInTheDocument();
    const explanation = await screen.findByRole("button", { name: "研究状态说明" });
    await userEvent.hover(explanation);
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "分钟回测暂时无法加载，请稍后重试。",
    );
    await userEvent.unhover(explanation);
    expect(screen.getByRole("button", { name: "开始研究" })).toBeDisabled();
    expect(creationAndNativeReads(requests.paths)).toEqual([
      expect.stringMatching(/\/minute-runtime\/studies\/capabilities$/),
    ]);
    await userEvent.click(screen.getByRole("button", { name: "收起配置" }));
    expect(screen.queryByRole("form", { name: "研究配置" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "查看原研究" })).toBeInTheDocument();
  } finally {
    requests.stop();
  }
});

it("study request intent: loads original comparison capabilities only after explicit inspection of a walk-forward result", async () => {
  installResult(completed);
  server.use(
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({ detail: "comparison capability unavailable" }, { status: 503 }),
    ),
  );
  const requests = observeMinuteRequests();
  try {
    await openStudyPage();
    await userEvent.click(await screen.findByRole("button", { name: "查看原研究" }));
    await screen.findByRole("region", { name: "验证结果" });
    expect(creationAndNativeReads(requests.paths)).toEqual([]);
    await userEvent.click(screen.getByRole("button", { name: "查看参数对照" }));
    await waitFor(() =>
      expect(creationAndNativeReads(requests.paths)).toEqual([
        expect.stringMatching(/\/minute-runtime\/studies\/capabilities$/),
      ]),
    );
    expect(await screen.findByText("参数对照暂不可用")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "重试参数对照" })).toBeEnabled();
    expect(screen.getByRole("region", { name: "验证结果" })).toHaveTextContent("-2.50%");
  } finally {
    requests.stop();
  }
});

it.each(["grid", "random", "ablation", "walk_forward"] as const)(
  "submits source-bound complete %s inputs without locally expanding trials or selecting future winners",
  async (mode) => {
    const bodies: Schemas["MinuteStudyCreateRequest"][] = [];
    server.use(
      http.post(base, async ({ request }) => {
        const raw: unknown = await request.json();
        const body = (await import("@/api/minuteBacktests")).restoreMinuteStudyRequest(
          JSON.stringify(raw),
        );
        if (!body) throw Error("Invalid actual public request");
        bodies.push(body);
        return HttpResponse.json({
          command_id: body.command_id,
          status: "pending",
          jobs: [],
          unavailable_reasons: [],
          message: "研究正在准备。",
        } satisfies Schemas["MinuteStudyCommandReceipt"]);
      }),
    );
    mount();
    await ready();
    if (mode === "ablation")
      fireEvent.change(screen.getByLabelText("研究策略族"), {
        target: { value: "growth_board_surge" },
      });
    fireEvent.change(screen.getByLabelText("研究方式"), { target: { value: mode } });
    fillDates();
    fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), {
      target: { value: mode === "ablation" ? "7" : "13" },
    });
    fireEvent.change(screen.getByLabelText("评分方式"), { target: { value: "synthetic-score-7" } });
    fireEvent.change(screen.getByLabelText("每时点最多入选"), { target: { value: "7" } });
    fireEvent.change(screen.getByLabelText("最少闭环交易"), { target: { value: "13" } });
    fireEvent.change(screen.getByLabelText("研究随机种子"), { target: { value: "137" } });
    if (mode === "grid" || mode === "random") {
      fireEvent.change(screen.getByLabelText("参数取值 1"), { target: { value: "3,7,13" } });
      expect(screen.queryByRole("option", { name: "范围" })).not.toBeInTheDocument();
      if (mode === "random")
        fireEvent.change(screen.getByLabelText("随机方案数量"), { target: { value: "2" } });
    } else if (mode === "walk_forward") {
      fireEvent.change(screen.getByLabelText("滚动窗口数量"), { target: { value: "4" } });
      fireEvent.change(screen.getByLabelText("最少训练交易日"), { target: { value: "31" } });
      fireEvent.change(screen.getByLabelText("验证交易日"), { target: { value: "7" } });
    } else
      expect(within(screen.getByLabelText("五组固定对照")).getAllByRole("listitem")).toHaveLength(
        5,
      );
    await waitFor(() => expect(screen.getByRole("button", { name: "开始研究" })).toBeEnabled());
    await userEvent.click(screen.getByRole("button", { name: "开始研究" }));
    await waitFor(() => expect(bodies).toHaveLength(1));
    expect(bodies[0]).toMatchObject({
      source_key: source.source_key,
      source_version: 7,
      full_input_hash: source.full_input_hash,
      mode,
      parameters: parameterSet({
        ...(mode === "ablation" ? growthParameters : nShapeParameters),
        max_hold_days: mode === "ablation" ? 7 : 13,
      }),
      protocol: originalRequest.protocol,
      settings: [{ score_profile: "synthetic-score-7", top_n: 7, min_trades: 13 }],
      random_seed: 137,
    });
    if (mode === "grid" || mode === "random")
      expect(bodies[0]?.search).toMatchObject({
        axes: [{ path: "max_hold_days", values: [3, 7, 13] }],
        mode,
        seed: 137,
        requested_trials: mode === "random" ? 2 : null,
      });
    else expect(bodies[0]?.search).toBeNull();
    expect(screen.getByRole("button", { name: "开始研究" })).toBeDisabled();
  },
);

it("restores a lost response after remount with the original UUID, time, source and full bytes", async () => {
  const bodies: string[] = [];
  server.use(
    http.post(base, async ({ request }) => {
      bodies.push(await request.text());
      return HttpResponse.error();
    }),
  );
  const initial = mount();
  await ready();
  fillDates();
  fireEvent.change(screen.getByLabelText("参数取值 1"), { target: { value: "3,7,13" } });
  await userEvent.click(screen.getByRole("button", { name: "开始研究" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "重试原研究" })).toBeEnabled());
  initial.unmount();
  installCapabilities({
    sources: [{ ...capability, source: { ...source, full_input_hash: "8".repeat(64) } }],
  });
  mount();
  await waitFor(() => expect(screen.getByRole("button", { name: "重试原研究" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "重试原研究" }));
  await waitFor(() => expect(bodies).toHaveLength(2));
  expect(bodies[1]).toBe(bodies[0]);
  expect(screen.getByLabelText("研究训练开始")).toHaveValue("2026-01-05");
});

it.each(["new", "restored-v1"] as const)(
  "auction-wire study %s saves the exact POST body and search base while retaining legacy recovery",
  async (mode) => {
    sessionStorage.clear();
    const sourceDefault = parameterSet(auctionParameters);
    const defaultBefore = JSON.stringify(sourceDefault);
    const original = {
      ...originalRequest,
      parameters: parameterSet({ ...auctionParameters, max_hold_days: 7 }),
      search: {
        ...originalRequest.search,
        base: parameterSet({ ...auctionParameters, max_hold_days: 7 }),
      },
    } satisfies Schemas["MinuteStudyCreateRequest"];
    const auctionCapability = {
      ...capability,
      source: {
        ...source,
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
      searchable_parameter_names: ["max_hold_days", "paper.stop_loss_pct"],
      heatmap_parameter_names: ["max_hold_days", "paper.stop_loss_pct"],
    } satisfies Schemas["MinuteStudySourceCapability"];
    const sent: string[] = [];
    const saved: (string | null)[] = [];
    let current: Schemas["MinuteStudyCreateRequest"] = original;
    server.use(
      http.post(base, async ({ request }) => {
        const raw = await request.text();
        const body = (await import("@/api/minuteBacktests")).restoreMinuteStudyRequest(raw);
        if (!body) throw Error("Invalid actual public request");
        current = body;
        sent.push(raw);
        saved.push(sessionStorage.getItem("rquant.minute.study:researcher"));
        return HttpResponse.error();
      }),
      http.get(`${base}/:command`, ({ params }) =>
        HttpResponse.json({
          data: {
            command_id: params.command,
            request: { ...current, command_id: params.command },
            status: "pending",
            plan_id: null,
            trials: [],
            missing_trial_indices: [],
            unavailable_reasons: [],
          },
          serving,
        }),
      ),
    );
    installCapabilities({ sources: [auctionCapability] });
    if (mode === "restored-v1")
      sessionStorage.setItem("rquant.minute.study:researcher", JSON.stringify(original));
    mount();
    if (mode === "new") {
      await ready();
      fireEvent.change(screen.getByLabelText("研究方式"), { target: { value: "grid" } });
      fillDates();
      fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "7" } });
      fireEvent.change(screen.getByLabelText("研究随机种子"), { target: { value: "137" } });
      fireEvent.change(screen.getByLabelText("参数取值 1"), { target: { value: "3,7,13" } });
      await waitFor(() => expect(screen.getByRole("button", { name: "开始研究" })).toBeEnabled());
      await userEvent.click(screen.getByRole("button", { name: "开始研究" }));
    } else {
      await waitFor(() => expect(screen.getByRole("button", { name: "重试原研究" })).toBeEnabled());
      await userEvent.click(screen.getByRole("button", { name: "重试原研究" }));
    }
    await waitFor(() => expect(sent).toHaveLength(1));
    await waitFor(() => expect(screen.getByRole("button", { name: "重试原研究" })).toBeEnabled());
    expect(saved).toEqual(sent);
    expect(current.parameters).toMatchObject({
      schema_version: mode === "new" ? 2 : 1,
      parameters: {
        ...auctionParameters,
        max_hold_days: 7,
        ...(mode === "new" ? { next_day_price_policy: "keep_candidate_mark_unavailable" } : {}),
      },
    });
    expect(current.search?.base).toEqual(current.parameters);
    expect(current.protocol).toEqual(original.protocol);
    expect(current.random_seed).toBe(137);
    if (mode === "restored-v1") expect(sent[0]).toBe(JSON.stringify(original));
    await userEvent.click(screen.getByRole("button", { name: "重试原研究" }));
    await waitFor(() => expect(sent).toHaveLength(2));
    expect(sent[1]).toBe(sent[0]);
    expect(saved).toEqual(sent);
    expect(JSON.stringify(sourceDefault)).toBe(defaultBefore);
  },
);

it("shows original three-window values and NAV while keeping unavailable validation and final scores blank", async () => {
  const reads: string[] = [];
  server.use(
    http.get(base, () =>
      HttpResponse.json({
        data: {
          available: true,
          studies: [
            {
              command_id: completed.command_id,
              mode: "walk_forward",
              family: "n_shape",
              display_name: "原分窗研究",
              source_key: source.source_key,
              source_version: 7,
              full_input_hash: source.full_input_hash,
              status: "submitted",
              requested_at: originalRequest.requested_at,
              completed_at: null,
              plan_id: completed.plan_id,
              trial_count: 1,
              submitted_count: 1,
            },
          ],
          next_cursor: null,
        },
        serving,
      }),
    ),
    http.get(`${base}/:command`, ({ params }) => {
      reads.push(String(params.command));
      return HttpResponse.json({ data: completed, serving });
    }),
  );
  const requests = observeMinuteRequests();
  try {
    await openStudyPage();
    await userEvent.click(await screen.findByRole("button", { name: "查看原分窗研究" }));
    await waitFor(() => expect(reads).toEqual([completed.command_id]));
    const validation = await screen.findByRole("region", { name: "验证结果" });
    expect(within(validation).getByLabelText("平均交易收益")).toHaveTextContent("-2.50%");
    const outer = screen.getByRole("region", { name: "最终测试结果" });
    expect(within(outer).getByLabelText("平均交易收益")).toHaveTextContent("-0.74%");
    expect(within(outer).getByLabelText("逐日净值 2026-08-03")).toHaveTextContent("99,256.51");
    const table = screen.getByRole("table", { name: "滚动分窗结果" });
    expect(within(table).getByRole("columnheader", { name: "测试均收益" })).toBeInTheDocument();
    expect(within(table).getByText("−0.74%")).toBeInTheDocument();
    expect(within(table).getByText("-0.1250")).toBeInTheDocument();
    expect(within(table).getAllByText("—").length).toBeGreaterThanOrEqual(2);
    expect(creationAndNativeReads(requests.paths)).toEqual([]);
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
    expect(screen.queryByText(trial.study_id)).not.toBeInTheDocument();
  } finally {
    requests.stop();
  }
});

it("shows five sealed ablations when the original result cannot rank zero training trades", async () => {
  const emptySummary = {
    trades: 0,
    mean_ret_pct: null,
    win_rate_pct: null,
    worst_ret_pct: null,
    gap_stop_rate_pct: null,
  };
  const value: Schemas["MinuteStudyResultData"] = {
    ...completed,
    request: {
      ...originalRequest,
      parameters: parameterSet(growthParameters),
      mode: "ablation",
      search: null,
      walk_forward: null,
    },
    status: "unavailable",
    trial_count: 5,
    message: null,
    unavailable_reasons: ["insufficient_training_trades"],
    // Shape and zero/null facts from the original R08 HTTP result, not a runnable source.
    trials: ["完整策略", "去掉VWAP", "去掉同刻放量", "去掉5分加速", "只看累计放量"].map(
      (label, index) => ({
        ...trial,
        index,
        label,
        parameters: parameterSet(growthParameters),
        training: { ...trial.training, summary: emptySummary, daily: [] },
        validation: { ...trial.validation, summary: emptySummary, daily: [] },
        out_of_sample: { ...trial.out_of_sample, summary: emptySummary, daily: [] },
        training_rank: null,
      }),
    ),
  };
  installResult(value);
  const requests = observeMinuteRequests();
  try {
    await openStudyPage();
    await userEvent.click(await screen.findByRole("button", { name: "查看原研究" }));
    const table = await screen.findByRole("table", { name: "五组消融对照" });
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(5);
    for (const row of rows) {
      expect(within(row).getAllByText("—")).toHaveLength(2);
      expect(within(row).getByText("0")).toBeInTheDocument();
    }
    expect(screen.getByText("训练交易不足，暂不能排名")).toBeInTheDocument();
    expect(screen.getByText("资料不足")).toBeInTheDocument();
    expect(screen.queryByText("研究资料暂不可用")).not.toBeInTheDocument();
    expect(
      requests.paths.filter((path) => path.endsWith(`/studies/${value.command_id}`)),
    ).toHaveLength(1);
    expect(creationAndNativeReads(requests.paths)).toEqual([]);
    expect(document.body.textContent).not.toContain("insufficient_training_trades");
    expect(findJargon(document.body.textContent ?? "")).toEqual([]);
  } finally {
    requests.stop();
  }
});

it("keeps the original unavailable state when there are no sealed study results", async () => {
  installResult({
    ...completed,
    status: "unavailable",
    trials: [],
    unavailable_reasons: ["insufficient_training_trades"],
  });
  mount();
  await ready();
  await userEvent.click(await screen.findByRole("button", { name: "查看原研究" }));
  expect(await screen.findByText("研究资料暂不可用")).toBeInTheDocument();
  expect(screen.queryByRole("table", { name: "五组消融对照" })).not.toBeInTheDocument();
  expect(screen.queryByRole("table", { name: "滚动分窗结果" })).not.toBeInTheDocument();
  expect(screen.queryByText("训练交易不足，暂不能排名")).not.toBeInTheDocument();
});

it("does not submit when permission is missing, meta fails, or original request persistence fails", async () => {
  let submitted = 0;
  server.use(
    http.post(base, () => {
      submitted += 1;
      return HttpResponse.error();
    }),
  );
  installCapabilities({ can_run: false });
  const initial = mount();
  await ready();
  expect(screen.getByRole("button", { name: "开始研究" })).toBeDisabled();
  initial.unmount();
  installCapabilities();
  const current = mount();
  await ready();
  fillDates();
  fireEvent.change(screen.getByLabelText("参数取值 1"), { target: { value: "3,7" } });
  const persist = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw Error("Storage unavailable");
  });
  await userEvent.click(screen.getByRole("button", { name: "开始研究" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("原请求未保存");
  expect(submitted).toBe(0);
  persist.mockRestore();
  server.use(
    http.get("*/api/v1/meta", () =>
      HttpResponse.json({ detail: "identity unavailable" }, { status: 503 }),
    ),
  );
  await act(async () => {
    await current.queryClient.invalidateQueries({ queryKey: META_QUERY_KEY });
  });
  expect(await screen.findByText("账号暂不可用")).toBeInTheDocument();
  expect(screen.queryByLabelText("研究训练开始")).not.toBeInTheDocument();
  expect(submitted).toBe(0);
});

it("keeps all original score choices visible and disables a choice with missing source facts", async () => {
  installCapabilities({
    sources: [
      {
        ...capability,
        score_profiles: capability.score_profiles.map((item, index) =>
          index === 6
            ? { ...item, available: false, missing_features: ["original_missing_training_field"] }
            : item,
        ),
      },
    ],
  });
  mount();
  await ready();
  const options = within(screen.getByLabelText("评分方式")).getAllByRole("option");
  expect(options).toHaveLength(11);
  expect(
    within(screen.getByLabelText("评分方式")).getByRole("option", { name: "原评分 7" }),
  ).toBeDisabled();
  expect(screen.queryByText("original_missing_training_field")).not.toBeInTheDocument();
});

it("passes nested percentage, boolean and list axes in their original units without expanding combinations", async () => {
  const bodies: Schemas["MinuteStudyCreateRequest"][] = [];
  server.use(
    http.post(base, async ({ request }) => {
      const body = (await import("@/api/minuteBacktests")).restoreMinuteStudyRequest(
        await request.text(),
      );
      if (!body) throw Error("Invalid actual public request");
      bodies.push(body);
      return HttpResponse.json({
        command_id: body.command_id,
        status: "pending",
        jobs: [],
        unavailable_reasons: [],
        message: "研究正在准备。",
      } satisfies Schemas["MinuteStudyCommandReceipt"]);
    }),
  );
  mount();
  await ready();
  fillDates();
  fireEvent.change(screen.getByLabelText("参数字段 1"), {
    target: { value: "paper.stop_loss_pct" },
  });
  fireEvent.change(screen.getByLabelText("参数取值 1"), { target: { value: "3,7" } });
  await userEvent.click(screen.getByRole("button", { name: "添加参数" }));
  fireEvent.change(screen.getByLabelText("参数字段 2"), {
    target: { value: "volume_profile.enabled" },
  });
  await userEvent.click(screen.getByRole("checkbox", { name: "启用价量分布：关闭" }));
  await userEvent.click(screen.getByRole("checkbox", { name: "启用价量分布：开启" }));
  await userEvent.click(screen.getByRole("button", { name: "添加参数" }));
  fireEvent.change(screen.getByLabelText("参数字段 3"), {
    target: { value: "volume_profile.lookback_days" },
  });
  fireEvent.change(screen.getByLabelText("参数取值 3"), { target: { value: "[1,3];[5,10]" } });
  await userEvent.click(screen.getByRole("button", { name: "开始研究" }));
  await waitFor(() => expect(bodies).toHaveLength(1));
  expect(bodies[0]?.search?.axes).toEqual([
    { path: "paper.stop_loss_pct", values: [0.03, 0.07] },
    { path: "volume_profile.enabled", values: [false, true] },
    {
      path: "volume_profile.lookback_days",
      values: [
        [1, 3],
        [5, 10],
      ],
    },
  ]);
  expect(bodies[0]?.parameters).toEqual(parameterSet(nShapeParameters));
});

it("clears current private results, axis inspection and draft selection on viewer and data change", async () => {
  installResult(completed);
  const current = mount();
  await ready();
  await userEvent.click(await screen.findByRole("button", { name: "查看原研究" }));
  await screen.findByRole("region", { name: "验证结果" });
  await userEvent.click(screen.getByRole("button", { name: "查看参数与依据" }));
  expect(await screen.findByRole("dialog", { name: "研究依据" })).toBeInTheDocument();
  const next = metaEnvelope({ viewer: "another-researcher", generationId: "b".repeat(64) });
  server.use(
    metaHandler(next),
    http.get(base, () =>
      HttpResponse.json({
        data: { available: true, studies: [], next_cursor: null },
        serving: next.serving,
      }),
    ),
  );
  await act(async () => current.queryClient.setQueryData(META_QUERY_KEY, next));
  await waitFor(() =>
    expect(screen.queryByRole("dialog", { name: "研究依据" })).not.toBeInTheDocument(),
  );
  expect(screen.queryByRole("region", { name: "验证结果" })).not.toBeInTheDocument();
  expect(screen.getByLabelText("研究训练开始")).toHaveValue("");
  expect(screen.queryByRole("button", { name: "重试原研究" })).not.toBeInTheDocument();
});

it("keeps a saved original body when a terminal detail belongs to different content", async () => {
  sessionStorage.setItem("rquant.minute.study:researcher", JSON.stringify(originalRequest));
  const different = { ...completed, request: { ...completed.request, random_seed: 999 } };
  server.use(
    http.get(`${base}/${different.command_id}`, () =>
      HttpResponse.json({ data: different, serving }),
    ),
  );
  mount();
  expect(await screen.findByText("原研究内容不一致")).toBeInTheDocument();
  expect(sessionStorage.getItem("rquant.minute.study:researcher")).toBe(
    JSON.stringify(originalRequest),
  );
  expect(screen.queryByRole("region", { name: "验证结果" })).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "开始研究" })).toBeDisabled();
});

it("requests the original two-axis result and current trial, then waits for a new owner result after changing axes", async () => {
  installResult({ ...completed, request: originalRequest });
  const head = {
    definition_id: "synthetic.study.recipe",
    definition_version: 1,
    evaluator_semantic_version: "synthetic-ui/1",
    executable_fingerprint: "a".repeat(64),
    parameter_fingerprint: "b".repeat(64),
    producer_commit: "a".repeat(40),
    registration_fingerprint: "c".repeat(64),
    spec_fingerprint: "d".repeat(64),
  } satisfies Schemas["MinuteStudyHead"];
  const protocol = {
    head,
    min_trades: 13,
    parameters: originalRequest.parameters,
    random_seed: 137,
    requested_at: originalRequest.requested_at,
    score_profile: "synthetic-score-7",
    source: {
      dataset_snapshot_id: "synthetic.baseline.facts",
      end_date: source.end_date,
      frequency: source.frequency,
      full_input_hash: source.full_input_hash,
      owner_id: "synthetic-ui-owner",
      published_at: source.provenance.published_at,
      source_key: source.source_key,
      source_version: source.source_version,
      start_date: source.start_date,
    },
    split: {
      train_start: "2026-01-05",
      train_end: "2026-02-27",
      test_start: "2026-03-02",
      test_end: "2026-08-03",
    },
    top_n: 7,
  } satisfies Schemas["MinuteStudyProtocol"];
  const observation = {
    available_at: "2026-03-01T00:00:00Z",
    head,
    parameter_fingerprint: head.parameter_fingerprint,
    result_hash: "2".repeat(64),
    source: protocol.source,
    study_id: "study.synthetic.original.137",
    summary: {
      trades: 13,
      mean_ret_pct: 1.25,
      win_rate_pct: 0,
      worst_ret_pct: -3.25,
      gap_stop_rate_pct: null,
    },
    train_start: "2026-01-05",
    train_end: "2026-02-27",
  } satisfies Schemas["MinuteStudyTrainingObservation"];
  let release: (() => void) | null = null;
  const wait = new Promise<void>((resolve) => {
    release = resolve;
  });
  const queries: URLSearchParams[] = [];
  server.use(
    http.get(`${base}/${completed.command_id}/heatmap`, async ({ request }) => {
      const query = new URL(request.url).searchParams;
      queries.push(query);
      const changed = query.get("x_parameter") === "volume_profile.enabled";
      if (changed) await wait;
      const xValue = changed ? false : 13;
      return HttpResponse.json({
        data: {
          command_id: completed.command_id,
          plan_id: completed.plan_id,
          read_at: "2026-10-07T00:02:00Z",
          heatmap: {
            current_study_id: "study.synthetic.original.137",
            selection_cutoff: "2026-03-02T00:00:00Z",
            trial_set_hash: changed ? "9".repeat(64) : "8".repeat(64),
            x_axis: { parameter_name: query.get("x_parameter") ?? "", values: [xValue] },
            y_axis: { parameter_name: query.get("y_parameter") ?? "", values: [0.03] },
            cells: [
              {
                x_index: 0,
                y_index: 0,
                x_value: xValue,
                y_value: 0.03,
                status: "available",
                is_current: true,
                study_id: "study.synthetic.original.137",
                protocol,
                observation,
                training_rank: trial.training_rank,
                training_score: changed ? -0.5 : -0.125,
                neighborhood: {
                  status: "unavailable",
                  reason: "no_neighbors",
                  coordinates: [],
                  unavailable_coordinates: [],
                  minimum_score: null,
                },
              },
            ],
          },
        } satisfies Schemas["MinuteStudyHeatmapData"],
        serving,
      });
    }),
  );
  const user = userEvent.setup();
  mount();
  await user.click(await screen.findByRole("button", { name: "查看原研究" }));
  const current = await screen.findByLabelText("当前参数评分");
  expect(current).toHaveTextContent("-0.1250");
  expect(screen.getByLabelText("当前参数邻域最低分")).toHaveTextContent("—");
  expect(queries[0]?.get("current_trial_index")).toBe("0");
  expect(queries[0]?.get("x_parameter")).toBe("max_hold_days");
  expect(queries[0]?.get("y_parameter")).toBe("paper.stop_loss_pct");
  fireEvent.change(screen.getByLabelText("横轴参数"), {
    target: { value: "volume_profile.enabled" },
  });
  await waitFor(() => expect(queries).toHaveLength(2));
  expect(screen.queryByLabelText("当前参数评分")).not.toBeInTheDocument();
  expect(screen.getByText("正在请求原研究图…")).toBeInTheDocument();
  if (release === null) throw Error("Missing explicit owner response release");
  await act(async () => release?.());
  expect(await screen.findByLabelText("当前参数评分")).toHaveTextContent("-0.5000");
  expect(findJargon(document.body.textContent ?? "")).toEqual([]);
});

it("opens the original page research entry with reads only and keeps archived playback separate", async () => {
  let writes = 0;
  server.use(
    http.get("*/api/v1/backtests", () =>
      HttpResponse.json({
        data: { available: false, runs: [], total: 0, next_offset: null },
        serving,
      }),
    ),
    http.get("*/api/v1/backtests/minute-runtime/capabilities", () =>
      HttpResponse.json({
        data: {
          available: true,
          can_run: false,
          can_export: false,
          source_count: 0,
          source_unavailable_count: 0,
          valuation_basis: "pit_asof_15:00",
        },
        serving,
      }),
    ),
    http.get("*/api/v1/backtests/minute-runtime/sources", () =>
      HttpResponse.json({ data: { available: true, sources: [], unavailable_count: 0 }, serving }),
    ),
    http.get("*/api/v1/backtests/minute-runtime/runs", () =>
      HttpResponse.json({ data: { available: true, jobs: [], next_cursor: null }, serving }),
    ),
    http.post(base, () => {
      writes += 1;
      return HttpResponse.error();
    }),
  );
  const user = userEvent.setup();
  renderApp("/backtest?view=minute");
  const entry = await screen.findByRole("button", { name: "打开参数研究" });
  expect(screen.queryByLabelText("研究配置")).not.toBeInTheDocument();
  await user.click(entry);
  await ready();
  expect(screen.getByRole("button", { name: "收起研究" })).toHaveAttribute("aria-expanded", "true");
  expect(screen.getByRole("region", { name: "历史归档" })).toBeInTheDocument();
  expect(screen.queryByText("等待研究来源")).not.toBeInTheDocument();
  expect(writes).toBe(0);
});
