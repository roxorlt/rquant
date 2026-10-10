import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { renderApp } from "@/test/render";
import { metaHandler, server } from "@/test/server";
import {
  auctionParameters,
  growthParameters,
  nShapeParameters,
  parameterSet,
} from "./MinuteParameterControls.fixture";

vi.mock("@/charts/EChart", () => ({ EChart: () => null }));
const base = "*/api/v1/backtests/minute-runtime";
const serving = metaEnvelope({ viewer: "researcher" }).serving;
const paramsSource = {
  source_key: "synthetic.ui.full-facts",
  source_version: 7,
  full_input_hash: "7".repeat(64),
  display_name: "完整分钟验证资料",
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
    visibility_policy_id: "synthetic-ui-validation",
    visibility_policy_version: 1,
    visibility_limitations: "仅验证界面交互，不代表真实行情。",
  },
  unavailable_reasons: [],
  capabilities: [
    {
      family: "n_shape",
      display_name: "N字形",
      default_parameters: parameterSet(nShapeParameters),
      supported_parameter_names: [
        ...Object.keys(nShapeParameters),
        ...Object.keys(nShapeParameters.paper).map((key) => `paper.${key}`),
        ...Object.keys(nShapeParameters.volume_profile).map((key) => `volume_profile.${key}`),
      ],
      unavailable_reasons: [],
    },
    {
      family: "auction_gap",
      display_name: "竞价缺口",
      default_parameters: parameterSet(auctionParameters),
      supported_parameter_names: [
        ...Object.keys(auctionParameters),
        ...Object.keys(auctionParameters.paper).map((key) => `paper.${key}`),
      ],
      unavailable_reasons: [],
    },
    {
      family: "growth_board_surge",
      display_name: "成长板放量",
      default_parameters: parameterSet(growthParameters),
      supported_parameter_names: [
        ...Object.keys(growthParameters),
        ...Object.keys(growthParameters.paper).map((key) => `paper.${key}`),
      ],
      unavailable_reasons: [],
    },
  ],
} satisfies Schemas["MinuteParameterFactSourceOption"];

function installSource(source: Schemas["MinuteParameterFactSourceOption"] = paramsSource) {
  server.use(
    http.get(`${base}/parameter-sources`, () =>
      HttpResponse.json({
        data: { available: true, sources: [source], unavailable_count: 0 },
        serving,
      }),
    ),
  );
}
function fillDates() {
  for (const [label, value] of [
    ["训练开始", "2026-01-05"],
    ["训练结束", "2026-02-27"],
    ["验证开始", "2026-03-02"],
    ["验证结束", "2026-04-30"],
    ["样本外开始", "2026-05-04"],
    ["样本外结束", "2026-08-03"],
  ]) {
    fireEvent.change(screen.getByLabelText(label ?? ""), { target: { value } });
  }
}
async function openMinutePlayback() {
  await screen.findByRole("button", { name: /^(打开|收起)分钟回放$/ });
  await waitFor(() =>
    expect(screen.getByRole("button", { name: /^(打开|收起)分钟回放$/ })).toBeEnabled(),
  );
  const entry = screen.getByRole("button", { name: /^(打开|收起)分钟回放$/ });
  if (entry.textContent === "打开分钟回放") await userEvent.click(entry);
}
async function openParameters() {
  await openMinutePlayback();
  await waitFor(() => expect(screen.getByLabelText("回测配置")).toBeEnabled());
  fireEvent.change(screen.getByLabelText("回测配置"), { target: { value: "parameters" } });
  await screen.findByRole("option", { name: /完整分钟验证资料/ });
}

beforeEach(() => {
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
      HttpResponse.json({ data: { available: true, sources: [], unavailable_count: 0 }, serving }),
    ),
    http.get(`${base}/runs`, () =>
      HttpResponse.json({ data: { available: true, jobs: [], next_cursor: null }, serving }),
    ),
  );
  installSource();
});

it("submits the complete source-bound recipe and restores identical bytes after a lost response and source change", async () => {
  const bodies: string[] = [];
  server.use(
    http.post(`${base}/runs`, async ({ request }) => {
      bodies.push(await request.text());
      return HttpResponse.error();
    }),
  );
  const user = userEvent.setup();
  const initial = renderApp("/backtest?view=minute");
  await openParameters();
  fillDates();
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "13" } });
  fireEvent.change(screen.getByLabelText("随机种子"), { target: { value: "137" } });
  expect(
    screen.queryByText(paramsSource.full_input_hash, { exact: false }),
  ).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "运行分钟回测" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "重试原请求" })).toBeEnabled());
  expect(bodies).toHaveLength(1);
  const expected: Schemas["MinuteParameterRunConfig"] = {
    kind: "minute_parameter_replay",
    source_key: paramsSource.source_key,
    source_version: paramsSource.source_version,
    full_input_hash: paramsSource.full_input_hash,
    parameters: parameterSet({ ...nShapeParameters, max_hold_days: 13 }),
    random_seed: 137,
    deadline: JSON.parse(bodies[0] ?? "null").config.deadline,
    protocol: {
      train_range: { start_date: "2026-01-05", end_date: "2026-02-27" },
      validation_range: { start_date: "2026-03-02", end_date: "2026-04-30" },
      frozen_outer_test_range: { start_date: "2026-05-04", end_date: "2026-08-03" },
    },
  };
  expect(JSON.parse(bodies[0] ?? "null")).toMatchObject({ config: expected });
  expect(screen.getByLabelText("最长持仓（交易日）")).toBeDisabled();
  initial.unmount();
  installSource({ ...paramsSource, full_input_hash: "8".repeat(64) });
  renderApp("/backtest?view=minute");
  await waitFor(() => expect(screen.getByRole("button", { name: "重试原请求" })).toBeEnabled());
  expect(screen.getByLabelText("最长持仓（交易日）")).toHaveValue(13);
  await user.click(screen.getByRole("button", { name: "重试原请求" }));
  await waitFor(() => expect(bodies).toHaveLength(2));
  expect(bodies[1]).toBe(bodies[0]);
});

it("switches complete families without carrying N terms into growth or auction", async () => {
  renderApp("/backtest?view=minute");
  await openParameters();
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "13" } });
  fireEvent.change(screen.getByLabelText("策略族"), { target: { value: "growth_board_surge" } });
  expect(screen.getByLabelText("最长持仓（交易日）")).toHaveValue(3);
  expect(screen.queryByLabelText("观察池")).not.toBeInTheDocument();
  await userEvent.setup().click(screen.getByText("放量与筛选"));
  expect(screen.getByLabelText("累计放量倍数")).toHaveValue(1.4);
  fireEvent.change(screen.getByLabelText("策略族"), { target: { value: "auction_gap" } });
  await userEvent.setup().click(screen.getByText("竞价与退出"));
  expect(screen.getByLabelText("竞价资料开始")).toHaveValue("2026-01-05");
  expect(screen.getByLabelText("最低因子评分")).toHaveValue(null);
});

it("disables unavailable capability and overlapping or out-of-source protocol instead of inventing a run", async () => {
  const user = userEvent.setup();
  renderApp("/backtest?view=minute");
  await openParameters();
  fillDates();
  fireEvent.change(screen.getByLabelText("样本外开始"), { target: { value: "2026-04-30" } });
  expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("样本外开始"), { target: { value: "2026-05-04" } });
  fireEvent.change(screen.getByLabelText("样本外结束"), { target: { value: "2026-08-04" } });
  expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeDisabled();
  fireEvent.change(screen.getByLabelText("样本外结束"), { target: { value: "2026-08-03" } });
  await waitFor(() => expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeEnabled());
  installSource({
    ...paramsSource,
    capabilities: paramsSource.capabilities.map((cap) => ({
      ...cap,
      unavailable_reasons: ["缺少完整同刻资料"],
    })),
  });
  await user.click(screen.getByRole("button", { name: "刷新来源与任务" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeDisabled());
  expect(screen.getByText("当前来源尚不支持此配置。")).toBeVisible();
});

it("a new viewer/generation clears private drafts and cannot restore the previous viewer's request", async () => {
  const { queryClient } = renderApp("/backtest?view=minute");
  await openParameters();
  fireEvent.change(screen.getByLabelText("最长持仓（交易日）"), { target: { value: "13" } });
  server.use(
    metaHandler(metaEnvelope({ viewer: "other-researcher", generationId: "b".repeat(64) })),
  );
  await act(async () => {
    queryClient.setQueryData(
      META_QUERY_KEY,
      metaEnvelope({ viewer: "other-researcher", generationId: "b".repeat(64) }),
    );
  });
  await openMinutePlayback();
  await waitFor(() => expect(screen.getByLabelText("回测配置")).toHaveValue("fixed"));
  expect(screen.queryByLabelText("最长持仓（交易日）")).not.toBeInTheDocument();
  await openParameters();
  expect(screen.getByLabelText("最长持仓（交易日）")).toHaveValue(5);
  expect(screen.queryByRole("button", { name: "重试原请求" })).not.toBeInTheDocument();
});

it("parameter source availability does not grant a write permission", async () => {
  server.use(
    http.get(`${base}/capabilities`, () =>
      HttpResponse.json({
        data: {
          available: true,
          can_run: false,
          can_export: false,
          source_count: 1,
          source_unavailable_count: 0,
          valuation_basis: "pit_asof_15:00",
        },
        serving,
      }),
    ),
  );
  renderApp("/backtest?view=minute");
  await openParameters();
  fillDates();
  expect(screen.getByRole("button", { name: "运行分钟回测" })).toBeDisabled();
  expect(screen.getByText("当前无法提交回测。")).toBeVisible();
});

it("does not send a new parameter command when its original body cannot be persisted", async () => {
  let calls = 0;
  server.use(
    http.post(`${base}/runs`, () => {
      calls += 1;
      return HttpResponse.error();
    }),
  );
  vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
    throw new DOMException("private synthetic quota", "QuotaExceededError");
  });
  renderApp("/backtest?view=minute");
  await openParameters();
  fillDates();
  await userEvent.setup().click(screen.getByRole("button", { name: "运行分钟回测" }));
  await screen.findByRole("button", { name: "重试原请求" });
  await waitFor(() => expect(calls).toBe(0));
  expect(screen.getByText("原请求无法保存，请保留页面后重试。")).toBeVisible();
});

// Generated DTO presentation facts only; no installed source or sealed worker claim.
const resultParameters = parameterSet({
  ...nShapeParameters,
  max_hold_days: 13,
  paper: { ...nShapeParameters.paper, stop_loss_pct: 0.07 },
  volume_profile: { ...nShapeParameters.volume_profile, enabled: true, bin_ratio: 0 },
});
const jobFields = {
  version: 1,
  created_at: "2026-10-07T00:00:00Z",
  updated_at: "2026-10-07T00:00:00Z",
  spec_hash: "2".repeat(64),
  source_key: "synthetic.ui.parameter-result",
  source_version: 3,
  full_input_hash: "3".repeat(64),
  native_name: "N字形",
  native_version: 1,
  start_date: "2026-07-31",
  end_date: "2026-08-03",
};
const parameterJob = {
  ...jobFields,
  job_id: "813d6dbb-35af-46e2-9d4c-3e75c4b150c1",
  status: "queued",
  result_hash: null,
  kind: "minute_parameter_replay",
  family: "n_shape",
  native_id: `np.${"s".repeat(52)}`,
  parameters: resultParameters,
  parameter_hash: "4".repeat(64),
  evaluator_semantic_version: "2.0.0",
} satisfies Schemas["MinuteParameterJob"];
const fixedJob = {
  ...jobFields,
  job_id: "728d2868-3af9-486f-9926-ae43d5c29b94",
  native_id: "n_shape",
  status: "queued",
  result_hash: null,
} satisfies Schemas["MinuteJob"];
const resultFields = {
  source_key: jobFields.source_key,
  source_version: jobFields.source_version,
  full_input_hash: jobFields.full_input_hash,
  core_input_hash: "5".repeat(64),
  seed_hash: "6".repeat(64),
  native_name: jobFields.native_name,
  native_version: jobFields.native_version,
  native_registration_hash: "8".repeat(64),
  native_executable_fingerprint: "9".repeat(64),
  wrapper_registration_hash: "a".repeat(64),
  profile_hash: "b".repeat(64),
  dataset_snapshot_id: "c".repeat(64),
  start_date: jobFields.start_date,
  end_date: jobFields.end_date,
  work_units: 600,
  provenance: paramsSource.provenance,
};
const parameterResultSource = {
  ...resultFields,
  native_id: parameterJob.native_id,
  kind: parameterJob.kind,
  family: parameterJob.family,
  parameters: parameterJob.parameters,
  parameter_hash: parameterJob.parameter_hash,
  evaluator_semantic_version: parameterJob.evaluator_semantic_version,
  baseline_source_key: paramsSource.source_key,
  baseline_source_version: paramsSource.source_version,
  baseline_full_input_hash: paramsSource.full_input_hash,
  source_nature: paramsSource.source_nature,
} satisfies Schemas["MinuteParameterResultSource"];

function installResult(
  source: Schemas["MinuteParameterResultSource"] = parameterResultSource,
  current: Schemas["MinuteParameterJob"] = parameterJob,
) {
  const summary: Schemas["MinuteSummaryData"] = {
    job: current,
    source,
    result_hash: null,
    tables: [],
    can_report: false,
    performance: null,
    message: current.status === "failed" ? "完整资料尚未通过校验。" : "结果尚未保存完成。",
  };
  const fixedSummary: Schemas["MinuteSummaryData"] = {
    job: fixedJob,
    source: { ...resultFields, native_id: "n_shape" },
    result_hash: null,
    tables: [],
    can_report: false,
  };
  server.use(
    http.get(`${base}/runs`, () =>
      HttpResponse.json({ data: { available: true, jobs: [current, fixedJob] }, serving }),
    ),
    http.get(`${base}/runs/:jobId`, ({ params }) =>
      HttpResponse.json({
        data: params.jobId === current.job_id ? summary : fixedSummary,
        serving,
      }),
    ),
  );
  return summary;
}

it("API08 selects parameter and fixed jobs by their actual UUID and exposes the complete read-only recipe only in details", async () => {
  const summary = installResult();
  const user = userEvent.setup();
  renderApp("/backtest?view=minute");
  await openMinutePlayback();
  await screen.findByRole("region", { name: "N字形 · 参数回测 · 版本 1" });
  const tasks = screen.getByRole("table", { name: "分钟策略任务" });
  expect(within(tasks).getByText("N字形 · 参数回测 · 版本 1")).toBeVisible();
  expect(screen.queryByText(parameterJob.native_id, { exact: false })).not.toBeInTheDocument();
  expect(screen.queryByText(parameterJob.parameter_hash, { exact: false })).not.toBeInTheDocument();
  await user.click(screen.getByRole("button", { name: "参数与来源" }));
  const drawer = await screen.findByRole("dialog", { name: "参数与来源" });
  expect(within(drawer).getByLabelText("最长持仓（交易日）")).toHaveValue(13);
  expect(within(drawer).getByLabelText("最长持仓（交易日）")).toBeDisabled();
  expect(within(drawer).getByLabelText("止损（%）")).toHaveValue(7);
  expect(JSON.parse(drawer.querySelector("pre")?.textContent ?? "null")).toEqual({
    job: summary.job,
    source: summary.source,
  });
  await user.keyboard("{Escape}");
  await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
  const fixedRow = within(tasks).getByRole("row", { name: /N字形 · 版本 1/ });
  fixedRow.focus();
  await user.keyboard("{Enter}");
  await screen.findByRole("region", { name: "N字形 · 版本 1" });
  expect(fixedRow).toHaveAttribute("aria-selected", "true");
  expect(screen.queryByRole("button", { name: "参数与来源" })).not.toBeInTheDocument();
});

it.each([
  ["竞价缺口", auctionParameters, `ap.${"t".repeat(52)}`],
  ["成长板放量", growthParameters, `gp.${"u".repeat(52)}`],
] as const)(
  "API08 preserves the result family and full recipe for %s",
  async (name, parameters, nativeId) => {
    const recipe = parameterSet(parameters);
    const current: Schemas["MinuteParameterJob"] = {
      ...parameterJob,
      native_id: nativeId,
      native_name: name,
      family: parameters.family,
      parameters: recipe,
    };
    installResult(
      {
        ...parameterResultSource,
        native_id: nativeId,
        native_name: name,
        family: parameters.family,
        parameters: recipe,
      },
      current,
    );
    const user = userEvent.setup();
    renderApp("/backtest?view=minute");
    await openMinutePlayback();
    await screen.findByRole("region", { name: `${name} · 参数回测 · 版本 1` });
    await user.click(screen.getByRole("button", { name: "参数与来源" }));
    const drawer = await screen.findByRole("dialog", { name: "参数与来源" });
    expect(within(drawer).getByLabelText("最长持仓（交易日）")).toHaveValue(
      parameters.max_hold_days,
    );
    const material: unknown = JSON.parse(drawer.querySelector("pre")?.textContent ?? "null");
    expect(material).toMatchObject({ job: { family: parameters.family, parameters: recipe } });
  },
);

it.each([
  ["real_retained", "真实留存"],
  ["historical_reconstruction", "历史重建"],
  ["synthetic_validation", "合成验证"],
] as const)(
  "API08 shows the owner source nature %s without relabelling it from provenance",
  async (nature, label) => {
    installResult({ ...parameterResultSource, source_nature: nature });
    renderApp("/backtest?view=minute");
    await openMinutePlayback();
    const panel = await screen.findByRole("region", { name: "N字形 · 参数回测 · 版本 1" });
    expect(
      within(panel).getByText(new RegExp(`2026-07-31 至 2026-08-03 · ${label}`)),
    ).toBeVisible();
  },
);

it.each(["running", "sealing", "failed"] as const)(
  "API08 %s parameter jobs do not invent a result or downloadable report",
  async (status) => {
    installResult(parameterResultSource, { ...parameterJob, status });
    renderApp("/backtest?view=minute");
    await openMinutePlayback();
    const panel = await screen.findByRole("region", { name: "N字形 · 参数回测 · 版本 1" });
    expect(
      within(panel).getByText(
        status === "failed" ? "完整资料尚未通过校验。" : "结果尚未保存完成。",
      ),
    ).toBeVisible();
    expect(screen.queryByRole("region", { name: "分钟绩效" })).not.toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "HTML 报告" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "准备完整 ZIP" })).not.toBeInTheDocument();
    expect(screen.queryByRole("region", { name: "分钟结果明细" })).not.toBeInTheDocument();
  },
);

it("API08 closes private recipe details when the viewer and generation change", async () => {
  installResult();
  const user = userEvent.setup();
  const { queryClient } = renderApp("/backtest?view=minute");
  await openMinutePlayback();
  await user.click(await screen.findByRole("button", { name: "参数与来源" }));
  await screen.findByRole("dialog", { name: "参数与来源" });
  server.use(
    http.get(`${base}/runs`, () =>
      HttpResponse.json({ data: { available: true, jobs: [] }, serving }),
    ),
  );
  const next = metaEnvelope({ viewer: "other-researcher", generationId: "b".repeat(64) });
  server.use(metaHandler(next));
  await act(async () => queryClient.setQueryData(META_QUERY_KEY, next));
  await waitFor(() =>
    expect(screen.queryByRole("dialog", { name: "参数与来源" })).not.toBeInTheDocument(),
  );
  expect(screen.queryByText(parameterJob.native_id, { exact: false })).not.toBeInTheDocument();
});
