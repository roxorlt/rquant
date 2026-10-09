import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import {
  portfolioCapabilities,
  portfolioDaily,
  portfolioHoldings,
  portfolioJobs,
  portfolioLog,
  portfolioMonthly,
  portfolioNav,
  portfolioSummary,
  portfolioTrades,
} from "../src/pages/backtest/portfolio.fixture.ts";
import { experimentFixture } from "../src/pages/experiments/formal.fixture.ts";
import { nativeExperimentFixture } from "../src/pages/experiments/native.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

// Synthetic browser projections only. Original owner/worker/seal proofs remain
// separate. Minute source/NAV literals follow the frozen C6 frontend checkpoint02.
const origin = "http://127.0.0.1:19369";
const api = "/app/api/v1";
const portfolioBase = `${api}/backtests/portfolio`;
const minuteBase = `${api}/backtests/minute-runtime`;
const minuteJobId = "6fd352e6-ccbd-4b86-8b41-73b0d90fde33";
const minuteResultHash = "4".repeat(64);
const serving = metaEnvelope({ viewer: "alice" }).serving;
const source: Schemas["MinuteSourceOption"] = {
  source_key: "synthetic-source",
  source_version: 1,
  full_input_hash: "a".repeat(64),
  core_input_hash: "b".repeat(64),
  seed_hash: "c".repeat(64),
  native_id: "n_shape",
  native_name: "N 字形态",
  native_version: 1,
  native_registration_hash: "d".repeat(64),
  native_executable_fingerprint: "e".repeat(64),
  wrapper_registration_hash: "f".repeat(64),
  profile_hash: "1".repeat(64),
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
const completed: Schemas["MinuteJob"] = {
  job_id: minuteJobId,
  status: "completed",
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
  result_hash: minuteResultHash,
};
const minuteNav: Schemas["Envelope_MinuteNavData_"] = {
  serving,
  data: {
    job_id: minuteJobId,
    result_hash: minuteResultHash,
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
};
// Complete original SignalEnvelope from frozen fresh-read-sealed-result34,
// replay.signals b_intent. Other projections retain their independent display cases.
const signalFact = {
  schema_version: 1,
  signal_id: "3af33a41ea453811bc992ada354be9007b8152591c0ec857991ff8f6bf6c05fe",
  strategy_id: "n_shape",
  strategy_version: "1",
  parameter_fingerprint: "c2151f878e4382301a76205e68aa00cb5098b8440450158d990f43f389a315d3",
  dataset_snapshot_id: "5cba84fe68e65a6806a24d270f73954a71003e28751fb707c43a9c0e3395ae6f",
  feature_snapshot_id: "21f28e108fb0f653027497381f71c4022878be4d6af4f5ccc73b1d4e37f52a90",
  event_time: "2026-07-31T01:31:00Z",
  available_at: "2026-07-31T01:31:05Z",
  candidate_id: "600000.SH",
  action: "b_intent",
  reason_codes: ["n_shape_breakout", "structure_supported", "vwap_supported"],
  evidence: {
    candidate_price_basis: "raw_session",
    historical_sessions: 20,
    latest_close: 10.1,
    limit_pct: 10.0,
    limit_up_price_session_raw: 12.0,
    price_over_vwap: 1.0018034265103697,
    rel_cumulative: 8.3175,
    rel_same_minute: 15.15,
    runner_transition: {
      candidate_effective_trade_date: "2026-07-31",
      candidate_generation_sha256:
        "668bec51772220dfbffde8ee4850c97c959eaba4c2c3eea6c7017ec8360d37ff",
      candidate_occurrence_id: "4f2e9c32c81c938e3105e978b8759c85d782b55a5f99d8f2fac4afeff4ceabc4",
      candidate_snapshot_schema_version: 3,
      candidate_variant: "baseline",
      evaluator_contract_fingerprint:
        "50669be02e43c663cdb9949aa626670292b81530da597aea5311bf5c6b88a7bf",
      event: "entry_ready",
      feature_batch_id: "b48b8487e852aabf528432f43adb37d196613182f6954e258ff721d6456ed2b8",
      feature_sequence: 1,
      from_state: "watching",
      to_state: "armed",
    },
    session_high: 10.11,
    session_low: 9.89,
    t_close_session_raw: 9.8,
    t_high_session_raw: 10.0,
    tick_rule_buy_sell_ratio_proxy: 21.0,
  },
  expires_at: "2026-07-31T01:33:05Z",
  producer_commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
} satisfies Schemas["JsonValue"];
const tableFacts: Record<Schemas["MinuteRowsData"]["table"], Schemas["JsonValue"]> = {
  signals: signalFact,
  orders: { ts_code: "000001.SZ", side: "BUY", quantity: 100 },
  fills: {
    executed_at: "2026-07-31T02:00:00Z",
    quantity: 100,
    price: "10.00",
    total_fees: "5.10",
  },
  paper_queue: { state: "filled", quote_snapshot_id: "6".repeat(64) },
  account: { nav: "100092.9000", cash: "90000" },
  daily_valuations: { trade_date: "2026-08-03", status: "unavailable", nav: null },
  execution_profile: { initial_cash: "100000", execution_lag: "PT5S" },
  replay_summary: { source_key: source.source_key, native_id: source.native_id },
};
const tables = [
  "signals",
  "orders",
  "fills",
  "paper_queue",
  "account",
  "daily_valuations",
  "execution_profile",
  "replay_summary",
] as const satisfies readonly Schemas["MinuteRowsData"]["table"][];
const tableCopy: Record<Schemas["MinuteRowsData"]["table"], string> = {
  signals: "买入意向",
  orders: "000001.SZ · 买入 · 数量 100",
  fills: "成交价 10.00 · 费用 5.10",
  paper_queue: "查看执行状态与报价依据",
  account: "最终净值 100,092.90 · 现金 90,000.00",
  daily_valuations: "2026-08-03 · 估值不可用",
  execution_profile: "查看执行约束与完整费用口径",
  replay_summary: "查看完整策略、成交与来源依据",
};

type Scenario = "monthly" | "native" | "portfolio" | "minute";

async function projectionApi(page: Page, scenario: Scenario) {
  const problems: string[] = [];
  const expectedLosses: string[] = [];
  const monthOffsets: number[] = [];
  const submitted: string[] = [];
  const readTables: string[] = [];
  let submissionAccepted = false;
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("requestfailed", (request) => {
    if (
      scenario === "minute" &&
      submitted.length === 1 &&
      request.method() === "POST" &&
      request.url() === `${origin}${minuteBase}/runs`
    )
      expectedLosses.push(request.url());
    else problems.push(`request failed: ${request.url()}`);
  });
  page.on("console", (message) => {
    if (message.type() !== "error") return;
    const url = message.location().url;
    const optionalUnavailable =
      url.includes("/statistics") || url.includes("/heatmap") || url.includes("/ai/");
    const deliberateLoss =
      scenario === "minute" && submitted.length === 1 && url === `${origin}${minuteBase}/runs`;
    if (!optionalUnavailable && !deliberateLoss) problems.push(`console error: ${message.text()}`);
  });
  page.on("request", (request) => {
    const url = request.url();
    if (!url.startsWith(`${origin}/`) && !url.startsWith("data:"))
      problems.push(`external request: ${url}`);
  });
  await page.clock.setFixedTime(new Date("2026-10-07T00:01:00Z"));
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    const respond = (value: unknown) => route.fulfill({ status: 200, json: value });
    if (path === `${api}/meta`)
      return respond(
        metaEnvelope({
          viewer: "alice",
          generationId:
            scenario === "native" || scenario === "portfolio"
              ? experimentFixture.generation_id
              : undefined,
        }),
      );
    if (path === `${api}/collaboration/me`) {
      const me: Schemas["Envelope_CollaborationMe_"] = {
        serving: { ...serving, generation_id: null },
        data: {
          available: false,
          mode: "legacy",
          username: "alice",
          role: null,
          revision: null,
          state_sha256: null,
          can_manage_users: false,
          can_research: false,
          can_read_audit: false,
          message: "协作权限尚未启用。",
        },
      };
      return respond(me);
    }
    if (path.startsWith(`${api}/ai/`) || path.endsWith("/statistics") || path.endsWith("/heatmap"))
      return route.fulfill({ status: 503, json: { detail: "本浏览器输入未提供此详情。" } });
    if (scenario === "monthly") {
      const jobId = portfolioSummary.data.job.job_id;
      const run = `${portfolioBase}/runs/${jobId}`;
      const dates = { start_date: "2022-01-01", end_date: "2026-08-11" };
      if (path === `${portfolioBase}/capabilities`) return respond(portfolioCapabilities);
      if (path === `${portfolioBase}/runs`)
        return respond({
          ...portfolioJobs,
          data: {
            ...portfolioJobs.data,
            jobs: portfolioJobs.data.jobs.map((job) => ({ ...job, ...dates })),
          },
        });
      if (path === run)
        return respond({
          ...portfolioSummary,
          data: { ...portfolioSummary.data, job: { ...portfolioSummary.data.job, ...dates } },
        });
      if (path === `${run}/nav`) return respond(portfolioNav);
      if (path === `${run}/rows`) {
        expect(url.searchParams.get("result_hash")).toBe(portfolioSummary.data.result_hash);
        const view = url.searchParams.get("view");
        if (view === "monthly") {
          const rows: Schemas["PortfolioMonthRow"][] = Array.from({ length: 60 }, (_, i) => ({
            year: 2022 + Math.floor(i / 12),
            month: (i % 12) + 1,
            return_rate: null,
          }));
          rows[47] = { year: 2025, month: 12, return_rate: -0.03 };
          rows[48] = { year: 2026, month: 1, return_rate: 0.075 };
          rows[49] = { year: 2026, month: 2, return_rate: 0 };
          const offset = Number(url.searchParams.get("offset"));
          expect(url.searchParams.get("limit")).toBe("50");
          monthOffsets.push(offset);
          const envelope: Schemas["Envelope_PortfolioRowsData_"] = {
            ...portfolioMonthly,
            data: {
              ...portfolioMonthly.data,
              monthly: rows.slice(offset, offset + 50),
              total: rows.length,
              next_offset: offset + 50 < rows.length ? offset + 50 : null,
            },
          };
          return respond(envelope);
        }
        return respond(
          view === "holdings"
            ? portfolioHoldings
            : view === "daily"
              ? portfolioDaily
              : view === "log"
                ? portfolioLog
                : portfolioTrades,
        );
      }
    }
    if (scenario === "native" || scenario === "portfolio") {
      const fixture = scenario === "native" ? nativeExperimentFixture : experimentFixture;
      if (path === `${api}/experiments/capabilities`) return respond(fixture.capabilities);
      if (path === `${api}/experiments/mine`) return respond(fixture.mine);
      if (path.startsWith(`${api}/experiments/families/`)) return respond(fixture.family);
      if (path.startsWith(`${api}/experiments/results/`)) {
        const id = path.split("/").at(-1);
        if (scenario === "native") return respond(nativeExperimentFixture.result);
        const result = id ? experimentFixture.results[id] : undefined;
        if (result) return respond(result);
      }
    }
    if (scenario === "minute") {
      if (path === `${api}/backtests`)
        return respond({
          data: { available: false, runs: [], total: 0, next_offset: null },
          serving,
        });
      if (path === `${minuteBase}/capabilities`) {
        const envelope: Schemas["Envelope_MinuteCapabilities_"] = {
          serving,
          data: {
            available: true,
            can_run: true,
            can_export: false,
            source_count: 1,
            source_unavailable_count: 0,
            valuation_basis: "pit_asof_15:00",
          },
        };
        return respond(envelope);
      }
      if (path === `${minuteBase}/sources`) {
        const envelope: Schemas["Envelope_MinuteSourcesData_"] = {
          serving,
          data: { available: true, sources: [source], unavailable_count: 0 },
        };
        return respond(envelope);
      }
      if (path === `${minuteBase}/runs` && request.method() === "POST") {
        expect(request.headers()["x-rquant-csrf"]).toBe("1");
        const raw = request.postData();
        if (raw === null) throw new Error("minute request body is missing");
        submitted.push(raw);
        if (submitted.length === 1) return route.abort("failed");
        const body: Schemas["MinuteCreateRequest"] = request.postDataJSON();
        const receipt: Schemas["MinuteCommandReceipt"] = {
          command_id: body.command_id,
          status: "submitted",
          job_id: minuteJobId,
          message: "已提交分钟回测。",
        };
        submissionAccepted = true;
        return respond(receipt);
      }
      if (path === `${minuteBase}/runs`) {
        const envelope: Schemas["Envelope_MinuteJobsData_"] = {
          serving,
          data: { available: true, jobs: submissionAccepted ? [completed] : [], next_cursor: null },
        };
        return respond(envelope);
      }
      if (path === `${minuteBase}/runs/${minuteJobId}`) {
        const envelope: Schemas["Envelope_MinuteSummaryData_"] = {
          serving,
          data: {
            job: completed,
            source,
            result_hash: minuteResultHash,
            daily_status: "unavailable",
            can_report: false,
            performance: null,
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
        };
        return respond(envelope);
      }
      if (path === `${minuteBase}/runs/${minuteJobId}/nav`) {
        expect(url.searchParams.get("result_hash")).toBe(minuteResultHash);
        return respond(minuteNav);
      }
      if (path === `${minuteBase}/runs/${minuteJobId}/rows`) {
        const table = url.searchParams.get("table");
        const selected = tables.find((key) => key === table);
        if (!selected) throw new Error("unknown minute table request");
        expect(url.searchParams.get("result_hash")).toBe(minuteResultHash);
        expect(url.searchParams.get("offset")).toBe("0");
        readTables.push(selected);
        const envelope: Schemas["Envelope_MinuteRowsData_"] = {
          serving,
          data: {
            job_id: minuteJobId,
            result_hash: minuteResultHash,
            table: selected,
            rows: [{ sequence: 0, payload: tableFacts[selected] }],
            total: 1,
            next_offset: null,
          },
        };
        return respond(envelope);
      }
    }
    problems.push(`unexpected API: ${request.method()} ${path}`);
    return route.fulfill({ status: 404, body: "unknown synthetic endpoint" });
  });
  return { problems, monthOffsets, submitted, readTables, expectedLosses };
}

test("月热力原值、完整跨年分页、键盘和触屏", async ({ page }, info) => {
  const state = await projectionApi(page, "monthly");
  await page.goto("./#/backtest");
  await page.getByRole("tab", { name: "月度", exact: true }).click();
  const grid = page.getByRole("group", { name: "月度热力" });
  await expect(grid.getByRole("button")).toHaveCount(60);
  const positive = grid.getByRole("button", { name: "2026年1月，月收益 7.50%" });
  const negative = grid.getByRole("button", { name: "2025年12月，月收益 -3.00%" });
  const zero = grid.getByRole("button", { name: "2026年2月，月收益 0.00%" });
  const missing = grid.getByRole("button", { name: "2026年3月，暂无月收益" });
  await expect(positive).toHaveAttribute("data-tone", "up");
  await expect(negative).toHaveAttribute("data-tone", "down");
  await expect(zero).toHaveAttribute("data-tone", "flat");
  await expect(missing).toHaveAttribute("data-tone", "unknown");
  await expect(grid.getByRole("button", { name: "2026年9月，不在本次区间" })).toHaveText(/—/);
  expect([...new Set(state.monthOffsets)].sort((a, b) => a - b)).toEqual([0, 50]);
  await expect(page.getByRole("table", { name: "月度", exact: true })).toBeVisible();
  if (info.project.name === "phone390") {
    await positive.tap();
    await expect(
      page.getByRole("tooltip", { name: "2026年1月 · 月收益 7.50%", exact: true }),
    ).toBeVisible();
    const first = await positive.boundingBox();
    const second = await zero.boundingBox();
    expect(first?.height).toBeGreaterThanOrEqual(44);
    expect(first?.y).toBe(second?.y);
    await page.getByRole("heading", { level: 1, name: "回测" }).tap();
  } else {
    await positive.focus();
    await page.keyboard.press("Tab");
    await expect(zero).toBeFocused();
    await expect(
      page.getByRole("tooltip", { name: "2026年2月 · 月收益 0.00%", exact: true }),
    ).toBeVisible();
    await page.getByRole("tab", { name: "月度", exact: true }).focus();
  }
  await expectNoHorizontalOverflow(page, `${info.project.name} month grid`);
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(state.problems).toEqual([]);
  await page.screenshot({ path: info.outputPath("monthly-heatmap.png"), fullPage: true });
});

test("原生实验原参数、资金和模拟盘配置，来源只在Tip", async ({ page }, info) => {
  const state = await projectionApi(page, "native");
  await page.goto("./#/experiments");
  await page
    .getByRole("table", { name: "我的实验" })
    .getByRole("button", { name: "分钟策略实验 · 1" })
    .click();
  await expect(page.getByRole("img", { name: "实验与基准净值" }).locator("canvas")).toHaveCount(1);
  const config = page.getByRole("region", { name: "分钟策略配置" }).last();
  await expect(config.getByText("突破高点比例", { exact: true })).toBeVisible();
  await expect(config.getByText("1.002 倍", { exact: true })).toBeVisible();
  await expect(config.getByText("100,000.00 元", { exact: true })).toBeVisible();
  await expect(config.getByText("买入意向 · 100 股", { exact: true })).toBeVisible();
  await expect(config.getByText("5 秒", { exact: true })).toBeVisible();
  expect(await config.innerText()).not.toContain("original-minute");
  expect(await config.innerText()).not.toContain("break_high_ratio");
  expect(findJargon(await config.innerText())).toEqual([]);
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  const sourceButton = config.getByRole("button", { name: "查看分钟数据来源" });
  await expect(sourceButton).toBeEnabled();
  if (info.project.name === "phone390") await sourceButton.tap();
  else await sourceButton.focus();
  await expect(
    page.getByRole("tooltip", { name: "来源 original-minute；第 1 版", exact: true }),
  ).toBeVisible();
  await expectNoHorizontalOverflow(page, `${info.project.name} native config and source`);
  expect(state.problems).toEqual([]);
  await page.screenshot({ path: info.outputPath("native-experiment.png"), fullPage: true });
});

test("原组合实验参数和净值仍可读", async ({ page }, info) => {
  const state = await projectionApi(page, "portfolio");
  const original = experimentFixture.family.data.items[0];
  if (!original) throw new Error("original portfolio experiment fixture is missing");
  await page.goto("./#/experiments");
  await page
    .getByRole("table", { name: "我的实验" })
    .getByRole("button", { name: `${original.family_name} · ${original.index + 1}` })
    .click();
  await expect(page.getByRole("img", { name: "实验与基准净值" }).locator("canvas")).toHaveCount(1);
  await expect(page.getByRole("region", { name: "分钟策略配置" })).toHaveCount(0);
  const definitions = page
    .getByRole("dialog", { name: original.family_name })
    .locator("dl.exp-metrics > div > dt:visible");
  await expect(definitions.filter({ hasText: /^最多持仓$/ })).toBeVisible();
  await expect(definitions.filter({ hasText: /^现金保留$/ })).toBeVisible();
  await expectNoHorizontalOverflow(page, `${info.project.name} original portfolio experiment`);
  expect(state.problems).toEqual([]);
});

test("原分钟来源、同UUID丢回执刷新恢复、八表与净值缺口", async ({ page }, info) => {
  const state = await projectionApi(page, "minute");
  await page.goto("./#/backtest?view=minute");
  await expect(page.getByLabel("策略版本与输入")).toContainText("N字形 · 版本 1");
  for (const [label, value] of [
    ["训练开始", "2026-07-01"],
    ["训练结束", "2026-07-15"],
    ["验证开始", "2026-07-16"],
    ["验证结束", "2026-07-30"],
  ]) {
    if (!label || !value) throw new Error("minute protocol fixture is incomplete");
    await page.getByLabel(label).fill(value);
  }
  await page.getByRole("button", { name: "运行分钟回测", exact: true }).click();
  await expect(page.getByText("提交状态待确认，请重试原请求。", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "运行分钟回测", exact: true })).toBeDisabled();
  await page.reload();
  await expect(page.getByRole("button", { name: "重试原请求", exact: true })).toBeEnabled();
  await expect(page.getByLabel("策略版本与输入")).toBeDisabled();
  await expect(page.getByLabel("训练开始")).toHaveValue("2026-07-01");
  await page.getByRole("button", { name: "重试原请求", exact: true }).click();
  await expect(page.getByText("已提交分钟回测。", { exact: true })).toBeVisible();
  expect(state.submitted).toHaveLength(2);
  expect(state.submitted[1]).toBe(state.submitted[0]);
  const body: Schemas["MinuteCreateRequest"] = JSON.parse(state.submitted[0] ?? "null");
  expect(body.command_id).toMatch(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/);
  expect(body.config).toMatchObject({
    source_key: source.source_key,
    source_version: 1,
    full_input_hash: source.full_input_hash,
    native_id: "n_shape",
    native_version: 1,
  });
  expect(body).not.toHaveProperty("actor_id");
  expect(
    await page.evaluate(() => sessionStorage.getItem("rquant.minute.pending:alice")),
  ).toBeNull();
  await expect(page.getByRole("img", { name: "分钟策略每日净值" }).locator("canvas")).toHaveCount(
    1,
  );
  await expect(
    page.getByText("部分交易日缺少有效报价，净值有缺口。", { exact: true }),
  ).toBeVisible();
  await page.getByText("2026-07-31 · 100,092.90", { exact: true }).click();
  await expect(page.getByText(/报价 2026-07-31 14:59:00/)).toBeVisible();
  await page.getByText("2026-08-03 · 估值不可用", { exact: true }).click();
  await expect(page.getByText("持仓缺少当时可见报价。", { exact: true })).toBeVisible();
  for (const table of tables) {
    await page.getByLabel("分钟结果内容").selectOption(table);
    const row = page.getByRole("table", { name: "分钟结果明细" }).locator("tbody tr:not(.pad)");
    await expect(row).toHaveCount(1);
    await expect(row).toContainText(tableCopy[table]);
    await expect.poll(() => state.readTables.includes(table)).toBe(true);
    if (table === "signals") {
      expect(await page.locator("main").innerText()).not.toContain(signalFact.candidate_id);
      expect(await page.locator("main").innerText()).not.toContain(signalFact.signal_id);
      if (info.project.name === "phone390") await row.tap();
      else {
        await row.focus();
        await page.keyboard.press("Enter");
      }
      const details = page.getByRole("dialog").locator("pre");
      await expect(details).toBeVisible();
      expect(JSON.parse(await details.innerText())).toEqual(tableFacts.signals);
      await expectNoHorizontalOverflow(page, `${info.project.name} signal details`);
      await page.keyboard.press("Escape");
      await expect(page.getByRole("dialog")).toHaveCount(0);
    }
  }
  expect([...new Set(state.readTables)].sort()).toEqual(tables.toSorted());
  expect(state.expectedLosses).toHaveLength(1);
  await expectNoHorizontalOverflow(page, `${info.project.name} original minute results`);
  expect(state.problems).toEqual([]);
  await page.screenshot({ path: info.outputPath("minute-runtime.png"), fullPage: true });
});
