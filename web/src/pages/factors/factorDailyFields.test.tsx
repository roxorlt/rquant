import { act, fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import { META_QUERY_KEY } from "@/api/useMeta";
import { metaEnvelope } from "@/test/fixtures";
import { findJargon } from "@/test/jargon";
import { renderApp } from "@/test/render";
import { server } from "@/test/server";
import { dailyCapability, dailyStoredFields } from "./factorDailyFields.fixture";
import {
  diagnosticAvailability,
  diagnosticFactor,
  diagnosticResearch,
  diagnosticResult,
} from "./factorDiagnostics.fixture";
import { RUN_OPERATION_KEY, readRun } from "./factorRunState";
import { SAVE_COMMAND_KEY, SAVE_DRAFT_KEY } from "./factorSaveState";
import {
  trackingKey,
  trackingPanel,
  trackingRequest,
  trackingResult,
} from "./factorTracking.fixture";

vi.mock("@/charts/EChart", () => ({
  EChart: ({ label }: { label: string }) => <div role="img" aria-label={label} />,
}));

const generation = metaEnvelope().serving.generation_id ?? "a".repeat(64);
const nextGeneration = "d".repeat(64);
const storedFactor: Schemas["FactorDefinitionItem"] = {
  ...diagnosticFactor,
  expression: "turnover_rate + ref(ma20, 1)",
  dependency_columns: ["ma20", "turnover_rate"],
};
const originalMedia = window.matchMedia;

function publish(
  rows: Schemas["FactorDefinitionItem"][] = [storedFactor],
  capability: Schemas["FactorCapabilitiesData"] = dailyCapability,
  panel = trackingPanel({ factor_id: storedFactor.factor_id }),
) {
  const metadata = metaEnvelope();
  server.use(
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: rows.length ? "populated" : "empty",
          available_at: metadata.serving.built_at,
          can_save: true,
          can_archive: true,
          definitions: rows,
        },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/capabilities", () =>
      HttpResponse.json({ data: capability, serving: metadata.serving }),
    ),
    http.get("*/api/v1/factors/:factorId/tracking", ({ params }) =>
      HttpResponse.json({
        data: { ...panel, factor_id: String(params.factorId) },
        serving: metadata.serving,
      }),
    ),
    http.get("*/api/v1/factors/run-availability", () =>
      HttpResponse.json({ data: diagnosticAvailability, serving: metadata.serving }),
    ),
  );
}
function capabilityEnvelope(capability = dailyCapability, id = generation) {
  return { data: capability, serving: metaEnvelope({ generationId: id }).serving };
}
async function editor() {
  const button = await screen.findByRole("button", { name: "新建因子" });
  await waitFor(() => expect(button).toBeEnabled());
  await userEvent.click(button);
  return screen.findByRole("dialog", { name: "新建因子" });
}
function showResults(
  newResearch: Schemas["FactorStreamResearchDisplay"],
  oldResearch?: Schemas["FactorStreamResearchDisplay"],
) {
  const result = diagnosticResult();
  const previous = diagnosticResult({
    job_id: "1".repeat(32),
    factor_version: 1,
    definition_status: "historical_unavailable",
    updated_at: "2026-09-20T07:31:00Z",
  });
  server.use(
    http.get("*/api/v1/factors/results", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: result.updated_at,
          results: oldResearch ? [result, previous] : [result],
        },
        serving: metaEnvelope().serving,
      }),
    ),
    http.get("*/api/v1/factors/results/:jobId", ({ params }) =>
      HttpResponse.json({
        data: {
          availability: "ready",
          available_at: result.updated_at,
          result: params.jobId === previous.job_id ? previous : result,
          research: params.jobId === previous.job_id ? oldResearch : newResearch,
        },
        serving: metaEnvelope().serving,
      }),
    ),
  );
}
const source: Schemas["FactorDailyFeatureSources"] = {
  source_sha256: "2".repeat(64),
  prepared_source_sha256: "3".repeat(64),
  prepared_snapshot_id: "synthetic-daily-fields",
  prepared_binding_hash: "4".repeat(64),
  scope_content_hash: "5".repeat(64),
  code_commit: "6".repeat(40),
  source_mode: "historical_retrospective",
  value_semantics: "stored_not_recomputed",
  price_basis: "unverified",
  recursive_initialization: "unverified",
  fields: dailyStoredFields.filter(
    (field) => field.column === "turnover_rate" || field.column === "ma20",
  ),
};
const research: Schemas["FactorStreamResearchDisplay"] = {
  ...diagnosticResearch,
  daily_features: source,
  daily_feature_coverage_days: [
    {
      trade_date: "2026-09-22",
      panel_date: "2026-09-21",
      computation_stock_count: 8,
      counts: [
        { column: "ma20", valid: 5, missing: 1, null: 1, non_finite: 1 },
        { column: "turnover_rate", valid: 6, missing: 2, null: 0, non_finite: 0 },
      ],
    },
  ],
};

beforeEach(() =>
  Object.defineProperty(navigator, "locks", {
    configurable: true,
    value: { request: async (_name: string, _options: unknown, action: () => unknown) => action() },
  }),
);
afterEach(() => {
  vi.restoreAllMocks();
  window.matchMedia = originalMedia;
  Object.defineProperty(navigator, "locks", { configurable: true, value: undefined });
});

it("22项可信字段默认六项，全部字段展开后按API顺序显示中文名", async () => {
  publish([]);
  const { container } = renderApp("/factors");
  const dialog = await editor();
  expect(within(dialog).getAllByRole("button", { name: /^插入/ })).toHaveLength(6);
  await userEvent.click(within(dialog).getByRole("button", { name: "全部字段" }));
  expect(
    within(dialog)
      .getAllByRole("button", { name: /^插入/ })
      .map((b) => b.textContent),
  ).toEqual(dailyCapability.fields.map((f) => f.name_zh));
  expect(findJargon(container.textContent ?? "")).toEqual([]);
  await userEvent.click(within(dialog).getByRole("button", { name: "收起字段" }));
  expect(within(dialog).getAllByRole("button", { name: /^插入/ })).toHaveLength(6);
});

it("中文搜索覆盖未展开目录，Enter不保存，空搜索结果诚实说明", async () => {
  publish([]);
  const saves = vi.fn();
  server.use(
    http.post("*/api/v1/factors/definitions/save", () => {
      saves();
      return HttpResponse.error();
    }),
  );
  renderApp("/factors");
  const dialog = await editor();
  await userEvent.type(within(dialog).getByRole("textbox", { name: "中文名" }), "换手因子");
  await userEvent.type(within(dialog).getByRole("textbox", { name: "表达式" }), "close");
  const search = within(dialog).getByRole("searchbox", { name: "搜索日线字段" });
  await userEvent.type(search, "换手{Enter}");
  expect(within(dialog).getAllByRole("button", { name: /^插入/ })).toHaveLength(1);
  expect(within(dialog).getByRole("button", { name: "插入换手率" })).toBeEnabled();
  expect(saves).not.toHaveBeenCalled();
  expect(localStorage.getItem(SAVE_COMMAND_KEY)).toBeNull();
  await userEvent.clear(search);
  await userEvent.type(search, "不存在");
  expect(within(dialog).getByText("没有匹配的字段")).toBeInTheDocument();
  expect(within(dialog).queryByRole("button", { name: /^插入/ })).toBeNull();
});

it("插入实际column替换选区并恢复光标，不把搜索或说明混入表达式", async () => {
  publish([]);
  renderApp("/factors");
  const dialog = await editor();
  const expression = within(dialog).getByRole<HTMLTextAreaElement>("textbox", { name: "表达式" });
  await userEvent.type(expression, "ref(close, 1)");
  expression.setSelectionRange(4, 9);
  await userEvent.type(within(dialog).getByRole("searchbox", { name: "搜索日线字段" }), "换手");
  await userEvent.click(within(dialog).getByRole("button", { name: "插入换手率" }));
  await waitFor(() => expect(expression).toHaveFocus());
  expect(expression).toHaveValue("ref(turnover_rate, 1)");
  expect(expression.selectionStart).toBe(17);
  expect(JSON.parse(localStorage.getItem(SAVE_DRAFT_KEY) ?? "null").expression).toBe(
    "ref(turnover_rate, 1)",
  );
});

it.each([false, true])("独立单位说明可键盘/触屏打开且不插入（触屏=%s）", async (touch) => {
  window.matchMedia = (query) => ({
    ...originalMedia(query),
    matches: touch && query === "(hover: none)",
  });
  publish([]);
  renderApp("/factors");
  const dialog = await editor();
  await userEvent.type(within(dialog).getByRole("searchbox", { name: "搜索日线字段" }), "总市值");
  const info = within(dialog).getByRole("button", { name: "总市值说明" });
  if (touch) await userEvent.click(info);
  else fireEvent.focus(info);
  expect(await screen.findByRole("tooltip")).toHaveTextContent("当日总市值原值，单位万元。");
  expect(within(dialog).getByRole("textbox", { name: "表达式" })).toHaveValue("");
});

it("未配置来源只展示API六项，不编造库存字段", async () => {
  publish([], {
    ...dailyCapability,
    fields: dailyCapability.fields.slice(0, 6),
    version: "daily_v1",
  });
  renderApp("/factors");
  const dialog = await editor();
  expect(within(dialog).getAllByRole("button", { name: /^插入/ })).toHaveLength(6);
  expect(within(dialog).queryByRole("button", { name: "全部字段" })).toBeNull();
  await userEvent.type(within(dialog).getByRole("searchbox", { name: "搜索日线字段" }), "均线");
  expect(within(dialog).getByText("没有匹配的字段")).toBeInTheDocument();
});

it.each(["shrink", "wrong_generation", "account"] as const)(
  "能力%s不改写已存表达式，坏能力不提供插入",
  async (change) => {
    publish([]);
    const app = renderApp("/factors");
    const dialog = await editor();
    const expression = within(dialog).getByRole("textbox", { name: "表达式" });
    await userEvent.type(expression, "ref(ma20, 1) + turnover_rate");
    if (change === "account") {
      server.use(
        http.get("*/api/v1/factors/capabilities", () => new HttpResponse(null, { status: 403 })),
      );
      act(() => app.queryClient.setQueryData(META_QUERY_KEY, metaEnvelope({ viewer: "other" })));
      await waitFor(() =>
        expect(within(dialog).getByRole("button", { name: "保存因子" })).toBeDisabled(),
      );
    } else {
      act(() =>
        app.queryClient.setQueryData(
          ["factors", "capabilities", generation, "tester", 0],
          capabilityEnvelope(
            { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) },
            change === "wrong_generation" ? nextGeneration : generation,
          ),
        ),
      );
      await waitFor(() =>
        expect(within(dialog).queryByRole("button", { name: "插入20日均线" })).toBeNull(),
      );
      if (change === "wrong_generation")
        expect(within(dialog).queryByRole("button", { name: /^插入/ })).toBeNull();
    }
    expect(expression).toHaveValue("ref(ma20, 1) + turnover_rate");
    expect(JSON.parse(localStorage.getItem(SAVE_DRAFT_KEY) ?? "null").expression).toBe(
      "ref(ma20, 1) + turnover_rate",
    );
  },
);

it("新增字段定义按可信依赖禁加入，仍能保存及单次检验", async () => {
  publish();
  renderApp("/factors");
  const join = await screen.findByRole("button", { name: "加入跟踪" });
  await waitFor(() => expect(join).toHaveAccessibleDescription("库存日线字段暂不支持持续跟踪。"));
  expect(join).toBeDisabled();
  await waitFor(() => expect(screen.getByRole("button", { name: "编辑" })).toBeEnabled());
  const params = await screen.findByRole("region", { name: "检验参数" });
  await waitFor(() =>
    expect(within(params).getByRole("button", { name: "运行检验" })).toBeEnabled(),
  );
});

it("跟踪权限成立但不能保存时仍核对实际能力，不按can_save判定支持", async () => {
  publish([diagnosticFactor], { ...dailyCapability, can_save: false });
  server.use(
    http.get("*/api/v1/factors/definitions", () =>
      HttpResponse.json({
        data: {
          availability: "populated",
          available_at: null,
          can_save: false,
          can_archive: false,
          definitions: [diagnosticFactor],
        },
        serving: metaEnvelope().serving,
      }),
    ),
  );
  renderApp("/factors");
  await screen.findByRole("button", { name: "加入跟踪" });
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "加入跟踪" }));
  expect(await screen.findByRole("dialog", { name: "加入因子跟踪" })).toBeInTheDocument();
});

it.each(["absent", "error", "wrong_generation"] as const)(
  "新增字段能力%s不可新加入，但原已有跟踪仍能取消",
  async (state) => {
    const panel = trackingPanel({
      factor_id: storedFactor.factor_id,
      tracked: true,
      status: "active",
      availability: "tracked",
      tracking_generation: "b".repeat(32),
      definition_head: {
        version: storedFactor.version,
        content_sha256: storedFactor.content_sha256,
      },
    });
    publish(
      [storedFactor],
      { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) },
      panel,
    );
    if (state === "error")
      server.use(
        http.get("*/api/v1/factors/capabilities", () => new HttpResponse(null, { status: 503 })),
      );
    if (state === "wrong_generation")
      server.use(
        http.get("*/api/v1/factors/capabilities", () =>
          HttpResponse.json(capabilityEnvelope(dailyCapability, nextGeneration)),
        ),
      );
    const seen: Schemas["FactorTrackingRequest"][] = [];
    server.use(
      http.post("*/api/v1/factors/tracking/commands", async ({ request }) => {
        const body = (await request.json()) as Schemas["FactorTrackingRequest"];
        seen.push(body);
        return HttpResponse.json({ data: trackingResult(body), serving: metaEnvelope().serving });
      }),
    );
    renderApp("/factors");
    const cancel = await screen.findByRole("button", { name: "取消跟踪" });
    await waitFor(() => expect(cancel).toBeEnabled());
    await userEvent.click(cancel);
    await waitFor(() => expect(seen).toHaveLength(1));
    expect(seen[0]?.tracked).toBe(false);
    expect(seen[0]?.expected_head).toEqual({
      version: storedFactor.version,
      content_sha256: storedFactor.content_sha256,
    });
  },
);

it("加入确认后能力缩减，锁内重验不创建请求；再次核验可取消旧跟踪", async () => {
  publish([diagnosticFactor]);
  const app = renderApp("/factors");
  await screen.findByRole("button", { name: "加入跟踪" });
  await waitFor(() => expect(screen.getByRole("button", { name: "加入跟踪" })).toBeEnabled());
  await userEvent.click(screen.getByRole("button", { name: "加入跟踪" }));
  const dialog = await screen.findByRole("dialog", { name: "加入因子跟踪" });
  act(() =>
    app.queryClient.setQueryData(
      ["factors", "capabilities", generation, "tester", 0],
      capabilityEnvelope({ ...dailyCapability, fields: [] }),
    ),
  );
  await waitFor(() =>
    expect(within(dialog).getByRole("button", { name: "确认加入" })).toBeDisabled(),
  );
  expect(localStorage.getItem(trackingKey)).toBeNull();
});

it("旧跟踪原请求在当前能力缺失时仍完整续查，不重造head或模式", async () => {
  const request = { ...trackingRequest(), serving_generation_id: generation };
  publish([diagnosticFactor]);
  const seen: unknown[] = [];
  localStorage.setItem(
    trackingKey,
    JSON.stringify({
      viewer: "tester",
      factorName: diagnosticFactor.name_zh,
      request,
      result: null,
      denied: false,
    }),
  );
  server.use(
    http.get("*/api/v1/factors/capabilities", () => new HttpResponse(null, { status: 503 })),
    http.post("*/api/v1/factors/tracking/commands/resume", async ({ request: incoming }) => {
      seen.push(await incoming.json());
      return HttpResponse.error();
    }),
  );
  renderApp("/factors");
  await waitFor(() => expect(seen).toEqual([request]));
  expect(JSON.parse(localStorage.getItem(trackingKey) ?? "null").request).toEqual(request);
});

it("新字段原检验失联跨重载与换代恢复完整请求，目录缩减不改变冻结定义", async () => {
  const original: Schemas["FactorRunRequest"] = {
    command_id: "99999999-9999-4999-8999-999999999999",
    requested_at: "2026-10-02T00:00:00Z",
    serving_generation_id: generation,
    parameters: {
      factor_id: storedFactor.factor_id,
      expected_head: { version: storedFactor.version, content_sha256: storedFactor.content_sha256 },
      selection: "all",
      start_date: "2026-09-17",
      end_date: "2026-09-22",
      holding_sessions: 5,
      group_count: 5,
      ic_method: "rank",
      neutralization: "none",
      extended_statistics: true,
    },
  };
  localStorage.setItem(
    RUN_OPERATION_KEY,
    JSON.stringify({
      viewer: "tester",
      factorName: storedFactor.name_zh,
      poolLabel: "全市场",
      request: original,
      result: null,
      denied: false,
    }),
  );
  publish([storedFactor], { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) });
  const seen: unknown[] = [];
  server.use(
    http.post("*/api/v1/factors/runs/resume", async ({ request }) => {
      seen.push(await request.json());
      return HttpResponse.error();
    }),
  );
  const app = renderApp("/factors");
  await waitFor(() => expect(seen).toEqual([original]));
  app.unmount();
  renderApp("/factors");
  await waitFor(() => expect(seen).toEqual([original, original]));
  expect(readRun()?.request).toEqual(original);
});

it("新结果只用自身库存说明与逐日缺因，当前目录缩减不重绑定历史", async () => {
  publish([storedFactor], { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) });
  showResults(research);
  const { container } = renderApp("/factors");
  const basis = await screen.findByText("日线字段口径");
  fireEvent.focus(basis.closest(".tip-anchor") ?? basis);
  expect(await screen.findByRole("tooltip")).toHaveTextContent(
    "百分数原值；0.5387表示0.5387%，不乘100。",
  );
  expect(screen.getByRole("tooltip")).toHaveTextContent("已存20日均线，价格基准未核验。");
  fireEvent.blur(basis.closest(".tip-anchor") ?? basis);
  await userEvent.click(screen.getByText("查看字段覆盖"));
  const table = await screen.findByRole("table", { name: "日线字段覆盖" });
  expect(table).toHaveTextContent("2026-09-22");
  expect(table).toHaveTextContent("2026-09-21");
  expect(
    within(table)
      .getAllByRole("cell")
      .map((cell) => cell.textContent),
  ).toEqual(["2026-09-22", "2026-09-21", "5 / 8", "1", "1", "1"]);
  await userEvent.selectOptions(
    screen.getByRole("combobox", { name: "覆盖字段" }),
    "turnover_rate",
  );
  expect(table).toHaveTextContent("6 / 8");
  expect(container.textContent).not.toContain(source.source_sha256);
  expect(findJargon(container.textContent ?? "")).toEqual([]);
});

it("切换旧结果不套用当前结果新来源；未提供覆盖明确空态", async () => {
  publish();
  showResults({ ...research, daily_feature_coverage_days: null }, diagnosticResearch);
  renderApp("/factors");
  await screen.findByText("日线字段口径");
  await userEvent.click(screen.getByText("查看字段覆盖"));
  expect(screen.getByText("当前结果未提供字段覆盖记录。")).toBeInTheDocument();
  await userEvent.click(
    within(screen.getByRole("table", { name: "最近检验" })).getByRole("cell", { name: "第 1 版" }),
  );
  await waitFor(() => expect(screen.queryByText("日线字段口径")).toBeNull());
  expect(screen.queryByText("查看字段覆盖")).toBeNull();
});
