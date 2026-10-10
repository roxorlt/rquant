import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import type { ScreenOriginalAction } from "../src/api/screen.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

// Root diagnostic: observe the actual browser close/focus sequence; assertions stay unchanged.
test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    const witness: unknown[] = [];
    const describe = (node: Element | null) =>
      node
        ? {
            tag: node.tagName,
            text: node.textContent?.slice(0, 70),
            label: node.getAttribute("aria-label"),
            entry: node.getAttribute("data-screen-query-entry"),
            connected: node.isConnected,
            disabled: node instanceof HTMLButtonElement ? node.disabled : null,
            ancestors: Array.from(
              (function* () {
                let parent: Element | null = node;
                while (parent) {
                  yield parent;
                  parent = parent.parentElement;
                }
              })(),
            )
              .slice(0, 8)
              .map((parent) => ({
                tag: parent.tagName,
                class: parent.className,
                inert: parent.hasAttribute("inert"),
                hidden: parent.getAttribute("aria-hidden"),
              })),
          }
        : null;
    const record = (kind: string, extra: unknown) => {
      if (witness.length < 220)
        witness.push({
          kind,
          at: performance.now(),
          active: describe(document.activeElement),
          extra,
        });
    };
    Object.assign(window, { rootM4FocusWitness: witness });
    const focus = HTMLElement.prototype.focus;
    HTMLElement.prototype.focus = function (...args) {
      record("focus-before", { target: describe(this), stack: new Error().stack?.slice(0, 1100) });
      focus.apply(this, args);
      record("focus-after", { target: describe(this) });
    };
    document.addEventListener(
      "focusin",
      (event) => record("focusin", describe(event.target as Element)),
      true,
    );
    document.addEventListener(
      "click",
      (event) => record("click", describe(event.target as Element)),
      true,
    );
    const frame = window.requestAnimationFrame.bind(window);
    window.requestAnimationFrame = (callback) => {
      const source = callback.toString();
      const relevant = source.includes("focus") || source.includes("isConnected");
      if (relevant) record("raf-scheduled", source.slice(0, 1600));
      return frame((at) => {
        if (relevant) record("raf-enter", source.slice(0, 1600));
        callback(at);
        if (relevant) record("raf-exit", null);
      });
    };
  });
});
test.afterEach(async ({ page }, info) => {
  const witness = await page.evaluate(() => ({
    events: (window as Window & { rootM4FocusWitness?: unknown[] }).rootM4FocusWitness,
    active: document.activeElement?.outerHTML?.slice(0, 1100),
    hoverNone: window.matchMedia("(hover: none)").matches,
    tooltip: Array.from(document.querySelectorAll('[role="tooltip"]')).map(
      (node) => node.textContent,
    ),
  }));
  await info.attach("root-focus-witness", {
    body: JSON.stringify(witness, null, 2),
    contentType: "application/json",
  });
});

const owner = "1".repeat(64);
const source = {
  mode: "daily" as const,
  identity: "a".repeat(64),
  updated_at: "2026-09-30T07:30:00Z",
};
const definition: Schemas["ScreenQueryDefinition"] = {
  schema_version: 1,
  description: "完整条件记录",
  mode: "daily",
  trade_date: "2026-09-30",
  cutoff: null,
  source_kind: "replica",
  source_identity: source.identity,
  conditions: [
    { name: "not_st", args: {} },
    { name: "above_ma", args: { period: 37, offset: 2 } },
  ],
  ranking: { top_n: 13, conditions: [{ metric: "CIRC_MV[0]", weight: 100, ascending: true }] },
};
const blocks: Schemas["ScreenBlock"][] = [
  {
    key: "not_st",
    label: "排除 ST",
    hint: "排除特殊处理股票。",
    category: "basic",
    category_label: "基础",
    parameters: [],
  },
  {
    key: "above_ma",
    label: "站上均线",
    hint: "比较价格与所选均线。",
    category: "trend",
    category_label: "趋势",
    parameters: [
      {
        key: "period",
        label: "均线周期",
        input: "number",
        initial: 20,
        required: true,
        minimum: 2,
        maximum: 250,
        scale: 1,
        custom_ma: false,
      },
      {
        key: "offset",
        label: "向前偏移",
        input: "number",
        initial: 0,
        required: true,
        minimum: 0,
        maximum: 250,
        scale: 1,
        custom_ma: false,
      },
    ],
  },
];
const catalog: Schemas["ScreenCatalogData"] = {
  available: true,
  dates: [definition.trade_date],
  source,
  source_kind: "replica",
  nl_generate_available: false,
  blocks,
  ranking_metrics: [{ value: "CIRC_MV[0]", label: "流通市值" }],
};
const original: Schemas["ExecuteScreenQuery"] = {
  kind: "execute_screen_query",
  command_id: "old-command",
  requested_at: "2026-10-01T01:00:00Z",
  definition,
  page_size: 20,
};
function execution(command = original, pending = false): Schemas["ScreenExecutionView"] {
  return {
    execution_id: command.command_id,
    sequence: 1,
    command_hash: "b".repeat(64),
    plan_hash: "c".repeat(64),
    original_command: command,
    definition: command.definition,
    source,
    started_at: command.requested_at,
    completed_at: pending ? null : command.requested_at,
    status: pending ? "processing" : "succeeded",
    base_count: pending ? null : 2,
    total: pending ? null : 0,
    unknown_count: pending ? null : 2,
    ranked_count: pending ? null : 0,
    steps: pending ? [] : [{ label: "排除 ST", count: 0, unknown_count: 2 }],
    artifact_sha256: pending ? null : "d".repeat(64),
    member_rank_sha256: pending ? null : "e".repeat(64),
    failure_code: null,
  };
}
function privateData(
  change: Partial<Schemas["ScreenQueryReadData"]> = {},
): Schemas["ScreenQueryReadData"] {
  return {
    available: true,
    owner_scope_tag: owner,
    presets: [],
    daily_run_evidence: [],
    ...change,
  };
}
async function setup(page: Page) {
  const errors: string[] = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/api/v1/meta", (route) =>
    route.fulfill({ json: metaEnvelope({ viewer: "alice" }) }),
  );
  await page.route("**/api/v1/screen/blocks?*", (route) => {
    const intraday = new URL(route.request().url()).searchParams.get("mode") === "intraday";
    return route.fulfill({
      json: {
        data: intraday
          ? { ...catalog, available: false, dates: [], source: null, source_kind: "intraday" }
          : catalog,
        serving: metaEnvelope().serving,
      },
    });
  });
  await page.route("**/api/v1/screen/blocks", (route) =>
    route.fulfill({ json: { data: catalog, serving: metaEnvelope().serving } }),
  );
  await page.route("**/api/v1/screen/query/history?*", (route) =>
    route.fulfill({
      json: privateData({ history: { owner_scope_tag: owner, items: [], next_cursor: null } }),
    }),
  );
  await page.route("**/api/v1/screen/query/presets", (route) =>
    route.fulfill({ json: privateData() }),
  );
  await page.route("**/api/v1/screen/tdx/preview/source", (route) =>
    route.fulfill({
      json: {
        available: true,
        dates: [definition.trade_date],
        source: { identity: source.identity, updated_at: source.updated_at },
      },
    }),
  );
  return errors;
}

test("private history and complete presets use confirmation and return keyboard focus", async ({
  page,
}, info) => {
  const errors = await setup(page);
  const done = execution();
  const pending = execution(
    {
      ...original,
      command_id: "pending-original",
      definition: { ...definition, description: "仍待确认的记录" },
    },
    true,
  );
  const preset: Schemas["ScreenQueryPreset"] = {
    preset_id: "preset-one",
    name: "常用一",
    definition,
    version: 3,
    updated_at: original.requested_at,
    command_hash: "f".repeat(64),
  };
  let presets = [preset];
  const saves: Schemas["ScreenPresetSaveRequest"][] = [];
  await page.route("**/api/v1/screen/query/history?*", (route) =>
    route.fulfill({
      json: privateData({
        history: { owner_scope_tag: owner, items: [done, pending], next_cursor: null },
      }),
    }),
  );
  await page.route("**/api/v1/screen/query/executions/old-command", (route) =>
    route.fulfill({ json: privateData({ execution: done }) }),
  );
  await page.route("**/api/v1/screen/query/presets", (route) =>
    route.fulfill({ json: privateData({ presets }) }),
  );
  await page.route("**/api/v1/screen/query/presets/save", async (route) => {
    const body: Schemas["ScreenPresetSaveRequest"] = route.request().postDataJSON();
    expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
    expect(body).not.toHaveProperty("owner_id");
    saves.push(body);
    presets = [
      { ...preset, name: body.preset.name, definition: body.preset.definition, version: 4 },
    ];
    await route.fulfill({
      json: privateData({
        presets,
        receipt: {
          command_id: body.command_id,
          status: "succeeded",
          enqueued_at: body.requested_at,
          completed_at: body.requested_at,
          result: {},
          error: null,
        },
      }),
    });
  });
  await page.goto("./#/screener");
  const history = page.getByRole("button", { name: "历史", exact: true });
  await history.focus();
  await page.keyboard.press("Enter");
  const drawer = page.getByRole("dialog", { name: "选股历史" });
  await expect(drawer.getByText("完整条件记录", { exact: true })).toBeVisible();
  await expect(drawer.getByText("结果待确认", { exact: true })).toBeVisible();
  expect(findJargon(await drawer.innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "private history");
  await page.screenshot({ path: info.outputPath("history.png") });
  await drawer.getByRole("button", { name: "回填条件" }).first().click();
  await expect(drawer).toBeHidden();
  await expect(history).toBeFocused();
  await expect(page.getByRole("spinbutton", { name: "均线周期" })).toHaveValue("37");
  await expect(page.getByRole("spinbutton", { name: "向前偏移" })).toHaveValue("2");
  await page.getByRole("button", { name: "常用条件", exact: true }).click();
  const saved = page.getByRole("dialog", { name: "常用条件" });
  await expect(saved.getByText("前 13 只")).toBeVisible();
  await saved.getByRole("button", { name: "改名常用一" }).click();
  await saved.getByRole("textbox", { name: "条件名称" }).fill("改名后的完整条件");
  await saved.getByRole("button", { name: "保存条件", exact: true }).click();
  expect(saves).toHaveLength(0);
  await page.getByRole("button", { name: "确认保存", exact: true }).click();
  await expect.poll(() => saves.length).toBe(1);
  expect(saves[0]?.expected_version).toBe(3);
  expect(saves[0]?.preset.definition).toEqual(definition);
  await expect(saved.getByText("条件已保存。", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page, "preset confirmation");
  await page.screenshot({ path: info.outputPath("presets.png") });
  expect(errors).toEqual([]);
});

test("lost private reply resumes the exact request and unavailable intraday data stays unknown", async ({
  page,
}, info) => {
  const errors = await setup(page);
  let command: Schemas["ExecuteScreenQuery"] | undefined;
  let finished: Schemas["ScreenExecutionView"] | undefined;
  let executes = 0;
  const resumed: ScreenOriginalAction[] = [];
  await page.route("**/api/v1/screen/query/execute", async (route) => {
    executes += 1;
    command = route.request().postDataJSON();
    expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
    await route.abort("failed");
  });
  await page.route("**/api/v1/screen/query/lookup", (route) =>
    route.fulfill({ status: 404, json: { detail: "未找到原请求。" } }),
  );
  await page.route("**/api/v1/screen/query/resume", async (route) => {
    const body: Schemas["ScreenResumeAction"] = route.request().postDataJSON();
    expect(command).toBeDefined();
    expect(body.original).toEqual({ action: "execute", command });
    resumed.push(body.original);
    if (!command) throw new Error("original command missing");
    finished = execution(command);
    await route.fulfill({
      json: privateData({
        receipt: {
          command_id: command.command_id,
          status: "succeeded",
          enqueued_at: command.requested_at,
          completed_at: command.requested_at,
          result: {},
          error: null,
        },
      }),
    });
  });
  await page.route("**/api/v1/screen/query/executions/*", (route) =>
    route.fulfill({ json: privateData({ execution: finished ?? null }) }),
  );
  await page.route("**/api/v1/screen/query/executions/*/results?*", (route) =>
    route.fulfill({
      json: privateData({
        results: finished
          ? {
              execution_id: finished.execution_id,
              artifact_sha256: "d".repeat(64),
              rows: [],
              next_cursor: null,
            }
          : null,
      }),
    }),
  );
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选", exact: true }).click();
  await expect(page.getByText("结果待确认，请核对原请求。", { exact: true })).toBeVisible();
  expect(executes).toBe(1);
  const same = structuredClone(command);
  await page.reload();
  await expect(page.getByRole("button", { name: "恢复原请求", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "恢复原请求", exact: true }).click();
  const resultHeading = page.getByRole("heading", {
    name: "结果 · 已确认命中 0 只 · 未判定 2 只",
    exact: true,
  });
  await expect(resultHeading).toBeVisible();
  expect(executes).toBe(1);
  expect(command).toEqual(same);
  expect(resumed).toEqual([{ action: "execute", command: same }]);
  const intraday = page.getByRole("button", { name: "盘中", exact: true });
  if (info.project.name === "phone") await intraday.tap();
  else await intraday.focus();
  await expect(page.getByRole("tooltip")).toContainText("使用同一时点的盘中行情");
  if (info.project.name !== "phone") await intraday.click();
  await expect(intraday).toHaveAttribute("aria-pressed", "true");
  await expect(page.getByRole("button", { name: "运行筛选", exact: true })).toBeDisabled();
  await expect(resultHeading).toBeHidden();
  await expectNoHorizontalOverflow(page, "intraday unavailable");
  await page.screenshot({ path: info.outputPath("recovery-and-intraday.png") });
  expect(errors).toEqual([]);
});

test("condition rules keep missing source disabled and preserve the full editor on mobile", async ({
  page,
}, info) => {
  const errors = await setup(page);
  const rule: Schemas["ConditionAlertRuleDefinition-Input"] = {
    schema_version: 1,
    rule_id: "saved-rule",
    name: "完整条件提醒",
    enabled: false,
    priority: "P2",
    conditions: definition.conditions,
    ranking: definition.ranking,
    scope: { kind: "market", universe_policy: "trusted_current" },
    frequency: { kind: "per_symbol_minutes", minutes: 5 },
    governance: { channels: ["pushdeer"], dedup_window_seconds: 60, notify_recovery: true },
    trading_hours: {
      timezone: "Asia/Shanghai",
      windows: [
        { start: "09:30:00", end: "11:30:00" },
        { start: "13:00:00", end: "14:57:00" },
      ],
    },
    source_policy: {
      condition_semantics_version: "screen-registry/v1",
      daily_anchor: "previous_closed_session",
      intraday_contract_id: "intraday-pit",
      minimum_intraday_contract_version: 4,
    },
  };
  const list: Schemas["ConditionAlertRuleListData"] = {
    availability: "ready",
    available_at: source.updated_at,
    message: "",
    can_write: true,
    can_enable: false,
    write_message: "",
    enable_message: "盘中数据暂不可用。",
    blocks,
    ranking_metrics: catalog.ranking_metrics,
    triggers: [],
    scopes: [
      {
        label: "全市场",
        scope: rule.scope,
        available: false,
        member_count: null,
        message: "盘中数据暂不可用。",
      },
    ],
    items: [
      {
        rule_id: rule.rule_id,
        rule,
        version: 2,
        updated_at: source.updated_at,
        scope_status: "unavailable",
        scope_message: "盘中数据暂不可用。",
        status_label: "未运行",
        matched_count: null,
        unknown_count: null,
      },
    ],
  };
  await page.route("**/api/v1/monitor/condition-rules", (route) =>
    route.fulfill({ json: { data: list, serving: metaEnvelope().serving } }),
  );
  await page.goto("./#/monitor");
  const region = page.getByRole("region", { name: "条件提醒", exact: true });
  await expect(region.getByText("完整条件提醒", { exact: true })).toBeVisible();
  await expect(region.getByRole("button", { name: "启用", exact: true })).toBeDisabled();
  const edit = region.getByRole("button", { name: "编辑条件 完整条件提醒" });
  await edit.focus();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "编辑条件规则" });
  await expect(dialog.getByRole("spinbutton", { name: "均线周期" })).toHaveValue("37");
  await expect(dialog.getByRole("spinbutton", { name: "向前偏移" })).toHaveValue("2");
  await expect(dialog.getByRole("spinbutton", { name: "排名保留数量" })).toHaveValue("13");
  await expect(dialog.getByRole("spinbutton", { name: "提醒间隔分钟" })).toHaveValue("5");
  await expect(dialog.getByRole("checkbox", { name: "启用条件规则" })).toBeDisabled();
  expect(findJargon(await dialog.innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "condition editor");
  await page.screenshot({ path: info.outputPath("condition-editor.png") });
  await dialog.getByRole("button", { name: "关闭", exact: true }).click();
  await expect(dialog).toBeHidden();
  await expect(edit).toBeFocused();
  expect(errors).toEqual([]);
});
