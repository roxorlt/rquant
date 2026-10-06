import { expect, type Page, type Response, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

type Execute = Schemas["ExecuteScreenQuery"];
type RunData = Schemas["ScreenRunData"];
const ownerScope = "1".repeat(64);
function privateScreenData(
  change: Partial<Schemas["ScreenQueryReadData"]> = {},
): Schemas["ScreenQueryReadData"] {
  return {
    available: true,
    owner_scope_tag: ownerScope,
    presets: [],
    daily_run_evidence: [],
    ...change,
  };
}

// Transport fixture over the same invented 30-stock universe; backend math has separate evidence.
function screenResult(command: Execute, change: Partial<RunData> = {}): RunData {
  const ranking = command.definition.ranking;
  const rows: Schemas["ScreenRow"][] = Array.from({ length: 30 }, (_, offset) => offset + 1)
    .filter((index) => index % 10 !== 0)
    .map((index, offset) => ({
      ts_code: `${600000 + index}.SH`,
      name: `样本${String(index).padStart(2, "0")}`,
      close: 10 + index,
      pct_chg: index - 15,
      rank_position: ranking ? offset + 1 : null,
      ranking_score: ranking ? 100 - offset : null,
    }));
  return {
    trade_date: command.definition.trade_date,
    status: "ready",
    base_count: 30,
    total: 27,
    unknown_count: 0,
    ranked_count: ranking ? Math.min(ranking.top_n, rows.length) : null,
    steps: [{ label: "排除 ST", count: 27, unknown_count: 0 }],
    rows: ranking ? rows.slice(0, ranking.top_n) : rows,
    next_cursor: null,
    source: {
      mode: command.definition.mode,
      identity: command.definition.source_identity,
      updated_at: "2026-09-24T07:31:00Z",
    },
    ...change,
  };
}

async function screenCommands(
  page: Page,
  reply: (command: Execute) => RunData = screenResult,
): Promise<void> {
  const records = new Map<
    string,
    { original: string; command: Execute; data: RunData; execution: Schemas["ScreenExecutionView"] }
  >();
  await page.route("**/api/v1/screen/query/history?*", (route) =>
    route.fulfill({
      json: privateScreenData({
        history: {
          owner_scope_tag: ownerScope,
          items: [...records.values()].map((record) => record.execution).reverse(),
          next_cursor: null,
        },
      }),
    }),
  );
  await page.route("**/api/v1/screen/query/presets", (route) =>
    route.fulfill({ json: privateScreenData() }),
  );
  await page.route("**/api/v1/screen/query/execute", async (route) => {
    const command: Execute = route.request().postDataJSON();
    expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
    expect(command.kind).toBe("execute_screen_query");
    expect(command.command_id).not.toBe("");
    const original = JSON.stringify(command);
    const prior = records.get(command.command_id);
    if (prior) expect(original).toBe(prior.original);
    else {
      const data = reply(command);
      expect(data.source?.identity).toBe(command.definition.source_identity);
      expect(data.trade_date).toBe(command.definition.trade_date);
      const execution: Schemas["ScreenExecutionView"] = {
        execution_id: command.command_id,
        sequence: records.size + 1,
        command_hash: "b".repeat(64),
        plan_hash: "c".repeat(64),
        original_command: command,
        definition: command.definition,
        source: data.source,
        started_at: command.requested_at,
        completed_at: command.requested_at,
        status: "succeeded",
        base_count: data.base_count,
        total: data.total,
        unknown_count: data.unknown_count,
        ranked_count: data.ranked_count,
        steps: data.steps,
        artifact_sha256: "d".repeat(64),
        member_rank_sha256: "e".repeat(64),
        failure_code: null,
      };
      records.set(command.command_id, { original, command, data, execution });
    }
    await route.fulfill({
      json: privateScreenData({
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
  await page.route(
    /\/api\/v1\/screen\/query\/executions\/[^/?]+(?:\/results)?(?:\?.*)?$/,
    async (route) => {
      const url = new URL(route.request().url());
      const match = url.pathname.match(/\/executions\/([^/]+)(\/results)?$/);
      const record = match ? records.get(decodeURIComponent(match[1] ?? "")) : undefined;
      if (!record) {
        await route.fulfill({ status: 404, json: { detail: "未找到原请求。" } });
        return;
      }
      expect(JSON.stringify(record.execution.original_command)).toBe(record.original);
      if (!match?.[2]) {
        await route.fulfill({ json: privateScreenData({ execution: record.execution }) });
        return;
      }
      const cursor = url.searchParams.get("cursor");
      const prefix = `${record.command.command_id}:`;
      const offset = cursor?.startsWith(prefix)
        ? Number(cursor.slice(prefix.length))
        : cursor === null
          ? 0
          : Number.NaN;
      if (!Number.isInteger(offset) || offset < 0 || offset > record.data.rows.length) {
        await route.fulfill({ status: 422, json: { detail: "原结果游标不匹配。" } });
        return;
      }
      const limit = record.command.page_size ?? 20;
      const next = offset + limit;
      const results: Schemas["ScreenExecutionResults"] = {
        execution_id: record.command.command_id,
        artifact_sha256: record.execution.artifact_sha256 ?? "d".repeat(64),
        rows: record.data.rows.slice(offset, next),
        next_cursor: next < record.data.rows.length ? `${prefix}${next}` : null,
      };
      await route.fulfill({ json: privateScreenData({ results }) });
    },
  );
}

test.beforeEach(async ({ page }) => {
  await screenCommands(page);
});

test("单股公式预览先检查再判断，来源换代后桌面与手机要求重跑", async ({ page }, testInfo) => {
  const watcher = watch(page);
  let identity = "a".repeat(64);
  await page.route("**/api/v1/screen/tdx/preview/source", async (route) => {
    await route.fulfill({
      status: 200,
      json: {
        available: true,
        dates: ["2026-09-24"],
        source: { identity, updated_at: "2026-09-24T07:31:00Z" },
      },
    });
  });
  await page.route("**/api/v1/screen/blocks", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "replica";
    body.data.source = { mode: "daily", identity, updated_at: "2026-09-24T07:31:00Z" };
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/tdx/parse", async (route) => {
    await route.fulfill({
      status: 200,
      json: {
        syntax_version: "tdx-v1",
        status: "parsed",
        capability: "parse_only",
        ast: null,
        translation: null,
        issues: [],
        unsupported: [],
      },
    });
  });
  await page.route("**/api/v1/screen/tdx/preview", async (route) => {
    const request = route.request().postDataJSON() as Schemas["TdxPreviewRequest"];
    expect(request.source_identity).toBe(identity);
    expect(request.stock_code).toBe("600001.SH");
    await route.fulfill({
      status: 200,
      json: {
        stock_code: request.stock_code,
        trade_date: request.trade_date,
        status: "match",
        reason: null,
        source_updated_at: "2026-09-24T07:31:00Z",
      },
    });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "导入公式" }).click();
  const dialog = page.getByRole("dialog", { name: "公式预览" });
  await dialog.getByRole("textbox", { name: "通达信公式" }).fill("CLOSE>MA(CLOSE,2)");
  await dialog.getByRole("textbox", { name: "股票代码" }).fill("600001.SH");
  await expect(dialog.getByRole("button", { name: "预览这只股票" })).toBeDisabled();
  await dialog.getByRole("button", { name: "检查公式" }).click();
  await expect(dialog.getByText("公式检查通过，可预览或批量运行。")).toBeVisible();
  await dialog.getByRole("button", { name: "预览这只股票" }).click();
  await expect(dialog.getByRole("status")).toContainText("符合");
  expect(await dialog.innerText()).not.toContain(identity);
  expect(findJargon(await dialog.innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "formula preview desktop");
  await page.screenshot({ path: testInfo.outputPath("formula-preview-desktop.png") });

  await dialog.getByRole("button", { name: "关闭" }).click();
  await expect(dialog).toBeHidden();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "导入公式" }).click();
  await expect(dialog).toBeVisible();
  await dialog.getByRole("textbox", { name: "通达信公式" }).fill("CLOSE>MA(CLOSE,2)");
  await dialog.getByRole("textbox", { name: "股票代码" }).fill("600001.SH");
  await dialog.getByRole("button", { name: "检查公式" }).click();
  await dialog.getByRole("button", { name: "预览这只股票" }).click();
  await expect(dialog.getByRole("status")).toContainText("符合");
  await expectNoHorizontalOverflow(page, "formula preview phone");
  await page.screenshot({ path: testInfo.outputPath("formula-preview-phone.png") });
  identity = "b".repeat(64);
  await dialog.getByRole("button", { name: "刷新公式预览数据" }).click();
  await expect(dialog.getByRole("status")).toContainText("公式预览数据已更新，请重新预览");
  await dialog.getByRole("button", { name: "预览这只股票" }).click();
  await expect(dialog.getByRole("status")).toContainText("符合");
  expect(watcher.problems).toEqual([]);
});

test("中文条件筛选、翻页和个股详情在桌面与手机宽度可用", async ({ page }) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await expect(page.getByRole("heading", { level: 1, name: "选股器" })).toBeVisible();
  await expect(page.getByRole("combobox", { name: "条件目录" })).toBeVisible();

  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.getByRole("region", { name: "逐条命中" })).toContainText("排除 ST");
  const table = page.getByRole("table", { name: "选股结果" });
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(20);
  await table.locator("tbody tr:not(.pad)").first().click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "关闭" }).click();

  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.locator(".screen-pages .hint")).toContainText("第 2 页");
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(7);
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expectNoHorizontalOverflow(page, "screener desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("heading", { level: 1, name: "选股器" })).toBeVisible();
  await expect(table).toBeVisible();
  await expectNoHorizontalOverflow(page, "screener phone");
  expect(watcher.problems).toEqual([]);
});

test("一句话建议经键盘预览和人工应用，手机手改后才运行真实筛选", async ({ page }, testInfo) => {
  const watcher = watch(page);
  const previews: Schemas["ScreenNlPreviewRequest"][] = [];
  const runs: Execute[] = [];
  let sourceIdentity: string | null = null;
  await page.route(/\/api\/v1\/screen\/blocks\?mode=daily$/, async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.nl_generate_available = true;
    sourceIdentity = body.data.source?.identity ?? null;
    await route.fulfill({ response, json: body });
  });
  await page.route("**/api/v1/screen/nl-preview", async (route) => {
    const request = route.request().postDataJSON() as Schemas["ScreenNlPreviewRequest"];
    expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
    expect(request.source_identity).toBe(sourceIdentity);
    previews.push(request);
    await route.fulfill({
      status: 200,
      json: {
        source_kind: request.source_kind,
        source_identity: request.source_identity,
        trade_date: request.trade_date,
        conditions: [
          { key: "not_st", args: {} },
          { key: "circ_mv_lt", args: { threshold_yi: 80 } },
        ],
      } satisfies Schemas["ScreenNlPreviewData"],
    });
  });
  await screenCommands(page, (request) => {
    expect(request.definition.source_identity).toBe(sourceIdentity);
    runs.push(request);
    return screenResult(request);
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  const description = page.getByRole("textbox", { name: "选股描述" });
  await expect(description).toHaveAttribute("maxlength", "500");
  await description.focus();
  await page.keyboard.type("排除 ST，流通市值低于 80 亿");
  await page.keyboard.press("Tab");
  await expect(page.getByRole("button", { name: "生成条件" })).toBeFocused();
  await page.keyboard.press("Enter");
  const preview = page.getByRole("region", { name: "建议条件" });
  await expect(preview).toContainText("排除 ST");
  await expect(preview).toContainText("市值上限（亿元） 80");
  await expect(preview).not.toContainText("命中");
  const recent = page.getByRole("region", { name: "最近描述" }).getByRole("button", {
    name: "排除 ST，流通市值低于 80 亿",
  });
  await expect(recent).toBeVisible();
  expect(previews).toHaveLength(1);
  expect(runs).toHaveLength(1);
  await expectNoHorizontalOverflow(page, "screen suggestion desktop");
  await page.screenshot({ path: testInfo.outputPath("screen-suggestion-desktop.png") });

  await preview.getByRole("button", { name: "应用到条件" }).click();
  await expect(page.getByRole("spinbutton", { name: "市值上限（亿元）" })).toHaveValue("80");
  await expect(page.getByText(/条件已改，请重新运行/)).toBeVisible();
  expect(runs).toHaveLength(1);
  await page.setViewportSize({ width: 390, height: 844 });
  await expectNoHorizontalOverflow(page, "screen suggestion applied phone");
  await page.screenshot({ path: testInfo.outputPath("screen-suggestion-phone.png") });
  await page.getByRole("spinbutton", { name: "市值上限（亿元）" }).fill("90");
  await expect(page.getByRole("button", { name: "撤销应用" })).toHaveCount(0);
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect.poll(() => runs.length).toBe(2);
  expect(runs[1]?.definition.conditions).toEqual([
    { name: "not_st", args: {} },
    { name: "circ_mv_lt", args: { threshold_yi: 90, offset: 0 } },
  ]);
  await expect(page.getByText(/条件已改，请重新运行/)).toHaveCount(0);
  await description.fill("下一次想看的股票");
  await recent.focus();
  await page.keyboard.press("Enter");
  await expect(description).toHaveValue("排除 ST，流通市值低于 80 亿");
  await expect(description).toBeFocused();
  expect(previews).toHaveLength(1);
  expect(runs).toHaveLength(2);
  await expectNoHorizontalOverflow(page, "screen recent descriptions phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("自定义 RSI 周期和偏移在桌面与手机可输入并提交", async ({ page }) => {
  const watcher = watch(page);
  const requests: Execute[] = [];
  let fulfilled = 0;
  const source: Schemas["ScreenSourceInfo"] = {
    mode: "daily",
    identity: "a".repeat(64),
    updated_at: "2026-09-24T07:31:00Z",
  };
  await page.route(/\/api\/v1\/screen\/blocks\?mode=daily$/, async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "replica";
    body.data.source = source;
    const rsi = body.data.blocks.find((block) => block.key === "rsi_oversold");
    const period = rsi?.parameters.find((parameter) => parameter.key === "period");
    if (!period) throw new Error("RSI period missing from fixture");
    period.label = "RSI 周期（日）";
    period.input = "integer";
    period.initial = 14;
    period.minimum = 2;
    period.maximum = 60;
    period.options = [];
    period.hint = "可填 2–60 个交易日";
    await route.fulfill({ response, json: body });
  });
  await screenCommands(page, (request) => {
    requests.push(request);
    expect(request.definition.source_identity).toBe(source.identity);
    fulfilled += 1;
    return screenResult(request, { source });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await page.getByRole("combobox", { name: "条件目录" }).selectOption("rsi_oversold");
  await page.getByRole("button", { name: "添加条件" }).click();
  const period = page.getByRole("spinbutton", { name: "RSI 周期（日）" });
  await expect(period).toHaveAttribute("max", "60");
  await period.fill("7");
  await page.getByRole("spinbutton", { name: "相对日期" }).fill("30");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect.poll(() => requests.length).toBe(1);
  expect(requests[0]?.definition.conditions[1]).toEqual({
    name: "rsi_oversold",
    args: { period: 7, threshold: 30, offset: 30 },
  });
  await expectNoHorizontalOverflow(page, "custom RSI desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(period).toBeVisible();
  await period.fill("14");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect.poll(() => requests.length).toBe(2);
  await expect.poll(() => fulfilled).toBe(2);
  expect(requests[1]?.definition.conditions[1]?.args?.period).toBe(14);
  await expectNoHorizontalOverflow(page, "custom RSI phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("排名条件可编辑、折算并按分数稳定翻页，手机上可修改前 N", async ({ page }) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await expect(page.getByRole("button", { name: "添加排名" })).toBeVisible();
  await expect(page.getByRole("option", { name: "20 日涨幅" })).toHaveCount(0);

  await page.getByRole("button", { name: "添加排名" }).click();
  await page.getByRole("combobox", { name: "第 1 项指标" }).selectOption("PCT_CHG[0]");
  await page.getByRole("button", { name: "添加排名" }).click();
  await page.getByRole("spinbutton", { name: "第 1 项权重" }).fill("60");
  await page.getByRole("spinbutton", { name: "第 2 项权重" }).fill("30");
  await page.getByRole("spinbutton", { name: "取前 N 只" }).fill("25");
  await expect(page.getByText(/权重合计 90%/)).toContainText("按比例折算为 100%");
  await page.getByRole("button", { name: "运行筛选" }).click();

  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.getByText("按排名分展示前 25 只")).toBeVisible();
  const table = page.getByRole("table", { name: "选股结果" });
  await expect(table.getByRole("columnheader", { name: "名次" })).toBeVisible();
  await expect(table.getByRole("columnheader", { name: "排名分" })).toBeVisible();
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(20);
  await page.getByRole("button", { name: "下一页" }).click();
  await expect(page.locator(".screen-pages .hint")).toContainText("第 2 页");
  await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(5);
  await expectNoHorizontalOverflow(page, "ranked screener desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("spinbutton", { name: "取前 N 只" })).toBeVisible();
  await page.getByRole("spinbutton", { name: "取前 N 只" }).fill("24");
  await expect(page.getByRole("status")).toContainText("条件已改，请重新运行");
  await expect(page.getByRole("button", { name: "下一页" })).toBeDisabled();
  await expectNoHorizontalOverflow(page, "ranked screener phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("手机宽度明确展示未判定股票，不把未知写成零命中", async ({ page }) => {
  const watcher = watch(page);
  await screenCommands(page, (request) =>
    screenResult(request, {
      total: 0,
      unknown_count: 1,
      steps: [{ label: "排除 ST", count: 0, unknown_count: 1 }],
      rows: [],
    }),
  );
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("未判定 1 只")).toBeVisible();
  await expect(page.getByRole("region", { name: "逐条命中" })).toContainText("未知 1 只");
  await expect(page.getByText("没有命中股票")).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "screen unknown phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

test("选股来源独立换代后保留条件，失效时桌面与手机都要求重试", async ({ page }, testInfo) => {
  const watcher = watch(page);
  let identity = "a".repeat(64);
  let unavailable = false;
  let catalogReads = 0;
  await page.route(/\/api\/v1\/screen\/blocks\?mode=daily$/, async (route) => {
    catalogReads += 1;
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "replica";
    body.data.source = unavailable
      ? null
      : { mode: "daily", identity, updated_at: "2026-09-24T07:31:00Z" };
    body.data.available = !unavailable;
    if (unavailable) body.data.dates = [];
    await route.fulfill({ response, json: body });
  });
  await screenCommands(page, (request) => {
    expect(request.definition.source_identity).toBe(identity);
    return screenResult(request);
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.clock.install();
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.locator(".screen-source")).toContainText("选股数据");
  expect(await page.locator("main").innerText()).not.toContain(identity);
  await page.screenshot({ path: testInfo.outputPath("screen-source-desktop.png") });

  await page.clock.fastForward(31_000);
  await page.waitForTimeout(100);
  expect(catalogReads).toBe(1);

  identity = "b".repeat(64);
  await page.getByRole("button", { name: "刷新选股数据" }).click();
  await expect.poll(() => catalogReads).toBe(2);
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expect(page.getByRole("table", { name: "选股结果" })).toHaveCount(0);
  await expect(page.getByRole("combobox", { name: "条件目录" })).toHaveValue("not_st");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await expect(page.getByRole("status")).toHaveCount(0);

  unavailable = true;
  await page.getByRole("button", { name: "刷新选股数据" }).click();
  await expect.poll(() => catalogReads).toBe(3);
  await expect(page.getByText("选股数据暂不可用")).toBeVisible();
  await expect(page.getByRole("button", { name: "运行筛选" })).toBeDisabled();
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expectNoHorizontalOverflow(page, "screen source unavailable phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(await page.locator("main").innerText()).not.toContain(identity);
  await page.screenshot({ path: testInfo.outputPath("screen-source-phone.png") });
  expect(watcher.problems).toEqual([]);
});

test("基本面条件在桌面和手机按单位输入，来源更新会清掉旧结果", async ({ page }) => {
  const watcher = watch(page);
  let identity = "a".repeat(64);
  const requests: Execute[] = [];
  await page.route(/\/api\/v1\/screen\/blocks\?mode=daily$/, async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "replica";
    body.data.source = { mode: "daily", identity, updated_at: "2026-09-24T07:31:00Z" };
    body.data.available = true;
    const options = [
      { value: "PE_TTM[0]", label: "市盈率（倍）" },
      { value: "ROE[0]", label: "净资产收益率（%）" },
    ];
    for (const block of body.data.blocks.filter((item) =>
      ["gt", "lt", "gte", "lte", "between"].includes(item.key),
    )) {
      for (const parameter of block.parameters.filter((item) =>
        ["left", "right", "field"].includes(item.key),
      )) {
        parameter.options = [...(parameter.options ?? []), ...options];
      }
    }
    await route.fulfill({ response, json: body });
  });
  await screenCommands(page, (body) => {
    requests.push(body);
    return screenResult(body, {
      base_count: 2,
      total: 1,
      unknown_count: 1,
      steps: [{ label: "基本面条件", count: 1, unknown_count: 1 }],
      rows: [{ ts_code: "600001.SH", name: "样本01", close: 11, pct_chg: 1 }],
    });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await page.getByRole("combobox", { name: "条件目录" }).selectOption("gt");
  await page.getByRole("button", { name: "添加条件" }).click();
  await page.getByRole("combobox", { name: "左侧" }).selectOption("PE_TTM[0]");
  await page.getByRole("combobox", { name: "右侧" }).selectOption("__number__");
  const peLimit = page.getByRole("spinbutton", { name: "右侧数值（倍）" });
  await expect(peLimit).toBeVisible();
  await peLimit.fill("9");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 1 只", { exact: false })).toBeVisible();
  expect(requests[0]?.definition.source_identity).toBe(identity);
  expect(requests[0]?.definition.conditions[1]).toEqual({
    name: "gt",
    args: { left: "PE_TTM[0]", right: 9 },
  });
  await expectNoHorizontalOverflow(page, "fundamental screen desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("combobox", { name: "条件目录" }).selectOption("between");
  await page.getByRole("button", { name: "添加条件" }).click();
  await page.getByRole("combobox", { name: "比较项" }).selectOption("ROE[0]");
  await page.getByRole("spinbutton", { name: "下限（%）" }).fill("8");
  await page.getByRole("spinbutton", { name: "上限（%）" }).fill("15");
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect.poll(() => requests.length).toBe(2);
  expect(requests[1]?.definition.conditions.at(-1)).toEqual({
    name: "between",
    args: { field: "ROE[0]", low: 8, high: 15 },
  });
  await expect(page.getByText(/未判定 1 只/)).toBeVisible();
  await expectNoHorizontalOverflow(page, "fundamental screen phone");

  identity = "c".repeat(64);
  await page.getByRole("button", { name: "刷新选股数据" }).click();
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expect(page.getByRole("table", { name: "选股结果" })).toHaveCount(0);
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(watcher.problems).toEqual([]);
});

function isScreenHistoryResponse(response: Response): boolean {
  return (
    response.request().method() === "GET" &&
    new URL(response.url()).pathname.endsWith("/api/v1/screen/query/history")
  );
}

async function advanceServingClock(page: Page): Promise<void> {
  const isMeta = (response: Response) =>
    response.request().method() === "GET" &&
    new URL(response.url()).pathname.endsWith("/api/v1/meta");
  const firstMeta = page.waitForResponse(isMeta);
  const refreshedHistory = page.waitForResponse(isScreenHistoryResponse);
  await page.clock.runFor(15_000);
  await (await firstMeta).finished();
  await (await refreshedHistory).finished();
  const secondMeta = page.waitForResponse(isMeta);
  await page.clock.runFor(16_000);
  await (await secondMeta).finished();
}

test("默认 Serving 换代重取选股目录，并要求旧结果重新筛选", async ({ page }) => {
  const watcher = watch(page);
  let generationId = "a".repeat(64);
  let catalogReads = 0;
  await page.route("**/api/v1/meta", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_MetaData_"];
    if (body.data.generation) body.data.generation.generation_id = generationId;
    body.serving.generation_id = generationId;
    await route.fulfill({ response, json: body });
  });
  await page.route(/\/api\/v1\/screen\/blocks\?mode=daily$/, async (route) => {
    catalogReads += 1;
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    body.data.source_kind = "serving";
    body.data.source = {
      mode: "daily",
      identity: generationId,
      updated_at: "2026-09-24T07:31:00Z",
    };
    await route.fulfill({ response, json: body });
  });
  await screenCommands(page, (request) => {
    expect(request.definition.source_identity).toBe(generationId);
    return screenResult(request);
  });

  await page.clock.install();
  const initialHistory = page.waitForResponse(isScreenHistoryResponse);
  await page.goto("./#/screener");
  await (await initialHistory).finished();
  const executedHistory = page.waitForResponse(isScreenHistoryResponse);
  await page.getByRole("button", { name: "运行筛选" }).click();
  await expect(page.getByText("命中 27 只")).toBeVisible();
  await (await executedHistory).finished();
  expect(catalogReads).toBe(1);
  generationId = "b".repeat(64);
  await advanceServingClock(page);
  await expect.poll(() => catalogReads).toBe(2);
  await expect(page.getByRole("status")).toContainText("选股数据已更新，请重新筛选");
  await expect(page.getByRole("table", { name: "选股结果" })).toHaveCount(0);
  await expect(page.getByRole("combobox", { name: "条件目录" })).toHaveValue("not_st");
  expect(watcher.problems).toEqual([]);
});

test("首次 Serving 数据代到来后，无需手动刷新即可运行选股", async ({ page }) => {
  const watcher = watch(page);
  let generationId: string | null = null;
  let catalogReads = 0;
  await page.route("**/api/v1/meta", async (route) => {
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_MetaData_"];
    if (generationId === null) {
      body.data.generation = null;
      body.serving.generation_id = null;
      body.serving.state = "unavailable";
    } else {
      if (body.data.generation) body.data.generation.generation_id = generationId;
      body.serving.generation_id = generationId;
    }
    await route.fulfill({ response, json: body });
  });
  await page.route(/\/api\/v1\/screen\/blocks\?mode=daily$/, async (route) => {
    catalogReads += 1;
    const response = await route.fetch();
    const body = (await response.json()) as Schemas["Envelope_ScreenCatalogData_"];
    if (generationId === null) {
      body.data.available = false;
      body.data.dates = [];
      body.data.source = null;
    } else {
      body.data.source = {
        mode: "daily",
        identity: generationId,
        updated_at: "2026-09-24T07:31:00Z",
      };
    }
    await route.fulfill({ response, json: body });
  });

  await page.clock.install();
  const initialHistory = page.waitForResponse(isScreenHistoryResponse);
  await page.goto("./#/screener");
  await (await initialHistory).finished();
  await expect(page.getByText("选股数据暂不可用")).toBeVisible();
  await expect(page.getByRole("button", { name: "运行筛选" })).toBeDisabled();
  expect(catalogReads).toBe(1);
  generationId = "a".repeat(64);
  await advanceServingClock(page);
  await expect.poll(() => catalogReads).toBe(2);
  await expect(page.getByRole("button", { name: "运行筛选" })).toBeEnabled();
  expect(watcher.problems).toEqual([]);
});
