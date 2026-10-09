import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import type { ExperimentWrite } from "../src/api/experiments.ts";
import { experimentFixture as fixture } from "../src/pages/experiments/formal.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

// All numbers are the actual typed responses from synthetic original sealed
// artifacts. HTTP state changes here prove UI behavior, never execution or gold.
async function syntheticApi(
  page: Page,
  baseURL: string | undefined,
  options: { unknownOnce?: boolean; queued?: boolean; pauseNote?: boolean } = {},
) {
  if (!baseURL) throw new Error("browser proof requires the actual configured baseURL");
  const origin = new URL(baseURL).origin;
  const problems: string[] = [];
  const expectedConflicts = new Set<string>();
  const observedConflicts: string[] = [];
  const commands: ExperimentWrite[] = [];
  const original = fixture.family.data;
  let note = original.note;
  let noteVersion = original.note_version;
  let outer = original.outer_admitted;
  let cancelled = false;
  let mixed = false;
  let releaseNote: (() => void) | null = null;
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() !== "error") return;
    if (
      message.text() ===
        "Failed to load resource: the server responded with a status of 409 (Conflict)" &&
      expectedConflicts.has(message.location().url)
    ) {
      observedConflicts.push(message.location().url);
      return;
    }
    problems.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", (request) => problems.push(`request failed: ${request.url()}`));
  page.on("request", (request) => {
    const url = request.url();
    if (!url.startsWith(`${origin}/`) && !url.startsWith("data:"))
      problems.push(`external request: ${url}`);
  });
  await page.clock.setFixedTime(new Date("2026-10-05T08:00:05Z"));
  const items = () =>
    original.items.map((item) =>
      options.queued
        ? {
            ...item,
            result_hash: null,
            status: cancelled ? ("cancelled" as const) : ("registered" as const),
            label: cancelled ? "已取消" : "已登记",
          }
        : item,
    );
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const respond = (value: unknown) => route.fulfill({ status: 200, json: value });
    const base = "/app/api/v1/experiments";
    const familyPath = `${base}/families/${encodeURIComponent(original.family_id)}`;
    if (url.pathname === "/app/api/v1/meta")
      return respond(metaEnvelope({ viewer: "alice", generationId: fixture.generation_id }));
    if (url.pathname === `${base}/capabilities`) return respond(fixture.capabilities);
    if (url.pathname === `${base}/mine`)
      return respond({ ...fixture.mine, data: { ...fixture.mine.data, items: items() } });
    if (url.pathname === familyPath)
      return respond({
        ...fixture.family,
        data: {
          ...original,
          items: items(),
          note,
          note_version: noteVersion,
          outer_admitted: outer,
          cancelled_count: cancelled ? original.planned_count : 0,
        },
      });
    const selected = original.items.find(
      (item) =>
        url.pathname === `${base}/results/${item.experiment_id}` ||
        url.pathname === `${base}/results/${item.experiment_id}/statistics`,
    );
    if (selected) {
      expect(url.searchParams.get("generation_id")).toBe(fixture.generation_id);
      if (mixed) {
        expectedConflicts.add(url.href);
        return route.fulfill({ status: 409, json: { detail: "数据已更新" } });
      }
      if (url.pathname.endsWith("/statistics"))
        return respond(fixture.statistics[selected.experiment_id]);
      expect(url.searchParams.get("result_hash")).toBe(selected.result_hash);
      return respond(fixture.results[selected.experiment_id]);
    }
    if (url.pathname === `${familyPath}/heatmap`) {
      expect(url.searchParams.get("x")).toBe("weight_rule.max_positions");
      expect(url.searchParams.get("y")).toBe("weight_rule.cash_reserve");
      expect(url.searchParams.get("generation_id")).toBe(fixture.generation_id);
      return respond(fixture.heatmap);
    }
    if (url.pathname === `${base}/compare`) {
      expect([url.searchParams.get("a"), url.searchParams.get("b")]).toEqual(
        original.items.slice(0, 2).map((item) => item.experiment_id),
      );
      return respond(fixture.comparison);
    }
    if (url.pathname === `${base}/commands` && request.method() === "POST") {
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      const body = request.postDataJSON() as ExperimentWrite;
      expect(body).not.toHaveProperty("actor_id");
      commands.push(body);
      const common = {
        command_id: body.command_id,
        family_id: original.family_id,
        job_ids: [],
        planned_count: null,
        version: null,
      };
      if (options.unknownOnce && commands.length === 1)
        return respond({ ...common, status: "unknown", message: "提交状态待确认，请重试原请求。" });
      let receipt: Schemas["ExperimentWriteReceipt"];
      if (body.kind === "set_experiment_note") {
        expect(body.expected_version).toBe(noteVersion);
        note = body.text;
        noteVersion += 1;
        if (options.pauseNote && noteVersion === 1)
          await new Promise<void>((resolve) => {
            releaseNote = resolve;
          });
        receipt = {
          ...common,
          status: "note_saved",
          message: "备注已保存。",
          version: noteVersion,
        };
      } else if (body.kind === "unseal_experiment_outer_test") {
        expect(outer).toBe(false);
        expect(body.result_hash).toBe(original.items[0]?.result_hash);
        expect(body.experiment_id).toBe(original.items[0]?.experiment_id);
        expect(body.confirmed).toBe(true);
        outer = true;
        receipt = {
          ...common,
          status: "outer_admitted",
          message: "已准入样本外，等待运行。",
          planned_count: 1,
        };
      } else if (body.kind === "cancel_experiment_family") {
        cancelled = true;
        receipt = {
          ...common,
          status: "cancelled",
          message: "未完成项已取消。",
          planned_count: original.planned_count,
        };
      } else if (body.kind === "set_experiment_holdout_policy") {
        receipt = { ...common, status: "policy_saved", message: "设置已保存。", version: 2 };
      } else {
        const request = body.request;
        if (!("base_config" in request)) throw new Error("期望组合策略实验请求");
        const count = request.method === "random" ? request.random_count : 4;
        receipt = {
          ...common,
          status: "registered",
          message: "已登记，等待运行。",
          planned_count: count,
          job_ids: original.items
            .slice(0, count)
            .map((item) => item.job_id)
            .filter((id): id is string => id !== null),
        };
      }
      return respond(receipt);
    }
    problems.push(`unexpected API: ${request.method()} ${url.pathname}`);
    return route.fulfill({ status: 404, json: { detail: "unexpected synthetic endpoint" } });
  });
  return {
    problems,
    commands,
    observedConflicts,
    mix: () => {
      mixed = true;
    },
    release: () => releaseNote?.(),
    noteWaiting: () => releaseNote !== null,
  };
}

test("完整结果、热图、两份对比、备注迟回执和一次解封", async ({ page, baseURL }, info) => {
  const api = await syntheticApi(page, baseURL, { pauseNote: true });
  await page.goto("./#/experiments");
  const first = page.getByRole("button", { name: "仓位实验 · 1", exact: true });
  await expect(first).toBeVisible();
  expect(findJargon(await page.locator("body").innerText())).toEqual([]);
  await first.click();
  const drawer = page.getByRole("dialog", { name: "仓位实验", exact: true });
  const nav = drawer.getByRole("img", { name: "实验与基准净值" });
  await expect(nav.locator("canvas")).toHaveCount(1);
  const chartBox = await nav.boundingBox();
  expect(chartBox?.width).toBeGreaterThan(250);
  expect(chartBox?.height).toBeGreaterThan(100);
  await drawer.locator("details summary").click();
  await expect(drawer.getByRole("table", { name: "实验1逐日净值" })).toBeVisible();
  await expect(drawer.getByLabel("热图横轴").getByRole("option")).toHaveText([
    "最多持仓",
    "现金保留",
  ]);
  const cells = drawer.getByRole("table", { name: "参数热力图" }).getByRole("button");
  await expect(cells).toHaveCount(4);
  await cells.nth(0).focus();
  await page.keyboard.press("ArrowRight");
  await expect(cells.nth(1)).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(drawer.getByRole("heading", { name: "第 2 项 · 完整结果" })).toBeVisible();
  await expect(drawer.getByText("邻域 3 / 3 格")).toBeVisible();
  const reason = drawer.getByText("暂不可计算 · 查看原因", { exact: true }).first().locator("..");
  if (info.project.name === "phone") await reason.tap();
  else await reason.focus();
  await expect(page.getByRole("tooltip")).toBeVisible();
  await drawer.getByLabel("研究备注").fill("第一稿");
  await drawer.getByRole("button", { name: "保存备注" }).click();
  await expect.poll(api.noteWaiting).toBe(true);
  await drawer.getByLabel("研究备注").fill("继续研究的新稿");
  api.release();
  await expect(drawer.getByRole("button", { name: "保存备注" })).toBeEnabled();
  await expect(drawer.getByLabel("研究备注")).toHaveValue("继续研究的新稿");
  await drawer.getByRole("button", { name: "保存备注" }).click();
  await expect(drawer.getByRole("button", { name: "保存备注" })).toBeDisabled();
  expect(api.commands.filter((body) => body.kind === "set_experiment_note")).toMatchObject([
    { expected_version: 0, text: "第一稿" },
    { expected_version: 1, text: "继续研究的新稿" },
  ]);
  await page.keyboard.press("Escape");
  await expect(first).toBeFocused();
  await first.click();
  const unseal = drawer.getByRole("button", { name: "解封样本外", exact: true });
  await unseal.click();
  let confirm = page.getByRole("dialog", { name: "解封样本外", exact: true });
  await expect(confirm.getByRole("button", { name: "确认解封" })).toBeDisabled();
  await confirm.getByRole("textbox").fill("别的实验");
  await expect(confirm.getByRole("button", { name: "确认解封" })).toBeDisabled();
  await confirm.getByRole("button", { name: /取\s*消/ }).click();
  await expect(unseal).toBeFocused();
  expect(api.commands.filter((body) => body.kind === "unseal_experiment_outer_test")).toEqual([]);
  await unseal.click();
  confirm = page.getByRole("dialog", { name: "解封样本外", exact: true });
  await confirm.getByRole("textbox").fill(fixture.family.data.name);
  await confirm.getByRole("button", { name: "确认解封" }).click();
  await expect(drawer.getByRole("button", { name: "已解封" })).toBeDisabled();
  await expect(confirm).toBeHidden();
  await expectNoHorizontalOverflow(page, `${info.project.name} result and confirmation`);
  await page.screenshot({ path: info.outputPath("experiment-full-result.png"), fullPage: true });
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();
  await expect(first).toBeFocused();
  const mine = page.getByRole("table", { name: "我的实验" });
  await mine.getByRole("checkbox", { name: "选择仓位实验第1项" }).check();
  await expect(mine.getByRole("checkbox", { name: "选择仓位实验第1项" })).toBeFocused();
  await mine.getByRole("checkbox", { name: "选择仓位实验第2项" }).check();
  await expect(mine.getByRole("checkbox", { name: "选择仓位实验第3项" })).toBeDisabled();
  await page.getByRole("button", { name: "对比所选" }).click();
  const compare = page.getByRole("img", { name: "两份实验净值" });
  await expect(compare.locator("canvas")).toHaveCount(1);
  await expect(page.getByRole("table", { name: "参数差异" })).toBeVisible();
  const articles = page.locator(".exp-compare-grid article");
  await expect(articles).toHaveCount(2);
  for (const [index, result] of [fixture.comparison.data.a, fixture.comparison.data.b].entries())
    for (const metric of result.metrics)
      await expect(articles.nth(index).getByText(metric.label, { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page, `${info.project.name} comparison`);
  await page.screenshot({ path: info.outputPath("experiment-comparison.png"), fullPage: true });
  expect(api.problems).toEqual([]);
});

test("新建网格与随机搜索、未知回执刷新核对和实际取消控件", async ({ page, baseURL }, info) => {
  const api = await syntheticApi(page, baseURL, { unknownOnce: true, queued: true });
  await page.goto("./#/experiments");
  const create = page.getByRole("button", { name: "新建实验" });
  await create.click();
  let form = page.getByRole("dialog", { name: "新建实验" });
  for (const label of ["训练开始", "训练结束", "验证开始", "验证结束", "样本外开始", "样本外结束"])
    await expect(form.getByLabel(label, { exact: true })).toBeVisible();
  await form.getByLabel("实验名称").fill("网格研究");
  await form.getByLabel("最多持仓范围").fill("2, 1");
  await form.getByRole("button", { name: "开始搜索" }).click();
  await expect(form.getByRole("alert")).toContainText("请检查参数范围");
  expect(api.commands).toEqual([]);
  await form.getByLabel("最多持仓范围").fill("1, 2");
  await form.getByRole("button", { name: "开始搜索" }).click();
  await page.keyboard.press("Escape");
  await expect(form).toBeHidden();
  const retry = page.getByRole("button", { name: "核对原请求" });
  await expect(retry).toBeFocused();
  expect(api.commands).toHaveLength(1);
  await page.reload();
  await page.getByRole("button", { name: "核对原请求" }).click();
  await expect.poll(() => api.commands.length).toBe(2);
  expect(api.commands[0]).toEqual(api.commands[1]);
  await expect(create).toBeEnabled();
  await create.click();
  form = page.getByRole("dialog", { name: "新建实验" });
  await form.getByLabel("实验名称").fill("随机研究");
  await form.getByLabel("搜索方式").selectOption("random");
  await form.getByLabel("抽取数量").fill("2");
  await form.getByLabel("随机种子").fill("19");
  await form.getByRole("button", { name: "开始搜索" }).click();
  await expect.poll(() => api.commands.length).toBe(3);
  expect(api.commands[2]).toMatchObject({
    request: { method: "random", random_count: 2, seed: 19 },
  });
  await page.getByRole("button", { name: "仓位实验 · 1", exact: true }).click();
  const detail = page.getByRole("dialog", { name: "仓位实验", exact: true });
  await detail.getByRole("button", { name: "取消未完成项" }).click();
  await expect(detail.getByText("取消 4", { exact: true })).toBeVisible();
  await expect(detail.getByRole("button", { name: "取消未完成项" })).toBeDisabled();
  await expect(detail.getByRole("button", { name: "解封样本外" })).toBeDisabled();
  await expectNoHorizontalOverflow(page, `${info.project.name} registration and cancellation`);
  await page.screenshot({ path: info.outputPath("experiment-cancelled.png"), fullPage: true });
  expect(api.problems).toEqual([]);
});

test("混代409撤下统计、曲线和选择", async ({ page, baseURL }) => {
  const api = await syntheticApi(page, baseURL);
  await page.goto("./#/experiments");
  api.mix();
  await page.getByRole("button", { name: "仓位实验 · 1", exact: true }).click();
  await expect(page.getByRole("alert")).toContainText("数据已更新");
  await expect(page.getByRole("img", { name: "实验与基准净值" })).toHaveCount(0);
  await expect(page.getByRole("table", { name: "我的实验" })).toHaveCount(0);
  expect(api.observedConflicts.length).toBeGreaterThan(0);
  expect(api.problems).toEqual([]);
});
