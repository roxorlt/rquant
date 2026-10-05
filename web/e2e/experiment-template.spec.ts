import { expect, type Page, test } from "@playwright/test";
import type { ExperimentWrite } from "../src/api/experiments.ts";
import { experimentFixture as fixture } from "../src/pages/experiments/formal.fixture.ts";
import { experimentTemplateFixture as template } from "../src/pages/experiments/template.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

// These constructed typed HTTP responses check UI behavior. Original template
// execution, full result binding and statistics are checked by the native proof.
async function api(page: Page, baseURL: string | undefined, preparing = false) {
  if (!baseURL) throw new Error("browser proof needs its configured local baseURL");
  const origin = new URL(baseURL).origin;
  const problems: string[] = [];
  const commands: ExperimentWrite[] = [];
  let cancelled = false;
  const original = fixture.family.data;
  const familyPath = `/app/api/v1/experiments/families/${encodeURIComponent(original.family_id)}`;
  page.on("pageerror", (error) => problems.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") problems.push(message.text());
  });
  page.on("requestfailed", (request) => problems.push(`failed ${request.url()}`));
  page.on("request", (request) => {
    if (!request.url().startsWith(`${origin}/`) && !request.url().startsWith("data:"))
      problems.push(`external ${request.url()}`);
  });
  await page.clock.setFixedTime(new Date("2026-10-05T08:00:05Z"));
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const respond = (value: unknown) => route.fulfill({ status: 200, json: value });
    if (url.pathname === "/app/api/v1/meta")
      return respond(metaEnvelope({ viewer: "alice", generationId: fixture.generation_id }));
    if (url.pathname === "/app/api/v1/strategy-templates") return respond(template.catalog);
    if (url.pathname === "/app/api/v1/strategy-templates/sources") return respond(template.sources);
    if (
      url.pathname === `/app/api/v1/strategy-templates/${template.detail.data.strategy_id}/versions`
    )
      return respond(template.versions);
    if (url.pathname === `/app/api/v1/strategy-templates/${template.detail.data.strategy_id}`)
      return respond(url.searchParams.get("version") === "1" ? template.detail : template.latest);
    if (url.pathname === "/app/api/v1/experiments/capabilities")
      return respond(template.capabilities);
    if (url.pathname === "/app/api/v1/experiments/mine")
      return respond(
        preparing
          ? {
              ...template.mine,
              data: {
                ...template.mine.data,
                preparing_families: template.mine.data.preparing_families.map((f) => ({
                  ...f,
                  state: cancelled ? "cancelled" : f.state,
                  cancelled_count: cancelled ? 3 : 0,
                })),
              },
            }
          : fixture.mine,
      );
    if (url.pathname === familyPath)
      return respond(
        preparing
          ? {
              ...template.family,
              data: {
                ...template.family.data,
                preparation_state: cancelled ? "cancelled" : "preparing",
                preparations: template.family.data.preparations.map((p) => ({
                  ...p,
                  definition_state:
                    cancelled && p.definition_state !== "failed" ? "cancelled" : p.definition_state,
                })),
                cancelled_count: cancelled ? 3 : 0,
              },
            }
          : fixture.family,
      );
    const selected = original.items.find(
      (item) =>
        url.pathname === `/app/api/v1/experiments/results/${item.experiment_id}` ||
        url.pathname === `/app/api/v1/experiments/results/${item.experiment_id}/statistics`,
    );
    if (selected) {
      expect(url.searchParams.get("generation_id")).toBe(fixture.generation_id);
      if (url.pathname.endsWith("/statistics"))
        return respond(fixture.statistics[selected.experiment_id]);
      const result = fixture.results[selected.experiment_id];
      if (!result) throw new Error("synthetic original result is missing");
      expect(url.searchParams.get("result_hash")).toBe(result.data.result_hash);
      return respond({
        ...result,
        data: {
          ...result.data,
          template: {
            strategy_id: template.detail.data.strategy_id,
            head: template.detail.data.head,
            content_hash: "b".repeat(64),
            rules: {
              ...template.detail.data.rules,
              weight_rule: result.data.configuration.weight_rule,
              rebalance_rule: result.data.configuration.rebalance_rule,
            },
          },
        },
      });
    }
    if (url.pathname === `${familyPath}/heatmap`) return respond(fixture.heatmap);
    if (url.pathname === "/app/api/v1/experiments/commands" && request.method() === "POST") {
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      const body = request.postDataJSON() as ExperimentWrite;
      expect(body).not.toHaveProperty("actor_id");
      commands.push(body);
      if (body.kind === "register_experiment_family") {
        expect(body.request.template).toEqual({
          strategy_id: template.detail.data.strategy_id,
          head: template.detail.data.head,
        });
        expect(body.request.base_config.weight_rule).toEqual(
          template.detail.data.rules.weight_rule,
        );
        expect(body.request.base_config.rebalance_rule).toEqual(
          template.detail.data.rules.rebalance_rule,
        );
        expect(body.request.template).not.toHaveProperty("rules");
        expect(body.request.template).not.toHaveProperty("owner_id");
        return respond({
          command_id: body.command_id,
          status: "registered",
          message: "实验已登记。",
          family_id: original.family_id,
          job_ids: original.items.map((i) => i.job_id),
          planned_count: 4,
          version: null,
        });
      }
      if (body.kind === "cancel_experiment_family" && preparing) {
        cancelled = true;
        return respond({
          command_id: body.command_id,
          status: "cancelled",
          message: "准备已取消。",
          family_id: original.family_id,
          job_ids: [],
          planned_count: 4,
          version: null,
        });
      }
    }
    problems.push(`unexpected ${request.method()} ${url.pathname}`);
    return route.fulfill({ status: 404, json: { detail: "unexpected synthetic endpoint" } });
  });
  return { problems, commands };
}

test("原模板版本、参数提交及完整规则结果", async ({ page, baseURL }, info) => {
  const proof = await api(page, baseURL);
  await page.goto("./#/experiments");
  await page.getByRole("button", { name: "新建实验" }).click();
  const create = page.getByRole("dialog", { name: "新建实验", exact: true });
  await create.getByLabel("实验执行方式").selectOption("template");
  await expect(create.getByRole("button", { name: "开始搜索" })).toBeDisabled();
  await create
    .getByLabel("实验策略", { exact: true })
    .selectOption(template.detail.data.strategy_id);
  await expect(create.getByText("6 个交易日", { exact: true })).toBeVisible();
  await create.getByLabel("实验策略版本").selectOption("1");
  await expect(create.getByText("5 个交易日", { exact: true })).toBeVisible();
  await create.getByLabel("实验名称").fill("规则参数研究");
  await create.getByRole("button", { name: "开始搜索" }).click();
  await expect(create).toBeHidden();
  expect(proof.commands).toHaveLength(1);
  await page.getByRole("button", { name: "仓位实验 · 1", exact: true }).click();
  const result = page.getByRole("dialog", { name: "仓位实验", exact: true });
  await expect(result.getByRole("img", { name: "实验与基准净值" })).toBeVisible();
  await expect(result.getByRole("region", { name: "完整策略规则" })).toBeVisible();
  await expect(result.getByText("5 个交易日", { exact: true })).toBeVisible();
  await expect(result.getByRole("heading", { name: "全区间指标" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "模板完整结果");
  expect(findJargon(await page.locator("body").innerText())).toEqual([]);
  await page.screenshot({ path: info.outputPath("template-full-result.png"), fullPage: true });
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "仓位实验 · 1", exact: true })).toBeFocused();
  expect(proof.problems).toEqual([]);
});

test("失败准备完整四项、取消及历史保留", async ({ page, baseURL }, info) => {
  const proof = await api(page, baseURL, true);
  await page.goto("./#/experiments");
  const preparing = page.getByRole("table", { name: "准备中的实验" });
  await expect(preparing.getByText("计划 4 次")).toBeVisible();
  await expect(preparing.getByText("已保存 2 · 已准备 1")).toBeVisible();
  await preparing.getByRole("button", { name: "退出规则实验" }).click();
  const drawer = page.getByRole("dialog", { name: "退出规则实验", exact: true });
  const rows = drawer.getByRole("table", { name: "完整准备清单" });
  await expect(rows.getByRole("row")).toHaveCount(5);
  await expect(rows.getByText("容量不足")).toBeVisible();
  await expect(drawer.getByRole("img", { name: "实验与基准净值" })).toHaveCount(0);
  await drawer.getByRole("button", { name: "取消未完成项" }).click();
  await expect(drawer.getByRole("heading", { name: "准备已取消" })).toBeVisible();
  await expect(rows.getByRole("row")).toHaveCount(5);
  await expect(drawer.getByRole("button", { name: "取消未完成项" })).toBeDisabled();
  await expectNoHorizontalOverflow(page, "模板准备取消");
  await page.screenshot({
    path: info.outputPath("template-preparation-cancelled.png"),
    fullPage: true,
  });
  await page.keyboard.press("Escape");
  await expect(preparing.getByRole("button", { name: "退出规则实验" })).toBeFocused();
  expect(proof.commands).toHaveLength(1);
  expect(proof.problems).toEqual([]);
});
