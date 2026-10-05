import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import {
  templateCatalog,
  templateDetail,
  templateEnvelope,
  templateGeneration,
  templateHead,
  templateId,
  templateSources,
} from "../src/pages/strategies/template.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

type Operation =
  | Schemas["SaveStrategyTemplate"]
  | Schemas["ArchiveStrategyTemplate"]
  | Schemas["RunStrategyTemplate"];
const base = "/app/api/v1/strategy-templates";
const newId = `template_${"9".repeat(32)}`;
const builtin: Schemas["StrategyCatalogItem"][] = [
  {
    strategy_id: "auction_gap",
    name: "集合竞价跳空",
    version: 1,
    registered_at: templateDetail.saved_at,
    parameters: [{ key: "min_gap_pct", label: "跳空幅度下限", display_value: "0%" }],
  },
  {
    strategy_id: "growth_board_surge",
    name: "科创及创业板放量",
    version: 1,
    registered_at: templateDetail.saved_at,
    parameters: [{ key: "allowed_boards", label: "适用板块", display_value: "创业板、科创板" }],
  },
  {
    strategy_id: "n_shape",
    name: "N 字形态",
    version: 1,
    registered_at: templateDetail.saved_at,
    parameters: [{ key: "expires_seconds", label: "信号有效期", display_value: "120 秒" }],
  },
];

async function fixture(page: Page, baseURL: string | undefined, uncertain = false) {
  if (baseURL === undefined) throw new Error("Strategy template browser fixture requires baseURL");
  const allowedOrigin = new URL(baseURL).origin;
  const problems: string[] = [];
  const operations: Operation[] = [];
  const retries: Operation[] = [];
  const versions: Schemas["StrategyTemplateDetailData"][] = [];
  let archived = false;
  let generation = templateGeneration;
  let viewer: string | null = "tester";
  let available = true;
  page.on("pageerror", (error) => problems.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") problems.push(message.text());
  });
  page.on("requestfailed", (request) => problems.push(`failed request: ${request.url()}`));
  page.on("request", (request) => {
    if (new URL(request.url()).origin !== allowedOrigin && !request.url().startsWith("data:"))
      problems.push(`external request: ${request.url()}`);
  });
  await page.clock.setFixedTime(new Date("2026-09-24T07:32:00Z"));
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const json = (data: unknown) => route.fulfill({ json: templateEnvelope(data, generation) });
    if (url.pathname === "/app/api/v1/meta")
      return route.fulfill({ json: metaEnvelope({ generationId: generation, viewer }) });
    if (url.pathname === "/app/api/v1/strategies")
      return json({ available: true, strategies: builtin });
    if (url.pathname === "/app/api/v1/health") return json({ available: false, units: [] });
    if (url.pathname.endsWith("/events")) {
      const data: Schemas["ResearchTaskEventsData"] = {
        generation_id: generation,
        state: "ready",
        note: "",
        updated_at: templateDetail.saved_at,
        truncated: false,
        events: [
          {
            event_id: 1,
            label: "已提交",
            status_label: "排队中",
            occurred_at: templateDetail.saved_at,
          },
        ],
      };
      return route.fulfill({ json: data });
    }
    if (request.method() === "POST") {
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      const body: Operation = request.postDataJSON();
      expect(Object.keys(body)).not.toContain("owner_id");
      expect(Object.keys(body)).not.toContain("actor");
      const resume = url.pathname.endsWith("/resume");
      if (resume) {
        retries.push(body);
        expect(body).toEqual(operations[0]);
      } else operations.push(body);
      if (uncertain && !resume)
        return json({
          command_id: body.command_id,
          status: "uncertain",
          message: "结果待确认，请继续查看。",
        });
      if (body.kind === "run_strategy_template")
        return json({
          command_id: body.command_id,
          status: "submitted",
          job_id: body.command_id,
          message: "回测已提交。",
        });
      if (body.kind === "save_strategy_template") {
        const head = {
          ...templateHead,
          version: versions.length + 1,
          record_hash: String(versions.length + 7)
            .repeat(64)
            .slice(0, 64),
        };
        const rates = body.rules.exit;
        const rules: Schemas["StrategyTemplate-Output"] = {
          ...body.rules,
          weight_rule: {
            ...body.rules.weight_rule,
            cash_reserve: String(body.rules.weight_rule.cash_reserve ?? 0),
            min_target_amount: String(body.rules.weight_rule.min_target_amount ?? 0),
            max_stock_weight: String(body.rules.weight_rule.max_stock_weight ?? 1),
            max_industry_weight:
              body.rules.weight_rule.max_industry_weight == null
                ? null
                : String(body.rules.weight_rule.max_industry_weight),
          },
          exit: {
            ...rates,
            stop_loss: rates?.stop_loss == null ? null : String(rates.stop_loss),
            take_profit: rates?.take_profit == null ? null : String(rates.take_profit),
            trailing_profit: rates?.trailing_profit == null ? null : String(rates.trailing_profit),
          },
        };
        const detail: Schemas["StrategyTemplateDetailData"] = {
          ...templateDetail,
          strategy_id: newId,
          name: body.name,
          head,
          current_head: head,
          rules,
          change_note: body.change_note ?? "",
          archived: false,
        };
        versions.push(detail);
        return json({
          command_id: body.command_id,
          status: "published",
          strategy_id: newId,
          head,
          current_head_updated: true,
          message: "已保存。",
        });
      }
      archived = true;
      return json({
        command_id: body.command_id,
        status: "published",
        strategy_id: newId,
        head: versions.at(-1)?.head ?? templateHead,
        current_head_updated: true,
        message: "已归档。",
      });
    }
    if (url.pathname === base) {
      const items = templateCatalog().templates;
      const newest = versions.at(-1);
      const newItem = newest ? templateCatalog(newest).templates[0] : undefined;
      if (newItem) items.push({ ...newItem, archived });
      return json({
        availability: available ? "populated" : "unavailable",
        available_at: templateDetail.saved_at,
        templates: available && viewer === "tester" ? items : [],
        can_create: available && viewer === "tester",
      });
    }
    if (url.pathname === `${base}/sources`)
      return json({
        ...templateSources,
        availability: available ? "populated" : "unavailable",
        can_create: available && viewer === "tester",
      });
    if (url.pathname.endsWith("/versions")) {
      const selected = url.pathname.includes(newId) ? versions : [templateDetail];
      const head = selected.at(-1)?.head ?? templateHead;
      return json({
        strategy_id: selected.at(-1)?.strategy_id ?? templateId,
        current_head: head,
        versions: selected.toReversed().map((detail) => ({
          head: detail.head,
          saved_at: detail.saved_at,
          change_note: detail.change_note,
          is_head: detail.head.version === head.version,
          latest_run: null,
        })),
        next_before_version: null,
      });
    }
    if (url.pathname === `${base}/${newId}`) {
      const newest = versions.at(-1);
      const selected =
        versions.find(
          (detail) => detail.head.version === Number(url.searchParams.get("version")),
        ) ?? newest;
      if (selected && newest)
        return json({
          ...selected,
          current_head: newest.head,
          archived,
          can_save: !archived,
          can_archive: !archived,
          can_run: !archived,
        });
    }
    if (url.pathname === `${base}/${templateId}`) return json(templateDetail);
    problems.push(`unhandled synthetic request: ${request.method()} ${url.pathname}`);
    return route.fulfill({ status: 404, json: { detail: "Unknown synthetic request" } });
  });
  return {
    problems,
    operations,
    retries,
    versions,
    setGeneration: (value: string) => {
      generation = value;
    },
    setViewer: (value: string | null) => {
      viewer = value;
    },
    setAvailable: (value: boolean) => {
      available = value;
    },
  };
}

test("新建完整规则、编辑不可变版本、历史版回测和归档", async ({ page, baseURL }, info) => {
  const state = await fixture(page, baseURL);
  await page.goto("./#/strategies");
  await expect(page.getByRole("table", { name: "策略列表" }).getByRole("row")).toHaveCount(4);
  const create = page.getByRole("button", { name: "新建策略", exact: true });
  await expect(create).toBeEnabled();
  await create.click();
  let drawer = page.getByRole("dialog", { name: "新建策略", exact: true });
  await drawer.getByLabel("策略名称").fill("每日观察");
  await drawer.getByLabel("入场方式").selectOption("pool");
  await drawer.getByRole("button", { name: "下一步", exact: true }).click();
  await drawer.getByRole("checkbox", { name: "止损", exact: true }).check();
  await drawer.getByLabel("止损幅度（%）").fill("100");
  await drawer.getByRole("button", { name: "下一步", exact: true }).click();
  await expect(drawer.getByRole("alert")).toContainText("止损和移动止盈须小于 100%。");
  await drawer.getByLabel("止损幅度（%）").fill("8.5");
  for (const label of ["止盈", "移动止盈", "持有上限"])
    await drawer.getByRole("checkbox", { name: label, exact: true }).check();
  await drawer.getByRole("checkbox", { name: /定时退出/ }).check();
  const tip = drawer.getByRole("img", { name: "定时退出说明" }).locator("..");
  if (info.project.name === "phone") await tip.tap();
  else await tip.focus();
  await expect(page.getByRole("tooltip")).toContainText("缺少价格");
  await expect(drawer.getByRole("checkbox", { name: "定时退出", exact: true })).toBeChecked();
  await drawer.getByLabel("退出时间").focus();
  await expectNoHorizontalOverflow(page, `${info.project.name} exits`);
  await drawer.getByRole("button", { name: "下一步", exact: true }).click();
  await drawer.getByLabel("分配方式").selectOption("rank_score");
  await drawer.getByLabel("调仓频率").selectOption("every_n");
  await drawer.getByLabel("调仓间隔（交易日）").fill("3");
  await drawer.getByRole("checkbox", { name: /指数过滤/ }).check();
  const indexTip = drawer.getByRole("img", { name: "指数过滤说明" }).locator("..");
  if (info.project.name === "phone") await indexTip.tap();
  else await indexTip.focus();
  await expect(drawer.getByRole("checkbox", { name: "指数过滤", exact: true })).toBeChecked();
  await drawer.getByLabel("指数代码").focus();
  await expectNoHorizontalOverflow(page, `${info.project.name} weights`);
  await drawer.getByRole("button", { name: "下一步", exact: true }).click();
  await expect(drawer.getByText("8.5%", { exact: true })).toBeVisible();
  await page.screenshot({ path: info.outputPath("template-full-rules.png"), fullPage: true });
  await drawer.getByRole("button", { name: "保存策略", exact: true }).click();
  await expect(page.getByText("已保存。", { exact: true })).toBeVisible();
  expect(state.operations[0]).toMatchObject({
    rules: {
      entry: { kind: "pool", pool_key: "my-pool", version: 2, body_hash: "5".repeat(64) },
      exit: { stop_loss: "0.085", exit_time: "14:50" },
      rebalance_rule: { kind: "every_n", every_n_days: 3 },
    },
  });
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await expect(create).toBeFocused();
  const row = page.getByRole("table", { name: "我的策略" }).getByRole("row", { name: /每日观察/ });
  await row.focus();
  await page.keyboard.press("Enter");
  drawer = page.getByRole("dialog", { name: "每日观察", exact: true });
  await drawer.getByRole("button", { name: "保存新版本", exact: true }).click();
  drawer = page.getByRole("dialog", { name: "保存新版本", exact: true });
  await drawer.getByLabel("改动说明").fill("调整观察范围");
  for (let step = 0; step < 3; step += 1)
    await drawer.getByRole("button", { name: "下一步", exact: true }).click();
  await drawer.getByRole("button", { name: "保存新版本", exact: true }).click();
  await expect.poll(() => state.versions.length).toBe(2);
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await expect(row).toBeFocused();
  await row.press("Enter");
  drawer = page.getByRole("dialog", { name: "每日观察", exact: true });
  await drawer.getByRole("button", { name: "第 1 版", exact: true }).click();
  await expect(drawer.getByText("第 1 版 · 历史版本", { exact: true })).toBeVisible();
  await drawer.getByRole("button", { name: "运行回测", exact: true }).click();
  drawer = page.getByRole("dialog", { name: "运行回测", exact: true });
  await drawer.getByLabel("开始日期").fill("2026-09-01");
  await drawer.getByLabel("结束日期").fill("2026-09-23");
  await drawer.getByRole("button", { name: "提交回测", exact: true }).click();
  await expect(page.getByText("回测已提交。", { exact: true })).toBeVisible();
  expect(state.operations.at(-1)).toMatchObject({
    head: state.versions[0]?.head,
    expected_head: state.versions[1]?.head,
    strategy_id: newId,
  });
  await page.keyboard.press("Escape");
  await expect(drawer).toHaveCount(0);
  await page.getByRole("button", { name: "查看回测进展", exact: true }).click();
  await expect(page.getByRole("dialog").getByText("排队中", { exact: true })).toBeVisible();
  await page.keyboard.press("Escape");
  await row.focus();
  await row.press("Enter");
  drawer = page.getByRole("dialog", { name: "每日观察", exact: true });
  await drawer.getByRole("button", { name: "归档策略", exact: true }).click();
  const confirmation = page.getByRole("dialog", { name: "归档策略", exact: true });
  await expect(confirmation).toBeVisible();
  await confirmation.getByRole("button", { name: "确认归档", exact: true }).click();
  await expect(page.getByText("已归档。", { exact: true })).toBeVisible();
  await expect(confirmation).toHaveCount(0);
  await expect(drawer.getByRole("button", { name: "运行回测", exact: true })).toBeDisabled();
  await expect(drawer.getByText("版本历史", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page, `${info.project.name} archived detail`);
  await page.screenshot({ path: info.outputPath("template-archived-history.png"), fullPage: true });
  await page.keyboard.press("Escape");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  expect(state.problems).toEqual([]);
});

test("未知回执刷新后按原请求恢复，换代保持原身份，换用户隐藏旧详情", async ({
  page,
  baseURL,
}, info) => {
  const state = await fixture(page, baseURL, true);
  await page.goto("./#/strategies");
  await page.getByRole("button", { name: "新建策略", exact: true }).click();
  const drawer = page.getByRole("dialog", { name: "新建策略", exact: true });
  await drawer.getByLabel("策略名称").fill("待确认观察");
  for (let step = 0; step < 3; step += 1)
    await drawer.getByRole("button", { name: "下一步", exact: true }).click();
  await drawer.getByRole("button", { name: "保存策略", exact: true }).click();
  await expect(page.getByText("结果待确认，请继续查看。", { exact: true })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(
    page.getByRole("button", { name: "新建策略", exact: true }).locator(".."),
  ).toBeFocused();
  state.setGeneration("b".repeat(64));
  await page.reload();
  await expect(page.getByRole("button", { name: "继续查看结果", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "继续查看结果", exact: true }).click();
  await expect(page.getByText("已保存。", { exact: true })).toBeVisible();
  expect(state.retries).toEqual([state.operations[0]]);
  expect(state.retries[0]?.generation_id).toBe(templateGeneration);
  await expectNoHorizontalOverflow(page, `${info.project.name} recovery`);
  await page.screenshot({
    path: info.outputPath("template-original-recovery.png"),
    fullPage: true,
  });
  state.setViewer(null);
  await page.reload();
  await expect(page.getByRole("table", { name: "我的策略" })).toHaveCount(0);
  await expect(page.getByText("待确认观察", { exact: true })).toHaveCount(0);
  expect(state.problems).toEqual([]);
});
