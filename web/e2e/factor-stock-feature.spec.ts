import { expect, type Locator, type Page, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { dailyCapability, dailyResearch } from "../src/pages/factors/factorDailyFields.fixture.ts";
import {
  diagnosticFactor,
  diagnosticResult,
} from "../src/pages/factors/factorDiagnostics.fixture.ts";
import {
  fullMixedStockResearch,
  stockCapability,
  stockResearch,
} from "../src/pages/factors/factorStockFeature.fixture.ts";
import { technicalResearch } from "../src/pages/factors/factorTechnicalHistory.fixture.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

async function expectTipInViewport(page: Page, tip: Locator): Promise<void> {
  await expect
    .poll(() =>
      tip.evaluate((element) => {
        let opacity = 1;
        for (let current: Element | null = element; current; current = current.parentElement)
          opacity *= Number(getComputedStyle(current).opacity);
        return opacity;
      }),
    )
    .toBe(1);
  const box = await tip.boundingBox();
  const viewport = page.viewportSize();
  expect(box).not.toBeNull();
  if (!box || !viewport) throw new Error("Tooltip viewport is required");
  expect(box.x).toBeGreaterThanOrEqual(0);
  expect(box.y).toBeGreaterThanOrEqual(0);
  expect(box.x + box.width).toBeLessThanOrEqual(viewport.width);
  expect(box.y + box.height).toBeLessThanOrEqual(viewport.height);
}

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`选股字段来源 ${viewport.label}`, () => {
    const phone = viewport.label === "phone";
    test.use({ hasTouch: phone, isMobile: phone });
    test("目录单位、窗口与缺因、切换和重载使用各自真实来源", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      let generation = metadata.serving.generation_id;
      let rawOnly = false;
      let wrongCapability = false;
      const serving = () => ({ ...metadata.serving, generation_id: generation });
      const current = diagnosticResult({ factor_version: 4 });
      const stock = diagnosticResult({ job_id: "3".repeat(32), factor_version: 3 });
      const technical = diagnosticResult({ job_id: "2".repeat(32), factor_version: 2 });
      const stored = diagnosticResult({ job_id: "1".repeat(32), factor_version: 1 });
      await page.route("**/api/v1/meta", (route) =>
        route.fulfill({
          json: {
            ...metadata,
            serving: serving(),
            data: {
              ...metadata.data,
              generation: metadata.data.generation
                ? { ...metadata.data.generation, generation_id: generation ?? "" }
                : null,
            },
          },
        }),
      );
      await page.route(/\/api\/v1\/factors\/definitions(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: serving().built_at,
              can_save: true,
              can_archive: true,
              definitions: [diagnosticFactor],
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorCatalogData_"],
        }),
      );
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: rawOnly
              ? { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) }
              : stockCapability,
            serving: wrongCapability ? { ...serving(), generation_id: "f".repeat(64) } : serving(),
          } satisfies Schemas["Envelope_FactorCapabilitiesData_"],
        }),
      );
      await page.route(
        new RegExp(`/api/v1/factors/${diagnosticFactor.factor_id}/tracking(?:\\?.*)?$`),
        (route) =>
          route.fulfill({
            json: {
              data: trackingPanel({
                availability: "unavailable",
                status: "unavailable",
                definition_head: null,
                can_set_tracked: false,
                reason: "跟踪数据尚未发布。",
              }),
              serving: serving(),
            } satisfies Schemas["Envelope_FactorTrackingPanel_"],
          }),
      );
      await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: serving().built_at,
              results: [current, stock, technical, stored],
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultListData_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/results\/[0-9a-f]{32}(?:\?.*)?$/, (route) => {
        const id = new URL(route.request().url()).pathname.split("/").at(-1);
        const result =
          [current, stock, technical, stored].find((row) => row.job_id === id) ?? current;
        const research =
          id === stored.job_id
            ? dailyResearch
            : id === technical.job_id
              ? technicalResearch
              : id === stock.job_id
                ? stockResearch
                : fullMixedStockResearch;
        return route.fulfill({
          json: {
            data: {
              can_report: false,
              availability: "ready",
              available_at: serving().built_at,
              result,
              research,
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultDetailData_"],
        });
      });
      await page.setViewportSize(viewport);
      await testInfo.attach("browser-context", {
        body: JSON.stringify({
          browser: page.context().browser()?.version(),
          viewport: page.viewportSize(),
          baseURL: APP_URL,
          navigator: await page.evaluate(() => ({
            userAgent: navigator.userAgent,
            maxTouchPoints: navigator.maxTouchPoints,
          })),
        }),
        contentType: "application/json",
      });
      await page.goto("./#/factors");
      await page.getByRole("button", { name: "编辑", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "编辑因子" });
      const search = dialog.getByRole("searchbox", { name: "搜索日线字段" });
      const expression = dialog.getByRole("textbox", { name: "表达式", exact: true });
      await dialog.getByRole("button", { name: "全部字段" }).click();
      await expect(dialog.getByRole("button", { name: /^插入/ })).toHaveCount(
        stockCapability.fields.length,
      );
      const show = async (anchor: Locator) => {
        if (phone) await anchor.tap();
        else await anchor.focus();
      };
      const hide = async (anchor: Locator) => {
        if (phone) await anchor.tap();
        else await anchor.evaluate((e: HTMLElement) => e.blur());
      };
      for (const [name, unit, description] of [
        ["90日实际观察数", "单位：观察数", "最近最多90个日线观察"],
        ["均线多头排列", "单位：0 / 1", "时为1，否则为0"],
        ["250日收盘百分位", "单位：比例", "取值0–1"],
        ["90日价格位置", "单位：百分数（%）", "平价为50%"],
      ]) {
        await search.fill(name ?? "");
        const info = dialog.getByRole("button", { name: `${name}说明` });
        await show(info);
        const tip = page.getByRole("tooltip").filter({ hasText: unit });
        await expect(tip).toBeVisible();
        await expect(tip).toContainText(description ?? "");
        await expect(expression).toHaveValue(diagnosticFactor.expression);
        await expectTipInViewport(page, tip);
        if (name === "均线多头排列")
          await page.screenshot({ path: testInfo.outputPath(`stock-unit-${viewport.label}.png`) });
        await hide(info);
        await expect(tip).toBeHidden();
      }
      await search.fill("均线多头排列");
      await expression.fill("close + ");
      await dialog.getByRole("button", { name: "插入均线多头排列" }).click();
      await expect(expression).toHaveValue("close + ma_alignment");
      await expect(expression).toBeFocused();
      await dialog.getByRole("button", { name: "关闭", exact: true }).last().click();
      const basis = page.getByText("日线字段口径", { exact: true });
      if (phone) await basis.tap();
      else await basis.hover();
      const sourceTip = page.getByRole("tooltip").filter({ hasText: "选股派生" });
      await expect(sourceTip).toBeVisible();
      await expect(sourceTip).toContainText("90 / 120 / 250 个实际观察");
      await expect(sourceTip).toContainText("吸筹窗口取参考日前 20 个观察");
      await expect(sourceTip).toContainText("历史推导：12项");
      await expect(sourceTip).toContainText("库存原值：4项");
      await expect(sourceTip).toContainText("历史断裂后不重新初始化");
      await expect(sourceTip).not.toContainText("指标价格基准与初始化未核验");
      await expectTipInViewport(page, sourceTip);
      await page.screenshot({
        path: testInfo.outputPath(`stock-mixed-source-${viewport.label}.png`),
      });
      if (phone) await basis.tap();
      else await page.getByRole("button", { name: "编辑", exact: true }).hover();
      await expect(sourceTip).toBeHidden();
      await page.getByText("查看字段覆盖", { exact: true }).click();
      const coverage = page.getByRole("table", { name: "日线字段覆盖" });
      const field = page.getByRole("combobox", { name: "覆盖字段" });
      await expect(field.locator("option")).toHaveCount(39);
      await field.selectOption("price_position_90d_pct");
      const valid = coverage.getByText("9 / 12", { exact: true }).first();
      await show(valid);
      const reasonTip = page.getByRole("tooltip").filter({ hasText: "缺少窗口复权因子：2" });
      await expect(reasonTip).toBeVisible();
      await expect(reasonTip).toContainText("缺少日线记录：1");
      await expect(reasonTip).not.toContainText("初始化");
      await expectTipInViewport(page, reasonTip);
      await page.screenshot({ path: testInfo.outputPath(`stock-reasons-${viewport.label}.png`) });
      await hide(valid);
      await expect(reasonTip).toBeHidden();
      await field.selectOption("price_window_days_90d");
      await expect(coverage.getByText("11 / 12", { exact: true })).toHaveCount(3);
      const explain = page.getByText("覆盖说明", { exact: true });
      await show(explain);
      const countTip = page.getByRole("tooltip").filter({ hasText: "计数有效不代表窗口可用" });
      await expect(countTip).toBeVisible();
      await expect(countTip).toContainText("90日实际观察数（选股派生）");
      await hide(explain);
      await expect(countTip).toBeHidden();
      for (const [column, name, description] of [
        ["ma5", "5日均线（历史推导）", "从首个有效历史观察推导"],
        ["turnover_rate", "换手率（库存原值）", "0.5387表示0.5387%，不乘100"],
      ]) {
        await field.selectOption(column ?? "");
        await show(explain);
        const info = page.getByRole("tooltip").filter({ hasText: name });
        await expect(info).toBeVisible();
        await expect(info).toContainText(description ?? "");
        await expectTipInViewport(page, info);
        await hide(explain);
        await expect(info).toBeHidden();
      }
      const recent = page.getByRole("table", { name: "最近检验" });
      await recent.getByRole("cell", { name: "第 1 版", exact: true }).click();
      await show(basis);
      const legacy = page.getByRole("tooltip").filter({ hasText: "指标价格基准与初始化未核验" });
      await expect(legacy).toBeVisible();
      await expect(legacy).not.toContainText("选股派生");
      await hide(basis);
      await recent.getByRole("cell", { name: "第 2 版", exact: true }).click();
      await show(basis);
      const history = page.getByRole("tooltip").filter({ hasText: "从首个有效历史观察初始化" });
      await expect(history).toBeVisible();
      await expect(history).not.toContainText("选股派生");
      await hide(basis);
      await recent.getByRole("cell", { name: "第 3 版", exact: true }).click();
      await show(basis);
      await expect(sourceTip).toBeVisible();
      await expect(sourceTip).not.toContainText("初始化");
      await expect(sourceTip).not.toContainText("库存原值");
      await expectTipInViewport(page, sourceTip);
      await page.screenshot({
        path: testInfo.outputPath(`stock-standalone-source-${viewport.label}.png`),
      });
      await hide(basis);
      rawOnly = true;
      generation = "d".repeat(64);
      await page.reload();
      await show(basis);
      await expect(sourceTip).toBeVisible();
      await expect(sourceTip).toContainText("历史推导：12项");
      await hide(basis);
      await page.getByRole("button", { name: "编辑", exact: true }).click();
      await search.fill("价格位置");
      await expect(dialog.getByText("没有匹配的字段", { exact: true })).toBeVisible();
      await expect(dialog.getByRole("button", { name: /^插入/ })).toHaveCount(0);
      await expect(expression).toHaveValue("close + ma_alignment");
      await dialog.getByRole("button", { name: "关闭", exact: true }).last().click();
      wrongCapability = true;
      await page.reload();
      await expect(page.getByRole("button", { name: "编辑", exact: true })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "新建因子", exact: true })).toHaveCount(0);
      await expect(page.getByRole("button", { name: /^插入/ })).toHaveCount(0);
      await page.getByText("查看字段覆盖", { exact: true }).click();
      await field.selectOption("ma_alignment");
      await expect(coverage.getByText("8 / 12", { exact: true })).toHaveCount(3);
      await show(coverage.getByText("8 / 12", { exact: true }).first());
      const restoredTip = page.getByRole("tooltip").filter({ hasText: "观察数不足：1" });
      await expect(restoredTip).toBeVisible();
      await expectTipInViewport(page, restoredTip);
      await expectNoHorizontalOverflow(page, `选股来源恢复 ${viewport.label}`);
      await page.screenshot({ path: testInfo.outputPath(`stock-restored-${viewport.label}.png`) });
      await hide(coverage.getByText("8 / 12", { exact: true }).first());
      await expect(restoredTip).toBeHidden();
      expect(findJargon(await page.locator("body").innerText())).toEqual([]);
      expect(await page.locator("body").innerText()).not.toMatch(
        /stock_features_derived|algorithm_version|implementation_sha256/,
      );
      wrongCapability = false;
      await page.reload();
      await page.getByRole("button", { name: "继续编辑草稿", exact: true }).click();
      await expect(expression).toHaveValue("close + ma_alignment");
      await dialog.getByRole("button", { name: "关闭", exact: true }).last().click();
      expect(watcher.problems).toEqual([]);
    });
  });
}
