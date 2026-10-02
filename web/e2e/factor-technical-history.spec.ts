import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { dailyCapability, dailyResearch } from "../src/pages/factors/factorDailyFields.fixture.ts";
import {
  diagnosticFactor,
  diagnosticResult,
} from "../src/pages/factors/factorDiagnostics.fixture.ts";
import {
  technicalCapability,
  technicalResearch,
} from "../src/pages/factors/factorTechnicalHistory.fixture.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`技术历史来源 ${viewport.label}`, () => {
    test.use({ hasTouch: viewport.label === "phone", isMobile: viewport.label === "phone" });
    test(`字段与历史结果独立口径、初始化缺因在 ${viewport.label} 可查看`, async ({
      page,
    }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      let generation = metadata.serving.generation_id;
      let shrink = false;
      const serving = () => ({ ...metadata.serving, generation_id: generation });
      const current = diagnosticResult();
      const older = diagnosticResult({
        job_id: "1".repeat(32),
        factor_version: 1,
        definition_status: "historical_unavailable",
        updated_at: "2026-09-20T07:31:00Z",
      });
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
            data: shrink
              ? { ...dailyCapability, fields: dailyCapability.fields.slice(0, 6) }
              : technicalCapability,
            serving: serving(),
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
              results: [current, older],
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultListData_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/results\/[0-9a-f]{32}(?:\?.*)?$/, (route) => {
        const old = new URL(route.request().url()).pathname.endsWith(older.job_id);
        return route.fulfill({
          json: {
            data: {
              availability: "ready",
              available_at: serving().built_at,
              result: old ? older : current,
              research: old ? dailyResearch : technicalResearch,
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultDetailData_"],
        });
      });
      await page.setViewportSize(viewport);
      await page.goto("./#/factors");
      const edit = page.getByRole("button", { name: "编辑", exact: true });
      await edit.focus();
      await edit.press("Enter");
      const drawer = page.getByRole("dialog", { name: "编辑因子" });
      const expression = drawer.getByRole("textbox", { name: "表达式", exact: true });
      const search = drawer.getByRole("searchbox", { name: "搜索日线字段" });
      await search.fill("KDJ D");
      const info = drawer.getByRole("button", { name: "KDJ D说明" });
      if (viewport.label === "phone") await info.tap();
      else await info.focus();
      const indicatorTip = page
        .getByRole("tooltip")
        .filter({ hasText: "从首个有效历史观察推导；历史断裂不重置。" });
      await expect(indicatorTip).toBeVisible();
      await expect(expression).toHaveValue(diagnosticFactor.expression);
      if (viewport.label === "phone") await info.tap();
      else await info.evaluate((element: HTMLElement) => element.blur());
      await expect(indicatorTip).toBeHidden();
      await search.fill("换手率");
      const basicInfo = drawer.getByRole("button", { name: "换手率说明" });
      if (viewport.label === "phone") await basicInfo.tap();
      else await basicInfo.focus();
      const basicTip = page
        .getByRole("tooltip")
        .filter({ hasText: "0.5387表示0.5387%，不乘100。" });
      await expect(basicTip).toBeVisible();
      if (viewport.label === "phone") await basicInfo.tap();
      else await basicInfo.evaluate((element: HTMLElement) => element.blur());
      await expect(basicTip).toBeHidden();
      await drawer.getByRole("button", { name: "关闭", exact: true }).last().click();
      const basis = page.getByText("日线字段口径", { exact: true });
      if (viewport.label === "phone") await basis.tap();
      else await basis.focus();
      const tip = page.getByRole("tooltip").filter({ hasText: "按各字段实际来源展示" });
      await expect(tip).toBeVisible();
      await expect(tip).toContainText("5日均线（历史推导）");
      await expect(tip).toContainText("换手率（库存原值）");
      await expect(tip).toContainText("历史起点 2026-07-01");
      await expect(tip).toContainText("历史断裂后不重新初始化");
      await expect(tip.locator("..")).toHaveCSS("opacity", "1");
      await expectNoHorizontalOverflow(page, `来源说明 ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`technical-history-source-${viewport.label}.png`),
      });
      if (viewport.label === "phone") await basis.tap();
      else await basis.evaluate((element: HTMLElement) => element.blur());
      await expect(tip).toBeHidden();
      await page.getByText("查看字段覆盖", { exact: true }).click();
      const table = page.getByRole("table", { name: "日线字段覆盖" });
      const valid = table.getByText("9 / 12", { exact: true }).first();
      if (viewport.label === "phone") await valid.tap();
      else await valid.focus();
      const coverageTip = page.getByRole("tooltip").filter({ hasText: "历史断裂：1" });
      await expect(coverageTip).toBeVisible();
      await expect(coverageTip).toContainText("缺少初始化历史：2");
      if (viewport.label === "phone") await valid.tap();
      else await valid.evaluate((element: HTMLElement) => element.blur());
      await expect(coverageTip).toBeHidden();
      await page.getByRole("combobox", { name: "覆盖字段" }).selectOption("turnover_rate");
      await expect(table).toContainText("12 / 12");
      await page
        .getByRole("table", { name: "最近检验" })
        .getByRole("cell", { name: "第 1 版", exact: true })
        .click();
      if (viewport.label === "phone") await basis.tap();
      else await basis.focus();
      const legacyTip = page.getByRole("tooltip").filter({ hasText: "价格基准与初始化未核验" });
      await expect(legacyTip).toBeVisible();
      await expect(legacyTip).not.toContainText("历史推导");
      if (viewport.label === "phone") await basis.tap();
      else await basis.evaluate((element: HTMLElement) => element.blur());
      await expect(legacyTip).toBeHidden();
      shrink = true;
      generation = "d".repeat(64);
      await page.reload();
      if (viewport.label === "phone") await basis.tap();
      else await basis.focus();
      await expect(tip).toBeVisible();
      await expect(tip).toContainText("5日均线（历史推导）");
      await expect(tip).not.toContainText("指标价格基准与初始化未核验");
      if (viewport.label === "phone") await basis.tap();
      else await basis.evaluate((element: HTMLElement) => element.blur());
      await expect(tip).toBeHidden();
      await page.getByText("查看字段覆盖", { exact: true }).click();
      await expect(table).toContainText("9 / 12");
      await expectNoHorizontalOverflow(page, `历史来源恢复 ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`technical-history-restored-${viewport.label}.png`),
      });
      expect(findJargon(await page.locator("body").innerText())).toEqual([]);
      expect(await page.locator("body").innerText()).not.toMatch(
        /history_derived|rquant-ta|schema_version|implementation_sha256/,
      );
      expect(watcher.problems).toEqual([]);
    });
  });
}
