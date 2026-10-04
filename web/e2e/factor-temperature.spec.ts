import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { diagnosticResult } from "../src/pages/factors/factorDiagnostics.fixture.ts";
import {
  marketTemperatureFactor,
  marketTemperatureResearch,
  mixedMarketTemperatureCapability,
  nullMarketTemperatureResearch,
} from "../src/pages/factors/factorMarketTemperature.fixture.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL, REPO_ROOT } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`市场温度 ${viewport.label}`, () => {
    test.use({ isMobile: viewport.label === "phone", hasTouch: viewport.label === "phone" });
    test("真实日值、来源提示和市场字段插入可用", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      let result = diagnosticResult();
      let research = marketTemperatureResearch;
      await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
      await page.route("**/api/v1/factors/definitions*", (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              can_save: true,
              can_archive: true,
              definitions: [marketTemperatureFactor],
            },
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorCatalogData_"],
        }),
      );
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: mixedMarketTemperatureCapability,
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorCapabilitiesData_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              results: [result],
            },
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorResultListData_"],
        }),
      );
      await page.route("**/api/v1/factors/results/*", (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "ready",
              available_at: metadata.serving.built_at,
              result,
              research,
            },
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorResultDetailData_"],
        }),
      );
      await page.route("**/api/v1/factors/*/tracking*", (route) =>
        route.fulfill({
          json: {
            data: trackingPanel({ factor_id: marketTemperatureFactor.factor_id }),
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorTrackingPanel_"],
        }),
      );
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto(`./#/factors?factor_id=${marketTemperatureFactor.factor_id}`);
      const dependencies = page.locator(".factor-detail").getByText("使用字段");
      const dependencyValues = dependencies.locator("..").locator("dd");
      await expect(dependencyValues).toHaveText("2 项");
      await expect(dependencyValues).not.toContainText("market_high_60d_ratio_pct");
      const dependencyCount = dependencyValues.getByText("2 项", { exact: true });
      if (viewport.label === "phone") await dependencyCount.tap();
      else await dependencyCount.focus();
      await expect(page.getByRole("tooltip").filter({ hasText: "60日新高占比" })).toBeVisible();
      if (viewport.label === "phone") await dependencyCount.tap();
      const source = page.getByRole("region", { name: "字段来源", exact: true });
      await expect(source.getByRole("heading", { name: "市场温度", exact: true })).toBeVisible();
      await expect(source).not.toContainText("日线字段口径");
      await expect(source).not.toContainText("分钟字段口径");
      expect(findJargon(await page.getByRole("main").innerText())).toEqual([]);
      const basis = source.getByText("市场温度口径", { exact: true });
      const basisTip = page.getByRole("tooltip").filter({ hasText: "全市场日值，同一天" });
      if (viewport.label === "phone") {
        expect(await page.evaluate(() => matchMedia("(hover: none)").matches)).toBe(true);
        await basis.tap();
      } else await basis.hover();
      await expect(basisTip).toContainText("不按所选股票池重算");
      await expect(basisTip).toContainText("仅用于历史回顾，不代表当时已知");
      if (viewport.label === "phone") await basis.tap();
      else {
        await source.getByRole("heading").hover();
        await expect(basisTip).toBeHidden();
        await basis.focus();
        await expect(basisTip).toContainText("09:25");
      }
      await source.getByText("查看市场温度", { exact: true }).click();
      const table = source.getByRole("table", { name: "市场温度日值" });
      await expect(table).toContainText("20.00%");
      await expect(table).toContainText("98.00%");
      await expect(table).not.toContainText("12 / 12");
      const value = table.getByText("20.00%", { exact: true });
      if (viewport.label === "phone") await value.tap();
      else await value.focus();
      const valueTip = page.getByRole("tooltip").filter({ hasText: "原值：20%" });
      await expect(valueTip).toContainText("字段日期：2026-07-03");
      await expect(valueTip).toContainText("原值：20%");
      await expectNoHorizontalOverflow(page, `市场日值 ${viewport.label}`);
      const valueScreenshot = `${REPO_ROOT}/data/verification/factor-temperature-20261004/frontend/values-${viewport.label}.png`;
      await page.screenshot({ path: valueScreenshot, fullPage: true });
      await testInfo.attach("market-values", { path: valueScreenshot, contentType: "image/png" });

      research = nullMarketTemperatureResearch;
      result = diagnosticResult({ job_id: "d".repeat(32), spec_sha256: "e".repeat(64) });
      // The null scenario has its own sealed job and a fresh query cache.
      await page.reload();
      await expect(source.getByRole("heading", { name: "市场温度", exact: true })).toBeVisible();
      await source.getByText("查看市场温度", { exact: true }).click();
      const missing = table.getByText("—", { exact: true });
      await expect(missing).toBeVisible();
      if (viewport.label === "phone") await missing.tap();
      else await missing.focus();
      await expect(page.getByRole("tooltip").filter({ hasText: "市场温度为空" })).toBeVisible();
      await expect(page.getByRole("button", { name: "加入跟踪", exact: true })).toBeEnabled();
      await page.getByRole("button", { name: "新建因子", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "新建因子" });
      const search = dialog.getByRole("searchbox", { name: "搜索字段" });
      await search.fill("新高");
      const market = dialog.getByRole("group", { name: "市场温度" });
      const info = market.getByRole("button", { name: "60日新高占比说明" });
      if (viewport.label === "phone") await info.tap();
      else await info.focus();
      await expect(
        page.getByRole("tooltip").filter({ hasText: "单位：百分比（%）" }),
      ).toBeVisible();
      await market.getByRole("button", { name: "插入60日新高占比" }).click();
      await expect(dialog.getByRole("textbox", { name: "表达式" })).toHaveValue(
        "market_high_60d_ratio_pct",
      );
      await search.fill("均线上方");
      await expect(market.getByRole("button", { name: "插入20日均线上方占比" })).toBeEnabled();
      await expectNoHorizontalOverflow(page, `市场字段编辑 ${viewport.label}`);
      const editorScreenshot = `${REPO_ROOT}/data/verification/factor-temperature-20261004/frontend/editor-${viewport.label}.png`;
      await page.screenshot({ path: editorScreenshot, fullPage: true });
      await testInfo.attach("market-editor", { path: editorScreenshot, contentType: "image/png" });
      await dialog.getByRole("button", { name: "关闭", exact: true }).last().click();
      expect(findJargon(await page.getByRole("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
