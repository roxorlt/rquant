import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { diagnosticResult } from "../src/pages/factors/factorDiagnostics.fixture.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import {
  mixedVolumeProfileCapability,
  nullVolumeProfileResearch,
  volumeProfileFactor,
  volumeProfileResearch,
} from "../src/pages/factors/factorVolumeProfile.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL, REPO_ROOT } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`90日成交分布 ${viewport.label}`, () => {
    test.use({ isMobile: viewport.label === "phone", hasTouch: viewport.label === "phone" });
    test("原值、稀疏覆盖、缺因及键盘插入可用", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      let result = diagnosticResult();
      let research = volumeProfileResearch;
      await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
      await page.route("**/api/v1/factors/definitions*", (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              can_save: true,
              can_archive: true,
              definitions: [volumeProfileFactor],
            },
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorCatalogData_"],
        }),
      );
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: mixedVolumeProfileCapability,
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
            data: trackingPanel({ factor_id: volumeProfileFactor.factor_id }),
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorTrackingPanel_"],
        }),
      );
      await page.setViewportSize(viewport);
      await page.goto(`./#/factors?factor_id=${volumeProfileFactor.factor_id}`);
      const source = page.getByRole("region", { name: "字段来源", exact: true });
      await expect(
        source.getByRole("heading", { name: "90日成交分布", exact: true }),
      ).toBeVisible();
      expect(findJargon(await page.getByRole("main").innerText())).toEqual([]);
      const basis = source.getByText("成交分布口径", { exact: true });
      if (viewport.label === "phone") await basis.tap();
      else await basis.focus();
      const basisTip = page.getByRole("tooltip").filter({ hasText: "90个实际日线日期" });
      await expect(basisTip).toContainText("历史回顾");
      await expect(basisTip).toContainText("分钟成交近似");
      if (viewport.label === "phone") await basis.tap();
      else await source.getByRole("heading").click();
      await source.getByText("成交分布示例（10只）", { exact: true }).click();
      const table = source.getByRole("table", { name: "成交分布原值示例" });
      const field = source.getByRole("combobox", { name: "成交分布字段" });
      await field.selectOption("vp90_total_vol");
      await expect(table).toContainText("800.00");
      await expect(table).toContainText("5 / 90 日");
      const shares = table.getByText("800.00", { exact: true }).first();
      if (viewport.label === "phone") await shares.tap();
      else await shares.focus();
      await expect(page.getByRole("tooltip").filter({ hasText: "原值：800股" })).toBeVisible();
      await field.selectOption("vp90_total_amount");
      await expect(table).toContainText("10,000.00");
      await field.selectOption("vp90_concentration_top5_pct");
      await expect(table).toContainText("100.00%");
      const coverage = table.getByText("5 / 90 日", { exact: true }).first();
      if (viewport.label === "phone") await coverage.tap();
      else await coverage.focus();
      await expect(
        page.getByRole("tooltip").filter({ hasText: "其中5日有分钟记录" }),
      ).toBeVisible();
      await source.getByText("查看字段覆盖", { exact: true }).click();
      await expect(source.getByRole("table", { name: "字段覆盖" })).toContainText("12 / 12");
      await expectNoHorizontalOverflow(page, `成交分布示例 ${viewport.label}`);
      const values = `${REPO_ROOT}/data/verification/factor-volume-profile-20261005/frontend/values-${viewport.label}.png`;
      await page.screenshot({ path: values, fullPage: true });
      await testInfo.attach("volume-profile-values", { path: values, contentType: "image/png" });
      research = nullVolumeProfileResearch;
      result = diagnosticResult({ job_id: "d".repeat(32), spec_sha256: "e".repeat(64) });
      await page.reload();
      await source.getByText("成交分布示例（10只）", { exact: true }).click();
      const missing = table.getByText("—", { exact: true }).first();
      if (viewport.label === "phone") await missing.tap();
      else await missing.focus();
      await expect(
        page.getByRole("tooltip").filter({ hasText: "成交量或成交额总和非正数" }),
      ).toBeVisible();
      await expect(page.getByRole("button", { name: "加入跟踪", exact: true })).toBeEnabled();
      await page.getByRole("button", { name: "新建因子", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "新建因子" });
      await expect(dialog).toBeInViewport({ ratio: 1 });
      await dialog.getByRole("searchbox", { name: "搜索字段" }).fill("90日");
      const group = dialog.getByRole("group", { name: "成交分布" });
      const info = group.getByRole("button", { name: "90日可比成交量说明" });
      if (viewport.label === "phone") await info.tap();
      else await info.focus();
      await expect(page.getByRole("tooltip").filter({ hasText: "单位：股" })).toBeVisible();
      await expectNoHorizontalOverflow(page, `成交分布字段提示 ${viewport.label}`);
      const insert = group.getByRole("button", { name: "插入90日成交均价" });
      await insert.focus();
      await page.keyboard.press("Enter");
      await expect(dialog.getByRole("textbox", { name: "表达式" })).toHaveValue("vp90_vwap");
      await expect(page.getByRole("tooltip").filter({ hasText: "单位：股" })).toBeHidden();
      await expectNoHorizontalOverflow(page, `成交分布编辑 ${viewport.label}`);
      const editor = `${REPO_ROOT}/data/verification/factor-volume-profile-20261005/frontend/editor-${viewport.label}.png`;
      await page.screenshot({ path: editor, fullPage: true });
      await testInfo.attach("volume-profile-editor", { path: editor, contentType: "image/png" });
      expect(watcher.problems).toEqual([]);
    });
  });
}
