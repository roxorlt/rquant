import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import {
  auctionFactor,
  auctionResearch,
  mixedAuctionCapability,
  nullAuctionResearch,
} from "../src/pages/factors/factorAuction.fixture.ts";
import { diagnosticResult } from "../src/pages/factors/factorDiagnostics.fixture.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL, REPO_ROOT } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`题材竞价 ${viewport.label}`, () => {
    test.use({ isMobile: viewport.label === "phone", hasTouch: viewport.label === "phone" });
    test("原值示例、完整覆盖、来源提示和字段插入可用", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      let result = diagnosticResult();
      let research = auctionResearch;
      await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
      await page.route("**/api/v1/factors/definitions*", (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              can_save: true,
              can_archive: true,
              definitions: [auctionFactor],
            },
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorCatalogData_"],
        }),
      );
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: mixedAuctionCapability,
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
            data: trackingPanel({ factor_id: auctionFactor.factor_id }),
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorTrackingPanel_"],
        }),
      );
      await page.setViewportSize(viewport);
      await page.goto(`./#/factors?factor_id=${auctionFactor.factor_id}`);
      const source = page.getByRole("region", { name: "字段来源", exact: true });
      await expect(source.getByRole("heading", { name: "竞价字段", exact: true })).toBeVisible();
      expect(findJargon(await page.getByRole("main").innerText())).toEqual([]);
      const basis = source.getByText("竞价字段口径", { exact: true });
      if (viewport.label === "phone") await basis.tap();
      else await basis.focus();
      const basisTip = page.getByRole("tooltip").filter({ hasText: "原题材完整成员" });
      await expect(basisTip).toContainText("09:30截止");
      await expect(basisTip).toContainText("历史回顾");
      if (viewport.label === "phone") await basis.tap();
      else await source.getByRole("heading").click();
      await source.getByText("原值示例（10只）", { exact: true }).click();
      const table = source.getByRole("table", { name: "竞价原值示例" });
      await expect(table).toContainText("0.6667");
      await expect(table).not.toContainText("66.67%");
      const example = source.getByText("示例说明", { exact: true });
      if (viewport.label === "phone") await example.tap();
      else await example.focus();
      await expect(page.getByRole("tooltip").filter({ hasText: "完整范围" })).toContainText("12只");
      await source.getByText("查看字段覆盖", { exact: true }).click();
      await expect(source.getByRole("table", { name: "字段覆盖" })).toContainText("3 / 12");
      await expectNoHorizontalOverflow(page, `竞价示例 ${viewport.label}`);
      const values = `${REPO_ROOT}/data/verification/factor-auction-20261005/frontend/values-${viewport.label}.png`;
      await page.screenshot({ path: values, fullPage: true });
      await testInfo.attach("auction-values", { path: values, contentType: "image/png" });
      research = nullAuctionResearch;
      result = diagnosticResult({ job_id: "d".repeat(32), spec_sha256: "e".repeat(64) });
      await page.reload();
      await source.getByText("原值示例（10只）", { exact: true }).click();
      const missing = table.getByText("—", { exact: true }).first();
      if (viewport.label === "phone") await missing.tap();
      else await missing.focus();
      await expect(page.getByRole("tooltip").filter({ hasText: "竞价值为空" })).toBeVisible();
      await expect(page.getByRole("button", { name: "加入跟踪", exact: true })).toBeEnabled();
      await page.getByRole("button", { name: "新建因子", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "新建因子" });
      await dialog.getByRole("searchbox", { name: "搜索字段" }).fill("竞价");
      const group = dialog.getByRole("group", { name: "竞价" });
      const info = group.getByRole("button", { name: "题材竞价高开占比说明" });
      if (viewport.label === "phone") await info.tap();
      else await info.focus();
      await expect(
        page.getByRole("tooltip").filter({ hasText: "单位：比例（0–1）" }),
      ).toBeVisible();
      await group.getByRole("button", { name: "插入题材竞价金额比" }).click();
      await expect(dialog.getByRole("textbox", { name: "表达式" })).toHaveValue(
        "board_auction_amount_ratio",
      );
      await expectNoHorizontalOverflow(page, `竞价编辑 ${viewport.label}`);
      const editor = `${REPO_ROOT}/data/verification/factor-auction-20261005/frontend/editor-${viewport.label}.png`;
      await page.screenshot({ path: editor, fullPage: true });
      await testInfo.attach("auction-editor", { path: editor, contentType: "image/png" });
      expect(watcher.problems).toEqual([]);
    });
  });
}
