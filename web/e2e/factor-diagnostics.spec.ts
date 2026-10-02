import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import {
  diagnosticAvailability,
  diagnosticFactor,
  diagnosticResearch,
  diagnosticResult,
  diagnosticStatistics,
} from "../src/pages/factors/factorDiagnostics.fixture.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`因子扩展统计 ${viewport.label}`, () => {
    test.use({ isMobile: viewport.label === "phone", hasTouch: viewport.label === "phone" });
    test(`MAD 确认、原请求恢复和历史诊断在 ${viewport.label} 可完成`, async ({
      page,
    }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      let published = false;
      const originalGeneration = metadata.serving.generation_id;
      const requests: Schemas["FactorRunRequest"][] = [];
      const latest = diagnosticResult({ definition_status: "historical_unavailable" });
      const older = diagnosticResult({
        job_id: "1".repeat(32),
        factor_version: 1,
        definition_status: "historical_unavailable",
        spec_sha256: "1".repeat(64),
        definition_content_sha256: "2".repeat(64),
        updated_at: "2026-09-20T07:31:00Z",
      });
      const research = {
        ...diagnosticResearch,
        mad_multiple: 4.5,
        extended_statistics: { ...diagnosticStatistics, ic_method: "normal" as const },
      };
      const {
        mad_multiple: _multiple,
        extended_statistics: _statistics,
        ...legacyResearch
      } = diagnosticResearch;
      await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
      await page.route("**/api/v1/factors/definitions*", (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              can_save: false,
              can_archive: true,
              definitions: [
                {
                  ...diagnosticFactor,
                  ...(published ? { version: 3, content_sha256: "e".repeat(64) } : {}),
                },
              ],
            },
            serving: metadata.serving,
          },
        }),
      );
      await page.route("**/api/v1/factors/run-availability", (route) =>
        route.fulfill({ json: { data: diagnosticAvailability, serving: metadata.serving } }),
      );
      await page.route(
        new RegExp(`/api/v1/factors/${diagnosticFactor.factor_id}/tracking(?:\\?.*)?$`),
        (route) => {
          expect(new URL(route.request().url()).searchParams.get("generation_id")).toBe(
            metadata.serving.generation_id,
          );
          return route.fulfill({
            json: {
              data: trackingPanel({
                factor_id: diagnosticFactor.factor_id,
                availability: "unavailable",
                status: "unavailable",
                can_set_tracked: false,
                reason: "跟踪数据尚未发布，暂时不能修改跟踪。",
                definition_head: null,
              }),
              serving: metadata.serving,
            },
          });
        },
      );
      await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: {
              availability: published ? "populated" : "empty",
              available_at: metadata.serving.built_at,
              results: published ? [latest, older] : [],
            },
            serving: metadata.serving,
          },
        }),
      );
      await page.route(/\/api\/v1\/factors\/results\/[a-f0-9]{32}(?:\?.*)?$/, (route) => {
        const old = new URL(route.request().url()).pathname.endsWith(older.job_id);
        return route.fulfill({
          json: {
            data: {
              availability: "ready",
              available_at: metadata.serving.built_at,
              result: old ? older : latest,
              research: old ? legacyResearch : research,
            },
            serving: metadata.serving,
          },
        });
      });
      await page.route(/\/api\/v1\/factors\/runs(?:\/(?:resume|retry))?$/, async (route) => {
        const body = route.request().postDataJSON() as Schemas["FactorRunRequest"];
        expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
        expect(
          await page.evaluate(
            () =>
              JSON.parse(localStorage.getItem("rquant.factor.run-operation.v1") ?? "null").request,
          ),
        ).toEqual(body);
        requests.push(body);
        const submitted = route.request().url().endsWith("/retry");
        const result: Schemas["FactorRunOperationResult"] = {
          original_request: body,
          status: submitted ? "submitted" : "uncertain",
          reason: null,
          job_id: submitted ? latest.job_id : null,
          spec_sha256: submitted ? (latest.spec_sha256 ?? null) : null,
        };
        await route.fulfill({ json: { data: result, serving: metadata.serving } });
      });
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto("./#/factors");
      const form = page.getByRole("region", { name: "检验参数" });
      await expect(form.getByRole("button", { name: "运行检验" })).toBeEnabled();
      const explanation = form.getByText("离群值处理说明", { exact: true });
      if (viewport.label === "phone") {
        expect(await page.evaluate(() => matchMedia("(hover: none)").matches)).toBe(true);
        await explanation.tap();
      } else await explanation.focus();
      await expect(page.getByRole("tooltip")).toContainText("先处理离群值，再做所选中性化");
      if (viewport.label === "phone") await explanation.tap();
      await form.getByRole("combobox", { name: "离群值处理" }).selectOption("mad");
      const multiple = form.getByRole("spinbutton", { name: "MAD 倍数" });
      await expect(multiple).toHaveValue("3");
      await multiple.fill("0");
      await expect(form.getByRole("button", { name: "运行检验" })).toBeDisabled();
      await multiple.fill("4.5");
      await form.getByRole("button", { name: "NormalIC", exact: true }).click();
      const run = page.locator(".ph-actions").getByRole("button", { name: "运行检验" });
      await run.focus();
      await page.keyboard.press("Enter");
      const dialog = page.getByRole("dialog", { name: "运行因子检验" });
      await expect(dialog).toContainText("MAD 4.5 倍");
      await expect(dialog).toContainText("NormalIC");
      await expect(dialog).toBeVisible();
      await expect(dialog).toHaveCSS("opacity", "1");
      expect(requests).toHaveLength(0);
      await expectNoHorizontalOverflow(page, `MAD confirm ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`factor-mad-confirm-${viewport.label}.png`),
        fullPage: true,
        animations: "disabled",
      });
      await dialog.getByRole("button", { name: "确认运行" }).click();
      await expect(
        page.getByText("检验结果暂未确认，请保留本次操作。", { exact: true }),
      ).toBeVisible();
      expect(requests[0]?.parameters).toMatchObject({
        mad_multiple: 4.5,
        extended_statistics: true,
        ic_method: "normal",
        expected_head: { version: 2, content_sha256: diagnosticFactor.content_sha256 },
      });
      await form.getByRole("combobox", { name: "离群值处理" }).selectOption("none");
      await page.reload();
      const retry = page.getByRole("button", { name: "用原请求重试检验" });
      await expect(retry).toBeEnabled();
      await expect.poll(() => requests.length).toBe(2);
      await expect(page.getByRole("region", { name: "本次检验" })).toContainText("MAD 4.5 倍");
      await retry.click();
      await expect(page.getByText("已提交，等待更新。", { exact: true })).toBeVisible();
      expect(requests).toEqual([requests[0], requests[0], requests[0]]);
      expect(requests[0]?.serving_generation_id).toBe(originalGeneration);
      await expect(page.getByText("检验完成。", { exact: true })).toHaveCount(0);
      published = true;
      if (metadata.data.generation === null) throw new Error("合成资料缺少数据记录");
      metadata.data.generation.generation_id = "d".repeat(64);
      metadata.serving.generation_id = "d".repeat(64);
      await page.locator(".ph-actions").getByRole("button", { name: "刷新", exact: true }).click();
      await expect(page.getByText("检验完成。", { exact: true })).toBeVisible();
      const resultArea = page.getByRole("region", { name: "检验结果", exact: true });
      await expect(resultArea).toContainText("MAD 4.5 倍");
      await expect(resultArea).toContainText("第 2 版检验 · 历史版本");
      const industry = resultArea.getByRole("region", { name: "行业 IC" });
      await expect(industry).toContainText("NormalIC");
      await resultArea.getByRole("button", { name: "RankIC", exact: true }).click();
      await expect(industry).toContainText("NormalIC");
      const note = industry.getByText("样本说明", { exact: true });
      if (viewport.label === "phone") await note.tap();
      else await note.focus();
      await expect(page.getByRole("tooltip")).toContainText(
        diagnosticStatistics.industry_reason ?? "",
      );
      if (viewport.label === "phone") await note.tap();
      await industry.getByText("查看行业 IC 明细", { exact: true }).click();
      await expect(industry.getByRole("table", { name: "行业 IC 明细" })).toContainText("农林牧渔");
      await industry.getByText("查看行业覆盖", { exact: true }).click();
      await expect(industry.getByRole("table", { name: "行业覆盖" })).toContainText("6 / 8");
      const autocorrelation = resultArea.getByRole("region", { name: "因子自相关" });
      const adjacent = autocorrelation.getByText("相邻评价期", { exact: true });
      if (viewport.label === "phone") await adjacent.tap();
      else await adjacent.focus();
      await expect(page.getByRole("tooltip", { name: /不跨期补值/ })).toContainText("不跨期补值");
      if (viewport.label === "phone") await adjacent.tap();
      await autocorrelation.getByText("查看自相关明细", { exact: true }).click();
      await expect(autocorrelation.getByRole("table", { name: "自相关明细" })).toContainText(
        "相邻两期没有共同样本",
      );
      await expect(
        resultArea.getByRole("img", { name: "行业 IC 均值" }).locator("canvas"),
      ).toBeVisible();
      await expect(
        resultArea.getByRole("img", { name: "相邻评价期因子自相关" }).locator("canvas"),
      ).toBeVisible();
      await expectNoHorizontalOverflow(page, `diagnostics ${viewport.label}`);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await page.screenshot({
        path: testInfo.outputPath(`factor-diagnostics-${viewport.label}.png`),
        fullPage: true,
        animations: "disabled",
      });
      await page.getByRole("button", { name: "继续查看结果", exact: true }).click();
      await resultArea.getByRole("cell", { name: "第 1 版", exact: true }).click();
      await expect(
        resultArea.getByText("这次检验未生成行业 IC 和自相关。", { exact: true }),
      ).toBeVisible();
      await expect(resultArea).toContainText("不处理离群值");
      await expect(resultArea.getByRole("img", { name: "行业 IC 均值" })).toHaveCount(0);
      await expectNoHorizontalOverflow(page, `legacy diagnostics ${viewport.label}`);
      expect(watcher.problems).toEqual([]);
    });
  });
}
