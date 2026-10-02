import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { dailyCapability, dailyResearch } from "../src/pages/factors/factorDailyFields.fixture.ts";
import {
  diagnosticAvailability,
  diagnosticFactor,
  diagnosticResearch,
  diagnosticResult,
} from "../src/pages/factors/factorDiagnostics.fixture.ts";
import { trackingPanel, trackingResult } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`日线字段 ${viewport.label}`, () => {
    test.use({ hasTouch: viewport.label === "phone", isMobile: viewport.label === "phone" });
    test(`中文字段插入、保存与原检验恢复在 ${viewport.label} 可完成`, async ({
      page,
    }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      let currentGeneration = metadata.serving.generation_id;
      let saved = false;
      let published = false;
      let shrink = false;
      let tracked = false;
      let cancelled = false;
      const factor = () => ({
        ...diagnosticFactor,
        name_zh: "库存价量",
        version: published ? 4 : saved ? 3 : 2,
        content_sha256: published
          ? "e".repeat(64)
          : saved
            ? "c".repeat(64)
            : diagnosticFactor.content_sha256,
        expression: "turnover_rate + ref(ma20, 1)",
        dependency_columns: ["ma20", "turnover_rate"],
      });
      const serving = () => ({ ...metadata.serving, generation_id: currentGeneration });
      const envelope = () => ({
        ...metadata,
        serving: serving(),
        data: {
          ...metadata.data,
          generation: metadata.data.generation
            ? { ...metadata.data.generation, generation_id: currentGeneration ?? "" }
            : null,
        },
      });
      const saves: Schemas["FactorSaveDraft"][] = [];
      const runs: Schemas["FactorRunRequest"][] = [];
      const cancellation: Schemas["FactorTrackingRequest"][] = [];
      const latest = diagnosticResult({
        factor_name_zh: "库存价量",
        factor_version: 3,
        definition_content_sha256: "c".repeat(64),
        definition_status: "historical_unavailable",
      });
      const older = diagnosticResult({
        job_id: "1".repeat(32),
        factor_version: 2,
        definition_status: "historical_unavailable",
        spec_sha256: "1".repeat(64),
        definition_content_sha256: diagnosticFactor.content_sha256,
        updated_at: "2026-09-20T07:31:00Z",
      });
      await page.route("**/api/v1/meta", (route) => route.fulfill({ json: envelope() }));
      await page.route(/\/api\/v1\/factors\/definitions(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              can_save: true,
              can_archive: true,
              definitions: [factor()],
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorCatalogData_"],
        }),
      );
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: {
              ...dailyCapability,
              ...(shrink
                ? { version: "daily_v1", fields: dailyCapability.fields.slice(0, 6) }
                : {}),
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorCapabilitiesData_"],
        }),
      );
      await page.route("**/api/v1/factors/run-availability", (route) =>
        route.fulfill({ json: { data: diagnosticAvailability, serving: serving() } }),
      );
      await page.route(
        new RegExp(`/api/v1/factors/${diagnosticFactor.factor_id}/tracking(?:\\?.*)?$`),
        (route) => {
          expect(new URL(route.request().url()).searchParams.get("generation_id")).toBe(
            currentGeneration,
          );
          return route.fulfill({
            json: {
              data: trackingPanel({
                availability: tracked ? "tracked" : "not_tracked",
                status: tracked ? "active" : "not_tracked",
                tracked,
                tracking_generation: cancelled ? "b".repeat(32) : tracked ? "8".repeat(32) : null,
                definition_head: {
                  version: factor().version,
                  content_sha256: factor().content_sha256,
                },
              }),
              serving: serving(),
            } satisfies Schemas["Envelope_FactorTrackingPanel_"],
          });
        },
      );
      await page.route(
        /\/api\/v1\/factors\/definitions\/save(?:\/(?:resume|retry))?$/,
        async (route) => {
          const body = route.request().postDataJSON() as Schemas["FactorSaveDraft"];
          expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
          expect(
            await page.evaluate(() =>
              JSON.parse(localStorage.getItem("rquant.factor.save-command.v1") ?? "null"),
            ),
          ).toEqual(body);
          saves.push(body);
          const resume = route.request().url().endsWith("/resume");
          if (resume) {
            saved = true;
            currentGeneration = "d".repeat(64);
          }
          await route.fulfill({
            json: {
              data: {
                command_id: body.command_id,
                status: resume ? "published" : "uncertain",
                message: resume ? "保存状态已核对。" : "保存结果尚未确认，请保留这次操作。",
                factor_id: resume ? diagnosticFactor.factor_id : null,
                version: resume ? 3 : null,
                content_sha256: resume ? "c".repeat(64) : null,
                current_head_updated: false,
              },
              serving: serving(),
            } satisfies Schemas["Envelope_FactorSaveCommandData_"],
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
            serving: serving(),
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
              research: old ? diagnosticResearch : dailyResearch,
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultDetailData_"],
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
        runs.push(body);
        const retry = route.request().url().endsWith("/retry");
        await route.fulfill({
          json: {
            data: {
              original_request: body,
              status: retry ? "submitted" : "uncertain",
              reason: null,
              job_id: retry ? latest.job_id : null,
              spec_sha256: retry ? (latest.spec_sha256 ?? null) : null,
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorRunOperationResult_"],
        });
      });
      await page.route("**/api/v1/factors/tracking/commands", async (route) => {
        const body = route.request().postDataJSON() as Schemas["FactorTrackingRequest"];
        expect(body.tracked).toBe(false);
        cancellation.push(body);
        expect(
          await page.evaluate(
            () =>
              JSON.parse(localStorage.getItem("rquant.factor.tracking-operation.v1") ?? "null")
                .request,
          ),
        ).toEqual(body);
        tracked = false;
        cancelled = true;
        currentGeneration = "f".repeat(64);
        await route.fulfill({ json: { data: trackingResult(body), serving: serving() } });
      });

      await page.setViewportSize(viewport);
      await page.goto("./#/factors");
      await expect(page.getByRole("button", { name: "加入跟踪" })).toBeDisabled();
      await expect(page.getByRole("button", { name: "加入跟踪" })).toHaveAttribute(
        "aria-description",
        "库存日线字段暂不支持持续跟踪。",
      );
      await page.getByRole("button", { name: "编辑", exact: true }).click();
      const drawer = page.getByRole("dialog", { name: "编辑因子" });
      await expect(drawer.getByRole("button", { name: /^插入/ })).toHaveCount(6);
      const expression = drawer.getByRole("textbox", { name: "表达式", exact: true });
      await expression.fill("ref(close, 1)");
      await expression.focus();
      await expression.evaluate((element: HTMLTextAreaElement) => element.setSelectionRange(4, 9));
      const search = drawer.getByRole("searchbox", { name: "搜索日线字段" });
      await search.fill("20日");
      await search.press("Enter");
      expect(saves).toHaveLength(0);
      const info = drawer.getByRole("button", { name: "20日均线说明" });
      if (viewport.label === "phone") {
        expect(await page.evaluate(() => matchMedia("(hover: none)").matches)).toBe(true);
        await info.tap();
      } else await info.focus();
      await expect(page.getByRole("tooltip")).toContainText("已存20日均线，价格基准未核验。");
      await expect(expression).toHaveValue("ref(close, 1)");
      if (viewport.label === "phone") await info.tap();
      await drawer.getByRole("button", { name: "插入20日均线" }).click();
      await expect(expression).toHaveValue("ref(ma20, 1)");
      await expect(expression).toBeFocused();
      expect(
        await expression.evaluate((element: HTMLTextAreaElement) => element.selectionStart),
      ).toBe(8);
      await expression.fill("turnover_rate + ref(ma20, 1)");
      await search.fill("");
      await drawer.getByRole("button", { name: "全部字段" }).click();
      await expect(drawer.getByRole("button", { name: /^插入/ })).toHaveCount(22);
      await expectNoHorizontalOverflow(page, `字段编辑 ${viewport.label}`);
      await expect(drawer).toHaveCSS("opacity", "1");
      await page.screenshot({
        path: testInfo.outputPath(`daily-fields-editor-${viewport.label}.png`),
        fullPage: true,
      });
      await drawer.getByRole("button", { name: "保存新版本" }).click();
      await expect(
        page.getByText("保存结果尚未确认，请保留这次操作。", { exact: true }),
      ).toBeVisible();
      await page.reload();
      await expect(page.getByText("已保存。", { exact: true })).toBeVisible();
      expect(saves).toHaveLength(2);
      expect(saves[1]).toEqual(saves[0]);
      expect(saves[0]?.expression).toBe("turnover_rate + ref(ma20, 1)");
      expect(saves[0]).not.toHaveProperty("daily_feature_source");
      await page.getByRole("button", { name: "继续查看因子", exact: true }).click();
      const params = page.getByRole("region", { name: "检验参数" });
      await expect(params.getByRole("button", { name: "运行检验" })).toBeEnabled();
      await params.getByRole("button", { name: "运行检验" }).click();
      const confirmation = page.getByRole("dialog", { name: "运行因子检验" });
      await expect(confirmation).toContainText("第 3 版");
      await confirmation.getByRole("button", { name: "确认运行" }).click();
      await expect(page.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled();
      await page.reload();
      await expect(page.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled();
      await page.getByRole("button", { name: "用原请求重试检验" }).click();
      await expect(page.getByText("已提交，等待更新。", { exact: true })).toBeVisible();
      expect(runs).toHaveLength(3);
      expect(runs[1]).toEqual(runs[0]);
      expect(runs[2]).toEqual(runs[0]);
      expect(runs[0]?.parameters.expected_head).toEqual({
        version: 3,
        content_sha256: "c".repeat(64),
      });
      expect(runs[0]?.parameters.extended_statistics).toBe(true);
      expect(runs[0]).not.toHaveProperty("daily_feature_source");
      published = true;
      shrink = true;
      currentGeneration = "e".repeat(64);
      await page.getByRole("button", { name: "刷新检验状态" }).click();
      await expect(page.getByText("检验完成。", { exact: true })).toBeVisible();
      await expect(page.getByText("日线字段口径", { exact: true })).toBeVisible();
      await page.getByText("查看字段覆盖", { exact: true }).click();
      const coverage = page.getByRole("table", { name: "日线字段覆盖" });
      await expect(coverage).toContainText("5 / 8");
      await expect(coverage).toContainText("2026-09-21");
      await page.getByRole("combobox", { name: "覆盖字段" }).selectOption("turnover_rate");
      await expect(coverage).toContainText("6 / 8");
      const basis = page.getByText("日线字段口径", { exact: true });
      if (viewport.label === "phone") await basis.tap();
      else await basis.focus();
      await expect(page.getByRole("tooltip")).toContainText("0.5387表示0.5387%");
      if (viewport.label === "phone") await basis.tap();
      else await basis.evaluate((element: HTMLElement) => element.blur());
      await expectNoHorizontalOverflow(page, `字段结果 ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`daily-fields-result-${viewport.label}.png`),
        fullPage: true,
      });
      await page.getByRole("button", { name: "继续查看结果" }).click();
      await page
        .getByRole("table", { name: "最近检验" })
        .getByRole("cell", { name: "第 2 版", exact: true })
        .click();
      await expect(page.getByText("日线字段口径", { exact: true })).toHaveCount(0);
      tracked = true;
      await page.getByRole("button", { name: "刷新", exact: true }).click();
      await expect(page.getByRole("button", { name: "取消跟踪", exact: true })).toBeEnabled();
      await page.getByRole("button", { name: "取消跟踪", exact: true }).click();
      await expect(page.getByText("已取消跟踪。", { exact: true })).toBeVisible();
      expect(cancellation).toHaveLength(1);
      expect(cancellation[0]?.expected_head).toEqual({
        version: 4,
        content_sha256: "e".repeat(64),
      });
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
