import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import {
  storedTrackingCapability,
  storedTrackingFactor,
} from "../src/pages/factors/factorStoredTracking.fixture.ts";
import {
  trackingPanel,
  trackingResult,
  trackingSummary,
} from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`库存字段跟踪 ${viewport.label}`, () => {
    test.use({ hasTouch: viewport.label === "phone", isMobile: viewport.label === "phone" });
    test(`加入、换代重载的原请求恢复与来源缺失后取消在 ${viewport.label} 可完成`, async ({
      page,
    }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      let generation = metadata.serving.generation_id;
      let updated = false;
      let missing = false;
      let panel = trackingPanel();
      const requests: Schemas["FactorTrackingRequest"][] = [];
      const serving = () => ({ ...metadata.serving, generation_id: generation });
      const factor = () =>
        updated
          ? { ...storedTrackingFactor, version: 3, content_sha256: "c".repeat(64) }
          : storedTrackingFactor;
      const publish = (id: string) => {
        generation = id;
      };
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
              ...storedTrackingCapability,
              ...(missing
                ? { version: "daily_v1", fields: storedTrackingCapability.fields.slice(0, 6) }
                : {}),
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorCapabilitiesData_"],
        }),
      );
      await page.route(
        new RegExp(`/api/v1/factors/${storedTrackingFactor.factor_id}/tracking(?:\\?.*)?$`),
        (route) => {
          expect(new URL(route.request().url()).searchParams.get("generation_id")).toBe(generation);
          return route.fulfill({
            json: {
              data: panel,
              serving: serving(),
            } satisfies Schemas["Envelope_FactorTrackingPanel_"],
          });
        },
      );
      await page.route("**/api/v1/factors/run-availability", (route) =>
        route.fulfill({
          json: {
            data: {
              enabled: true,
              reason: null,
              pools: [{ selection: "all", label: "全市场", available: true, reason: null }],
              start_date: "2026-09-01",
              end_date: "2026-09-23",
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorRunAvailability_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: { availability: "empty", available_at: metadata.serving.built_at, results: [] },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultListData_"],
        }),
      );
      await page.route(
        /\/api\/v1\/factors\/tracking\/commands(?:\/(?:resume|retry))?$/,
        async (route) => {
          const body = route.request().postDataJSON() as Schemas["FactorTrackingRequest"];
          expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
          expect(
            await page.evaluate(
              () =>
                JSON.parse(localStorage.getItem("rquant.factor.tracking-operation.v1") ?? "null")
                  .request,
            ),
          ).toEqual(body);
          requests.push(body);
          const applied = !body.tracked || route.request().url().endsWith("/retry");
          const result = trackingResult(body, applied ? "applied" : "uncertain");
          if (!body.tracked && result.receipt) result.receipt.tracking_generation = "c".repeat(32);
          await route.fulfill({
            json: {
              data: result,
              serving: serving(),
            } satisfies Schemas["Envelope_FactorTrackingOperationResult_"],
          });
        },
      );

      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto(`./#/factors?factor_id=${storedTrackingFactor.factor_id}&panel=tracking`);
      const area = page.getByRole("region", { name: "因子跟踪", exact: true });
      await expect(area).toBeFocused();
      const strategy = area.getByText("跟踪策略", { exact: true });
      if (viewport.label === "phone") {
        expect(await page.evaluate(() => matchMedia("(hover: none)").matches)).toBe(true);
        await strategy.tap();
      } else await strategy.focus();
      await expect(page.getByRole("tooltip")).toContainText("18:40");
      await expect(page.getByRole("tooltip")).toContainText("5组 · RankIC · 无运行后中性化");
      if (viewport.label === "phone") await strategy.tap();
      const params = page.getByRole("region", { name: "检验参数" });
      await params.getByRole("combobox", { name: "分组数" }).selectOption("10");
      const join = page.getByRole("button", { name: "加入跟踪", exact: true });
      await expect(join).toBeEnabled();
      await join.focus();
      await page.keyboard.press("Enter");
      const dialog = page.getByRole("dialog", { name: "加入因子跟踪" });
      await expect(dialog).toContainText("库存价量 · 第 2 版");
      await expect(dialog).toContainText("全市场 · 每日 · 5组 · RankIC · 无运行后中性化");
      expect(requests).toHaveLength(0);
      await expect(dialog).toHaveCSS("opacity", "1");
      await expectNoHorizontalOverflow(page, `库存跟踪确认 ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`stored-tracking-confirm-${viewport.label}.png`),
        animations: "disabled",
      });
      await dialog.getByRole("button", { name: "确认加入" }).click();
      await expect(
        page.getByText("跟踪状态暂未确认，请保留本次操作。", { exact: true }),
      ).toBeVisible();
      expect(requests).toHaveLength(1);
      expect(Object.keys(requests[0] ?? {}).sort()).toEqual([
        "command_id",
        "expected_head",
        "expected_tracking_generation",
        "factor_id",
        "requested_at",
        "serving_generation_id",
        "tracked",
      ]);
      updated = true;
      missing = true;
      panel = trackingPanel({ definition_head: { version: 3, content_sha256: "c".repeat(64) } });
      publish("e".repeat(64));
      await page.reload();
      await expect(page.getByRole("button", { name: "用原请求重试跟踪" })).toBeEnabled();
      await expect.poll(() => requests.length).toBe(2);
      const retryPanel = page.waitForResponse(
        (response) =>
          requests.length === 3 &&
          new URL(response.url()).pathname ===
            new URL(`api/v1/factors/${storedTrackingFactor.factor_id}/tracking`, APP_URL)
              .pathname &&
          new URL(response.url()).searchParams.get("generation_id") === "e".repeat(64),
      );
      const retryMeta = page.waitForResponse(
        (response) =>
          requests.length === 3 &&
          new URL(response.url()).pathname === new URL("api/v1/meta", APP_URL).pathname,
      );
      await page.getByRole("button", { name: "用原请求重试跟踪" }).click();
      await Promise.all([retryPanel, retryMeta]);
      await expect(page.getByText("已保存，等待同步。", { exact: true })).toBeVisible();
      expect(requests).toEqual([requests[0], requests[0], requests[0]]);
      expect(requests[0]?.expected_head).toEqual({
        version: 2,
        content_sha256: storedTrackingFactor.content_sha256,
      });
      await expect(page.getByText("已加入跟踪。", { exact: true })).toHaveCount(0);
      panel = trackingPanel({
        availability: "tracked",
        status: "paused",
        tracked: true,
        tracking_generation: "b".repeat(32),
        summary: trackingSummary,
        actual_start_date: "2026-08-27",
        updated_at: "2026-09-24T07:31:00Z",
        reason: "定义已更新，请重新加入跟踪。",
      });
      publish("d".repeat(64));
      await page.locator(".ph-actions").getByRole("button", { name: "刷新", exact: true }).click();
      await expect(page.getByText("已加入跟踪。", { exact: true })).toBeVisible();
      await expect(page.getByRole("region", { name: "本次跟踪" })).toContainText("第 2 版");
      await page.getByRole("button", { name: "继续查看跟踪", exact: true }).click();
      await expect(page.getByRole("button", { name: "重新加入", exact: true })).toBeDisabled();
      await expect(page.getByRole("button", { name: "取消跟踪", exact: true })).toBeEnabled();
      await expect(area).toContainText("+7.89%");
      await expect(area).toContainText("2026-08-27");
      await area.scrollIntoViewIfNeeded();
      await expectNoHorizontalOverflow(page, `库存跟踪恢复 ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`stored-tracking-restored-${viewport.label}.png`),
        animations: "disabled",
      });
      const oldPanel = page.waitForResponse(
        (response) =>
          requests.length === 4 &&
          new URL(response.url()).pathname ===
            new URL(`api/v1/factors/${storedTrackingFactor.factor_id}/tracking`, APP_URL)
              .pathname &&
          new URL(response.url()).searchParams.get("generation_id") === "d".repeat(64),
      );
      const oldMeta = page.waitForResponse(
        (response) =>
          requests.length === 4 &&
          new URL(response.url()).pathname === new URL("api/v1/meta", APP_URL).pathname,
      );
      await page.getByRole("button", { name: "取消跟踪", exact: true }).click();
      await Promise.all([oldPanel, oldMeta]);
      await expect(page.getByText("已保存，等待同步。", { exact: true })).toBeVisible();
      await expect(page.getByText("已取消跟踪。", { exact: true })).toHaveCount(0);
      expect(requests[3]).toMatchObject({
        tracked: false,
        expected_tracking_generation: "b".repeat(32),
        expected_head: { version: 3, content_sha256: "c".repeat(64) },
      });
      panel = trackingPanel({
        tracking_generation: "c".repeat(32),
        definition_head: { version: 3, content_sha256: "c".repeat(64) },
      });
      publish("9".repeat(64));
      await page.locator(".ph-actions").getByRole("button", { name: "刷新", exact: true }).click();
      await expect(page.getByText("已取消跟踪。", { exact: true })).toBeVisible();
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
