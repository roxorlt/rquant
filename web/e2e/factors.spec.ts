import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { dailyCapability } from "../src/pages/factors/factorDailyFields.fixture.ts";
import rejectionContract from "../src/pages/factors/factorDiagnosticsRejection.fixture.json" with {
  type: "json",
};
import {
  trackedFactor,
  trackingPanel,
  trackingResult,
  trackingSummary,
} from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const neutralizationCases: Schemas["FactorRunNeutralizationOption"][] = [
  { neutralization: "none", label: "无", available: true, reason: null },
  { neutralization: "industry", label: "行业", available: true, reason: null },
  { neutralization: "industry_size", label: "行业 + 市值", available: true, reason: null },
];

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`因子跟踪 ${viewport.label}`, () => {
    test.use({ isMobile: viewport.label === "phone", hasTouch: viewport.label === "phone" });
    test(`因子跟踪原请求恢复、同代确认与取消在 ${viewport.label} 可完成`, async ({
      page,
    }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      metadata.data.viewer = "tester";
      const requests: Schemas["FactorTrackingRequest"][] = [];
      let panel = trackingPanel();
      const publish = (generationId: string) => {
        if (metadata.data.generation === null) throw new Error("合成夹具缺少数据记录");
        metadata.data.generation.generation_id = generationId;
        metadata.serving.generation_id = generationId;
      };
      await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: {
              ...dailyCapability,
              can_save: false,
              version: "daily_v1",
              fields: dailyCapability.fields.slice(0, 6),
            },
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorCapabilitiesData_"],
        }),
      );
      await page.route("**/api/v1/factors/definitions*", (route) =>
        route.fulfill({
          json: {
            data: {
              availability: "populated",
              available_at: metadata.serving.built_at,
              definitions: [trackedFactor],
              can_save: false,
              can_archive: true,
            },
            serving: metadata.serving,
          },
        }),
      );
      await page.route("**/api/v1/factors/*/tracking*", (route) =>
        route.fulfill({ json: { data: panel, serving: metadata.serving } }),
      );
      await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: { availability: "empty", available_at: metadata.serving.built_at, results: [] },
            serving: metadata.serving,
          },
        }),
      );
      await page.route(
        /\/api\/v1\/factors\/tracking\/commands(?:\/(?:resume|retry))?$/,
        async (route) => {
          const request = route.request().postDataJSON() as Schemas["FactorTrackingRequest"];
          expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
          expect(
            await page.evaluate(
              () =>
                JSON.parse(localStorage.getItem("rquant.factor.tracking-operation.v1") ?? "null")
                  .request,
            ),
          ).toEqual(request);
          requests.push(request);
          const accepted = !request.tracked || route.request().url().endsWith("/retry");
          const result = trackingResult(request, accepted ? "applied" : "uncertain");
          if (!request.tracked && result.receipt !== null && result.receipt !== undefined)
            result.receipt.tracking_generation = "c".repeat(32);
          await route.fulfill({ json: { data: result, serving: metadata.serving } });
        },
      );
      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto(`./#/factors?factor_id=${trackedFactor.factor_id}&panel=tracking`);
      const area = page.getByRole("region", { name: "因子跟踪", exact: true });
      await expect(area).toBeFocused();
      const strategy = area.getByText("跟踪策略", { exact: true });
      if (viewport.label === "phone") {
        expect(await page.evaluate(() => matchMedia("(hover: none)").matches)).toBe(true);
        await strategy.tap();
      } else await strategy.focus();
      await expect(page.getByRole("tooltip")).toContainText("18:40");
      await expect(page.getByRole("tooltip")).toContainText("5组 · RankIC · 无运行后中性化");
      const join = page.getByRole("button", { name: "加入跟踪", exact: true });
      if (viewport.label === "phone") await strategy.tap();
      await join.focus();
      await page.keyboard.press("Enter");
      const dialog = page.getByRole("dialog", { name: "加入因子跟踪" });
      await expect(dialog).toContainText("价量动量 · 第 2 版");
      await expect(dialog).toContainText("全市场 · 每日 · 5组 · RankIC · 无运行后中性化");
      expect(requests).toHaveLength(0);
      await expectNoHorizontalOverflow(page, `tracking confirm ${viewport.label}`);
      await expect(dialog).toBeVisible();
      await expect(dialog).toHaveCSS("opacity", "1");
      await page.screenshot({
        path: testInfo.outputPath(`factor-tracking-confirm-${viewport.label}.png`),
        fullPage: true,
        animations: "disabled",
      });
      await dialog.getByRole("button", { name: "确认加入" }).click();
      await expect(
        page.getByText("跟踪状态暂未确认，请保留本次操作。", { exact: true }),
      ).toBeVisible();
      expect(requests).toHaveLength(1);
      await page.reload();
      const retry = page.getByRole("button", { name: "用原请求重试跟踪", exact: true });
      await expect(retry).toBeEnabled();
      await expect.poll(() => requests.length).toBe(2);
      await retry.click();
      await expect(page.getByText("已保存，等待同步。", { exact: true })).toBeVisible();
      expect(requests).toEqual([requests[0], requests[0], requests[0]]);
      await expect(area).toContainText("本次跟踪尚未同步，请刷新状态。");
      await expect(page.getByRole("button", { name: "取消跟踪", exact: true })).toHaveCount(0);
      panel = trackingPanel({
        availability: "tracked",
        status: "active",
        tracked: true,
        tracking_generation: "b".repeat(32),
        summary: trackingSummary,
        actual_start_date: "2026-08-27",
        updated_at: "2026-09-24T07:31:00Z",
      });
      publish("e".repeat(64));
      await page.locator(".ph-actions").getByRole("button", { name: "刷新", exact: true }).click();
      await expect(page.getByText("已加入跟踪。", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "继续查看跟踪", exact: true }).click();
      await expect(area).toContainText("+1.23%");
      await expect(area).toContainText("−2.56%");
      await expect(area).toContainText("+7.89%");
      await expect(area).toContainText("20 / 20");
      await expect(area).toContainText("2026-08-27");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await expectNoHorizontalOverflow(page, `tracking active ${viewport.label}`);
      await page.screenshot({
        path: testInfo.outputPath(`factor-tracking-active-${viewport.label}.png`),
        fullPage: true,
        animations: "disabled",
      });
      await page.getByRole("button", { name: "取消跟踪", exact: true }).click();
      await expect(page.getByText("已保存，等待同步。", { exact: true })).toBeVisible();
      expect(requests[3]).toMatchObject({
        tracked: false,
        expected_tracking_generation: "b".repeat(32),
        expected_head: requests[0]?.expected_head,
      });
      expect(requests[3]?.command_id).not.toBe(requests[0]?.command_id);
      await expect(page.getByText("已取消跟踪。", { exact: true })).toHaveCount(0);
      panel = trackingPanel({ tracking_generation: "c".repeat(32) });
      publish("d".repeat(64));
      await page.locator(".ph-actions").getByRole("button", { name: "刷新", exact: true }).click();
      await expect(page.getByText("已取消跟踪。", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "继续查看跟踪", exact: true }).click();
      await expect(join).toBeEnabled();
      expect(watcher.problems).toEqual([]);
    });
  });
}

for (const viewport of [
  { width: 1440, height: 900, label: "desktop" },
  { width: 390, height: 844, label: "phone" },
]) {
  test.describe(`因子中性化 ${viewport.label}`, () => {
    test.use({ isMobile: viewport.label === "phone", hasTouch: viewport.label === "phone" });
    for (const option of neutralizationCases) {
      const title =
        option.neutralization === "none"
          ? `因子运行参数、确认、重载原请求恢复在 ${viewport.label} 可完成`
          : `因子${option.label}中性化确认与原请求恢复在 ${viewport.label} 可完成`;
      test(title, async ({ page }, testInfo) => {
        const watcher = watch(page);
        const metadata = (await (
          await page.request.get(new URL("api/v1/meta", APP_URL).toString())
        ).json()) as MetaEnvelope;
        metadata.data.viewer = "tester";
        const availability: Schemas["FactorRunAvailability"] = {
          enabled: true,
          reason: null,
          start_date: "2026-09-01",
          end_date: "2026-09-23",
          pools: [
            { selection: "all", label: "全市场（沪深非 ST）", available: true, reason: null },
            { selection: "gem", label: "创业板与科创板", available: true, reason: null },
            { selection: "hs300", label: "沪深300", available: false, reason: "缺少历史成分记录" },
            { selection: "zz1000", label: "中证1000", available: true, reason: null },
          ],
          ...(option.neutralization === "none" ? {} : { neutralizations: neutralizationCases }),
        };
        const requests: Schemas["FactorRunRequest"][] = [];
        await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
        await page.route("**/api/v1/factors/definitions*", (route) =>
          route.fulfill({
            json: {
              data: {
                availability: "populated",
                available_at: metadata.serving.built_at,
                definitions: definitions.map((item) => ({ ...item, category: "technical" })),
                can_save: false,
                can_archive: true,
              },
              serving: metadata.serving,
            },
          }),
        );
        await page.route("**/api/v1/factors/run-availability", (route) =>
          route.fulfill({ json: { data: availability, serving: metadata.serving } }),
        );
        await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
          route.fulfill({
            json: {
              data: { availability: "empty", available_at: metadata.serving.built_at, results: [] },
              serving: metadata.serving,
            },
          }),
        );
        await page.route(/\/api\/v1\/factors\/runs(?:\/(?:resume|retry))?$/, async (route) => {
          const request = route.request().postDataJSON() as Schemas["FactorRunRequest"];
          expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
          expect(
            await page.evaluate(
              () =>
                JSON.parse(localStorage.getItem("rquant.factor.run-operation.v1") ?? "null")
                  .request,
            ),
          ).toEqual(request);
          requests.push(request);
          if (option.neutralization !== "none") {
            availability.neutralizations = neutralizationCases.map((mode) => ({
              ...mode,
              available: mode.neutralization === "none",
              reason: mode.neutralization === "none" ? null : "缺少相应历史数据",
            }));
          }
          const submitted = route.request().url().endsWith("/retry");
          const result: Schemas["FactorRunOperationResult"] = {
            original_request: request,
            status: submitted ? "submitted" : "uncertain",
            reason: null,
            job_id: submitted ? "b".repeat(32) : null,
            spec_sha256: submitted ? "c".repeat(64) : null,
          };
          await route.fulfill({ json: { data: result, serving: metadata.serving } });
        });
        await page.setViewportSize({ width: viewport.width, height: viewport.height });
        await page.goto("./#/factors");
        const params = page.getByRole("region", { name: "检验参数" });
        await expect(params.getByRole("button", { name: "运行检验" })).toBeEnabled();
        await expect(params.getByRole("combobox", { name: "股票池" })).toHaveValue("all");
        await expect(params.getByRole("combobox", { name: "调仓周期" })).toHaveValue("5");
        await expect(params.getByRole("combobox", { name: "分组数" })).toHaveValue("5");
        await expect(params.getByRole("button", { name: "RankIC" })).toHaveAttribute(
          "aria-pressed",
          "true",
        );
        const tip = params.getByText("中性化说明");
        if (viewport.label === "phone") {
          expect(await page.evaluate(() => window.matchMedia("(hover: none)").matches)).toBe(true);
          await tip.tap();
        } else {
          await tip.focus();
        }
        await expect(page.getByRole("tooltip")).toContainText(
          option.neutralization === "none" ? "尚未开放" : "行业去除行业差异",
        );
        await params.getByRole("combobox", { name: "中性化" }).selectOption(option.neutralization);
        await params.getByRole("combobox", { name: "分组数" }).selectOption("10");
        const topRun = page.locator(".ph-actions").getByRole("button", { name: "运行检验" });
        await topRun.click();
        const dialog = page.getByRole("dialog", { name: "运行因子检验" });
        await expect(dialog).toContainText("价量动量 · 第 2 版");
        await expect(dialog).toContainText("2026-09-01 至 2026-09-23");
        await expect(dialog).toContainText("10 组 · RankIC");
        await expect(dialog).toContainText(`${option.label}中性化`);
        expect(requests).toHaveLength(0);
        await expectNoHorizontalOverflow(page, `factor run confirm ${viewport.label}`);
        await expect(dialog).toBeVisible();
        await expect(dialog).toHaveCSS("opacity", "1");
        await page.screenshot({
          path: testInfo.outputPath(`factor-run-confirm-${viewport.label}.png`),
          fullPage: true,
          animations: "disabled",
        });
        await dialog.getByRole("button", { name: "确认运行" }).click();
        await expect(
          page.getByText("检验结果暂未确认，请保留本次操作。", { exact: true }),
        ).toBeVisible();
        expect(requests).toHaveLength(1);
        await params.getByRole("combobox", { name: "中性化" }).selectOption("none");
        await expect(page.getByRole("region", { name: "本次检验" })).toContainText(
          `${option.label}中性化`,
        );
        await page.reload();
        await expect(page.getByRole("button", { name: "用原请求重试检验" })).toBeEnabled();
        await expect.poll(() => requests.length).toBe(2);
        await page.getByRole("button", { name: "用原请求重试检验" }).click();
        await expect(page.getByText("已提交，等待更新。", { exact: true })).toBeVisible();
        expect(requests).toEqual([requests[0], requests[0], requests[0]]);
        expect(requests[0]?.parameters).toMatchObject({
          selection: "all",
          holding_sessions: 5,
          group_count: 10,
          ic_method: "rank",
          neutralization: option.neutralization,
        });
        await expect(page.getByText("检验完成。", { exact: true })).toHaveCount(0);
        await expect(page.getByRole("region", { name: "本次检验" })).toContainText(
          `${option.label}中性化`,
        );
        await expect(page.getByRole("button", { name: "归档", exact: true })).toHaveCount(0);
        await expectNoHorizontalOverflow(page, `factor run recovered ${viewport.label}`);
        expect(findJargon(await page.locator("main").innerText())).toEqual([]);
        await page.screenshot({
          path: testInfo.outputPath(
            `factor-run-${option.neutralization}-recovered-${viewport.label}.png`,
          ),
          fullPage: true,
        });
        expect(watcher.problems).toEqual([]);
      });
    }
  });

  test(`FR-FINAL-01 真实拒绝回执重载后可改参并再次确认在 ${viewport.label} 可完成`, async ({
    page,
  }, testInfo) => {
    const watcher = watch(page);
    const original = rejectionContract.request as Schemas["FactorRunRequest"];
    const captured = rejectionContract.responses.submit;
    expect(captured.status).toBe(200);
    const rejected = captured.body as Schemas["Envelope_FactorRunOperationResult_"];
    expect(rejected.data.original_request).toEqual(original);
    expect(rejected.data.status).toBe("rejected");
    expect(rejected.data.job_id).toBeNull();
    expect(rejected.data.spec_sha256).toBeNull();
    const metadata = (await (
      await page.request.get(new URL("api/v1/meta", APP_URL).toString())
    ).json()) as MetaEnvelope;
    metadata.data.viewer = rejectionContract.actor;
    metadata.serving.generation_id = original.serving_generation_id;
    if (metadata.data.generation === null) throw new Error("合成数据代缺失");
    metadata.data.generation.generation_id = original.serving_generation_id;
    const factor: Schemas["FactorDefinitionItem"] = {
      factor_id: original.parameters.factor_id,
      name_zh: "源校验因子",
      category: "technical",
      category_label: "技术",
      direction: "higher_is_better",
      direction_label: "偏好高值",
      version: original.parameters.expected_head.version,
      content_sha256: original.parameters.expected_head.content_sha256,
      earliest_available_date: null,
      archived: false,
      expression: "ref(close, 2)",
      dependency_columns: ["close"],
      max_history_window: 2,
    };
    await page.addInitScript(
      ({ firstId, nextId, requestedAt }) => {
        Object.defineProperty(crypto, "randomUUID", {
          configurable: true,
          value: () => {
            const count = Number(sessionStorage.getItem("fr-final-01-uuid") ?? "0");
            sessionStorage.setItem("fr-final-01-uuid", String(count + 1));
            return count === 0 ? firstId : nextId;
          },
        });
        Date.prototype.toISOString = () => requestedAt;
      },
      {
        firstId: original.command_id,
        nextId: "66666666-6666-4666-8666-666666666666",
        requestedAt: original.requested_at,
      },
    );
    await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metadata }));
    await page.route("**/api/v1/factors/definitions*", (route) =>
      route.fulfill({
        json: {
          data: {
            availability: "populated",
            available_at: metadata.serving.built_at,
            definitions: [factor],
            can_save: false,
            can_archive: true,
          },
          serving: metadata.serving,
        },
      }),
    );
    await page.route(
      new RegExp(`/api/v1/factors/${factor.factor_id}/tracking(?:\\?.*)?$`),
      (route) => {
        expect(new URL(route.request().url()).searchParams.get("generation_id")).toBe(
          metadata.serving.generation_id,
        );
        return route.fulfill({
          json: {
            data: trackingPanel({
              factor_id: factor.factor_id,
              availability: "unavailable",
              status: "unavailable",
              definition_head: null,
              reason: "跟踪数据尚未发布。",
              can_set_tracked: false,
            }),
            serving: metadata.serving,
          } satisfies Schemas["Envelope_FactorTrackingPanel_"],
        });
      },
    );
    await page.route("**/api/v1/factors/run-availability", (route) =>
      route.fulfill({ json: { data: rejectionContract.availability, serving: metadata.serving } }),
    );
    await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
      route.fulfill({
        json: {
          data: { availability: "empty", available_at: metadata.serving.built_at, results: [] },
          serving: metadata.serving,
        },
      }),
    );
    const requests: Schemas["FactorRunRequest"][] = [];
    let resumed = 0;
    await page.route("**/api/v1/factors/runs/resume", (route) => {
      resumed += 1;
      return route.fulfill({ status: 500 });
    });
    await page.route(/\/api\/v1\/factors\/runs$/, async (route) => {
      const request = route.request().postDataJSON() as Schemas["FactorRunRequest"];
      expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
      expect(
        await page.evaluate(
          () =>
            JSON.parse(localStorage.getItem("rquant.factor.run-operation.v1") ?? "null").request,
        ),
      ).toEqual(request);
      requests.push(request);
      if (requests.length === 1) {
        expect(request).toEqual(original);
        // Replay the trusted factory→UDS→Web rejection body unchanged.
        await route.fulfill({ status: captured.status, json: captured.body });
      } else {
        const pending: Schemas["FactorRunOperationResult"] = {
          original_request: request,
          status: "pending",
          reason: null,
          job_id: null,
          spec_sha256: null,
        };
        await route.fulfill({ json: { data: pending, serving: metadata.serving } });
      }
    });
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.goto("./#/factors");
    const params = page.getByRole("region", { name: "检验参数" });
    await expect(params.getByRole("button", { name: "运行检验" })).toBeEnabled();
    await params
      .getByRole("combobox", { name: "股票池" })
      .selectOption(original.parameters.selection);
    await params
      .getByRole("combobox", { name: "调仓周期" })
      .selectOption(String(original.parameters.holding_sessions));
    await params
      .getByRole("combobox", { name: "分组数" })
      .selectOption(String(original.parameters.group_count));
    await params
      .getByRole("button", {
        name: original.parameters.ic_method === "rank" ? "RankIC" : "NormalIC",
      })
      .click();
    await params.getByLabel("开始日期").fill(original.parameters.start_date);
    await params.getByLabel("结束日期").fill(original.parameters.end_date);
    await params.getByRole("button", { name: "运行检验" }).click();
    await page
      .getByRole("dialog", { name: "运行因子检验" })
      .getByRole("button", { name: "确认运行" })
      .click();
    await expect(page.getByRole("button", { name: "修改检验参数" })).toBeEnabled();
    await expect(page.getByRole("region", { name: "本次检验" })).toContainText(
      rejected.data.reason ?? "",
    );
    await expect(page.getByRole("button", { name: "归档", exact: true })).toHaveCount(0);
    await page.reload();
    await expect(page.getByRole("button", { name: "修改检验参数" })).toBeEnabled();
    await expect(page.getByLabel("开始日期")).toHaveValue(original.parameters.start_date);
    expect(resumed).toBe(0);
    expect(requests).toEqual([original]);
    await expectNoHorizontalOverflow(page, `factor rejected ${viewport.label}`);
    await page.screenshot({
      path: testInfo.outputPath(`factor-run-rejected-${viewport.label}.png`),
      fullPage: true,
    });
    await page.getByRole("button", { name: "修改检验参数" }).click();
    await expect(page.getByRole("button", { name: "归档", exact: true })).toBeEnabled();
    expect(
      await page.evaluate(() => localStorage.getItem("rquant.factor.run-operation.v1")),
    ).toBeNull();
    await params.getByLabel("开始日期").fill(original.parameters.end_date);
    await params.getByRole("button", { name: "运行检验" }).click();
    const nextConfirmation = page.getByRole("dialog", { name: "运行因子检验" });
    await expect(nextConfirmation).toContainText(
      `第 ${original.parameters.expected_head.version} 版`,
    );
    await expect(nextConfirmation).toContainText(
      `${original.parameters.end_date} 至 ${original.parameters.end_date}`,
    );
    await expectNoHorizontalOverflow(page, `factor rejection correction ${viewport.label}`);
    await page.screenshot({
      path: testInfo.outputPath(`factor-run-rejection-correction-${viewport.label}.png`),
      fullPage: true,
    });
    expect(requests).toHaveLength(1);
    await nextConfirmation.getByRole("button", { name: "确认运行" }).click();
    await expect(page.getByText("等待检验。", { exact: true })).toBeVisible();
    expect(requests).toEqual([
      original,
      {
        ...original,
        command_id: "66666666-6666-4666-8666-666666666666",
        parameters: { ...original.parameters, start_date: original.parameters.end_date },
      },
    ]);
    await expect(page.getByRole("button", { name: "归档", exact: true })).toHaveCount(0);
    await expect(page.getByText("检验完成。", { exact: true })).toHaveCount(0);
    expect(findJargon(await page.locator("main").innerText())).toEqual([]);
    expect(watcher.problems).toEqual([]);
  });
}

const definitions = [
  {
    factor_id: "price_volume_factor",
    content_sha256: "a".repeat(64),
    name_zh: "价量动量",
    category_label: "技术",
    direction: "higher_is_better",
    direction_label: "偏好高值",
    version: 2,
    earliest_available_date: "2024-01-02",
    archived: false,
    expression: "ts_mean(close, 5) / ref(volume, 2)",
    dependency_columns: ["close", "volume"],
    max_history_window: 5,
  },
  {
    factor_id: "old_factor",
    content_sha256: "b".repeat(64),
    name_zh: "成交变化",
    category_label: "技术",
    direction: "lower_is_better",
    direction_label: "偏好低值",
    version: 1,
    earliest_available_date: "2025-03-04",
    archived: true,
    expression: "ts_mean(volume, 3)",
    dependency_columns: ["volume"],
    max_history_window: 3,
  },
];

test.beforeEach(async ({ page }) => {
  const metadata = (await (
    await page.request.get(new URL("api/v1/meta", APP_URL).toString())
  ).json()) as MetaEnvelope;
  await page.route("**/api/v1/factors/run-availability", (route) =>
    route.fulfill({
      json: {
        data: {
          enabled: false,
          reason: "尚未准备历史行情",
          pools: [],
          start_date: null,
          end_date: null,
        },
        serving: metadata.serving,
      },
    }),
  );
});

test("因子库桌面和手机列表详情可读、键盘选择且无横向溢出", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:31:00Z", definitions },
        serving,
      },
    }),
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/factors");
  const list = page.getByRole("table", { name: "因子列表" });
  await expect(list).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toContainText("价量动量");
  await page.screenshot({ path: "test-results/factors-desktop.png", fullPage: true });
  const archived = list.getByRole("row", { name: /成交变化/ });
  await archived.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("region", { name: "因子详情" })).toContainText("已归档");
  await expect(page.getByText("ts_mean(volume, 3)")).toBeVisible();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expect(page.getByRole("button", { name: "运行检验", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "加入跟踪", exact: true })).toBeDisabled();
  await expect(page.getByRole("region", { name: "因子跟踪" })).toContainText("跟踪数据尚未发布。");
  await expect(page.getByRole("region", { name: "检验参数" })).toContainText("尚未准备历史行情");
  await expectNoHorizontalOverflow(page, "factor desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(list).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "factor phone");
  await page.screenshot({ path: "test-results/factors-phone.png", fullPage: true });
  expect(watcher.problems).toEqual([]);
});

test("因子库可信空状态与换代错误不展示旧详情", async ({ page }) => {
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "empty", available_at: "2026-09-24T07:31:00Z", definitions: [] },
        serving,
      },
    }),
  );
  await page.goto("./#/factors");
  await expect(page.getByText("还没有因子")).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toHaveCount(0);
  await page.unroute("**/api/v1/factors/definitions*");
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:31:00Z", definitions },
        serving: { ...serving, generation_id: "f".repeat(64) },
      },
    }),
  );
  await page.reload();
  await expect(page.getByText("数据已更新，请重新查看因子。")).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toHaveCount(0);
});

test("归档确认、刷新续查及手机布局", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const metadata = await meta.json();
  const serving = metadata.serving;
  const nextGeneration = "e".repeat(64);
  let published = false;
  const commandIds: string[] = [];
  const trackingGenerations: string[] = [];
  await page.route(/\/api\/v1\/factors\/price_volume_factor\/tracking(?:\?.*)?$/, (route) => {
    const generationId = new URL(route.request().url()).searchParams.get("generation_id");
    expect(generationId).not.toBeNull();
    expect([serving.generation_id, nextGeneration]).toContain(generationId);
    trackingGenerations.push(generationId ?? "");
    // A query may already have captured the previous generation when publication arrives.
    const requestedServing = { ...serving, generation_id: generationId };
    return route.fulfill({
      json: {
        data: trackingPanel({
          availability: "unavailable",
          status: "unavailable",
          definition_head: null,
          reason: "跟踪数据尚未发布。",
          can_set_tracked: false,
        }),
        serving: requestedServing,
      } satisfies Schemas["Envelope_FactorTrackingPanel_"],
    });
  });
  await page.route("**/api/v1/meta", (route) =>
    route.fulfill({
      json: published
        ? {
            ...metadata,
            data: {
              ...metadata.data,
              generation: { ...metadata.data.generation, generation_id: nextGeneration },
            },
            serving: { ...serving, generation_id: nextGeneration },
          }
        : metadata,
    }),
  );
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: {
          availability: "populated",
          available_at: "2026-09-24T07:31:00Z",
          definitions: published
            ? [{ ...definitions[0], archived: true }, definitions[1]]
            : definitions,
          can_archive: true,
        },
        serving: published ? { ...serving, generation_id: nextGeneration } : serving,
      },
    }),
  );
  await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
    route.fulfill({
      json: {
        data: { availability: "unavailable", available_at: null, results: [] },
        serving: published ? { ...serving, generation_id: nextGeneration } : serving,
      },
    }),
  );
  await page.route(
    /\/api\/v1\/factors\/definitions\/price_volume_factor\/archive(?:\/resume)?$/,
    async (route) => {
      const body = route.request().postDataJSON() as { command_id: string };
      commandIds.push(body.command_id);
      published = route.request().url().endsWith("/resume");
      await route.fulfill({
        json: {
          data: {
            status: published ? "published" : "succeeded_waiting_publication",
            command_id: body.command_id,
            factor_id: "price_volume_factor",
            version: 2,
            content_sha256: "a".repeat(64),
            current_head_updated: false,
            message: published ? "已归档，历史记录仍会保留。" : "已提交，等待更新。",
          },
          serving: published ? { ...serving, generation_id: nextGeneration } : serving,
        },
      });
    },
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/factors");
  await page.getByRole("button", { name: "归档" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByText("归档当前定义，历史记录仍会保留。")).toBeVisible();
  await page.getByRole("button", { name: "确认归档" }).click();
  await expect(page.getByText("已提交，等待更新。")).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.screenshot({ path: "test-results/factors-archive-desktop.png", fullPage: true });
  await page.reload();
  await expect(page.getByText("已归档，历史记录仍会保留。")).toBeVisible();
  await expect.poll(() => trackingGenerations.includes(nextGeneration)).toBe(true);
  expect(commandIds).toHaveLength(2);
  expect(commandIds[0]).toBe(commandIds[1]);
  await page.setViewportSize({ width: 390, height: 844 });
  await expectNoHorizontalOverflow(page, "factor archive phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await page.screenshot({ path: "test-results/factors-archive-phone.png", fullPage: true });
  expect(watcher.problems).toEqual([]);
});

test("真实检验图表可切 IC、分组并用键盘打开明细；手机无横向溢出", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  const jobId = "1".repeat(32);
  const item = {
    job_id: jobId,
    factor_id: "price_volume_factor",
    factor_version: 2,
    factor_name_zh: "价量动量",
    definition_status: "current",
    status: "succeeded",
    status_label: "已完成",
    failure_message: null,
    display_status: "available",
    display_message: "结果已发布。",
    updated_at: "2026-09-24T07:30:00Z",
    as_of_time: "2026-09-23T07:00:00Z",
  };
  const summary = {
    status: "ok",
    mean: 0.0312,
    sample_std: 0.11,
    ir: 0.2836,
    positive_rate: 0.5,
    strong_signal_rate: 0.5,
    t_value: 0.4,
    p_value: 0.72,
    skewness: null,
    excess_kurtosis: null,
    source_day_count: 2,
    valid_day_count: 1,
    insufficient_day_count: 1,
    zero_variance_day_count: 0,
  };
  const research = {
    neutralization: "industry_size",
    neutralization_label: "行业 + 市值",
    context_basis_label: "行业按回顾口径评价，可能包含事后信息。",
    context_note: "缺少行业或市值的数据已排除。",
    basis_label: "收盘价至下次调仓收盘价",
    pool_label: "固定样本",
    return_price_basis: "raw",
    holding_sessions: 5,
    summary_status: "evaluated",
    ic_summary: { normal_ic: summary, rank_ic: { ...summary, mean: -0.0142 } },
    ic_points: [
      {
        decision_date: "2026-09-21",
        normal_ic: {
          status: "ok",
          value: 0.0312,
          source_sample_count: 4,
          effective_sample_count: 3,
        },
        rank_ic: {
          status: "ok",
          value: -0.0142,
          source_sample_count: 4,
          effective_sample_count: 3,
        },
        normal_ic_cumulative_sum: 0.0312,
        rank_ic_cumulative_sum: -0.0142,
      },
      {
        decision_date: "2026-09-22",
        normal_ic: {
          status: "insufficient_samples",
          value: null,
          source_sample_count: 4,
          effective_sample_count: 0,
        },
        rank_ic: {
          status: "insufficient_samples",
          value: null,
          source_sample_count: 4,
          effective_sample_count: 0,
        },
        normal_ic_cumulative_sum: null,
        rank_ic_cumulative_sum: null,
      },
    ],
    decay_periods: Array.from({ length: 10 }, (_, index) => ({
      lag: index + 1,
      status: index === 1 ? "no_valid_days" : "evaluated",
      ic_summary: index === 1 ? null : { normal_ic: summary, rank_ic: summary },
      source_day_count: 2,
      valid_pair_count: index === 1 ? 0 : 3,
    })),
    portfolio_status: "available_partial",
    portfolio_days: [
      {
        decision_at: "2026-09-21T07:00:00Z",
        decision_date: "2026-09-21",
        return_end_at: "2026-09-28T07:00:00Z",
        source_sample_count: 4,
        effective_sample_count: 3,
        groupings: [3, 5].map((count) => ({
          group_count: count,
          status: "ok",
          source_sample_count: 4,
          effective_sample_count: 3,
          long_short_return: 0.02,
          long_short_cumulative_spread: 0.02,
          groups: Array.from({ length: count }, (_, index) => ({
            group_number: index + 1,
            member_count: 1,
            period_return: index * 0.01,
            cumulative_return: index * 0.01,
            target_weight_turnover: index === 1 ? null : 0.25,
          })),
        })),
      },
    ],
    coverage_days: [
      {
        decision_date: "2026-09-21",
        status: "evaluated",
        coverage: {
          expected_count: 4,
          valid_count: 3,
          factor_missing_count: 1,
          return_missing_count: 0,
          factor_missing_by_reason: [],
          return_missing_by_reason: [],
        },
      },
      {
        decision_date: "2026-09-22",
        status: "no_samples",
        coverage: {
          expected_count: 4,
          valid_count: 0,
          factor_missing_count: 4,
          return_missing_count: 0,
          factor_missing_by_reason: [],
          return_missing_by_reason: [],
        },
      },
    ],
  };
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: {
          availability: "populated",
          available_at: "2026-09-24T07:30:00Z",
          definitions,
          can_archive: true,
        },
        serving,
      },
    }),
  );
  await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:30:00Z", results: [item] },
        serving,
      },
    }),
  );
  await page.route(/\/api\/v1\/factors\/results\/[0-9a-f]{32}(?:\?.*)?$/, (route) =>
    route.fulfill({
      json: {
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:30:00Z",
          result: item,
          research,
        },
        serving,
      },
    }),
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/factors");
  const area = page.getByRole("region", { name: "检验结果" });
  await expect(area.getByRole("img", { name: "IC 时序与累计 IC" })).toBeVisible();
  await expect(area.getByText("部分日期可计算")).toBeVisible();
  await expect(area.getByText("行业 + 市值中性化")).toBeVisible();
  await expect(area.getByText("有效 3 / 8")).toBeVisible();
  await area.getByText("行业口径").focus();
  await expect(page.getByRole("tooltip")).toContainText(research.context_basis_label);
  await area.getByText("样本说明").focus();
  await expect(page.getByRole("tooltip").filter({ hasText: research.context_note })).toBeVisible();
  await area.getByRole("button", { name: "RankIC" }).focus();
  await page.keyboard.press("Enter");
  await expect(area.getByRole("region", { name: "IC 统计" }).getByText("−0.0142")).toBeVisible();
  await area.getByRole("button", { name: "5 组" }).click();
  await expect(area.getByRole("button", { name: "5 组" })).toHaveAttribute("aria-pressed", "true");
  await area.getByText("查看 IC 明细").focus();
  await page.keyboard.press("Enter");
  await expect(area.getByRole("table", { name: "IC 明细" })).toBeVisible();
  await expect(area.getByRole("table", { name: "IC 明细" })).toContainText("—");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expect(page.getByRole("button", { name: "运行检验", exact: true })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "加入跟踪", exact: true })).toBeDisabled();
  await expect(page.getByRole("region", { name: "因子跟踪" })).toContainText("跟踪数据尚未发布。");
  await expectNoHorizontalOverflow(page, "factor research desktop");
  await page.screenshot({ path: "test-results/factors-research-desktop.png", fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  await expect(area.getByRole("img", { name: "分组累计收益" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "factor research phone");
  await page.screenshot({ path: "test-results/factors-research-phone.png", fullPage: true });
  expect(watcher.problems).toEqual([]);
});
