import { expect, type Locator, type Page, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { dailyCapability, dailyResearch } from "../src/pages/factors/factorDailyFields.fixture.ts";
import {
  diagnosticFactor,
  diagnosticResult,
} from "../src/pages/factors/factorDiagnostics.fixture.ts";
import {
  minuteResearch,
  mixedMinuteCapability,
  mixedMinuteResearch,
} from "../src/pages/factors/factorMinuteFeature.fixture.ts";
import { stockResearch } from "../src/pages/factors/factorStockFeature.fixture.ts";
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
  test.describe(`分钟字段来源 ${viewport.label}`, () => {
    const phone = viewport.label === "phone";
    test.use({ hasTouch: phone, isMobile: phone });
    test("目录插入、固定时点与单位、缺因和跟踪详情按真实来源切换恢复", async ({
      page,
    }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      let generation = metadata.serving.generation_id;
      let rawOnly = false;
      let wrongCapability = false;
      const serving = () => ({ ...metadata.serving, generation_id: generation });
      const current = diagnosticResult({ factor_version: 5 });
      const minuteOnly = diagnosticResult({ job_id: "4".repeat(32), factor_version: 4 });
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
              : mixedMinuteCapability,
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
                factor_id: diagnosticFactor.factor_id,
                availability: "tracked",
                status: "waiting",
                tracked: true,
                tracking_generation: "a".repeat(32),
                definition_head: {
                  version: diagnosticFactor.version,
                  content_sha256: diagnosticFactor.content_sha256,
                },
                can_set_tracked: false,
                reason: "等待成熟数据。",
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
              results: [current, minuteOnly, stock, technical, stored],
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultListData_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/results\/[0-9a-f]{32}(?:\?.*)?$/, (route) => {
        const id = new URL(route.request().url()).pathname.split("/").at(-1);
        const result =
          [current, minuteOnly, stock, technical, stored].find((row) => row.job_id === id) ??
          current;
        const research =
          id === stored.job_id
            ? dailyResearch
            : id === technical.job_id
              ? technicalResearch
              : id === stock.job_id
                ? stockResearch
                : id === minuteOnly.job_id
                  ? minuteResearch
                  : mixedMinuteResearch;
        return route.fulfill({
          json: {
            data: { availability: "ready", available_at: serving().built_at, result, research },
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
      const tracking = page.getByRole("region", { name: "因子跟踪", exact: true });
      await expect(tracking.getByText("等待成熟数据。").first()).toBeVisible();
      await expect(tracking.locator('[data-state="idle"]')).toHaveCount(1);
      await expect(tracking.locator('[data-state="crit"]')).toHaveCount(0);
      await page.getByRole("button", { name: "编辑", exact: true }).click();
      const dialog = page.getByRole("dialog", { name: "编辑因子" });
      const search = dialog.getByRole("searchbox", { name: "搜索字段" });
      const expression = dialog.getByRole("textbox", { name: "表达式", exact: true });
      await dialog.getByRole("button", { name: "全部字段" }).click();
      await expect(dialog.getByRole("button", { name: /^插入/ })).toHaveCount(56);
      const show = async (anchor: Locator) => (phone ? anchor.tap() : anchor.focus());
      const hide = async (anchor: Locator) =>
        phone ? anchor.tap() : anchor.evaluate((e: HTMLElement) => e.blur());
      for (const [name, unit, words] of [
        ["15:00分钟成交额", "元", "缺该分钟不回退"],
        ["历史分钟观察日数", "观察日数", "0有效"],
        ["开盘段标记", "0 / 1", "缺目标分钟无值"],
        ["近5次成交额加速", "倍数", "不保证连续5分钟"],
        ["近10次成交额加速", "倍数", "不保证连续10分钟"],
      ]) {
        await search.fill(name ?? "");
        const info = dialog.getByRole("button", { name: `${name}说明` });
        await show(info);
        const tip = page.getByRole("tooltip").filter({ hasText: `单位：${unit}` });
        await expect(tip).toBeVisible();
        await expect(tip).toContainText(words ?? "");
        await expect(tip).not.toContainText("原核");
        await expectTipInViewport(page, tip);
        if (name === "15:00分钟成交额" || name === "近5次成交额加速")
          await page.screenshot({
            path: testInfo.outputPath(`minute-unit-${name}-${viewport.label}.png`),
          });
        await hide(info);
        await expect(tip).toBeHidden();
      }
      await search.fill("近5次成交额加速");
      await expression.fill("close + ");
      await dialog.getByRole("button", { name: "插入近5次成交额加速" }).click();
      await expect(expression).toHaveValue("close + signal_amount_accel_5m");
      await expect(expression).toBeFocused();
      await page.screenshot({ path: testInfo.outputPath(`minute-insert-${viewport.label}.png`) });
      await dialog.getByRole("button", { name: "关闭", exact: true }).last().click();
      const basis = page.getByText("分钟字段口径", { exact: true });
      await show(basis);
      const sourceTip = page.getByRole("tooltip").filter({ hasText: "精确15:00" });
      await expect(sourceTip).toContainText("下一交易日09:25");
      await expect(sourceTip).toContainText("不以14:59替代");
      await expect(sourceTip).toContainText("最多20个实际观察日");
      await expect(sourceTip).toContainText("不代表当时已知");
      await expectTipInViewport(page, sourceTip);
      await page.screenshot({ path: testInfo.outputPath(`minute-policy-${viewport.label}.png`) });
      await hide(basis);
      await expect(sourceTip).toBeHidden();
      const dailyBasis = page.getByText("日线字段口径", { exact: true });
      await show(dailyBasis);
      const dailyTip = page.getByRole("tooltip").filter({ hasText: "选股派生" });
      await expect(dailyTip).toContainText("历史推导：12项");
      await expect(dailyTip).toContainText("库存原值：4项");
      await expect(dailyTip).not.toContainText("库存原值：15项");
      await expectTipInViewport(page, dailyTip);
      await hide(dailyBasis);
      await page.getByText("查看字段覆盖", { exact: true }).click();
      const field = page.getByRole("combobox", { name: "覆盖字段" });
      const table = page.getByRole("table", { name: "字段覆盖", exact: true });
      await expect(field.locator("option")).toHaveCount(50);
      await field.selectOption("signal_opening_segment_amount");
      const nullValue = table.getByText("0 / 12", { exact: true }).first();
      await show(nullValue);
      const missing = page.getByRole("tooltip").filter({ hasText: "此口径不适用：10" });
      await expect(missing).toContainText("缺少15:00分钟：2");
      await expectTipInViewport(page, missing);
      await page.screenshot({ path: testInfo.outputPath(`minute-missing-${viewport.label}.png`) });
      await hide(nullValue);
      await expect(missing).toBeHidden();
      for (const column of ["hist_intraday_days_20d", "signal_opening_segment"]) {
        await field.selectOption(column);
        await expect(table.getByText("10 / 12", { exact: true })).toHaveCount(3);
        await expect(table.getByText("0", { exact: true }).first()).toBeVisible();
      }
      await field.selectOption("signal_rel_cum_amount_asof_20d");
      const ratioValue = table.getByText("9 / 12", { exact: true }).first();
      await show(ratioValue);
      const zero = page.getByRole("tooltip").filter({ hasText: "累计基准为零：1" });
      await expect(zero).toContainText("缺少15:00分钟：2");
      await expectTipInViewport(page, zero);
      await hide(ratioValue);
      const recent = page.getByRole("table", { name: "最近检验" });
      for (const version of [1, 2, 3]) {
        await recent.getByRole("cell", { name: `第 ${version} 版`, exact: true }).click();
        await expect(basis).toHaveCount(0);
        await expect(dailyBasis).toBeVisible();
      }
      await recent.getByRole("cell", { name: "第 4 版", exact: true }).click();
      await expect(dailyBasis).toHaveCount(0);
      await show(basis);
      await expect(sourceTip).toBeVisible();
      await expect(sourceTip).not.toContainText("库存原值");
      await expectTipInViewport(page, sourceTip);
      await page.screenshot({ path: testInfo.outputPath(`minute-only-${viewport.label}.png`) });
      await hide(basis);
      rawOnly = true;
      generation = "d".repeat(64);
      await page.reload();
      await expect(basis).toBeVisible();
      await page.getByText("查看字段覆盖", { exact: true }).click();
      await expect(field.locator("option")).toHaveCount(50);
      await field.selectOption("signal_minute_amount");
      await expect(table.getByText("10 / 12", { exact: true })).toHaveCount(3);
      await page.getByRole("button", { name: "编辑", exact: true }).click();
      await dialog.getByRole("searchbox").fill("近5次成交额加速");
      await expect(dialog.getByText("没有匹配的字段", { exact: true })).toBeVisible();
      await expect(dialog.getByRole("button", { name: /^插入/ })).toHaveCount(0);
      await expect(expression).toHaveValue("close + signal_amount_accel_5m");
      await dialog.getByRole("button", { name: "关闭", exact: true }).last().click();
      wrongCapability = true;
      await page.reload();
      await expect(page.getByRole("button", { name: "编辑", exact: true })).toHaveCount(0);
      await expect(page.getByRole("button", { name: /^插入/ })).toHaveCount(0);
      await show(basis);
      await expect(sourceTip).toBeVisible();
      await expect(sourceTip).toContainText("前一交易日精确15:00");
      await expectTipInViewport(page, sourceTip);
      await expectNoHorizontalOverflow(page, `分钟来源恢复 ${viewport.label}`);
      await page.screenshot({ path: testInfo.outputPath(`minute-restored-${viewport.label}.png`) });
      await hide(basis);
      await expect(sourceTip).toBeHidden();
      expect(findJargon(await page.locator("body").innerText())).toEqual([]);
      expect(await page.locator("body").innerText()).not.toMatch(
        /minute_features_derived|daily_minute_v1|implementation_sha256/,
      );
      expect(watcher.problems).toEqual([]);
    });
  });
}
