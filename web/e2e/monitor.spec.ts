import { expect, test } from "@playwright/test";
import type { MetaEnvelope } from "../src/api/client.ts";
import type { MonitorChannelsData } from "../src/api/endpoints.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const width of [1440, 390]) {
  test.describe(`告警时间线 ${width}px`, () => {
    test.use({ viewport: { width, height: 844 } });

    test("shows published records and opens a stock from the keyboard", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/monitor");
      await expect(page.getByRole("heading", { level: 1, name: "盯盘与告警" })).toBeVisible();
      if (!process.env.RQ_E2E_REPLAY_ROOT) {
        await expect(page.getByRole("region", { name: "推送通道状态" })).toContainText(
          "推送记录暂无法核对",
        );
      }
      const timeline = page.getByRole("list", { name: "告警时间线" });
      await expect(timeline).toBeVisible();
      const entries = timeline.locator(":scope > li");
      expect(await entries.count()).toBeGreaterThan(0);
      const legacyReplay = Boolean(process.env.RQ_E2E_LEGACY_NOTIFICATION);
      if (process.env.RQ_E2E_REPLAY_ROOT) {
        await expect(entries).toHaveCount(legacyReplay ? 5 : 3);
        if (legacyReplay) {
          await expect(entries.nth(0)).toContainText("通知记录");
          await expect(entries.nth(1)).toContainText("通知记录");
          await expect(entries.filter({ hasText: "提交成功" })).toHaveCount(1);
          await expect(entries.filter({ hasText: "提交失败" })).toHaveCount(1);
          await expect(entries.nth(0).getByRole("button", { name: /查看.+详情/ })).toHaveCount(0);
          const api = await page.request.get("api/v1/monitor/timeline");
          expect(api.ok()).toBe(true);
          expect(await api.text()).not.toContain("SECRET-CANARY");
          expect(await page.locator("body").innerText()).not.toContain("SECRET-CANARY");
          await entries.filter({ hasText: "提交成功" }).getByText("提交成功").hover();
          const submissionTip = page.getByRole("tooltip");
          await expect(submissionTip).toContainText("无法确认手机是否收到");
          await expect(submissionTip).not.toContainText("SECRET-CANARY");
          await page.mouse.move(0, 0);
        }
        await expect(entries.nth(legacyReplay ? 2 : 0)).toContainText("上攻突破");
        await expect(entries.nth(legacyReplay ? 3 : 1)).toContainText("爆量");
        await expect(entries.nth(legacyReplay ? 4 : 2)).not.toContainText("暂无回执");
        await expect(
          entries.nth(legacyReplay ? 4 : 2).locator('[aria-label="通知回执"]'),
        ).toHaveCount(1);
      }
      if (width === 390) {
        const receiptTextOverhang = await page
          .locator('[data-kpi="receipts"] .val')
          .evaluate((value) => {
            const fullText = document.createRange();
            fullText.selectNodeContents(value);
            const cell = value.closest(".kpi");
            if (!cell) throw new Error("Receipt KPI cell is missing");
            return fullText.getBoundingClientRect().right - cell.getBoundingClientRect().right;
          });
        expect(receiptTextOverhang).toBeLessThanOrEqual(0);
      }
      await expect(page.getByText("可向前翻看历史")).toHaveCount(0);
      await entries.first().locator(".monitor-event-time .tip-anchor").hover();
      await expect(page.getByRole("tooltip", { name: /2026-09-24/ })).toBeVisible();
      await page.mouse.move(5, 70);
      await expect(page.getByRole("tooltip", { name: /2026-09-24/ })).toHaveCount(0);
      await expect(
        page
          .getByRole("region", { name: "价格提醒规则" })
          .getByRole("button", { name: "新建规则" }),
      ).toBeDisabled();
      await expect(page.getByRole("button", { name: "确认" })).toHaveCount(0);
      await expectNoHorizontalOverflow(page, "monitor");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      const captureDir = process.env.RQ_E2E_CAPTURE_DIR;
      await test.info().attach(`monitor-${width}px`, {
        body: await page.screenshot({
          fullPage: true,
          path: captureDir ? `${captureDir}/monitor-${width}.png` : undefined,
        }),
        contentType: "image/png",
      });

      const stock = entries.nth(legacyReplay ? 2 : 0).getByRole("button", { name: /查看.+详情/ });
      await stock.focus();
      await expect(stock).toBeFocused();
      await stock.press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      await expectNoHorizontalOverflow(page, "monitor stock detail");
      expect(watcher.problems).toEqual([]);
    });

    test("keeps both verified channel cards readable and explains submission by keyboard", async ({
      page,
    }) => {
      const metaResponse = await page.request.get("api/v1/meta");
      expect(metaResponse.ok()).toBe(true);
      const meta = (await metaResponse.json()) as MetaEnvelope;
      const channels = {
        state: "ready",
        channels: [
          {
            channel: "pushdeer",
            channel_label: "PushDeer",
            today_submitted: 14,
            seven_day_attempts: 20,
            seven_day_submitted: 19,
            seven_day_success_pct: 95,
            last_success_at: "2026-09-24T07:30:00Z",
          },
          {
            channel: "pushplus",
            channel_label: "PushPlus",
            today_submitted: 0,
            seven_day_attempts: 0,
            seven_day_submitted: 0,
            seven_day_success_pct: null,
            last_success_at: null,
          },
        ],
      } satisfies MonitorChannelsData;
      await page.route("**/api/v1/monitor/channels", (route) =>
        route.fulfill({ json: { serving: meta.serving, data: channels } }),
      );
      await page.goto("./#/monitor");
      const section = page.getByRole("region", { name: "推送通道状态" });
      const deer = section.getByRole("article", { name: "PushDeer" });
      const plus = section.getByRole("article", { name: "PushPlus" });
      await expect(deer).toContainText("95.0%");
      await expect(plus).toContainText("近 7 日无提交记录");
      await deer.locator(".monitor-channel-main .tip-anchor").focus();
      await expect(page.getByRole("tooltip")).toContainText("不代表手机送达");
      await expectNoHorizontalOverflow(page, "channel status");
      for (const card of [deer, plus]) {
        expect(
          await card.evaluate((element) => element.scrollWidth - element.clientWidth),
        ).toBeLessThanOrEqual(0);
      }
      const captureDir = process.env.RQ_E2E_CAPTURE_DIR;
      await test.info().attach(`channels-${width}px`, {
        body: await page.screenshot({
          fullPage: true,
          path: captureDir ? `${captureDir}/channels-${width}.png` : undefined,
        }),
        contentType: "image/png",
      });
    });
  });
}
