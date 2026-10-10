import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import type { MonitorChannelsData } from "../src/api/endpoints.ts";
import { formatCount } from "../src/format/number.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const nativeFactory = Boolean(process.env.RQ_E2E_MONITOR_NATIVE_FACTORY);

for (const width of [1440, 390]) {
  test.describe(`告警时间线 ${width}px`, () => {
    test.use({ viewport: { width, height: 844 }, hasTouch: width === 390 });

    test("shows published records and opens a stock from the keyboard", async ({ page }) => {
      test.skip(nativeFactory, "Uses the original September published-record fixture.");
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
      const time = entries.first().locator(".monitor-event-time .tip-anchor");
      if (width === 390) await time.tap();
      else await time.hover();
      await expect(page.getByRole("tooltip", { name: /2026-09-24/ })).toBeVisible();
      if (width === 390) await time.tap();
      else await page.mouse.move(5, 70);
      await expect(page.getByRole("tooltip", { name: /2026-09-24/ })).toHaveCount(0);
      await expect(timeline.getByRole("button", { name: "确认" })).toHaveCount(0);
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
      test.skip(nativeFactory, "This case only tests the old channel response presentation.");
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
      const submission = deer.locator(".monitor-channel-main .tip-anchor");
      if (width === 390) await submission.tap();
      else await submission.focus();
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

    test("reads the original builtin owners and actual merged ledger without response substitution", async ({
      page,
    }) => {
      test.skip(
        !nativeFactory,
        "Requires Root's original monitor pipeline, Serving publisher and trusted actor fixture.",
      );
      const watcher = watch(page);
      const metaResponse = await page.request.get("api/v1/meta");
      expect(metaResponse.ok()).toBe(true);
      const meta = (await metaResponse.json()) as MetaEnvelope;
      expect(meta.data.viewer).not.toBeNull();
      await page.clock.setFixedTime(new Date(meta.data.server_time));
      const response = await page.request.get("api/v1/monitor/runtime");
      expect(response.ok()).toBe(true);
      const runtime = (await response.json()) as Schemas["Envelope_MonitorRuntimeData_"];
      expect(runtime.serving.generation_id).toBe(meta.serving.generation_id);
      expect(runtime.data.state).toBe("ready");
      const builtins = runtime.data.builtins ?? [];
      const channels = runtime.data.channels ?? [];
      expect(builtins).toHaveLength(4);
      expect(channels.length).toBeGreaterThan(0);
      const timelineResponse = await page.request.get("api/v1/monitor/timeline?page_size=50");
      expect(timelineResponse.ok()).toBe(true);
      const original = (await timelineResponse.json()) as Schemas["Envelope_MonitorTimelineData_"];
      expect(original.serving.generation_id).toBe(runtime.serving.generation_id);
      const market = original.data.items.find(
        (item) => item.kind === "builtin" && item.subject === "market",
      );
      expect(market).toBeDefined();

      await page.goto("./#/monitor");
      await expect(page.getByRole("heading", { level: 1, name: "盯盘与告警" })).toBeVisible();
      const rules = page.getByRole("region", { name: "内置规则" });
      await expect(rules.getByRole("article")).toHaveCount(4);
      for (const fact of builtins) {
        const card = rules.getByRole("article", { name: fact.label, exact: true });
        await expect(card.locator("dd").first()).toHaveText(formatCount(fact.matched_count));
        const source = card.getByRole("button", { name: `${fact.label}来源说明` });
        if (width === 390) await source.tap();
        else await source.focus();
        await expect(source).toHaveAttribute("aria-describedby", /\S/);
        const tipId = await source.getAttribute("aria-describedby");
        if (tipId === null) throw new Error("The source explanation is not linked to its control.");
        const sourceTip = page.locator(`[role="tooltip"][id="${tipId}"]`);
        await expect(sourceTip).toHaveCount(1);
        await expect(sourceTip).toBeVisible();
        await expect(sourceTip).toContainText(fact.source_note);
        if (width === 390) await source.tap();
        else await source.blur();
        await expect(sourceTip).toHaveCount(0);
      }
      const current = page.getByRole("region", { name: "当前通道尝试" });
      for (const fact of channels) {
        const card = current.getByRole("article", {
          name: `${fact.channel_label}当前通知`,
          exact: true,
        });
        await expect(card).toBeVisible();
        expect(fact.logical_count).toBeGreaterThan(0);
        expect(fact.physical_requests).toBe(0);
        await expect(
          card
            .locator(".monitor-channel-facts > div")
            .filter({ has: page.locator("dt", { hasText: "逻辑通知" }) })
            .locator("dd"),
        ).toHaveText(formatCount(fact.logical_count));
        await expect(
          card
            .locator(".monitor-channel-facts > div")
            .filter({ has: page.locator("dt", { hasText: "成员尝试" }) })
            .locator("dd"),
        ).toHaveText(formatCount(fact.member_attempts));
        await expect(
          card
            .locator(".monitor-channel-facts > div")
            .filter({ has: page.locator("dt", { hasText: "实际请求" }) })
            .locator("dd"),
        ).toHaveText("0");
        await expect(card.locator(".monitor-channel-rate")).toHaveText("—");
      }
      const timeline = page.getByRole("list", { name: "告警时间线" });
      if (market?.kind === "builtin") {
        const item = timeline
          .locator(":scope > li")
          .filter({ hasText: market.event_label })
          .filter({ hasText: "全市场" })
          .first();
        await expect(item).toBeVisible();
        await expect(item.getByRole("button", { name: /查看.+详情/ })).toHaveCount(0);
      }
      const sourceUnavailable = original.data.items.filter(
        (item) =>
          "acknowledgment" in item &&
          item.acknowledgment !== undefined &&
          !item.acknowledgment.eligible,
      );
      for (const item of sourceUnavailable) {
        if (!("event_label" in item)) continue;
        const records = timeline.locator(":scope > li").filter({ hasText: item.event_label });
        if ((await records.count()) === 1)
          await expect(records.getByRole("button", { name: "确认" })).toHaveCount(0);
      }
      await expectNoHorizontalOverflow(page, "original monitor owners");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      const captureDir = process.env.RQ_E2E_CAPTURE_DIR;
      await test.info().attach(`monitor-original-${width}px`, {
        body: await page.screenshot({
          fullPage: true,
          path: captureDir ? `${captureDir}/monitor-original-${width}.png` : undefined,
        }),
        contentType: "image/png",
      });
      expect(watcher.problems).toEqual([]);
    });

    test("prepares and cancels the original mode request without committing or pushing", async ({
      page,
    }) => {
      test.skip(
        !process.env.RQ_E2E_MONITOR_CONTROL_READY,
        "Requires the original PageControl/operator with explicit signed synthetic Ops installation evidence; no real POST or Linux install is implied.",
      );
      const watcher = watch(page);
      const metaResponse = await page.request.get("api/v1/meta");
      expect(metaResponse.ok()).toBe(true);
      const meta = (await metaResponse.json()) as MetaEnvelope;
      await page.clock.setFixedTime(new Date(meta.data.server_time));
      const capabilitiesResponse = await page.request.get(
        `api/v1/tasks/control-capabilities?generation_id=${meta.serving.generation_id}`,
      );
      expect(capabilitiesResponse.ok()).toBe(true);
      const capabilities =
        (await capabilitiesResponse.json()) as Schemas["TaskControlCapabilitiesData"];
      expect(capabilities.notifier_mode.available).toBe(true);
      expect(capabilities.notifier_mode.can_request).toBe(true);
      const destination = capabilities.notifier_mode.mode === "live" ? "仅记录" : "正式推送";
      if (destination === "正式推送") expect(capabilities.notifier_mode.can_set_live).toBe(true);
      let commits = 0;
      page.on("request", (request) => {
        if (
          request.method() === "POST" &&
          new URL(request.url()).pathname.endsWith("/tasks/notifications/mode")
        )
          commits += 1;
      });
      await page.goto("./#/monitor");
      const switchMode = page.getByRole("button", { name: `切换为${destination}`, exact: true });
      await expect(switchMode).toBeEnabled();
      await switchMode.focus();
      const prepared = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          new URL(response.url()).pathname.endsWith("/tasks/notifications/mode/prepare"),
      );
      await switchMode.press("Enter");
      const actual = await prepared;
      expect(actual.ok()).toBe(true);
      const receipt = (await actual.json()) as Schemas["TaskControlCommandData"];
      expect(receipt.status).toBe("prepared");
      expect(receipt.original_request.kind).toBe("prepare_notifier_delivery_mode");
      if (receipt.original_request.kind === "prepare_notifier_delivery_mode") {
        expect(receipt.original_request.command_id).not.toBe(
          receipt.original_request.run.command_id,
        );
        expect(receipt.original_request.generation_id).toBe(meta.serving.generation_id);
      }
      expect(receipt.confirmation_id).toBeTruthy();
      expect(receipt.confirmation_expires_at).toBeTruthy();
      const dialog = page.getByRole("dialog", { name: "切换通知模式" });
      await expect(dialog).toBeVisible();
      await expect(dialog.getByRole("button", { name: "确认切换" })).toBeDisabled();
      await dialog.getByRole("textbox").fill(destination);
      await expect(dialog.getByRole("button", { name: "确认切换" })).toBeEnabled();
      await expectNoHorizontalOverflow(page, "original mode confirmation");
      await dialog.getByRole("button", { name: /取消/ }).click();
      await expect(dialog).toHaveCount(0);
      await expect(switchMode).toBeFocused();
      expect(commits).toBe(0);
      expect(watcher.problems).toEqual([]);
    });
  });
}
