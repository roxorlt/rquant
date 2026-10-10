import { expect, type Page, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { API_NOW } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const PATH = "**/api/v1/monitor/price-rules";
type Runtime = Schemas["Envelope_PriceAlertRuntimeData_"];
type Events = Schemas["Envelope_PriceAlertRecentEventsData_"];

async function fixtures(page: Page) {
  const meta = (await (await page.request.get("api/v1/meta")).json()) as MetaEnvelope;
  if (meta.serving.built_at === null) throw new Error("synthetic generation is missing");
  // Pin this routed business scenario independently of the shared API's elapsed clock.
  meta.serving = {
    ...meta.serving,
    state: "ready",
    age_seconds: (Date.parse(API_NOW) - Date.parse(meta.serving.built_at)) / 1_000,
    message: null,
  };
  meta.data.server_time = API_NOW;
  await page.route("**/api/v1/meta", (route) => route.fulfill({ json: meta }));
  const at = meta.data.server_time;
  const runtime: Runtime = {
    serving: meta.serving,
    data: {
      availability: "ready",
      generation_id: meta.serving.generation_id,
      status: "normal",
      status_label: "正常",
      message: "价格提醒已检查。",
      evaluated_at: at,
      quote_updated_at: at,
      applied_at: at,
      mode: "notification",
      items: [
        {
          rule_id: "fixture-rule",
          version: 1,
          membership_version: 1,
          status: "normal",
          status_label: "正常",
          message: "价格已达到阈值。",
          evaluated_at: at,
          last_triggered_at: at,
          next_allowed_at: new Date(Date.parse(at) + 300_000).toISOString(),
          state: "triggered",
        },
      ],
    },
  };
  const events: Events = {
    serving: meta.serving,
    data: {
      availability: "ready",
      generation_id: meta.serving.generation_id,
      message: "",
      items: [
        {
          event_id: "b".repeat(64),
          rule_id: "fixture-rule",
          rule_version: 1,
          membership_version: 1,
          rule_name: "突破提醒",
          ts_code: "600001.SH",
          comparison: "gte",
          threshold: "10.123456",
          price: "10.123456789012345678901",
          triggered_at: at,
          route_message: "",
          notifications: [
            {
              channel: "pushdeer",
              state: "admitted",
              label: "已准入发送",
              message: "可能仍会发送；尚无通知结果。",
              updated_at: at,
            },
          ],
        },
      ],
    },
  };
  const rules: Schemas["Envelope_PriceAlertRuleListData_"] = {
    serving: meta.serving,
    data: {
      availability: "ready",
      available_at: at,
      message: "",
      can_write: true,
      write_message: "",
      members: [{ ts_code: "600001.SH", version: 1, expires_at: null }],
      priority_options: [{ value: "P2", label: "普通" }],
      items: [
        {
          rule_id: "fixture-rule",
          version: 1,
          ts_code: "600001.SH",
          membership_version: 1,
          name: "突破提醒",
          priority: "P2",
          priority_label: "普通",
          enabled: true,
          comparison: "gte",
          threshold: "10.123456",
          valid_from: "09:30:00",
          valid_until: "14:57:00",
          updated_at: at,
          scope_status: "bound",
          status_label: "未运行",
          scope_message: "等待检查。",
        },
      ],
    },
  };
  return { meta, runtime, events, rules };
}

for (const width of [1440, 390]) {
  test.describe(`到价运行 ${width}px`, () => {
    test.use({
      viewport: { width, height: 844 },
      hasTouch: width === 390,
      isMobile: width === 390,
    });

    test("运行、最近提醒、准确价格提示和刷新期间保留编辑", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const values = await fixtures(page);
      const { meta } = values;
      let polls = 0;
      await page.clock.install({ time: new Date(meta.data.server_time) });
      await page.route(PATH, (route) => route.fulfill({ json: values.rules }));
      await page.route(`${PATH}/runtime`, (route) => {
        polls += 1;
        return route.fulfill({ json: values.runtime });
      });
      await page.route(`${PATH}/events`, (route) => route.fulfill({ json: values.events }));
      await page.goto("./#/monitor");
      const rules = page.getByRole("region", { name: "到价规则" });
      const recent = page.getByRole("region", { name: "最近到价提醒" });
      await expect(rules.getByRole("row", { name: /突破提醒/ })).toContainText("正常");
      await expect(recent).toContainText("最近 20 条");
      await expect(recent).toContainText("已准入发送");
      await expect(recent).not.toContainText("已送达");
      await expect(recent).not.toContainText("b".repeat(64));
      const price = recent.locator(".price-runtime-event-price .tip-anchor").first();
      if (width === 390) await price.tap();
      else await price.focus();
      await expect(page.getByRole("tooltip")).toContainText("完整报价 10.123456789012345678901");
      await rules.getByRole("button", { name: "编辑 突破提醒" }).click();
      const drawer = page.getByRole("dialog", { name: "编辑到价规则" });
      await drawer.getByLabel("规则名称").fill("正在编辑的草稿");
      await drawer.getByLabel("阈值价格").fill("10.123456789");
      await drawer.getByLabel("阈值价格").focus();
      const before = polls;
      await page.clock.fastForward(5_100);
      await expect.poll(() => polls).toBeGreaterThan(before);
      await expect(drawer.getByLabel("阈值价格")).toBeFocused();
      await expect(drawer.getByLabel("规则名称")).toHaveValue("正在编辑的草稿");
      await expect(drawer.getByLabel("阈值价格")).toHaveValue("10.123456789");
      await expectNoHorizontalOverflow(page, `price runtime editing ${width}px`);
      await drawer.getByRole("button", { name: "取消编辑" }).click();
      await page.getByRole("button", { name: "放弃修改" }).click();
      await expect(drawer).not.toBeVisible();
      await expect(rules.getByRole("button", { name: "编辑 突破提醒" })).toBeFocused();
      await expectNoHorizontalOverflow(page, `price runtime ${width}px`);
      await testInfo.attach(`price-runtime-${width}px`, {
        body: await page.screenshot({ fullPage: true }),
        contentType: "image/png",
      });
      expect(watcher.problems).toEqual([]);
    });

    test("新旧代冲突隐藏统计，正常等待与空记录不冒称通知成功", async ({ page }, testInfo) => {
      const values = await fixtures(page);
      values.runtime.data.status = "not_running";
      values.runtime.data.status_label = "未运行";
      values.runtime.data.message = "等待交易时段。";
      const original = values.runtime.data.items[0];
      if (!original) throw new Error("synthetic runtime lacks its rule");
      values.runtime.data.items[0] = {
        ...original,
        status: "not_running",
        status_label: "未运行",
        message: "等待交易时段。",
        last_triggered_at: null,
        next_allowed_at: null,
      };
      values.events.data.items = [];
      values.events.data.message = "等待交易时段。";
      await page.route(PATH, (route) => route.fulfill({ json: values.rules }));
      await page.route(`${PATH}/runtime`, (route) => route.fulfill({ json: values.runtime }));
      await page.route(`${PATH}/events`, (route) => route.fulfill({ json: values.events }));
      await page.goto("./#/monitor");
      const recent = page.getByRole("region", { name: "最近到价提醒" });
      await expect(recent).toContainText("暂未触发到价提醒");
      await expect(recent).toContainText("等待交易时段。");
      await expect(recent.locator('[data-state="crit"]')).toHaveCount(0);
      values.runtime.data.generation_id = "e".repeat(64);
      await recent.getByRole("button", { name: "刷新提醒" }).click();
      await expect(recent).toContainText("设置已更新，等待运行端同步。");
      await expect(recent).not.toContainText("暂未触发到价提醒");
      await expect(recent.locator(".price-runtime-events")).toHaveCount(0);
      await expectNoHorizontalOverflow(page, `price runtime generation change ${width}px`);
      await testInfo.attach(`price-runtime-waiting-${width}px`, {
        body: await page.screenshot({ fullPage: true }),
        contentType: "image/png",
      });
    });
  });
}
