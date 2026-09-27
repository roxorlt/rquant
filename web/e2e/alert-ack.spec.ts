import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { metaEnvelope, monitorEnvelope, overviewEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const CUTOFF = "2026-09-24T05:10:00Z";
const READY: Schemas["UnacknowledgedSummary"] = {
  state: "ready",
  count: 12,
  count_as_of: CUTOFF,
  label: "12 条待确认",
  note: "已核对全部告警来源。",
};

async function useSyntheticApi(
  page: Page,
  summary: Schemas["UnacknowledgedSummary"],
): Promise<void> {
  const original = monitorEnvelope();
  const acknowledgments: Schemas["AlertAcknowledgmentView"][] = [
    { state: "unconfirmed", eligible: true, label: "待确认" },
    {
      state: "confirmed",
      eligible: false,
      alert_id: "2".repeat(64),
      confirmation_id: "confirmed-first",
      label: "已确认",
      confirmed_at: "2026-09-24T05:06:00Z",
    },
    { state: "historical", eligible: false, label: "历史告警", note: "启用前的告警" },
    { state: "unavailable", eligible: false, label: "确认状态暂不可用" },
  ];
  const monitor = monitorEnvelope({
    unacknowledged: summary,
    items: [
      ...original.data.items.map((item, index) => ({
        ...item,
        acknowledgment: acknowledgments[index],
      })),
      {
        kind: "notification",
        event_key: "notification:old",
        at: "2026-09-23T02:06:00Z",
        scene_label: "价位提醒",
        channel_label: "PushDeer",
        submitted: true,
        submission_label: "提交成功",
      },
    ],
  });
  await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metaEnvelope() }));
  await page.route(/\/api\/v1\/monitor\/timeline(?:\?|$)/, (route) =>
    route.fulfill({ json: monitor }),
  );
  await page.route("**/api/v1/overview", (route) =>
    route.fulfill({ json: overviewEnvelope({ unacknowledged: summary }) }),
  );
}

for (const width of [1440, 390]) {
  test.describe(`确认状态 ${width}px`, () => {
    test.use({ viewport: { width, height: 844 } });

    test("uses one count and cutoff on both pages, with four event states", async ({ page }) => {
      const watcher = watch(page);
      await page.clock.setFixedTime(new Date("2026-09-24T05:12:00Z"));
      await useSyntheticApi(page, READY);
      await page.goto("./#/monitor");
      const timeline = page.getByRole("list", { name: "告警时间线" });
      await expect(timeline).toBeVisible();
      const rows = timeline.locator(":scope > li");
      await expect(rows.nth(0).locator(".monitor-event-head .status")).toContainText("待确认");
      await expect(rows.nth(1).locator(".monitor-event-head .status")).toContainText("已确认");
      await expect(rows.nth(2).locator(".monitor-event-head .status")).toContainText("历史告警");
      await expect(rows.nth(3).locator(".monitor-event-head .status")).toContainText("暂不可用");
      await expect(rows.nth(4)).toContainText("通知记录");
      await expect(rows.nth(4)).not.toContainText("待确认");
      const confirmed = rows.nth(1).locator(".monitor-event-head .tip-anchor").last();
      await confirmed.focus();
      await expect(
        page.getByRole("tooltip").filter({ hasText: "2026-09-24 13:06:00" }),
      ).toBeVisible();
      await expect(page.getByRole("button", { name: /^确认$/ })).toHaveCount(0);
      const monitorCard = page.locator('[data-kpi="unacknowledged"]');
      await expect(monitorCard.locator(".val")).toContainText("12条");
      await expect(monitorCard.locator(".sub")).toContainText("截至");
      if (width === 390) {
        const cards = page.locator(".monitor-content .kpi");
        const first = await cards.nth(0).boundingBox();
        const third = await cards.nth(2).boundingBox();
        expect(third?.x).toBe(first?.x);
        expect(third?.y).toBeGreaterThan(first?.y ?? 0);
      }
      await expectNoHorizontalOverflow(page, "monitor alert acknowledgment");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);

      await page.goto("./#/overview");
      await expect(page.getByRole("table", { name: "最新信号" })).toBeVisible();
      const overviewCard = page.locator('[data-kpi="unacknowledged"]');
      await expect(overviewCard.locator(".val")).toContainText("12条");
      await expect(overviewCard.locator(".sub")).toContainText("截至");
      if (width === 390) {
        const strip = page.getByRole("region", { name: "今日关键数字" });
        const cards = strip.locator(".kpi");
        await expect(cards).toHaveCount(7);
        const first = await cards.first().boundingBox();
        const last = await cards.last().boundingBox();
        const bounds = await strip.boundingBox();
        if (!first || !last || !bounds) throw new Error("总览关键数字未显示");
        expect(last.x).toBe(first.x);
        expect(last.y).toBeGreaterThan(first.y);
        expect(last.width).toBeGreaterThan(first.width * 1.9);
        expect(Math.abs(last.x + last.width - (bounds.x + bounds.width - 1))).toBeLessThan(2);
        const captureDir = process.env.RQ_E2E_CAPTURE_DIR;
        if (captureDir) {
          await page.screenshot({
            fullPage: true,
            path: `${captureDir}/alert-ack-overview-390.png`,
          });
        }
      }
      await overviewCard.locator(".sub .tip-anchor").focus();
      await expect(
        page.getByRole("tooltip").filter({ hasText: "2026-09-24 13:10:00" }),
      ).toBeVisible();
      await expectNoHorizontalOverflow(page, "overview alert acknowledgment");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });

    test("shows an unknown count on both pages when source coverage is incomplete", async ({
      page,
    }) => {
      const watcher = watch(page);
      await useSyntheticApi(page, {
        state: "source_incomplete",
        count: null,
        count_as_of: null,
        label: "待确认数暂不可用",
        note: "告警来源尚未核对完整。",
      });
      await page.goto("./#/monitor");
      await expect(page.getByRole("list", { name: "告警时间线" })).toBeVisible();
      const monitorCard = page.locator('[data-kpi="unacknowledged"]');
      await expect(monitorCard.locator(".val")).toHaveText("—");
      await expect(monitorCard.locator(".sub")).toHaveText("数量未知");
      await expectNoHorizontalOverflow(page, "monitor unknown count");
      await page.goto("./#/overview");
      await expect(page.getByRole("table", { name: "最新信号" })).toBeVisible();
      const overviewCard = page.locator('[data-kpi="unacknowledged"]');
      await expect(overviewCard.locator(".val")).toHaveText("—");
      await expect(overviewCard.locator(".sub")).toHaveText("数量未知");
      await expectNoHorizontalOverflow(page, "overview unknown count");
      expect(watcher.problems).toEqual([]);
    });
  });
}

for (const width of [1440, 390]) {
  test.describe(`告警确认操作 ${width}px`, () => {
    test.use({ viewport: { width, height: 844 }, hasTouch: width === 390 });

    test("keeps the count until a matching new version confirms the event", async ({ page }) => {
      const watcher = watch(page);
      await page.clock.setFixedTime(new Date("2026-09-24T05:12:00Z"));
      const alertId = "a".repeat(64);
      const first = monitorEnvelope();
      const active = monitorEnvelope({
        unacknowledged: { ...READY, count: 3 },
        items: first.data.items.map((item, index) => ({
          ...item,
          acknowledgment:
            index === 0
              ? {
                  state: "unconfirmed" as const,
                  eligible: true,
                  alert_id: alertId,
                  label: "待确认",
                }
              : { state: "historical" as const, eligible: false, label: "历史告警" },
        })),
      });
      const nextGeneration = "c".repeat(64);
      const originalGeneration = active.serving.generation_id;
      if (!originalGeneration) throw new Error("synthetic data version is missing");
      let generation = originalGeneration;
      let confirmed = false;
      const requests: Schemas["AckCommandRequest"][] = [];
      await page.route("**/api/v1/meta", (route) =>
        route.fulfill({ json: metaEnvelope({ generationId: generation }) }),
      );
      await page.route(/\/api\/v1\/monitor\/timeline(?:\?|$)/, (route) =>
        route.fulfill({
          json: confirmed
            ? {
                ...active,
                serving: { ...active.serving, generation_id: nextGeneration },
                data: {
                  ...active.data,
                  unacknowledged: { ...READY, count: 2 },
                  items: active.data.items.map((item, index) =>
                    index === 0
                      ? {
                          ...item,
                          acknowledgment: {
                            state: "confirmed" as const,
                            eligible: false,
                            alert_id: alertId,
                            confirmation_id: "first-confirmation",
                            label: "已确认",
                          },
                        }
                      : item,
                  ),
                },
              }
            : active,
        }),
      );
      await page.route("**/api/v1/monitor/ack", async (route) => {
        const request = route.request();
        expect(request.headers()["x-rquant-csrf"]).toBe("1");
        const body = request.postDataJSON() as Schemas["AckCommandRequest"];
        requests.push(body);
        await route.fulfill({
          json: {
            command_id: body.command_id,
            status: "succeeded",
            confirmation_id: "first-confirmation",
            message: "已受理，正在同步",
          },
        });
      });
      await page.goto("./#/monitor");
      const timeline = page.getByRole("list", { name: "告警时间线" });
      await expect(timeline).toBeVisible();
      const button = timeline.getByRole("button", { name: /^确认$/ });
      if (width === 390) await button.tap();
      else {
        await button.focus();
        await page.keyboard.press("Enter");
      }
      await expect(timeline.locator(":scope > li").first()).toContainText("已受理，正在同步");
      await expect(page.locator('[data-kpi="unacknowledged"] .val')).toContainText("3条");
      expect(requests).toHaveLength(1);
      await expectNoHorizontalOverflow(page, "alert acknowledgment command");

      generation = nextGeneration;
      confirmed = true;
      await page.reload();
      await expect(timeline.locator(":scope > li").first()).toContainText("已确认");
      await expect(page.locator('[data-kpi="unacknowledged"] .val')).toContainText("2条");
      const captureDir = process.env.RQ_E2E_CAPTURE_DIR;
      if (width === 390 && captureDir) {
        await page.screenshot({
          fullPage: true,
          path: `${captureDir}/alert-ack-monitor-confirmed-390.png`,
        });
      }
      expect(requests).toHaveLength(1);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}

test("旧数据的确认持续被拒后，刷新到新数据才生成新请求", async ({ page }) => {
  const watcher = watch(page);
  const first = monitorEnvelope();
  const oldGeneration = first.serving.generation_id;
  if (!oldGeneration) throw new Error("synthetic data version is missing");
  const newGeneration = "c".repeat(64);
  const alertId = "a".repeat(64);
  const active = monitorEnvelope({
    unacknowledged: { ...READY, count: 1 },
    items: first.data.items.map((item, index) => ({
      ...item,
      acknowledgment:
        index === 0
          ? { state: "unconfirmed" as const, eligible: true, alert_id: alertId, label: "待确认" }
          : { state: "historical" as const, eligible: false, label: "历史告警" },
    })),
  });
  let generation = oldGeneration;
  const requests: Schemas["AckCommandRequest"][] = [];
  await page.route("**/api/v1/meta", (route) =>
    route.fulfill({ json: metaEnvelope({ generationId: generation }) }),
  );
  await page.route(/\/api\/v1\/monitor\/timeline(?:\?|$)/, (route) =>
    route.fulfill({
      json: { ...active, serving: { ...active.serving, generation_id: generation } },
    }),
  );
  await page.route("**/api/v1/monitor/ack", async (route) => {
    const body = route.request().postDataJSON() as Schemas["AckCommandRequest"];
    requests.push(body);
    await route.fulfill(
      body.generation_id === oldGeneration
        ? { status: 409, json: { detail: "数据已更新，请刷新告警时间线。" } }
        : {
            json: {
              command_id: body.command_id,
              status: "succeeded",
              confirmation_id: "first-confirmation",
              message: "已受理，正在同步",
            },
          },
    );
  });
  await page.goto("./#/monitor");
  const row = page.getByRole("list", { name: "告警时间线" }).locator(":scope > li").first();
  await row.getByRole("button", { name: "确认" }).click();
  await expect(row).toContainText("数据已更新");
  await expect(row.getByRole("button", { name: "重新确认" })).toHaveCount(0);
  await page.reload();
  await expect(row).toContainText("数据已更新");
  expect(requests).toHaveLength(1);

  generation = newGeneration;
  await page.reload();
  await row.getByRole("button", { name: "重新确认" }).click();
  await expect(row).toContainText("已受理，正在同步");
  expect(requests).toHaveLength(2);
  expect(requests[1]?.generation_id).toBe(newGeneration);
  expect(requests[1]?.command_id).not.toBe(requests[0]?.command_id);
  expect(watcher.problems).toHaveLength(2);
  expect(watcher.problems.every((problem) => problem.includes("409"))).toBe(true);
});
