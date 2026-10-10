import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW, REPLAY_ROOT } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test.beforeEach(async ({ page }) => {
  await page.clock.setFixedTime(new Date(Date.parse(API_NOW) + 20_000));
});

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} paper accounts`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("shows published money and switches accounts with keyboard", async ({ page }) => {
      const watcher = watch(page);
      await page.goto("./#/paper");
      await expect(page.getByRole("heading", { level: 1, name: "模拟盘" })).toBeVisible();
      const table = page.getByRole("table", { name: "模拟账户持仓" });
      await expect(table.locator("tbody tr:not(.pad)")).toHaveCount(2);
      await expect(page.getByRole("region", { name: "账户资产" })).toContainText("100,042.00");
      await expect(table).toContainText("可卖 100");
      if (viewport.name === "phone") {
        await expect(table.locator("tbody tr").first().locator(".paper-mobile-pnl")).toBeVisible();
      }
      await expectNoHorizontalOverflow(page, "paper account");
      expect(
        findJargon(await page.locator("main").innerText()),
        "paper page internal wording",
      ).toEqual([]);

      if (!REPLAY_ROOT) {
        const choices = page.getByRole("group", { name: "选择模拟账户" });
        await expect(choices.getByRole("button")).toHaveCount(2);
        const second = choices.getByRole("button", { name: "模拟账户 2" });
        await second.focus();
        await second.press("Enter");
        await expect(second).toHaveAttribute("aria-pressed", "true");
        await expect(page.getByRole("region", { name: "账户资产" })).toContainText("5,000.00");
        await expect(page.getByText("当前账户没有持仓")).toBeVisible();
        await expect(page.getByRole("table", { name: "模拟账户持仓" })).toHaveCount(0);
        await expectNoHorizontalOverflow(page, "cash-only account");
      }
      expect(watcher.problems).toEqual([]);
    });

    test("opens an actual fill and keeps instruction history with its account", async ({
      page,
    }) => {
      test.skip(Boolean(REPLAY_ROOT), "replayed data has no deterministic instruction window");
      await page.route("**/api/v1/paper/accounts", async (route) => {
        const response = await route.fetch();
        const envelope = (await response.json()) as Schemas["Envelope_PaperAccountsData_"];
        const first = envelope.data.accounts[0];
        if (!first) throw new Error("synthetic fixture has no paper account");
        envelope.data.history = {
          source_state: "ready",
          source_updated_at: "2026-09-24T07:31:00Z",
          source_note: null,
          account_id: first.account_id,
          total_orders: 2,
          has_more: false,
          newest_updated_at: "2026-09-24T07:30:00Z",
          oldest_updated_at: "2026-09-23T02:00:00Z",
          orders: [
            {
              order_id: "order-e2e-current",
              code: "600005.SH",
              name: "样本05",
              side: "BUY",
              side_label: "买入",
              order_type: "LIMIT",
              quantity: 300,
              filled_quantity: 100,
              average_fill_price: "12.3456",
              status: "PARTIALLY_FILLED",
              status_label: "部分成交",
              reject_reason: null,
              reject_message: null,
              created_at: "2026-09-24T01:30:00Z",
              updated_at: "2026-09-24T07:30:00Z",
              fills: [
                {
                  fill_id: "fill-e2e-current",
                  sequence: 1,
                  quantity: 100,
                  price: "12.3456",
                  commission: "1.2345",
                  transfer_fee: "0.10",
                  tax: "0",
                  total_fees: "1.3345",
                  executed_at: "2026-09-24T01:32:00Z",
                  persisted_at: "2026-09-24T01:32:01Z",
                },
              ],
            },
            {
              order_id: "order-e2e-older",
              code: "600001.SH",
              name: "样本01",
              side: "SELL",
              side_label: "卖出",
              order_type: "MARKET",
              quantity: 100,
              filled_quantity: 0,
              average_fill_price: null,
              status: "REJECTED",
              status_label: "未接受",
              reject_reason: "SUSPENDED",
              reject_message: "股票停牌",
              created_at: "2026-09-23T01:30:00Z",
              updated_at: "2026-09-23T02:00:00Z",
              fills: [],
            },
          ],
        };
        await route.fulfill({ response, json: envelope });
      });

      const watcher = watch(page);
      await page.goto("./#/paper");
      const today = page.getByRole("table", { name: "当日模拟指令" });
      await expect(today).toContainText("样本05");
      await expect(today).not.toContainText("样本01");
      const row = today.getByRole("row", { name: /样本05/ });
      await row.focus();
      await row.press("Enter");
      const detail = page.getByRole("dialog", { name: /样本05.*指令详情/ });
      await expect(detail).toContainText("12.35");
      await expect(detail).toContainText("1.33");
      await expect(detail).not.toContainText("12.3456");
      await expect(detail).not.toContainText("1.3345");
      await expectNoHorizontalOverflow(page, "paper instruction detail");
      await page.keyboard.press("Escape");
      await expect(detail).toHaveCount(0);

      await page.getByRole("button", { name: "最近指令" }).click();
      await expect(page.getByRole("table", { name: "最近模拟指令" })).toContainText("样本01");
      await page.getByRole("button", { name: "模拟账户 2" }).click();
      await expect(page.getByText("当前账户的指令记录尚未发布")).toBeVisible();
      await expect(page.getByRole("table", { name: "最近模拟指令" })).toHaveCount(0);
      await expectNoHorizontalOverflow(page, "cash account instruction history");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await expect(page.locator("body")).not.toContainText("order-e2e");
      expect(watcher.problems).toEqual([]);
    });
  });
}
