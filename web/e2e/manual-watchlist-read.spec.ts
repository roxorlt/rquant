import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

type List = Schemas["Envelope_ManualWatchlistListData_"];
type Exact = Schemas["Envelope_ManualWatchlistExactData_"];

for (const width of [1440, 390]) {
  test(`手动名单与抽屉只读状态 ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 844 });
    const watcher = watch(page);
    const response = await page.request.get("api/v1/meta");
    expect(response.ok()).toBe(true);
    const meta = (await response.json()) as MetaEnvelope;
    const list: List = {
      serving: meta.serving,
      data: {
        availability: "ready",
        available_at: meta.data.generation?.built_at ?? meta.data.server_time,
        message: "",
        items: [
          {
            ts_code: "600001.SH",
            version: 2,
            source: "detail",
            price_levels: ["10.00"],
            expires_at: "2026-09-24T08:00:00Z",
            updated_at: "2026-09-24T07:30:00Z",
          },
        ],
      },
    };
    const exact: Exact = {
      serving: meta.serving,
      data: {
        availability: "ready",
        available_at: meta.data.generation?.built_at ?? meta.data.server_time,
        message: "",
        ts_code: "600001.SH",
        status: "active",
        version: 2,
        source: "detail",
        price_levels: ["10.00"],
        expires_at: "2026-09-24T08:00:00Z",
        updated_at: "2026-09-24T07:30:00Z",
      },
    };
    await page.route("**/api/v1/watchlist", (route) => route.fulfill({ json: list }));
    await page.route("**/api/v1/watchlist/600001.SH", (route) => route.fulfill({ json: exact }));
    await page.goto("./#/monitor");
    const section = page.getByRole("region", { name: "手动盯盘" });
    await expect(section).toContainText("600001.SH");
    await expect(section).toContainText("来自个股详情");
    await expect(section).not.toContainText("正在告警");
    await expectNoHorizontalOverflow(page, `manual watchlist ${width}px`);

    const stock = section.getByRole("button", { name: "查看 600001.SH 详情" });
    if (width === 1440) {
      await stock.focus();
      await expect(stock).toBeFocused();
      await stock.press("Enter");
    } else {
      await stock.click();
    }
    const drawer = page.getByRole("dialog", { name: /600001\.SH/ });
    await expect(drawer).toContainText("已加入盯盘");
    await expect(drawer.getByRole("button", { name: "加入盯盘" })).toHaveCount(0);
    await expect(drawer.getByRole("button", { name: "移出盯盘" })).toHaveCount(0);
    await expectNoHorizontalOverflow(page, `manual stock detail ${width}px`);
    await testInfo.attach(`manual-watchlist-${width}px`, {
      body: await page.screenshot({ fullPage: true }),
      contentType: "image/png",
    });
    expect(watcher.problems).toEqual([]);
  });
}
