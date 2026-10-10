import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const width of [1440, 390]) {
  test(`选股本页批量加入盯盘 ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 844 });
    const watcher = watch(page);
    const metaResponse = await page.request.get("api/v1/meta");
    expect(metaResponse.ok()).toBe(true);
    const meta = (await metaResponse.json()) as MetaEnvelope;
    let codes: string[] = [];
    const sent: Schemas["ManualWatchlistCommandRequest"][] = [];
    await page.route(
      /\/api\/v1\/screen\/query\/executions\/[^/?]+(?:\/results)?(?:\?.*)?$/,
      async (route) => {
        const response = await route.fetch();
        const body = (await response.json()) as Schemas["ScreenQueryReadData"];
        if (new URL(route.request().url()).pathname.endsWith("/results")) {
          if (!body.results) throw new Error("original screen results missing");
          body.results.rows = body.results.rows.slice(0, 2);
          codes = body.results.rows.map((row) => row.ts_code);
        } else {
          if (!body.execution) throw new Error("original screen execution missing");
          body.execution.total = 43;
        }
        await route.fulfill({ response, json: body });
      },
    );
    await page.route("**/api/v1/watchlist", (route) =>
      route.fulfill({
        json: {
          serving: meta.serving,
          data: {
            availability: "ready",
            available_at: meta.data.generation?.built_at ?? meta.data.server_time,
            message: "",
            items: [],
          },
        } satisfies Schemas["Envelope_ManualWatchlistListData_"],
      }),
    );
    await page.route(/\/api\/v1\/watchlist\/\d{6}\.(SH|SZ|BJ)$/, (route) => {
      const code = route.request().url().split("/").at(-1) ?? "";
      return route.fulfill({
        json: {
          serving: meta.serving,
          data: {
            availability: "ready",
            available_at: meta.data.generation?.built_at ?? meta.data.server_time,
            message: "",
            ts_code: code,
            status: "absent",
            version: null,
            source: null,
            price_levels: [],
            expires_at: null,
            updated_at: null,
          },
        } satisfies Schemas["Envelope_ManualWatchlistExactData_"],
      });
    });
    await page.route("**/api/v1/watchlist/commands", (route) => {
      const body = route.request().postDataJSON() as Schemas["ManualWatchlistCommandRequest"];
      sent.push(body);
      expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
      expect(body.source).toBe("screen_result");
      expect(body.action).toBe("add");
      expect(body.expected_version).toBeNull();
      return route.fulfill({
        status: sent.length === 1 ? 200 : 409,
        json: {
          command_id: body.command_id,
          ts_code: body.ts_code,
          action: "add",
          status: sent.length === 1 ? "saved_syncing" : "capacity",
          version: sent.length === 1 ? 1 : null,
          message: sent.length === 1 ? "已保存，正在同步。" : "盯盘名单已满。",
        } satisfies Schemas["ManualWatchlistCommandReceipt"],
      });
    });

    await page.goto("./#/screener");
    await page.getByRole("button", { name: "运行筛选" }).click();
    await expect(page.getByText("命中 43 只")).toBeVisible();
    const add = page.getByRole("button", { name: "加入本页 2 只" });
    await expect(add).toBeEnabled();
    if (width === 1440) {
      await add.focus();
      await expect(add).toBeFocused();
      await add.press("Enter");
    } else {
      await add.click();
    }
    const dialog = page.getByRole("dialog", { name: "加入本页 2 只" });
    await expect(dialog).toContainText("第 1 页");
    await expect(dialog).not.toContainText("43 只");
    await dialog.getByRole("button", { name: "确认加入本页 2 只" }).click();
    const summary = page.locator(".screen-watchlist-summary");
    await expect(summary).toContainText("已保存，正在同步 1");
    await expect(summary).toContainText("名单已满 1");
    await expect(summary).not.toContainText("已加入 1");
    expect(sent.map((body) => body.ts_code)).toEqual(codes);
    expect(findJargon(await page.locator("main").innerText())).toEqual([]);
    await expectNoHorizontalOverflow(page, `screen batch ${width}px`);
    await testInfo.attach(`screen-batch-${width}px`, {
      body: await page.screenshot({ fullPage: true }),
      contentType: "image/png",
    });
    expect([...watcher.problems].sort()).toEqual(
      [
        `HTTP 409: ${new URL("api/v1/watchlist/commands", APP_URL).href}`,
        "console error: Failed to load resource: the server responded with a status of 409 (Conflict)",
      ].sort(),
    );
  });
}
