import { expect, test } from "@playwright/test";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test(`${viewport.name} finds a dataset and checks its fields`, async ({ page }) => {
    const observer = watch(page);
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.goto("./#/datacenter");
    await expect(page.getByRole("heading", { level: 1, name: "数据中心" })).toBeVisible();
    await expect(page.getByRole("list", { name: "数据集" }).getByRole("button")).toHaveCount(24);
    await expectNoHorizontalOverflow(page, "data directory");

    const daily = page.getByRole("button", { name: /股票日线/ });
    await daily.focus();
    await page.keyboard.press("Enter");
    const fields = page.getByRole("table", { name: "字段字典" });
    await expect(fields).toBeVisible();
    await expect(fields).toContainText("涨跌幅");
    await expect(page.getByText("下一交易日可见")).toBeVisible();
    await expect(page.getByText("样例数据尚未发布")).toBeVisible();
    await page.getByRole("searchbox", { name: "搜索字段" }).fill("pct_chg");
    await expect(fields.locator("tbody tr:not(.pad)")).toHaveCount(1);
    await expect(fields).toContainText("DOUBLE");
    await expect(fields).toContainText("%");
    await expectNoHorizontalOverflow(page, "field dictionary");

    if (viewport.name === "phone") {
      await page.getByRole("button", { name: "返回目录" }).click();
      await expect(page.getByRole("list", { name: "数据集" })).toBeVisible();
      await expectNoHorizontalOverflow(page, "mobile directory return");
    }
    expect(observer.problems).toEqual([]);
  });

  test(`${viewport.name} shows only reviewed sample columns`, async ({ page }) => {
    const observer = watch(page);
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.route("**/api/v1/data/catalog/daily_bar", async (route) => {
      const upstream = await route.fetch();
      const response = await upstream.json();
      response.data.sample_fields = response.data.fields.filter((field: { key: string }) =>
        ["ts_code", "trade_date", "pct_chg"].includes(field.key),
      );
      response.data.sample_available = true;
      response.data.sample = {
        state: "available",
        rows: [
          {
            ts_code: "000001.SZ",
            trade_date: "2026-09-25",
            pct_chg: 3.14,
            source_file: "/private/secrets/a.json",
            conflict_reason: "notifier.admin.shadow.v1",
            snapshot_hash: "a".repeat(64),
          },
        ],
      };
      await route.fulfill({ response: upstream, json: response });
    });
    await page.route("**/api/v1/data/catalog/stock_suspend_coverage", async (route) => {
      const upstream = await route.fetch();
      const response = await upstream.json();
      response.data.sample_fields = response.data.fields.filter((field: { key: string }) =>
        ["trade_date", "row_count", "queried_at"].includes(field.key),
      );
      response.data.sample_available = true;
      response.data.sample = {
        state: "available",
        rows: [
          {
            trade_date: "2026-09-25",
            row_count: 1234,
            queried_at: "2026-09-25T02:00:00+00:00",
            source: "SVCINTERNAL",
          },
        ],
      };
      await route.fulfill({ response: upstream, json: response });
    });
    await page.goto("./#/datacenter");
    await page.getByRole("button", { name: /股票日线/ }).click();
    const sample = page.getByRole("table", { name: "样例数据" });
    await expect(sample).toBeVisible();
    await expect(sample).toContainText("000001.SZ");
    await expect(sample).toContainText("3.14%");
    const body = await page.locator("main").innerText();
    for (const secret of [
      "source_file",
      "conflict_reason",
      "/private/secrets/a.json",
      "notifier.admin.shadow.v1",
      "a".repeat(64),
    ]) {
      expect(body).not.toContain(secret);
    }
    await expectNoHorizontalOverflow(page, "sample table");
    if (viewport.name === "phone") {
      await page.getByRole("button", { name: "返回目录" }).click();
    }
    await page.getByRole("button", { name: /停复牌采集记录/ }).click();
    const coverageSample = page.getByRole("table", { name: "样例数据" });
    await expect(coverageSample).toContainText("1,234");
    await expect(coverageSample).toContainText("2026-09-25 10:00:00");
    expect(await page.locator("main").innerText()).not.toContain("SVCINTERNAL");
    await expectNoHorizontalOverflow(page, "Shanghai sample time");
    if (viewport.name === "phone") {
      await page.getByRole("button", { name: "返回目录" }).click();
      await expect(page.getByRole("list", { name: "数据集" })).toBeVisible();
    }
    expect(observer.problems).toEqual([]);
  });
}
