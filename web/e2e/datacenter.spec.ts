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
    await expect(page.getByRole("list", { name: "数据集" }).getByRole("button")).toHaveCount(23);
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
}
