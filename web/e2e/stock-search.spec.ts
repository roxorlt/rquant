import { expect, test } from "@playwright/test";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test("代码和名称可选中个股，抽屉显示最新价、池子与日 K", async ({ page }, testInfo) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/overview");
  const input = page.getByRole("combobox", { name: "搜索股票" });
  await input.fill("600001");
  await expect(page.getByRole("option", { name: /样本01.*600001\.SH/ })).toBeVisible();
  await input.press("ArrowDown");
  await input.press("Enter");
  let drawer = page.getByRole("dialog", { name: /样本01/ });
  await expect(drawer).toBeVisible();
  await expect(drawer).toContainText("8.80");
  await expect(drawer).toContainText("N 字一池");
  await expect(drawer.getByRole("img", { name: /日 K/ })).toBeVisible();
  await expect.poll(async () => (await drawer.boundingBox())?.x ?? 1440).toBeLessThan(1100);
  await page.screenshot({ path: testInfo.outputPath("stock-drawer-desktop.png") });
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();

  await input.fill("样本30");
  await page.getByRole("option", { name: /样本30.*600030\.SH/ }).click();
  drawer = page.getByRole("dialog", { name: /样本30/ });
  await expect(drawer).toContainText("暂无所在池子");
  await expect(drawer).toContainText("暂无日 K 数据");
  await expect(page).toHaveURL(/#\/overview$/);
  expect(watcher.problems).toEqual([]);
});

test("手机顶栏可搜索，个股抽屉保持在屏幕内", async ({ page }, testInfo) => {
  const watcher = watch(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("./#/panorama");
  const input = page.getByRole("combobox", { name: "搜索股票" });
  await expect(input).toBeVisible();
  await input.fill("600005");
  await page.getByRole("option", { name: /样本05.*600005\.SH/ }).click();
  const drawer = page.getByRole("dialog", { name: /样本05/ });
  await expect(drawer).toContainText("二池盯盘");
  await expect(drawer.getByRole("img", { name: /日 K/ })).toBeVisible();
  await expect.poll(async () => (await drawer.boundingBox())?.x ?? 390).toBeLessThan(5);
  await expectNoHorizontalOverflow(page, "stock drawer on phone");
  await page.screenshot({ path: testInfo.outputPath("stock-drawer-phone.png") });
  await page.keyboard.press("Escape");
  await expect(drawer).toBeHidden();
  expect(watcher.problems).toEqual([]);
});

test("搜索无结果与接口失败有明确状态，失败后可重试", async ({ page }) => {
  let failing = true;
  await page.route("**/api/v1/stocks/search**", async (route) => {
    if (failing) {
      await route.fulfill({ status: 500, contentType: "application/json", body: "{}" });
    } else {
      await route.continue();
    }
  });
  await page.goto("./#/overview");
  const input = page.getByRole("combobox", { name: "搜索股票" });
  await input.fill("600001");
  const error = page.getByRole("alert", { name: "搜索暂时无法加载" });
  await expect(error).toBeVisible();
  failing = false;
  await error.getByRole("button", { name: "重试" }).click();
  await expect(page.getByRole("option", { name: /样本01/ })).toBeVisible();
  await input.fill("没有这只");
  await expect(page.getByText("没有找到股票")).toBeVisible();
  await input.press("Escape");
  await expect(page.getByRole("listbox", { name: "股票搜索结果" })).toBeHidden();
});
