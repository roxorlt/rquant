import { expect, test } from "@playwright/test";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

test("read-only pool map keeps nodes operable on desktop and phone", async ({ page }, testInfo) => {
  const serving = metaEnvelope().serving;
  const formula = "CLOSE>MA(CLOSE,2)";
  let poolCount = 1;
  await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metaEnvelope() }));
  await page.route("**/api/v1/pools", (route) =>
    route.fulfill({
      json: {
        data: {
          state: "ready",
          latest_trade_date: "2026-09-24",
          definitions_available: true,
          rules_available: false,
          canvases: [],
          canvases_truncated: false,
          pools: [],
          pools_truncated: false,
        },
        serving,
      },
    }),
  );
  await page.route("**/api/v1/pools/formula", (route) =>
    route.fulfill({
      json: {
        data: {
          availability: "ready",
          available_at: "2026-09-24T07:31:00Z",
          message: "",
          pools: Array.from({ length: poolCount }, (_, index) => ({
            pool_name: `user/趋势池${index + 1}`,
            display_name: `趋势池${index + 1}`,
            formula,
            syntax_version: "tdx-v1",
            created_at: "2026-09-24T07:31:00Z",
            status_label: "尚未运行",
            version: "f".repeat(64),
            latest_result: null,
          })),
        },
        serving,
      },
    }),
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/pools");
  const condition = page.locator(".flow-graph-node.condition");
  await expect(condition).toBeVisible();
  await condition.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("region", { name: "公式条件" })).toContainText(formula);
  await expect(page.locator(".flow-graph-edge")).toHaveCount(1);
  await expect(page.locator(".react-flow__attribution")).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "formula pool map desktop");
  await page.screenshot({
    path: testInfo.outputPath("rquant-formula-pool-map-desktop.png"),
    fullPage: true,
  });
  await page.setViewportSize({ width: 390, height: 844 });
  await page.reload();
  const phoneNode = page.locator(".flow-graph-node.condition");
  const phoneGraph = page.getByRole("group", { name: "已发布池子；关系尚未发布" });
  expect((await phoneGraph.boundingBox())?.height).toBeLessThan(340);
  await phoneNode.scrollIntoViewIfNeeded();
  await phoneNode.click();
  await expect(page.getByRole("region", { name: "公式条件" })).toContainText(formula);
  await expectNoHorizontalOverflow(page, "formula pool map phone");
  await page.screenshot({
    path: testInfo.outputPath("rquant-formula-pool-map-phone.png"),
    fullPage: true,
  });

  poolCount = 8;
  await page.reload();
  const crowdedGraph = page.getByRole("group", { name: "已发布池子；关系尚未发布" });
  await expect(crowdedGraph.locator(".flow-graph-node")).toHaveCount(16);
  const bounds = await crowdedGraph.evaluate((element) => ({
    clientWidth: element.clientWidth,
    clientHeight: element.clientHeight,
    scrollWidth: element.scrollWidth,
    scrollHeight: element.scrollHeight,
  }));
  expect(bounds.clientHeight).toBeLessThanOrEqual(480);
  expect(bounds.scrollWidth > bounds.clientWidth || bounds.scrollHeight > bounds.clientHeight).toBe(
    true,
  );
  const lastCondition = crowdedGraph.locator(".flow-graph-node.condition").last();
  await lastCondition.focus();
  await lastCondition.press("Enter");
  await expect(lastCondition).toHaveAttribute("aria-pressed", "true");
  await expectNoHorizontalOverflow(page, "crowded formula pool map phone");
});
