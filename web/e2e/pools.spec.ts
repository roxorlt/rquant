import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} published pools`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("reads published rules and last members with keyboard and fits the page", async ({
      page,
    }) => {
      const watcher = watch(page);
      await page.goto("./#/pools");
      await expect(page.getByRole("heading", { level: 1, name: "池子画布" })).toBeVisible();
      const list = page.getByRole("group", { name: "池子列表" });
      await expect(list.getByRole("button")).toHaveCount(4);
      const condition = list.getByRole("button", { name: "查看 N 形态二池条件" });
      await condition.focus();
      await condition.press("Enter");
      await expect(condition).toHaveAttribute("aria-pressed", "true");
      const detail = page.getByRole("region", { name: "池子详情" });
      await expect(detail.getByRole("region", { name: "规则详情" })).toContainText("已发布");
      await expect(detail.getByRole("list", { name: "已发布条件" }).locator("li")).toHaveCount(3);
      await expect(detail).toContainText("使用父池前 2 个交易日内的成员");
      await expect(detail).toContainText("上次选股结果");
      await expect(page.getByText("上次结果与当前规则的对应关系尚未确认。")).toBeVisible();
      const pool = list.getByRole("button", { name: "查看 N 形态一池成员" });
      await pool.focus();
      await pool.press("Enter");
      await expect(pool).toHaveAttribute("aria-pressed", "true");
      await expect(page.getByRole("region", { name: "池子详情" })).toContainText("3 只");
      const graph = page.getByRole("group", { name: "已发布规则与池子" });
      await expect(graph.locator('.react-flow__node[data-id^="condition:"]')).toHaveCount(2);
      await expect(graph.locator(".react-flow__edge")).toHaveCount(3);
      if (viewport.name === "phone") {
        const bounds = await graph
          .locator('.react-flow__node[data-id="condition:n-shape-pool1"]')
          .boundingBox();
        expect(bounds?.width).toBeGreaterThanOrEqual(120);
      }
      const secondCondition = graph.locator('.react-flow__node[data-id="condition:n-shape-pool2"]');
      await secondCondition.focus();
      await secondCondition.press("Space");
      await expect(detail.getByRole("list", { name: "已发布条件" }).locator("li")).toHaveCount(3);
      const secondNode = graph.locator('.react-flow__node[data-id="n-shape-pool2"]');
      await secondNode.focus();
      await secondNode.press("Enter");
      await expect(page.getByRole("region", { name: "池子详情" })).toContainText("2 只");
      const firstNode = graph.locator('.react-flow__node[data-id="n-shape-pool1"]');
      await firstNode.focus();
      await expect(firstNode).toBeFocused();
      await firstNode.press("Space");
      await expect(page.getByRole("region", { name: "池子详情" })).toContainText("3 只");
      await page.screenshot({
        path: join(tmpdir(), `rquant-pools-${viewport.name}.png`),
        fullPage: true,
      });
      const member = page.getByRole("table", { name: "池子成员" }).locator("tbody tr").first();
      await member.focus();
      await member.press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      await expectNoHorizontalOverflow(page, "published pools");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
