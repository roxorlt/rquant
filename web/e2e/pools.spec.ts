import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} published pools`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("keeps verified rank order and score readable with keyboard", async ({ page }) => {
      const watcher = watch(page);
      let firstCode = "";
      await page.route("**/api/v1/pools", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolsData_"];
        const pool = body.data.pools.find((item) => item.key === "n-shape-pool1");
        if (!pool || pool.members.length !== 3) throw new Error("synthetic pool is incomplete");
        firstCode = pool.members[2]?.code ?? "";
        pool.members = pool.members.map((member, index) => ({
          ...member,
          rank_position: 3 - index,
          ranking_score: 85 + index * 5,
        }));
        pool.result = {
          state: "current_rules",
          status_label: "结果已按当前规则更新",
          trade_date: "2026-09-23",
          hit_count: pool.member_count,
        };
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.goto("./#/pools");
      const table = page.getByRole("table", { name: "池子成员" });
      await expect(table.getByRole("columnheader", { name: "名次" })).toBeVisible();
      if (viewport.name === "desktop") {
        await expect(table.getByRole("columnheader", { name: "评分" })).toBeVisible();
      }
      const rows = table.locator("tbody tr");
      await expect(rows.first()).toContainText(firstCode);
      await expect(rows.first()).toContainText("95.00");
      await rows.first().focus();
      await rows.first().press("ArrowDown");
      await expect(rows.nth(1)).toBeFocused();
      await rows.nth(1).press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      await expectNoHorizontalOverflow(page, "ranked pool members");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });

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
      await expect(detail.getByRole("region", { name: "上次选股结果" })).toContainText(
        "结果版本待确认",
      );
      const pool = list.getByRole("button", { name: "查看 N 形态一池成员" });
      await pool.focus();
      await pool.press("Enter");
      await expect(pool).toHaveAttribute("aria-pressed", "true");
      await expect(page.getByRole("region", { name: "池子详情" })).toContainText("3 只");
      await expect(page.getByText("暂无可信排名", { exact: true })).toHaveCount(1);
      const graph = page.getByRole("group", { name: "已发布规则与池子" });
      await expect(graph.locator('.flow-graph-node[data-id^="condition:"]')).toHaveCount(2);
      await expect(graph.locator(".flow-graph-edge")).toHaveCount(3);
      if (viewport.name === "phone") {
        const bounds = await graph
          .locator('.flow-graph-node[data-id="condition:n-shape-pool1"]')
          .boundingBox();
        expect(bounds?.width).toBeGreaterThanOrEqual(120);
      }
      const secondCondition = graph.locator('.flow-graph-node[data-id="condition:n-shape-pool2"]');
      await secondCondition.focus();
      await secondCondition.press("Space");
      await expect(detail.getByRole("list", { name: "已发布条件" }).locator("li")).toHaveCount(3);
      const secondNode = graph.locator('.flow-graph-node[data-id="n-shape-pool2"]');
      await secondNode.focus();
      await secondNode.press("Enter");
      await expect(page.getByRole("region", { name: "池子详情" })).toContainText("2 只");
      const firstNode = graph.locator('.flow-graph-node[data-id="n-shape-pool1"]');
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

    test("shows verified zero hits and a changed rule version with keyboard", async ({ page }) => {
      const watcher = watch(page);
      await page.route("**/api/v1/pools", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolsData_"];
        const first = body.data.pools.find((pool) => pool.key === "n-shape-pool1");
        const second = body.data.pools.find((pool) => pool.key === "n-shape-pool2");
        if (!first || !second) throw new Error("synthetic pools are missing");
        first.member_count = 0;
        first.members = [];
        first.steps = [];
        first.members_truncated = false;
        first.result = {
          state: "current_rules",
          status_label: "结果已按当前规则更新",
          trade_date: "2026-09-23",
          hit_count: 0,
          zero_hit_label: "该交易日没有符合条件的股票",
        };
        second.result = {
          state: "rules_changed",
          status_label: "规则已更新，等待下次选股",
          trade_date: "2026-09-23",
          hit_count: second.member_count,
        };
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.goto("./#/pools");
      const list = page.getByRole("group", { name: "池子列表" });
      const first = list.getByRole("button", { name: "查看 N 形态一池成员" });
      await first.focus();
      await first.press("Enter");
      const result = page.getByRole("region", { name: "上次选股结果" });
      await expect(result).toContainText("结果已按当前规则更新");
      await expect(result).toContainText("该交易日没有符合条件的股票");
      await expect(result).toContainText("0 只");
      await expect(result.locator(".pools-step")).toHaveCount(0);
      await expect(page.getByRole("table", { name: "池子成员" })).toHaveCount(0);
      const second = list.getByRole("button", { name: "查看 N 形态二池成员" });
      await second.focus();
      await second.press("Space");
      await expect(result).toContainText("规则已更新，等待下次选股");
      await expect(page.getByRole("region", { name: "规则详情" })).toContainText("已发布");
      await expectNoHorizontalOverflow(page, "pool receipts");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });

    test("shows an entry day and marks only the matching generation's daily candle", async ({
      page,
    }) => {
      const watcher = watch(page);
      let entryCode: string | null = null;
      let dailyGeneration: string | null = null;
      let factorChanged = false;
      await page.route("**/api/v1/pools", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolsData_"];
        const pool = body.data.pools.find((item) => item.key === "n-shape-pool1");
        const member = pool?.members[0];
        if (!pool || !member) throw new Error("synthetic pool member is missing");
        entryCode = member.code;
        member.entry_trade_date = "2026-09-22";
        member.entry_close = 10.25;
        member.gain_pct = 12.5;
        member.gain_through_date = "2026-09-23";
        member.entry_line_price = factorChanged ? null : 10.25;
        pool.gain_verified_count = 1;
        pool.gain_sample_avg_pct = 12.5;
        pool.result = {
          state: "current_rules",
          status_label: "结果已按当前规则更新",
          trade_date: "2026-09-23",
          hit_count: pool.member_count,
        };
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/panorama/stocks/*/daily", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_DailyData_"];
        if (entryCode !== null) body.data.ts_code = entryCode;
        body.data.bars = [
          {
            date: "2026-09-22",
            open: 10,
            high: 11,
            low: 9.8,
            close: 10.25,
            volume: 1000,
            ma5: null,
            ma10: null,
            ma20: null,
            provisional: false,
          },
          {
            date: "2026-09-23",
            open: 10.5,
            high: 11.5,
            low: 10.2,
            close: 11.3,
            volume: 1000,
            ma5: null,
            ma10: null,
            ma20: null,
            provisional: false,
          },
        ];
        if (dailyGeneration !== null) body.serving.generation_id = dailyGeneration;
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.goto("./#/pools");
      const table = page.getByRole("table", { name: "池子成员" });
      await expect(table.getByRole("columnheader", { name: "入池日", exact: true })).toBeVisible();
      await expect(table.getByRole("columnheader", { name: "入池后复权涨幅" })).toBeVisible();
      const row = table.locator("tbody tr").first();
      await expect(row).toContainText("2026-09-22");
      await expect(row).toContainText("12.50%");
      await expect(page.getByRole("region", { name: "上次选股结果" })).toContainText(
        "已核验样本平均",
      );
      await row.focus();
      await row.press("Enter");
      const drawer = page.getByRole("dialog");
      await expect(drawer.getByText("入池 · 2026-09-22")).toBeVisible();
      await expect(drawer.getByRole("img", { name: /日 K/ })).toBeVisible();
      await expect(drawer).toBeInViewport({ ratio: 0.98 });
      await page.screenshot({
        path: join(tmpdir(), `rquant-pool-entry-${viewport.name}.png`),
      });
      await expectNoHorizontalOverflow(page, "pool entry");
      factorChanged = true;
      await page.reload();
      const changedRow = page.getByRole("table", { name: "池子成员" }).locator("tbody tr").first();
      await changedRow.focus();
      await changedRow.press("Enter");
      await expect(page.getByRole("dialog").getByText("价格口径不同")).toBeVisible();
      await expectNoHorizontalOverflow(page, "changed factor");
      dailyGeneration = "another-generation";
      await page.reload();
      const nextRow = page.getByRole("table", { name: "池子成员" }).locator("tbody tr").first();
      await nextRow.focus();
      await nextRow.press("Enter");
      await expect(page.getByRole("dialog").getByRole("img", { name: /日 K/ })).toBeVisible();
      await expect(page.getByText("入池 · 2026-09-22")).toHaveCount(0);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
