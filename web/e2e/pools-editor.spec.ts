import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const VERSION = "b".repeat(64);

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} pool editor`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("previews a typed child rule, keeps both commands, and fits the viewport", async ({
      page,
    }) => {
      const watcher = watch(page);
      let posted = 0;
      await page.route("**/api/v1/pools", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolsData_"];
        body.data.canvases = [
          {
            name: "研究画布",
            description: "日终观察",
            pool_keys: ["n-shape-pool1"],
            refs_truncated: false,
          },
        ];
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/pools/editor", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolEditorData_"];
        body.data = {
          state: "ready",
          canvas_create_available: true,
          pools: [],
          copy_sources: [
            {
              key: "n-shape-pool1",
              display_name: "N 形态一池",
              description: "",
              version: "d".repeat(64),
              depends_on: null,
              delay_mode: "none",
              delay_days: 0,
              rule_calls: [{ name: "volume_ratio_gte", args: { n: 2 } }],
              include_columns: [],
              copyable: true,
              copy_block_reason: null,
            },
          ],
          canvases: [
            {
              command_id: "canvas-observe",
              record_hash: "e".repeat(64),
              name: "研究画布",
              description: "日终观察",
              version: "c".repeat(64),
              pool_refs: ["n-shape-pool1"],
            },
          ],
        };
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/screen/blocks", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_ScreenCatalogData_"];
        body.data.blocks = [
          {
            key: "volume_ratio_gte",
            label: "成交量放大",
            hint: "比近期更活跃",
            category: "indicator",
            category_label: "指标",
            parameters: [
              {
                key: "n",
                label: "放量倍数",
                input: "number",
                initial: 2,
                required: true,
                minimum: 1,
                maximum: 10,
                scale: 1,
                options: [],
                hint: null,
              },
            ],
          },
        ];
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/pools/editor/commands", async (route) => {
        const body = route.request().postDataJSON() as
          | Schemas["SavePoolCommand"]
          | Schemas["AttachPoolCommand"];
        const journal = await page.evaluate(() =>
          JSON.parse(sessionStorage.getItem("rquant.pool-editor-command.v1") ?? "{}"),
        );
        expect(body).toEqual(body.kind === "save_user_pool_v2" ? journal.save : journal.attach);
        posted += 1;
        await route.fulfill({
          json:
            body.kind === "save_user_pool_v2"
              ? {
                  command_id: body.command_id,
                  status: "succeeded",
                  message: "池子已保存",
                  pool_version: VERSION,
                }
              : {
                  command_id: body.command_id,
                  status: "succeeded",
                  message: "池子已加入当前画布",
                  pool_version: VERSION,
                  canvas_name: "研究画布",
                },
        });
      });
      await page.goto("./#/pools");
      const add = page.getByRole("button", { name: "添加条件节点" });
      await expect(add).toBeEnabled();
      await add.focus();
      await add.press("Enter");
      const drawer = page.getByRole("dialog", { name: "添加条件节点" });
      await expect(drawer).toBeVisible();
      await drawer.getByRole("textbox", { name: "池子名称" }).fill("放量观察");
      await drawer.getByRole("combobox", { name: "条件目录" }).selectOption("volume_ratio_gte");
      await drawer.getByRole("button", { name: "添加条件" }).click();
      await drawer.getByRole("spinbutton", { name: "放量倍数" }).fill("3");
      await drawer.getByRole("button", { name: "预览变更" }).click();
      await expect(drawer.getByRole("region", { name: "变更预览" })).toContainText("成交量放大");
      await expect(page.locator(".pools-editor-evidence")).toHaveCount(0);
      await page.screenshot({
        path: join(tmpdir(), `rquant-pool-editor-${viewport.name}-draft.png`),
      });
      await drawer.getByRole("button", { name: "保存并加入画布" }).click();
      await expect(drawer.getByText("池子已保存", { exact: true })).toBeVisible();
      await expect(drawer.getByText("加入请求已完成，等待画布更新")).toBeVisible();
      await expect(page.locator(".pools-editor-evidence")).toHaveCount(0);
      expect(posted).toBe(2);
      await expectNoHorizontalOverflow(page, `pool editor ${viewport.name}`);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(findJargon(await drawer.innerText())).toEqual([]);
      await page.screenshot({ path: join(tmpdir(), `rquant-pool-editor-${viewport.name}.png`) });
      await drawer.getByRole("button", { name: "返回画布" }).click();
      await expect(drawer).toBeHidden();
      await expect(add).toBeVisible();

      const condition = page.getByRole("button", { name: "查看 N 形态一池条件" });
      await condition.focus();
      await condition.press("Enter");
      const copy = page.getByRole("button", { name: "复制为自建池" });
      await expect(copy).toBeEnabled();
      await copy.click();
      const copyDrawer = page.getByRole("dialog", { name: "复制为自建池" });
      await expect(page.locator(".pools-editor-evidence")).toHaveCount(0);
      await expect(copyDrawer).not.toContainText("池子已保存");
      await expect(copyDrawer.getByRole("textbox", { name: "池子名称" })).toHaveValue(
        "N形态一池副本",
      );
      await expect(copyDrawer.getByRole("combobox", { name: "父池" })).toHaveValue("");
      await copyDrawer.getByRole("button", { name: "预览变更" }).click();
      await expect(copyDrawer.getByRole("region", { name: "变更预览" })).toContainText(
        "来自「N 形态一池」",
      );
      await copyDrawer.getByRole("button", { name: "保存并加入画布" }).click();
      await expect(copyDrawer.getByText("池子已保存", { exact: true })).toBeVisible();
      await expect(copyDrawer.getByText("加入请求已完成，等待画布更新")).toBeVisible();
      expect(posted).toBe(4);
      await expectNoHorizontalOverflow(page, `builtin copy ${viewport.name}`);
      expect(findJargon(await copyDrawer.innerText())).toEqual([]);
      await page.screenshot({
        path: join(tmpdir(), `rquant-pool-editor-${viewport.name}-copy.png`),
      });
      expect(watcher.problems).toEqual([]);
    });

    test("offers a keyboard exit for a saved conflict after reload", async ({ page }) => {
      const watcher = watch(page);
      let posted = 0;
      await page.addInitScript(() => {
        sessionStorage.setItem(
          "rquant.pool-editor-command.v1",
          JSON.stringify({
            schema: 1,
            save: {
              kind: "save_user_pool_v2",
              command_id: "save-before-reload",
              requested_at: "2026-09-27T07:00:00Z",
              base_name: "自建观察",
              display_name: "自建观察",
              description: "",
              depends_on: "n-shape-pool1",
              delay_days: 1,
              rule_calls: [{ name: "not_st", args: {} }],
              include_columns: [],
              expected_version: "a".repeat(64),
            },
            canvasName: null,
            saveVersion: null,
            saveStatus: "failed",
            saveConflict: true,
            attach: null,
            attachStatus: "idle",
          }),
        );
      });
      await page.route("**/api/v1/pools/editor/commands", async (route) => {
        posted += 1;
        await route.abort();
      });
      await page.goto("./#/pools");
      const banner = page.locator(".pools-editor-evidence");
      await expect(banner).toContainText("上次保存未完成");
      const end = banner.getByRole("button", { name: "结束本次编辑" });
      await end.focus();
      await end.press("Enter");
      await expect(banner).toHaveCount(0);
      expect(posted).toBe(0);
      await expectNoHorizontalOverflow(page, `conflict exit ${viewport.name}`);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(watcher.problems).toEqual([]);
    });
  });
}
