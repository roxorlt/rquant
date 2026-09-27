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
          pools: [],
          canvases: [
            {
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
      await expect(drawer).toContainText("已加入当前画布");
      await expect(page.locator(".pools-editor-evidence")).toContainText("池子已保存");
      expect(posted).toBe(2);
      await expectNoHorizontalOverflow(page, `pool editor ${viewport.name}`);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await page.screenshot({ path: join(tmpdir(), `rquant-pool-editor-${viewport.name}.png`) });
      await drawer.getByRole("button", { name: "返回画布" }).click();
      await expect(drawer).toBeHidden();
      await expect(add).toBeVisible();
      expect(watcher.problems).toEqual([]);
    });
  });
}
