import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const HASH = "a".repeat(64);

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} canvas creation`, () => {
    test.use({ viewport: { width: viewport.width, height: viewport.height } });

    test("keeps the create request through refresh and opens only the matching published canvas", async ({
      page,
    }) => {
      const watcher = watch(page);
      let published = false;
      let sent: Schemas["CreateCanvasCommand"] | null = null;
      await page.route("**/api/v1/meta", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_MetaData_"];
        const generation = published ? "canvas-published" : "canvas-old";
        body.serving.generation_id = generation;
        if (body.data.generation) body.data.generation.generation_id = generation;
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/pools", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolsData_"];
        body.serving.generation_id = published ? "canvas-published" : "canvas-old";
        body.data.state = "no_data";
        body.data.latest_trade_date = null;
        body.data.pools = [];
        body.data.definitions_available = true;
        body.data.canvases = published
          ? [{ name: "晨盘观察", description: "观察候选池", pool_keys: [], refs_truncated: false }]
          : [];
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/pools/editor", async (route) => {
        const upstream = await route.fetch();
        const body = (await upstream.json()) as Schemas["Envelope_PoolEditorData_"];
        body.serving.generation_id = published ? "canvas-published" : "canvas-old";
        body.data = {
          state: "ready",
          canvas_create_available: true,
          pools: [],
          copy_sources: [],
          canvases: published
            ? [
                {
                  name: "晨盘观察",
                  description: "观察候选池",
                  version: "b".repeat(64),
                  command_id: sent?.command_id ?? "",
                  record_hash: HASH,
                  pool_refs: [],
                },
              ]
            : [],
        };
        await route.fulfill({ response: upstream, body: JSON.stringify(body) });
      });
      await page.route("**/api/v1/pools/editor/commands", async (route) => {
        const body = route.request().postDataJSON() as Schemas["CreateCanvasCommand"];
        expect(body.kind).toBe("create_canvas");
        expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
        expect(body).toEqual(
          await page.evaluate(
            () =>
              JSON.parse(sessionStorage.getItem("rquant.canvas-create-command.v1") ?? "{}").body,
          ),
        );
        sent = body;
        await route.fulfill({
          json: {
            command_id: body.command_id,
            status: "succeeded",
            message: "画布已保存，等待发布",
            canvas_name: body.name,
            canvas_record_hash: HASH,
          },
        });
      });

      await page.goto("./#/pools");
      const create = page.getByRole("button", { name: "新建画布" });
      await expect(create).toBeEnabled();
      await create.focus();
      await create.press("Enter");
      const dialog = page.getByRole("dialog", { name: "新建画布" });
      await expect(dialog).toBeVisible();
      await expect(dialog).toBeInViewport({ ratio: 0.99 });
      const name = dialog.getByRole("textbox", { name: "画布名称" });
      const description = dialog.getByRole("textbox", { name: /简短说明/ });
      await name.focus();
      await page.keyboard.type("晨盘观察");
      await name.press("Tab");
      await expect(description).toBeFocused();
      await page.keyboard.type("观察候选池");
      await page.screenshot({
        path: join(tmpdir(), `rquant-canvas-create-${viewport.name}-draft.png`),
      });
      const submit = dialog.getByRole("button", { name: "创建画布" });
      await submit.focus();
      await submit.press("Enter");
      await expect(dialog).toContainText("已保存，等待发布");
      await expect(dialog.getByRole("button", { name: "打开画布" })).toHaveCount(0);
      expect(sent).not.toBeNull();
      await page.reload();
      await expect(page.getByRole("status", { name: "画布创建状态" })).toContainText(
        "已保存，等待发布",
      );
      published = true;
      await page.reload();
      const state = page.getByRole("status", { name: "画布创建状态" });
      await expect(state).toContainText("画布已可用");
      const open = state.getByRole("button", { name: "打开画布" });
      await open.focus();
      await open.press("Enter");
      await expect(page.getByRole("combobox", { name: "选择画布" })).toHaveValue("晨盘观察");
      await expect(page.getByText("这张画布还是空的")).toBeVisible();
      await expectNoHorizontalOverflow(page, `${viewport.name} canvas creation`);
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await page.screenshot({ path: join(tmpdir(), `rquant-canvas-create-${viewport.name}.png`) });
      expect(watcher.problems).toEqual([]);
    });
  });
}
