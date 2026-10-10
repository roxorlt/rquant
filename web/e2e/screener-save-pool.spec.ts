import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test(`最新筛选结果可在 ${viewport.name} 命名保存为无排名池子`, async ({ page }, testInfo) => {
    const watcher = watch(page);
    const runs: Schemas["ExecuteScreenQuery"][] = [];
    const saves: Schemas["SaveRankedPoolCommand"][] = [];
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    await page.route("**/api/v1/screen/query/execute", async (route) => {
      runs.push(route.request().postDataJSON() as Schemas["ExecuteScreenQuery"]);
      await route.continue();
    });
    await page.route("**/api/v1/pools/editor/commands", async (route) => {
      const body = route.request().postDataJSON() as Schemas["SaveRankedPoolCommand"];
      const latest = runs.at(-1);
      if (!latest) throw new Error("save sent before a successful run");
      expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
      expect(body).toMatchObject({
        kind: "save_user_pool_v3",
        base_name: "浏览器观察",
        display_name: "浏览器观察",
        depends_on: null,
        delay_days: 0,
        expected_version: null,
        ranking: null,
        rule_calls: latest.definition.conditions.map((condition) => ({
          name: condition.name,
          args: condition.args ?? {},
        })),
      });
      const persisted = await page.evaluate(() => {
        const key = Object.keys(sessionStorage).find((item) =>
          item.startsWith("rquant.screen-pool-save.v1:"),
        );
        return key ? JSON.parse(sessionStorage.getItem(key) ?? "null") : null;
      });
      expect(persisted).toMatchObject({ body, status: "pending" });
      saves.push(body);
      await route.fulfill({
        status: 200,
        json: {
          command_id: body.command_id,
          status: "succeeded",
          message: "池子已保存",
          pool_version: "b".repeat(64),
        } satisfies Schemas["PoolEditorReceipt"],
      });
    });

    await page.goto("./#/screener");
    const save = page.getByRole("button", { name: "保存为池子" });
    await expect(save).toBeDisabled();
    await page.getByRole("button", { name: "运行筛选" }).click();
    await expect(page.getByText("命中 27 只")).toBeVisible();
    await expect(save).toBeEnabled();
    await save.focus();
    await save.press("Enter");
    const dialog = page.getByRole("dialog", { name: "保存为池子" });
    const name = dialog.getByRole("textbox", { name: "池子名称" });
    await expect(name).toBeFocused();
    await name.fill("浏览器观察");
    await expect(dialog.getByRole("region", { name: "将保存的规则" })).toContainText("排除 ST");
    await dialog.getByRole("button", { name: "保存池子" }).click();
    await expect(dialog.getByText("保存请求已完成")).toBeVisible();
    await expect(dialog.getByText("等待规则发布")).toBeVisible();
    await expect(dialog.getByText("等待日终结果确认")).toBeVisible();
    expect(runs).toHaveLength(1);
    expect(saves).toHaveLength(1);
    expect(findJargon(await page.locator("main").innerText())).toEqual([]);
    expect(findJargon(await dialog.innerText())).toEqual([]);
    await expectNoHorizontalOverflow(page, `save pool ${viewport.name}`);
    await page.screenshot({ path: testInfo.outputPath(`save-pool-${viewport.name}.png`) });
    expect(watcher.problems).toEqual([]);
  });
}
