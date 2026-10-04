import { readFile } from "node:fs/promises";
import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

const catalog: Schemas["QueryCatalogData"] = {
  available: true,
  save_enabled: true,
  source_at: "2026-09-30T08:00:00Z",
  message: "",
  tables: [
    {
      name: "daily_bar",
      label: "日线行情",
      row_count: 2,
      earliest_date: "2026-09-29",
      latest_date: "2026-09-30",
      columns: [{ name: "ts_code", data_type: "VARCHAR", description: "股票代码，如 600001.SH" }],
    },
  ],
};
const result: Schemas["QueryResult"] = {
  status: "ready",
  columns: [
    { name: "值", data_type: "VARCHAR" },
    { name: "值", data_type: "VARCHAR" },
  ],
  rows: [["<script>alert(1)</script>", "=1+1"]],
  elapsed_ms: 25,
  source_at: catalog.source_at,
  snapshot_sha256: "a".repeat(64),
  message: "",
};
const envelope = <T>(data: T) => ({ data, serving: metaEnvelope().serving });

test.beforeEach(async ({ page }) => {
  await page.route("**/app/api/v1/meta", (route) =>
    route.fulfill({ json: metaEnvelope({ viewer: "alice" }) }),
  );
  await page.route("**/app/api/v1/research/catalog", (route) =>
    route.fulfill({ json: envelope(catalog) }),
  );
  await page.route("**/app/api/v1/research/queries", (route) =>
    route.fulfill({ json: envelope({ available: true, items: [], message: "" }) }),
  );
  await page.route("**/app/api/v1/research/query", async (route) => {
    const body = route.request().postDataJSON();
    expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
    await route.fulfill({
      json: envelope(body.mode === "explain" ? { ...result, rows: [["实际扫描计划"]] } : result),
    });
  });
});

for (const width of [1440, 390]) {
  test(`查询交互、文本、CSV 与字段提示 ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: width === 390 ? 844 : 900 });
    const errors: string[] = [];
    page.on("pageerror", (error) => errors.push(error.message));
    const response = await page.goto("./#/query");
    expect(response?.headers()["content-security-policy"]).toContain("script-src 'self'");
    const editor = page.getByRole("textbox", { name: "SQL 查询" });
    await editor.fill("SELECT '<script>alert(1)</script>', '=1+1'");
    await expect(page.getByRole("button", { name: "运行", exact: true })).toBeEnabled();
    await editor.press("Control+Enter");
    await expect(page.getByText("<script>alert(1)</script>", { exact: true })).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "值", exact: true })).toHaveCount(2);
    const download = page.waitForEvent("download");
    await page.getByRole("button", { name: "导出 CSV" }).click();
    const file = await download;
    expect(file.suggestedFilename()).toBe("查询结果.csv");
    const path = await file.path();
    expect(path).not.toBeNull();
    expect(await readFile(path as string, "utf8")).toBe(
      "\uFEFF值,值\r\n<script>alert(1)</script>,'=1+1\r\n",
    );
    await page.screenshot({
      path: `../data/verification/research-query-20261005/query-${width}.png`,
      fullPage: true,
    });
    await page.getByRole("button", { name: "查看计划" }).click();
    await expect(page.getByText("实际扫描计划", { exact: true })).toBeVisible();
    await page.locator(".query-table-info summary").click();
    await page.locator(".query-table-info .tip-anchor").focus();
    await expect(page.getByRole("tooltip")).toContainText("股票代码，如 600001.SH");
    await expectNoHorizontalOverflow(page, "query");
    expect(errors).toEqual([]);
  });
}

test("保存失联、刷新和恢复仍使用原命令", async ({ page }) => {
  let original: Schemas["SaveResearchQuery"] | undefined;
  await page.route("**/app/api/v1/research/queries/save", async (route) => {
    original = route.request().postDataJSON();
    expect(original).not.toHaveProperty("owner_id");
    await route.fulfill({ status: 503, json: { detail: "保存暂时不可用" } });
  });
  await page.route("**/app/api/v1/research/queries/resume", async (route) => {
    const body = route.request().postDataJSON() as Schemas["SaveResearchQuery"];
    expect(body).toEqual(original);
    await route.fulfill({
      json: envelope({
        receipt: {
          command_id: body.command_id,
          status: "succeeded",
          enqueued_at: body.requested_at,
          completed_at: body.requested_at,
          result: { query_id: body.query_id, version: 1, code: "saved" },
          error: null,
        },
        message: "",
      }),
    });
  });
  await page.goto("./#/query");
  await page.getByRole("textbox", { name: "查询名称" }).fill("我的行情");
  await page.getByRole("button", { name: "保存", exact: true }).click();
  await expect(page.locator(".query-save-feedback")).toContainText(
    "保存结果尚未确认，请恢复原命令。",
  );
  await page.reload();
  await expect(page.getByRole("button", { name: "恢复保存" })).toBeVisible();
  await page.getByRole("button", { name: "恢复保存" }).click();
  await expect(page.getByText("已保存", { exact: true })).toBeVisible();
  expect(
    await page.evaluate(() => sessionStorage.getItem("rquant.query-save.alice.v1")),
  ).toBeNull();
});
