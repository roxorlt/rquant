import { expect, test } from "@playwright/test";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const definitions = [
  {
    factor_id: "price_volume_factor",
    content_sha256: "a".repeat(64),
    name_zh: "价量动量",
    category_label: "技术",
    direction: "higher_is_better",
    direction_label: "偏好高值",
    version: 2,
    earliest_available_date: "2024-01-02",
    archived: false,
    expression: "ts_mean(close, 5) / ref(volume, 2)",
    dependency_columns: ["close", "volume"],
    max_history_window: 5,
  },
  {
    factor_id: "old_factor",
    content_sha256: "b".repeat(64),
    name_zh: "成交变化",
    category_label: "技术",
    direction: "lower_is_better",
    direction_label: "偏好低值",
    version: 1,
    earliest_available_date: "2025-03-04",
    archived: true,
    expression: "ts_mean(volume, 3)",
    dependency_columns: ["volume"],
    max_history_window: 3,
  },
];

test("因子库桌面和手机列表详情可读、键盘选择且无横向溢出", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:31:00Z", definitions },
        serving,
      },
    }),
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/factors");
  const list = page.getByRole("table", { name: "因子列表" });
  await expect(list).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toContainText("价量动量");
  await page.screenshot({ path: "test-results/factors-desktop.png", fullPage: true });
  const archived = list.getByRole("row", { name: /成交变化/ });
  await archived.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("region", { name: "因子详情" })).toContainText("已归档");
  await expect(page.getByText("ts_mean(volume, 3)")).toBeVisible();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expect(page.getByRole("button", { name: /运行检验|加入跟踪/ })).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "factor desktop");

  await page.setViewportSize({ width: 390, height: 844 });
  await expect(list).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "factor phone");
  await page.screenshot({ path: "test-results/factors-phone.png", fullPage: true });
  expect(watcher.problems).toEqual([]);
});

test("因子库可信空状态与换代错误不展示旧详情", async ({ page }) => {
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const serving = (await meta.json()).serving;
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "empty", available_at: "2026-09-24T07:31:00Z", definitions: [] },
        serving,
      },
    }),
  );
  await page.goto("./#/factors");
  await expect(page.getByText("还没有因子")).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toHaveCount(0);
  await page.unroute("**/api/v1/factors/definitions*");
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: { availability: "populated", available_at: "2026-09-24T07:31:00Z", definitions },
        serving: { ...serving, generation_id: "f".repeat(64) },
      },
    }),
  );
  await page.reload();
  await expect(page.getByText("数据已更新，请重新查看因子。")).toBeVisible();
  await expect(page.getByRole("region", { name: "因子详情" })).toHaveCount(0);
});

test("归档确认、刷新续查及手机布局", async ({ page }) => {
  const watcher = watch(page);
  const meta = await page.request.get(new URL("api/v1/meta", APP_URL).toString());
  const metadata = await meta.json();
  const serving = metadata.serving;
  const nextGeneration = "e".repeat(64);
  let published = false;
  const commandIds: string[] = [];
  await page.route("**/api/v1/meta", (route) =>
    route.fulfill({
      json: published
        ? {
            ...metadata,
            data: {
              ...metadata.data,
              generation: { ...metadata.data.generation, generation_id: nextGeneration },
            },
            serving: { ...serving, generation_id: nextGeneration },
          }
        : metadata,
    }),
  );
  await page.route("**/api/v1/factors/definitions*", (route) =>
    route.fulfill({
      json: {
        data: {
          availability: "populated",
          available_at: "2026-09-24T07:31:00Z",
          definitions: published
            ? [{ ...definitions[0], archived: true }, definitions[1]]
            : definitions,
          can_archive: true,
        },
        serving: published ? { ...serving, generation_id: nextGeneration } : serving,
      },
    }),
  );
  await page.route(
    /\/api\/v1\/factors\/definitions\/price_volume_factor\/archive(?:\/resume)?$/,
    async (route) => {
      const body = route.request().postDataJSON() as { command_id: string };
      commandIds.push(body.command_id);
      published = route.request().url().endsWith("/resume");
      await route.fulfill({
        json: {
          data: {
            status: published ? "published" : "succeeded_waiting_publication",
            command_id: body.command_id,
            factor_id: "price_volume_factor",
            version: 2,
            content_sha256: "a".repeat(64),
            current_head_updated: false,
            message: published ? "已归档，历史记录仍会保留。" : "已提交，等待更新。",
          },
          serving: published ? { ...serving, generation_id: nextGeneration } : serving,
        },
      });
    },
  );
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/factors");
  await page.getByRole("button", { name: "归档" }).focus();
  await page.keyboard.press("Enter");
  await expect(page.getByText("归档当前定义，历史记录仍会保留。")).toBeVisible();
  await page.getByRole("button", { name: "确认归档" }).click();
  await expect(page.getByText("已提交，等待更新。")).toBeVisible();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await page.screenshot({ path: "test-results/factors-archive-desktop.png", fullPage: true });
  await page.reload();
  await expect(page.getByText("已归档，历史记录仍会保留。")).toBeVisible();
  expect(commandIds).toHaveLength(2);
  expect(commandIds[0]).toBe(commandIds[1]);
  await page.setViewportSize({ width: 390, height: 844 });
  await expectNoHorizontalOverflow(page, "factor archive phone");
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await page.screenshot({ path: "test-results/factors-archive-phone.png", fullPage: true });
  expect(watcher.problems).toEqual([]);
});
