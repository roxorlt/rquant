import { expect, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { trackingPanel } from "../src/pages/factors/factorTracking.fixture.ts";
import { findJargon } from "../src/test/jargon.ts";
import { APP_URL } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const capability: Schemas["FactorCapabilitiesData"] = {
  can_save: true,
  fields: [
    { column: "open", name_zh: "开盘价", description_zh: "当日开盘价" },
    { column: "high", name_zh: "最高价", description_zh: "当日最高价" },
    { column: "low", name_zh: "最低价", description_zh: "当日最低价" },
    { column: "close", name_zh: "收盘价", description_zh: "当日收盘价" },
    { column: "vol", name_zh: "成交量", description_zh: "当日成交量" },
    { column: "amount", name_zh: "成交额", description_zh: "当日成交额" },
  ],
  runnable_operators: ["+", "ts_mean", "ref"],
  unavailable_operators: [
    { name: "industry_neutralize", reason_zh: "缺行业归属" },
    { name: "market_cap_neutralize", reason_zh: "缺市值上下文" },
  ],
  coverage_note_zh: "实际数据范围在检验时核对。",
  source_mode: "historical_retrospective",
  version: "daily_v1",
};

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`factor-save-${viewport.name}`, () => {
    test.use({ hasTouch: viewport.name === "phone", isMobile: viewport.name === "phone" });
    test(`因子新建、原请求重载续查及编辑在 ${viewport.name} 可完成`, async ({ page }, testInfo) => {
      const watcher = watch(page);
      const metadata = (await (
        await page.request.get(new URL("api/v1/meta", APP_URL).toString())
      ).json()) as MetaEnvelope;
      let currentId = metadata.serving.generation_id;
      let rows: Schemas["FactorDefinitionItem"][] = [];
      const requests: { action: string; body: Schemas["FactorSaveDraft"] }[] = [];
      let createResumeCount = 0;
      const serving = () => ({ ...metadata.serving, generation_id: currentId });
      await page.route("**/api/v1/meta", (route) =>
        route.fulfill({
          json: {
            ...metadata,
            data: {
              ...metadata.data,
              generation: metadata.data.generation
                ? { ...metadata.data.generation, generation_id: currentId ?? "" }
                : null,
            },
            serving: serving(),
          } satisfies MetaEnvelope,
        }),
      );
      await page.route(/\/api\/v1\/factors\/definitions(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: {
              availability: rows.length ? "populated" : "empty",
              available_at: "2026-09-29T07:00:00Z",
              can_save: true,
              can_archive: true,
              definitions: rows,
            },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorCatalogData_"],
        }),
      );
      await page.route("**/api/v1/factors/capabilities", (route) =>
        route.fulfill({
          json: {
            data: capability,
            serving: serving(),
          } satisfies Schemas["Envelope_FactorCapabilitiesData_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/results(?:\?.*)?$/, (route) =>
        route.fulfill({
          json: {
            data: { availability: "unavailable", available_at: null, results: [] },
            serving: serving(),
          } satisfies Schemas["Envelope_FactorResultListData_"],
        }),
      );
      await page.route(/\/api\/v1\/factors\/created_factor\/tracking(?:\?.*)?$/, (route) => {
        expect(new URL(route.request().url()).searchParams.get("generation_id")).toBe(currentId);
        return route.fulfill({
          json: {
            data: trackingPanel({
              factor_id: "created_factor",
              availability: "unavailable",
              status: "unavailable",
              definition_head: null,
              reason: "跟踪数据尚未发布。",
              can_set_tracked: false,
            }),
            serving: serving(),
          } satisfies Schemas["Envelope_FactorTrackingPanel_"],
        });
      });
      await page.route(
        /\/api\/v1\/factors\/definitions\/save(?:\/(?:resume|retry))?$/,
        async (route) => {
          const body = route.request().postDataJSON() as Schemas["FactorSaveDraft"];
          const action = route.request().url().split("/").at(-1) ?? "";
          expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
          const persisted = await page.evaluate(() =>
            JSON.parse(localStorage.getItem("rquant.factor.save-command.v1") ?? "null"),
          );
          expect(persisted).toEqual(body);
          expect(body).not.toHaveProperty("category_label");
          requests.push({ action, body });
          let status: Schemas["FactorSaveCommandData"]["status"];
          if (body.mode === "create") {
            if (action === "resume") createResumeCount += 1;
            status =
              action === "save" || (createResumeCount === 1 && action === "resume")
                ? "uncertain"
                : action === "retry"
                  ? "pending"
                  : createResumeCount === 2
                    ? "succeeded_waiting_publication"
                    : "published";
          } else {
            status = action === "save" ? "succeeded_waiting_publication" : "published";
          }
          const version = body.mode === "create" ? 1 : 2;
          const digest = body.mode === "create" ? "c".repeat(64) : "f".repeat(64);
          if (status === "published") {
            currentId = body.mode === "create" ? "d".repeat(64) : "e".repeat(64);
            rows = [
              {
                factor_id: "created_factor",
                content_sha256: digest,
                version,
                name_zh: body.name_zh,
                category: body.category,
                category_label: "技术",
                direction: body.direction,
                direction_label: body.direction === "lower_is_better" ? "偏好低值" : "偏好高值",
                archived: false,
                earliest_available_date: null,
                expression: body.expression,
                dependency_columns: ["close"],
                max_history_window: body.mode === "create" ? 0 : 2,
              },
            ];
          }
          await route.fulfill({
            json: {
              data: {
                command_id: body.command_id,
                status,
                message: "保存状态已核对。",
                factor_id:
                  status === "published" || status === "succeeded_waiting_publication"
                    ? "created_factor"
                    : null,
                version:
                  status === "published" || status === "succeeded_waiting_publication"
                    ? version
                    : null,
                content_sha256:
                  status === "published" || status === "succeeded_waiting_publication"
                    ? digest
                    : null,
                current_head_updated: false,
              },
              serving: serving(),
            } satisfies Schemas["Envelope_FactorSaveCommandData_"],
          });
        },
      );

      await page.setViewportSize({ width: viewport.width, height: viewport.height });
      await page.goto("./#/factors");
      await expect(page.getByText("还没有因子")).toBeVisible();
      await page.getByRole("button", { name: "新建因子" }).focus();
      await page.keyboard.press("Enter");
      const create = page.getByRole("dialog", { name: "新建因子" });
      await create.getByRole("textbox", { name: "中文名" }).click({ trial: true });
      await create.getByRole("textbox", { name: "中文名" }).fill("收盘均值");
      await create.getByRole("combobox", { name: "方向" }).selectOption("lower_is_better");
      const field = create.getByRole("button", { name: "插入收盘价" });
      await field.focus();
      await page.keyboard.press("Enter");
      await expect(create.getByRole("textbox", { name: "表达式" })).toHaveValue("close");
      await expect(create.getByRole("textbox", { name: "表达式" })).toBeFocused();
      if (viewport.name === "phone") await create.getByText("算子帮助").tap();
      else await create.getByText("算子帮助").focus();
      const operatorTip = page.getByRole("tooltip", { name: /可用算子/ });
      await expect(operatorTip).toContainText("ref");
      await expect(operatorTip).toContainText("缺行业归属");
      await create.getByRole("textbox", { name: "表达式" }).focus();
      expect(findJargon(await create.innerText())).toEqual([]);
      await expectNoHorizontalOverflow(page, `factor create ${viewport.name}`);
      await create.getByRole("button", { name: "保存因子" }).focus();
      await page.keyboard.press("Enter");
      await expect(page.getByText("保存结果尚未确认，请保留这次操作。")).toBeVisible();
      await expect(page.getByRole("button", { name: /新建因子|^编辑$|^归档$/ })).toHaveCount(0);
      await page.reload();
      await expect.poll(() => requests.filter((item) => item.action === "resume").length).toBe(1);
      await expect(page.getByText("保存结果尚未确认，请保留这次操作。")).toBeVisible();
      await page.getByRole("button", { name: "用原请求重试" }).click();
      await expect(page.getByText("正在保存，请稍后查看。")).toBeVisible();
      expect(requests.slice(0, 3).map((item) => item.body)).toEqual([
        requests[0]?.body,
        requests[0]?.body,
        requests[0]?.body,
      ]);
      await page.getByRole("button", { name: "刷新状态" }).click();
      await expect(page.getByText("已提交，等待更新。")).toBeVisible();
      await expect(page.getByText("已保存。", { exact: true })).toHaveCount(0);
      await page.getByRole("button", { name: "刷新状态" }).click();
      await expect(page.getByText("已保存。", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "继续查看因子" }).click();
      await expect(page.getByRole("region", { name: "因子详情" })).toContainText("收盘均值");
      await expect(page.getByRole("region", { name: "因子详情" })).toContainText("待检验");

      await page.getByRole("button", { name: "编辑", exact: true }).focus();
      await page.keyboard.press("Enter");
      const editor = page.getByRole("dialog", { name: "编辑因子" });
      await editor.getByRole("textbox", { name: "中文名" }).click({ trial: true });
      await expect(editor.getByRole("combobox", { name: "分类" })).toHaveValue("技术");
      await expect(
        editor.getByRole("combobox", { name: "分类" }).locator("option:checked"),
      ).toHaveText("技术");
      await editor.getByRole("textbox", { name: "中文名" }).fill("收盘均值修订");
      await editor.getByRole("textbox", { name: "表达式" }).fill("ref(close, 2)");
      await expect(editor.getByText("保存新版本，旧版本仍保留。")).toBeVisible();
      await expect(editor.getByRole("button", { name: "保存新版本" })).toBeVisible();
      expect(findJargon(await editor.innerText())).toEqual([]);
      await expectNoHorizontalOverflow(page, `factor edit ${viewport.name}`);
      expect(
        await editor.evaluate((node) => node.scrollWidth - node.clientWidth),
      ).toBeLessThanOrEqual(1);
      await page.screenshot({
        path: testInfo.outputPath(`factor-editor-${viewport.name}.png`),
        fullPage: true,
      });
      await editor.getByRole("button", { name: "保存新版本" }).focus();
      await page.keyboard.press("Enter");
      await expect(page.getByText("已提交，等待更新。")).toBeVisible();
      const editRequest = requests.find(
        (item) => item.action === "save" && item.body.mode === "edit",
      )?.body;
      expect(editRequest).toMatchObject({
        generation_id: "d".repeat(64),
        mode: "edit",
        factor_id: "created_factor",
        expected_head: { version: 1, content_sha256: "c".repeat(64) },
        category: "技术",
        expression: "ref(close, 2)",
      });
      await page.getByRole("button", { name: "刷新状态" }).click();
      await expect(page.getByText("已保存。", { exact: true })).toBeVisible();
      await page.getByRole("button", { name: "继续查看因子" }).click();
      await expect(page.getByRole("region", { name: "因子详情" })).toContainText("收盘均值修订");
      await expect(page.getByRole("region", { name: "因子详情" })).toContainText("第 2 版");
      expect(
        await page.evaluate(() => localStorage.getItem("rquant.factor.save-command.v1")),
      ).toBeNull();
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await expect(page.getByRole("button", { name: "运行检验" })).toHaveCount(0);
      await expect(page.getByRole("button", { name: "加入跟踪" })).toBeDisabled();
      await expect(page.getByRole("region", { name: "因子跟踪", exact: true })).toContainText(
        "跟踪数据尚未发布。",
      );
      expect(watcher.problems).toEqual([]);
    });
  });
}
