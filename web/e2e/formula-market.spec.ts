import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

test("公式全市场运行从确认到历史结果，桌面与手机均可恢复和翻页", async ({ page }, testInfo) => {
  const watcher = watch(page);
  const serving = metaEnvelope().serving;
  const taskId = "a".repeat(32);
  const formula = "CLOSE>MA(CLOSE,2)";
  let published = false;
  let resultReadable = true;
  let cursorInvalid = true;
  let sawInvalidCursor = false;
  let firstPageReads = 0;
  const commands: Schemas["FormulaMarketCommandRequest"][] = [];
  const cursors: (string | null)[] = [];
  const job: Schemas["FormulaMarketJobItem"] = {
    task_id: taskId,
    status: "succeeded",
    status_label: "已完成",
    hint: "可以查看命中股票。",
    formula,
    trade_date: "2026-09-24",
    created_at: "2026-09-24T07:30:00Z",
    updated_at: "2026-09-24T07:31:00Z",
    result_available: true,
  };
  const summary: Schemas["FormulaMarketResultSummary"] = {
    market_total: 100,
    listed_count: 100,
    paused_count: 0,
    match_count: 51,
    no_match_count: 40,
    unknown_count: 9,
    unknown_reasons: [{ reason: "missing_value", label: "行情字段缺失", count: 9 }],
  };

  await page.route("**/api/v1/meta", (route) => route.fulfill({ json: metaEnvelope() }));
  await page.route("**/api/v1/screen/tdx/preview/source", (route) =>
    route.fulfill({
      json: {
        available: true,
        dates: ["2026-09-24"],
        source: { identity: "b".repeat(64), updated_at: "2026-09-24T07:31:00Z" },
      },
    }),
  );
  await page.route("**/api/v1/screen/tdx/parse", (route) =>
    route.fulfill({
      json: {
        syntax_version: "tdx-v1",
        status: "parsed",
        capability: "parse_only",
        ast: null,
        translation: null,
        issues: [],
        unsupported: [],
      },
    }),
  );
  await page.route("**/api/v1/screen/tdx/market/jobs", (route) =>
    route.fulfill({
      json: {
        data: {
          availability: published ? "ready" : "empty",
          available_at: "2026-09-24T07:32:00Z",
          has_older_tasks: false,
          jobs: published ? [{ ...job, result_available: resultReadable }] : [],
          message: published ? "" : "还没有选股任务。",
          total_task_count: published ? 1 : 0,
        } satisfies Schemas["FormulaMarketJobListData"],
        serving,
      },
    }),
  );
  await page.route(`**/api/v1/screen/tdx/market/jobs/${taskId}`, (route) =>
    route.fulfill(
      published
        ? { json: { data: { job, summary }, serving } }
        : { status: 404, json: { detail: "没有找到这项选股任务。" } },
    ),
  );
  await page.route(`**/api/v1/screen/tdx/market/jobs/${taskId}/matches*`, (route) => {
    const cursor = new URL(route.request().url()).searchParams.get("cursor");
    cursors.push(cursor);
    if (cursor !== null && cursorInvalid) {
      sawInvalidCursor = true;
      return route.fulfill({
        status: 409,
        json: { detail: "选股结果已更新，请重新打开查看。" },
      });
    }
    if (cursor === null && sawInvalidCursor) cursorInvalid = false;
    if (cursor === null) firstPageReads += 1;
    return route.fulfill({
      json: {
        data: {
          task_id: taskId,
          total: 51,
          offset: cursor ? 50 : 0,
          match_codes: cursor ? ["600051.SH"] : [firstPageReads === 1 ? "600001.SH" : "600002.SH"],
          next_cursor: cursor ? null : "page-2",
        } satisfies Schemas["FormulaMarketMatchesData"],
        serving,
      },
    });
  });
  await page.route("**/api/v1/screen/tdx/market/commands", async (route) => {
    const request = route.request();
    expect(request.headers()["x-rquant-csrf"]).toBe("1");
    const body = request.postDataJSON() as Schemas["FormulaMarketCommandRequest"];
    commands.push(body);
    await route.fulfill({
      json: {
        command_id: body.command_id,
        status: "queued",
        task_id: taskId,
        message: "已提交，等待选股结果。",
      } satisfies Schemas["FormulaMarketCommandReceipt"],
    });
  });

  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("./#/screener");
  await page.getByRole("button", { name: "导入公式" }).click();
  let drawer = page.getByRole("dialog", { name: "公式预览" });
  await drawer.getByRole("textbox", { name: "通达信公式" }).fill(formula);
  await drawer.getByRole("button", { name: "检查公式" }).click();
  await expect(drawer.getByText("公式检查通过，可预览或批量运行。")).toBeVisible();
  await drawer.getByLabel("运行日期").fill("2026-09-24");
  await drawer.getByRole("button", { name: "运行全市场" }).click();
  const confirm = page.getByRole("dialog", { name: "运行全市场公式选股" });
  await expect(confirm).toContainText("2026-09-24");
  expect(commands).toHaveLength(0);
  await confirm.getByRole("button", { name: "确认运行" }).focus();
  await page.keyboard.press("Enter");
  await expect(drawer.getByText("已提交，等待任务出现")).toBeVisible();
  expect(commands).toHaveLength(1);
  expect(commands[0]).toMatchObject({ formula, trade_date: "2026-09-24" });
  expect(await drawer.innerText()).not.toContain(taskId);
  expect(findJargon(await drawer.innerText())).toEqual([]);

  await drawer.getByRole("button", { name: "关闭" }).click();
  await page.getByRole("button", { name: "导入公式" }).click();
  drawer = page.getByRole("dialog", { name: "公式预览" });
  await expect(drawer.getByText("已提交，等待任务出现")).toBeVisible();
  published = true;
  await drawer.getByRole("button", { name: "刷新任务" }).click();
  await expect(drawer.getByRole("region", { name: "市场结果" })).toContainText("51");
  await expect(drawer.getByText("行情字段缺失")).toBeVisible();
  await expect(drawer.getByRole("button", { name: /600001.SH/ })).toBeVisible();
  await expect(drawer.getByRole("region", { name: "保存公式池" })).toHaveCount(1);
  await expect(drawer.getByRole("textbox", { name: "池子名称" })).toBeVisible();
  await expectNoHorizontalOverflow(page, "formula market desktop");
  await page.screenshot({
    path: testInfo.outputPath("rquant-formula-market-react-desktop.png"),
    fullPage: true,
  });
  await drawer.getByRole("region", { name: "市场结果" }).scrollIntoViewIfNeeded();
  await page.screenshot({
    path: testInfo.outputPath("rquant-formula-market-react-desktop-result.png"),
  });
  await drawer.getByRole("button", { name: "下一页" }).click();
  await expect(drawer.getByText("结果已更新")).toBeVisible({ timeout: 15_000 });
  await drawer.getByRole("button", { name: "从第一页重看" }).click();
  await expect(drawer.getByRole("button", { name: /600002.SH/ })).toBeVisible();
  await expect(drawer.getByText("第 1 页")).toBeVisible();
  await drawer.getByRole("button", { name: "下一页" }).click();
  await expect(drawer.getByRole("button", { name: /600051.SH/ })).toBeVisible();
  expect(cursors[0]).toBeNull();
  expect(cursors.slice(1, -2).length).toBeGreaterThan(0);
  expect(cursors.slice(1, -2).every((cursor) => cursor === "page-2")).toBe(true);
  expect(cursors.slice(-2)).toEqual([null, "page-2"]);
  await drawer.getByRole("textbox", { name: "通达信公式" }).fill("CLOSE>OPEN");
  await expect(drawer.getByText("历史公式与日期")).toBeVisible();
  await expect(drawer.getByRole("button", { name: /600002.SH/ })).toBeVisible();
  await expect(drawer.getByRole("button", { name: "运行全市场" })).toBeDisabled();

  await drawer.getByRole("button", { name: "关闭" }).click();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "导入公式" }).click();
  drawer = page.getByRole("dialog", { name: "公式预览" });
  await expect(drawer.getByRole("button", { name: /CLOSE>MA/ })).toBeVisible();
  await drawer.getByRole("button", { name: /CLOSE>MA/ }).click();
  await expect(drawer.getByRole("region", { name: "市场结果" })).toContainText("51");
  await expectNoHorizontalOverflow(page, "formula market phone");
  await page.screenshot({
    path: testInfo.outputPath("rquant-formula-market-react-phone.png"),
    fullPage: true,
  });
  await drawer.getByRole("region", { name: "市场结果" }).scrollIntoViewIfNeeded();
  await page.screenshot({
    path: testInfo.outputPath("rquant-formula-market-react-phone-result.png"),
  });
  resultReadable = false;
  await drawer
    .getByRole("heading", { name: "最近运行" })
    .locator("..")
    .getByRole("button", { name: "刷新" })
    .click();
  await expect(drawer.getByText("结果暂时无法读取")).toBeVisible();
  await expect(drawer.getByRole("region", { name: "市场结果" })).toHaveCount(0);
  await expect(drawer.getByRole("button", { name: /600002.SH/ })).toHaveCount(0);
  resultReadable = true;
  await drawer
    .getByRole("heading", { name: "运行详情" })
    .locator("..")
    .getByRole("button", { name: "刷新" })
    .click();
  await expect(drawer.getByRole("region", { name: "市场结果" })).toContainText("51");
  await expect(drawer.getByRole("region", { name: "保存公式池" })).toHaveCount(1);
  expect(findJargon(await drawer.innerText())).toEqual([]);
  await drawer.getByRole("region", { name: "保存公式池" }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: testInfo.outputPath("rquant-formula-save-recovery-phone.png") });
  const saves: Schemas["FormulaPoolSaveCommandRequest"][] = [];
  let terminal = false;
  await page.route("**/api/v1/pools/formula/commands", async (route) => {
    const request = route.request().postDataJSON() as Schemas["FormulaPoolSaveCommandRequest"];
    saves.push(request);
    await route.fulfill({
      json: {
        command_id: request.command_id,
        status: terminal ? "failed" : "pending",
        message: terminal ? "保存失败，请稍后重试。" : "保存状态待确认，请继续核对。",
      } satisfies Schemas["FormulaPoolSaveCommandReceipt"],
    });
  });
  const poolName = drawer.getByRole("textbox", { name: "池子名称" });
  await poolName.focus();
  await page.keyboard.type("趋势池");
  await drawer.getByRole("button", { name: "保存为池子" }).focus();
  await page.keyboard.press("Enter");
  await expect(drawer.getByText("保存状态待确认")).toBeVisible();
  await expect(drawer.getByRole("region", { name: "保存公式池" })).toHaveCount(1);
  expect(saves).toHaveLength(1);
  await drawer.getByRole("button", { name: "关闭" }).click();
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.getByRole("button", { name: "导入公式" }).click();
  drawer = page.getByRole("dialog", { name: "公式预览" });
  await expect(drawer.getByText("保存状态待确认")).toBeVisible();
  await drawer.getByRole("button", { name: "继续核对" }).focus();
  await page.keyboard.press("Enter");
  await expect.poll(() => saves.length).toBe(2);
  expect(saves[1]).toEqual(saves[0]);
  await expect(drawer.getByRole("region", { name: "保存公式池" })).toHaveCount(1);
  await expectNoHorizontalOverflow(page, "formula pool recovery desktop");
  await drawer.getByRole("region", { name: "保存公式池" }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: testInfo.outputPath("rquant-formula-save-recovery-desktop.png") });
  resultReadable = false;
  terminal = true;
  await page.setViewportSize({ width: 390, height: 844 });
  await drawer
    .getByRole("heading", { name: "最近运行" })
    .locator("..")
    .getByRole("button", { name: "刷新" })
    .click();
  await expect(drawer.getByText("结果暂时无法读取")).toBeVisible();
  await drawer.getByRole("button", { name: "继续核对" }).focus();
  await page.keyboard.press("Enter");
  await expect(drawer.getByText("保存失败，请稍后重试。")).toBeVisible();
  await expect(drawer.getByText("刷新最近运行，结果可读后可重新保存。")).toBeVisible();
  await expect(drawer.getByRole("region", { name: "保存公式池" })).toHaveCount(1);
  await expectNoHorizontalOverflow(page, "formula pool terminal recovery phone");
  expect(saves[2]).toEqual(saves[0]);
  await drawer.getByRole("region", { name: "保存公式池" }).scrollIntoViewIfNeeded();
  await page.screenshot({ path: testInfo.outputPath("rquant-formula-save-terminal-phone.png") });
  expect(
    watcher.problems.filter(
      (problem) =>
        !(problem.includes("HTTP 409") && problem.includes("/matches")) &&
        !problem.includes("server responded with a status of 409"),
    ),
  ).toEqual([]);
});
