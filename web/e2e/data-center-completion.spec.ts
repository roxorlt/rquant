import { expect, type Locator, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client";
import { findJargon } from "../src/test/jargon";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const INITIAL_TIME = "2026-10-06T10:15:00Z";

// These fields belong only to the private fixture's real-worker advance route.
interface FixtureAdvanceResult {
  now: string;
  status: { execution_id: string; status: string; control_sequence: number };
  sdk_calls: string[];
}

async function originalCommand(page: Page, action: () => Promise<void>) {
  const pending = page.waitForResponse(
    (response) =>
      response.url().endsWith("/api/v1/data/executions/commands") &&
      response.request().method() === "POST",
  );
  await action();
  const response = await pending;
  expect(response.request().headers()["x-rquant-csrf"]).toBe("1");
  expect(response.status(), await response.text()).toBe(200);
  const receipt: Schemas["DataCenterCommandReceipt"] = await response.json();
  return receipt;
}

async function advance(
  page: Page,
  executionId: string,
  options: { limit?: number; failure?: "import_receipt_io"; advance_seconds?: number } = {},
) {
  const response = await page.request.post("./api/__fixture/advance", {
    headers: { "X-Rquant-Csrf": "1" },
    data: { execution_id: executionId, ...options },
  });
  expect(response.status(), await response.text()).toBe(200);
  const result: FixtureAdvanceResult = await response.json();
  expect(result.status.execution_id).toBe(executionId);
  await page.clock.setFixedTime(new Date(result.now));
  return result;
}

async function refresh(page: Page, panel: Locator) {
  const pending = page.waitForResponse(
    (response) =>
      response.url().split("?")[0]?.endsWith("/api/v1/data/executions") === true &&
      response.request().method() === "GET",
  );
  await panel.getByRole("button", { name: "刷新状态", exact: true }).click();
  const response = await pending;
  expect(response.status(), await response.text()).toBe(200);
  await expectNoHorizontalOverflow(page, "data task after refresh");
}

async function confirmExecution(page: Page, panel: Locator, name: "日线回补" | "财务采集") {
  const prepare = panel.getByRole("button", { name: `确认${name}范围`, exact: true });
  await expect(prepare).toBeEnabled();
  const receipt = await originalCommand(page, async () => {
    await prepare.focus();
    await page.keyboard.press("Enter");
  });
  expect(receipt.status).toBe("prepared");
  if (!receipt.confirmation) throw new Error("original confirmation is required");
  const confirmation = receipt.confirmation;
  const dialog = page.getByRole("dialog", { name: `开始${name}` });
  await expect(dialog).toBeVisible();
  const submit = dialog.getByRole("button", { name: "开始执行", exact: true });
  await expect(submit).toBeDisabled();
  await dialog.getByRole("textbox").fill("未确认");
  await expect(submit).toBeDisabled();
  await expectNoHorizontalOverflow(page, `${name} confirmation`);
  await dialog.getByRole("textbox").fill(name);
  await expect(submit).toBeEnabled();
  const accepted = await originalCommand(page, async () => {
    await submit.focus();
    await page.keyboard.press("Enter");
  });
  expect(accepted.execution_id).toBe(confirmation.execution_id);
  await expect(dialog).not.toBeVisible();
  return confirmation;
}

for (const viewport of [
  { name: "desktop", width: 1440, height: 900, touch: false },
  { name: "phone", width: 390, height: 844, touch: true },
] as const) {
  test.describe(`${viewport.name} original data center completion`, () => {
    test.use({
      viewport: { width: viewport.width, height: viewport.height },
      hasTouch: viewport.touch,
      isMobile: viewport.touch,
    });

    test("confirms, pauses, recovers and reads actual completed facts", async ({ page }, info) => {
      test.skip(
        process.env.RQ_E2E_DATA_CENTER_COMPLETION !== "1",
        "requires the private original-worker fixture, a fresh owned root for each viewport",
      );
      test.setTimeout(90_000);
      const observer = watch(page);
      await page.clock.setFixedTime(new Date(INITIAL_TIME));
      await page.goto("./#/datacenter");
      await expect(page.getByRole("heading", { level: 1, name: "数据中心" })).toBeVisible();
      const plans = page.getByRole("button", { name: "回补计划", exact: true });
      await plans.focus();
      await page.keyboard.press("Enter");
      const backfill = page.locator(".panel").filter({
        has: page.getByRole("heading", { name: "执行回补", exact: true }),
      });
      await expect(backfill.getByText("还没有运行记录", { exact: true })).toBeVisible();
      await expect(page.getByRole("region", { name: "缺失交易日", exact: true })).toContainText(
        "2026-10-04",
      );
      await expectNoHorizontalOverflow(page, "original exact-day plan");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      const first = await confirmExecution(page, backfill, "日线回补");
      expect(first.missing_date_count).toBe(1);
      await refresh(page, backfill);
      await expect(backfill.getByRole("region", { name: "日线回补任务进度" })).toContainText(
        "等待运行",
      );

      const collected = await advance(page, first.execution_id, { limit: 1 });
      expect(collected.status.status).toBe("running");
      await refresh(page, backfill);
      await expect(backfill.getByRole("region", { name: "日线回补任务进度" })).toContainText(
        "正在运行",
      );
      const paused = await originalCommand(page, () =>
        backfill.getByRole("button", { name: "暂停", exact: true }).click(),
      );
      expect(paused.execution_id).toBe(first.execution_id);
      await refresh(page, backfill);
      await expect(backfill.getByRole("region", { name: "日线回补任务进度" })).toContainText(
        "正在暂停",
      );
      const unchanged = await advance(page, first.execution_id, { limit: 1 });
      expect(unchanged.status.status).toBe("paused");
      expect(unchanged.sdk_calls).toEqual(collected.sdk_calls);
      await refresh(page, backfill);
      await expect(backfill.getByRole("region", { name: "日线回补任务进度" })).toContainText(
        "已暂停",
      );
      const resumed = await originalCommand(page, () =>
        backfill.getByRole("button", { name: "继续", exact: true }).click(),
      );
      expect(resumed.execution_id).toBe(first.execution_id);
      const completed = await advance(page, first.execution_id, { limit: 8 });
      expect(completed.status.status).toBe("completed");
      await refresh(page, backfill);
      await expect(backfill.getByText("所选范围已完成", { exact: true })).toBeVisible();
      const history = backfill.getByRole("region", { name: "日线回补最近运行记录" });
      await expect(history.getByRole("list", { name: "最近运行记录" })).toBeVisible();
      expect(await history.getByRole("listitem").count()).toBeGreaterThan(0);
      expect(await history.getByRole("listitem").count()).toBeLessThanOrEqual(20);
      await expectNoHorizontalOverflow(page, "backfill completed records");
      await page.screenshot({ path: info.outputPath(`backfill-completed-${viewport.width}.png`) });

      // The second original audit must observe its real ten-minute cooldown.
      await advance(page, first.execution_id, { limit: 1, advance_seconds: 660 });
      await refresh(page, backfill);
      await page.getByRole("button", { name: "财务", exact: true }).click();
      const financial = page.locator(".panel").filter({
        has: page.getByRole("heading", { name: "采集财务", exact: true }),
      });
      await expect(page.getByRole("region", { name: "财务接口权益" })).toBeVisible();
      await expect(page.locator(".dc-financial-source")).toHaveCount(7);
      await financial.getByLabel("财务开始日期").fill("2026-10-05");
      await financial.getByLabel("财务结束日期").fill("2026-10-05");
      await financial.getByLabel("财务报告期", { exact: true }).fill("2026-06-30");
      await financial.getByRole("checkbox", { name: "选择可用股票" }).check();
      await expectNoHorizontalOverflow(page, "financial selection");
      const second = await confirmExecution(page, financial, "财务采集");
      expect(second.security_count).toBe(1);
      expect(second.query_count).toBe(7);
      const partial = await advance(page, second.execution_id, {
        limit: 1,
        failure: "import_receipt_io",
      });
      expect(partial.status.status).toBe("partial");
      await refresh(page, financial);
      await expect(financial.getByRole("region", { name: "财务采集任务进度" })).toContainText(
        "部分完成",
      );
      await page.screenshot({ path: info.outputPath(`financial-partial-${viewport.width}.png`) });
      const continued = await originalCommand(page, () =>
        financial.getByRole("button", { name: "继续", exact: true }).click(),
      );
      expect(continued.execution_id).toBe(second.execution_id);
      const finished = await advance(page, second.execution_id, { limit: 8 });
      expect(finished.status.status).toBe("completed");
      expect(finished.sdk_calls).toEqual(partial.sdk_calls);
      await refresh(page, financial);
      await expect(financial.getByText("所选范围已完成", { exact: true })).toBeVisible();
      await expectNoHorizontalOverflow(page, "financial completed records");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);

      const detail = financial.getByText("所选范围已完成", { exact: true }).locator("..");
      if (viewport.touch) await detail.tap();
      else await detail.focus();
      await expect(page.getByRole("tooltip")).toContainText("只确认本次选定范围");
      await page.getByRole("heading", { level: 1, name: "数据中心" }).click();
      await expect(page.getByRole("tooltip")).not.toBeVisible();
      await page.screenshot({ path: info.outputPath(`financial-completed-${viewport.width}.png`) });

      // A new read checks the final original replica without an old source token.
      await page.reload();
      await page.getByRole("button", { name: "财务", exact: true }).click();
      await expect(page.getByRole("heading", { name: "财务概况", exact: true })).toBeVisible();
      await expect(page.getByRole("list", { name: "财务字段记录数" })).toBeVisible();
      await expect(
        page.getByRole("list", { name: "财务字段记录数" }).getByRole("listitem"),
      ).toHaveCount(6);
      await expectNoHorizontalOverflow(page, "final original fundamental summary");
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      expect(observer.problems).toEqual([]);
    });
  });
}
