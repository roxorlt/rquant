import { readFile } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import {
  portfolioCapabilities,
  portfolioDaily,
  portfolioHoldings,
  portfolioJobs,
  portfolioLog,
  portfolioMonthly,
  portfolioNav,
  portfolioSummary,
  portfolioTrades,
} from "../src/pages/backtest/portfolio.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

const jobId = portfolioSummary.data.job.job_id;
const resultHash = portfolioSummary.data.result_hash;
const base = "/app/api/v1/backtests/portfolio";
const runPath = `${base}/runs/${jobId}`;
const html =
  "<!doctype html><html lang='zh-CN'><meta charset='utf-8'><title>合成浏览器报告</title><p>合成浏览器报告</p></html>";
const zipId = "33333333-3333-4333-8333-333333333333";
let binaryServer: Server | undefined;

// Browser-managed download requests bypass page.route. Only this fixed synthetic
// HTML is served over actual HTTP; the native proof separately verifies sealed bytes.
test.beforeAll(async () => {
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? "/", "http://127.0.0.1:14874");
    if (
      request.method !== "GET" ||
      url.pathname !== `${runPath.slice("/app".length)}/report.html` ||
      url.searchParams.get("result_hash") !== resultHash ||
      url.searchParams.size !== 1
    ) {
      response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
      response.end("unknown synthetic binary");
      return;
    }
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "content-disposition": 'attachment; filename="portfolio-report.html"',
      "content-security-policy":
        "default-src 'none'; style-src 'unsafe-inline'; img-src data:; base-uri 'none'; frame-ancestors 'none'",
      "content-length": Buffer.byteLength(html),
      "cache-control": "no-store",
    });
    response.end(html);
  });
  binaryServer = server;
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(14874, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
});

test.afterAll(async () => {
  if (binaryServer?.listening) {
    const server = binaryServer;
    server.closeAllConnections();
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
  }
});

async function syntheticApi(page: Page, unavailable = false): Promise<string[]> {
  const problems: string[] = [];
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() === "error") problems.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", (request) => problems.push(`request failed: ${request.url()}`));
  page.on("request", (request) => {
    if (!request.url().startsWith("http://127.0.0.1:14873/") && !request.url().startsWith("data:"))
      problems.push(`external request: ${request.url()}`);
  });
  await page.clock.setFixedTime(new Date("2026-10-05T00:01:00Z"));
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const respond = (value: unknown) => route.fulfill({ status: 200, json: value });
    if (url.pathname === "/app/api/v1/meta") return respond(metaEnvelope());
    if (url.pathname === `${base}/capabilities`)
      return respond({
        ...portfolioCapabilities,
        data: unavailable
          ? {
              ...portfolioCapabilities.data,
              can_run: false,
              sources: [],
              default_config: null,
              message: "尚无可用回测来源，请先准备完整候选与行情。",
            }
          : { ...portfolioCapabilities.data, can_export: true },
      });
    if (url.pathname === `${base}/runs`) return respond(portfolioJobs);
    if (url.pathname === runPath) return respond(portfolioSummary);
    if (url.pathname === `${runPath}/nav`) {
      expect(url.searchParams.get("result_hash")).toBe(resultHash);
      return respond(portfolioNav);
    }
    if (url.pathname === `${runPath}/rows`) {
      expect(url.searchParams.get("result_hash")).toBe(resultHash);
      const view = url.searchParams.get("view");
      return respond(
        view === "holdings"
          ? portfolioHoldings
          : view === "daily"
            ? portfolioDaily
            : view === "monthly"
              ? portfolioMonthly
              : view === "log"
                ? portfolioLog
                : portfolioTrades,
      );
    }
    if (url.pathname === `${base}/exports` && request.method() === "POST") {
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      const body: Schemas["PortfolioExportRequest"] = request.postDataJSON();
      expect(body.job_id).toBe(jobId);
      expect(body.result_hash).toBe(resultHash);
      const receipt: Schemas["PortfolioCommandReceipt"] = {
        command_id: body.command_id,
        status: "exported",
        message: "报告已准备好。",
        job_id: jobId,
        result_hash: resultHash,
        zip_request_id: zipId,
        sha256: "a".repeat(64),
      };
      return respond(receipt);
    }
    problems.push(`unexpected API: ${request.method()} ${url.pathname}`);
    return route.fulfill({ status: 404, body: "unexpected synthetic endpoint" });
  });
  return problems;
}

test("完整组合页面、真实图表、五标签、键盘和报告入口", async ({ page }, testInfo) => {
  const problems = await syntheticApi(page);
  await page.goto("./#/backtest");
  await expect(page.getByRole("heading", { level: 1, name: "回测" })).toBeVisible();
  const nav = page.getByRole("img", { name: "组合与基准净值" });
  const drawdown = page.getByRole("img", { name: "组合回撤" });
  await expect(nav.locator("canvas")).toHaveCount(1);
  await expect(drawdown.locator("canvas")).toHaveCount(1);
  for (const chart of [nav, drawdown]) {
    const box = await chart.boundingBox();
    expect(box?.width).toBeGreaterThan(250);
    expect(box?.height).toBeGreaterThan(100);
  }
  for (const label of ["成交", "持仓", "每日", "月度", "日志"]) {
    await page.getByRole("tab", { name: label, exact: true }).click();
    const table = page.getByRole("table", { name: label, exact: true });
    await expect(table).toBeVisible();
    const row = table.locator("tbody tr:not(.pad)").first();
    await expect(row).toBeVisible();
    if (label === "成交") {
      await expect(table.locator("tbody tr:not(.pad) td:nth-child(8)")).toHaveText([
        "已成交",
        "已成交",
        "已成交",
      ]);
      await expect(row.locator("td").nth(4)).toHaveText("10.00");
      if (testInfo.project.name === "desktop") {
        await row.locator("td").nth(4).locator(".tip-anchor").focus();
        await expect(page.getByRole("tooltip")).toHaveText("完整成交价：10.0000");
        await row.focus();
        await expect(page.getByRole("tooltip")).toHaveCount(0);
      }
    }
    if (label === "日志") {
      await expect(table.locator("tbody tr:not(.pad) td:nth-child(4)")).toHaveText([
        "订单已成交。",
        "订单已成交。",
        "订单已成交。",
      ]);
    }
    if (label === "成交" || label === "持仓" || label === "每日") {
      await row.focus();
      await page.keyboard.press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      if (label === "成交")
        await expect(page.getByRole("dialog").getByText("10.0000", { exact: true })).toBeVisible();
      await expectNoHorizontalOverflow(page, `${testInfo.project.name} ${label} detail`);
      await page.keyboard.press("Escape");
      await expect(page.getByRole("dialog")).toHaveCount(0);
    }
  }
  await page.getByRole("tab", { name: "成交", exact: true }).click();
  const report = page.getByRole("link", { name: "HTML 报告" });
  await expect(report).toHaveAttribute(
    "href",
    new RegExp(`${runPath}/report.html\\?result_hash=${resultHash}$`),
  );
  const downloaded = page.waitForEvent("download");
  await report.click();
  const download = await downloaded;
  expect(download.suggestedFilename()).toBe("portfolio-report.html");
  const path = await download.path();
  expect(path).not.toBeNull();
  if (path !== null) expect(await readFile(path, "utf8")).toBe(html);
  await page.getByRole("button", { name: "导出 ZIP", exact: true }).click();
  await expect(page.getByRole("link", { name: "下载 ZIP" })).toHaveAttribute(
    "href",
    new RegExp(`${runPath}/exports/${zipId}\\.zip\\?result_hash=${resultHash}$`),
  );
  await expectNoHorizontalOverflow(page, `${testInfo.project.name} main`);
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("portfolio-page.png"), fullPage: true });

  await page.getByRole("button", { name: "绩效详情" }).click();
  const metrics = page.getByRole("dialog");
  await expect(metrics.getByRole("table", { name: "基准与超额" })).toBeVisible();
  await expect(metrics.getByRole("table", { name: "收益分布区间" })).toBeVisible();
  await expect(metrics.getByText("过拟合检验 · 尚未评估")).toBeVisible();
  await expectNoHorizontalOverflow(page, `${testInfo.project.name} metrics`);
  await page.keyboard.press("Escape");

  await page.getByRole("button", { name: "新建回测", exact: true }).click();
  const config = page.getByRole("dialog");
  await expect(config.getByLabel("初始资金（元）")).toBeVisible();
  await config.getByLabel("初始资金（元）").focus();
  await page.keyboard.press("Tab");
  await expect(config.getByLabel("调仓频率")).toBeFocused();
  await config.getByRole("checkbox", { name: "启用回撤限制" }).check();
  await expect(config.getByLabel("触发回撤（%）")).toBeVisible();
  await expectNoHorizontalOverflow(page, `${testInfo.project.name} configuration`);
  await page.screenshot({ path: testInfo.outputPath("portfolio-config.png"), fullPage: true });
  await page.keyboard.press("Escape");
  expect(problems).toEqual([]);
});

test("缺来源时仍能查看历史结果并说明如何继续", async ({ page }) => {
  const problems = await syntheticApi(page, true);
  await page.goto("./#/backtest");
  await expect(page.getByText("尚无可用回测来源，请先准备完整候选与行情。")).toBeVisible();
  await expect(page.getByRole("button", { name: "新建回测", exact: true })).toBeDisabled();
  await expect(page.getByRole("img", { name: "组合与基准净值" })).toBeVisible();
  await page.getByRole("button", { name: "查看配置" }).click();
  const config = page.getByRole("dialog");
  await expect(config.getByRole("option", { name: "原始候选来源（当前不可用）" })).toBeAttached();
  await expect(config.getByRole("button", { name: "运行回测" })).toBeDisabled();
  await expectNoHorizontalOverflow(page, "unavailable source configuration");
  await page.keyboard.press("Escape");
  expect(problems).toEqual([]);
});
