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
    if (url.pathname === "/app/api/v1/collaboration/me") {
      const envelope: Schemas["Envelope_CollaborationMe_"] = {
        serving: { ...metaEnvelope().serving, generation_id: null },
        data: {
          available: false,
          mode: "legacy",
          username: metaEnvelope().data.viewer,
          role: null,
          revision: null,
          state_sha256: null,
          can_manage_users: false,
          can_research: false,
          can_read_audit: false,
          message: "协作权限尚未启用。",
        },
      };
      return respond(envelope);
    }
    if (url.pathname === "/app/api/v1/ai/capabilities") {
      const envelope: Schemas["Envelope_AICapabilities_"] = {
        serving: metaEnvelope().serving,
        data: {
          available: false,
          can_generate: false,
          can_prepare_backtest: false,
          daily_limit: null,
          remaining_calls: null,
          message: "助手尚未配置，可继续手动编辑。",
        },
      };
      return respond(envelope);
    }
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
    if (label === "持仓") {
      await expect(row.locator("td").nth(5)).toHaveText("10.05");
      await expect(row.locator("td").nth(6)).toHaveText("10.00");
      if (testInfo.project.name === "desktop") {
        await row.locator("td").nth(5).locator(".tip-anchor").focus();
        await expect(page.getByRole("tooltip")).toHaveText("完整成本价：10.0500");
        await row.focus();
        await expect(page.getByRole("tooltip")).toHaveCount(0);
        await row.locator("td").nth(6).locator(".tip-anchor").focus();
        await expect(page.getByRole("tooltip")).toHaveText("完整市价：10");
        await row.focus();
        await expect(page.getByRole("tooltip")).toHaveCount(0);
      }
    }
    if (label === "成交" || label === "持仓" || label === "每日") {
      await row.focus();
      await page.keyboard.press("Enter");
      await expect(page.getByRole("dialog")).toBeVisible();
      if (label === "成交")
        await expect(page.getByRole("dialog").getByText("10.0000", { exact: true })).toBeVisible();
      if (label === "持仓")
        await expect(page.getByRole("dialog").getByText("10.0500", { exact: true })).toBeVisible();
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

test("新任务运行失败取消时保留上一份结果，控制和新成功结果分别绑定", async ({ page }, testInfo) => {
  const problems = await syntheticApi(page);
  const newHash = "2".repeat(64);
  const ids = [
    "22222222-2222-4222-8222-222222222222",
    "44444444-4444-4444-8444-444444444444",
    "55555555-5555-4555-8555-555555555555",
  ];
  let submissions = 0;
  let selectedId: string | null = null;
  let status: Schemas["PortfolioJob"]["status"] = "running";
  let version = 7;
  const controls: Schemas["LabControlRequest"][] = [];
  const requests: { job: string; hash: string | null }[] = [];
  const next = (): Schemas["Envelope_PortfolioSummaryData_"] => ({
    ...portfolioSummary,
    data: {
      ...portfolioSummary.data,
      job: {
        ...portfolioSummary.data.job,
        job_id: selectedId ?? ids[0] ?? "",
        status,
        label: {
          queued: "排队中",
          running: "运行中",
          paused: "已暂停",
          sealing: "正在保存",
          failed: "失败",
          cancelled: "已取消",
          completed: "已完成",
        }[status],
        version,
        result_hash: status === "completed" ? newHash : null,
        progress: status === "running" ? 0.1 : null,
        can_pause: status === "running",
        can_resume: status === "paused",
        can_cancel: ["running", "paused", "queued", "sealing"].includes(status),
        can_retry: status === "failed",
      },
      available: status === "completed",
      result_hash: status === "completed" ? newHash : null,
      result_status: status === "completed" ? "complete" : null,
      performance: status === "completed" ? portfolioSummary.data.performance : null,
      can_report: status === "completed",
      message: status === "completed" ? null : "完成后可查看新结果。",
    },
    serving: {
      ...portfolioSummary.serving,
      generation_id: status === "completed" ? newHash : null,
      state: status === "completed" ? "ready" : "unavailable",
    },
  });
  await page.route("**/api/v1/backtests/portfolio/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    if (url.pathname === `${base}/runs` && request.method() === "POST") {
      const body: Schemas["PortfolioCreateRequest"] = request.postDataJSON();
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      selectedId = ids[submissions] ?? null;
      expect(selectedId).not.toBeNull();
      submissions += 1;
      status = "running";
      version = 7;
      return route.fulfill({
        json: {
          command_id: body.command_id,
          status: "submitted",
          message: "已提交",
          job_id: selectedId,
        },
      });
    }
    if (url.pathname === `${base}/runs` && selectedId !== null)
      return route.fulfill({
        json: {
          ...portfolioJobs,
          data: { ...portfolioJobs.data, jobs: [next().data.job, ...portfolioJobs.data.jobs] },
        },
      });
    if (selectedId !== null && url.pathname === `${base}/runs/${selectedId}`)
      return route.fulfill({ json: next() });
    if (url.pathname.endsWith("/nav") || url.pathname.endsWith("/rows")) {
      const requestedJob = url.pathname.split("/").at(-2) ?? "";
      requests.push({ job: requestedJob, hash: url.searchParams.get("result_hash") });
      if (requestedJob === selectedId) {
        expect(status).toBe("completed");
        expect(url.searchParams.get("result_hash")).toBe(newHash);
        return route.fulfill({
          json: url.pathname.endsWith("/nav")
            ? {
                ...portfolioNav,
                data: { ...portfolioNav.data, result_hash: newHash },
                serving: { ...portfolioNav.serving, generation_id: newHash },
              }
            : {
                ...portfolioTrades,
                data: {
                  ...portfolioTrades.data,
                  result_hash: newHash,
                  trades: portfolioTrades.data.trades
                    .slice(0, 1)
                    .map((row) => ({ ...row, ts_code: "600123.SH" })),
                },
                serving: { ...portfolioTrades.serving, generation_id: newHash },
              },
        });
      }
    }
    return route.fallback();
  });
  await page.route("**/api/v1/tasks/jobs/commands", async (route) => {
    const body: Schemas["LabControlRequest"] = route.request().postDataJSON();
    expect(body.job_id).toBe(selectedId);
    expect(body.expected_version).toBe(version);
    expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
    controls.push(body);
    status =
      body.action === "pause" ? "paused" : body.action === "resume" ? "running" : "cancelled";
    version += 1;
    return route.fulfill({
      json: {
        command_id: body.command_id,
        status: "submitted",
        message: "操作已提交。",
        job_id: body.job_id,
      },
    });
  });
  await page.goto("./#/backtest");
  await expect(page.getByRole("img", { name: "组合与基准净值" }).locator("canvas")).toHaveCount(1);
  const start = async () => {
    await page.getByRole("button", { name: "新建回测", exact: true }).click();
    await page.getByRole("dialog").getByRole("button", { name: "运行回测" }).click();
    await expect(page.getByRole("dialog")).toHaveCount(0);
    await expect(page.getByText("上一份结果", { exact: true })).toBeVisible();
  };
  const retained = async () => {
    await expect(page.getByRole("img", { name: "组合与基准净值" }).locator("canvas")).toHaveCount(
      1,
    );
    await expect(page.getByRole("img", { name: "组合回撤" }).locator("canvas")).toHaveCount(1);
    await expect(page.getByRole("table", { name: "成交", exact: true })).toBeVisible();
    await expect(page.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
      "href",
      new RegExp(`${runPath}/report.html\\?result_hash=${resultHash}$`),
    );
    await expect(page.getByText("上一份结果", { exact: true })).toBeVisible();
    await expectNoHorizontalOverflow(page, `${testInfo.project.name} previous ${status}`);
  };
  await start();
  await retained();
  await page.getByRole("button", { name: "暂停", exact: true }).click();
  await expect(page.getByRole("button", { name: "继续", exact: true })).toBeVisible();
  await retained();
  await page.getByRole("button", { name: "继续", exact: true }).click();
  await expect(page.getByRole("button", { name: "暂停", exact: true })).toBeVisible();
  status = "failed";
  version += 1;
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.getByRole("button", { name: "重跑失败任务" })).toBeVisible();
  await retained();
  await page.getByRole("button", { name: "导出 ZIP", exact: true }).click();
  await expect(page.getByRole("link", { name: "下载 ZIP" })).toHaveAttribute(
    "href",
    new RegExp(`${runPath}/exports/${zipId}\\.zip\\?result_hash=${resultHash}$`),
  );
  await start();
  await page.getByRole("button", { name: "取消", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: "取消回测" }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.getByText("已取消", { exact: true }).last()).toBeVisible();
  await retained();
  expect(requests.every((item) => item.job === jobId && item.hash === resultHash)).toBe(true);
  await start();
  status = "completed";
  version += 1;
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.getByRole("link", { name: "HTML 报告" })).toHaveAttribute(
    "href",
    new RegExp(`${base}/runs/${ids[2]}/report.html\\?result_hash=${newHash}$`),
  );
  await expect(page.getByText("上一份结果", { exact: true })).toHaveCount(0);
  await expect(
    page.getByRole("table", { name: "成交", exact: true }).getByText("600123.SH"),
  ).toBeVisible();
  await expect(page.getByRole("link", { name: "下载 ZIP" })).toHaveCount(0);
  expect(controls.map((body) => [body.job_id, body.action])).toEqual([
    [ids[0], "pause"],
    [ids[0], "resume"],
    [ids[1], "cancel"],
  ]);
  await expectNoHorizontalOverflow(page, `${testInfo.project.name} new result`);
  expect(problems).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("portfolio-new-result.png"), fullPage: true });
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
