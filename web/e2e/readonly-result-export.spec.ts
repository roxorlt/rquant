import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { type Browser, expect, type Locator, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

async function offlineFile(
  browser: Browser,
  link: Locator,
  title: RegExp,
  marker: string,
): Promise<void> {
  const page = link.page();
  const promised = page.waitForEvent("download");
  await link.click();
  const download = await promised;
  expect(await download.failure()).toBeNull();
  const filename = download.suggestedFilename();
  expect(filename).toMatch(/\.html$/i);
  const file = test.info().outputPath(filename);
  await download.saveAs(file);
  const html = await readFile(file, "utf8");
  expect(html).not.toMatch(/<script\b|<iframe\b|<link\b|\son[a-z]+\s*=|javascript:/i);
  expect(html).toContain(marker);
  const context = await browser.newContext({
    offline: true,
    javaScriptEnabled: false,
    viewport: page.viewportSize() ?? { width: 390, height: 844 },
  });
  try {
    const local = await context.newPage();
    const remote: string[] = [];
    local.on("request", (request) => {
      if (!request.url().startsWith("file:") && !request.url().startsWith("data:"))
        remote.push(request.url());
    });
    await local.goto(pathToFileURL(file).href);
    await expect(local.getByRole("heading", { level: 1 })).toHaveText(title);
    await expect(local.locator("body")).toContainText(marker);
    await expect(local.locator("script, iframe, link[rel='stylesheet']")).toHaveCount(0);
    await expectNoHorizontalOverflow(local, "offline original report");
    expect(remote).toEqual([]);
  } finally {
    await context.close();
  }
}
for (const mode of ["desktop", "phone"] as const) {
  test.describe(`${mode} readonly export`, () => {
    test.use({
      viewport: mode === "phone" ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
      hasTouch: mode === "phone",
      isMobile: mode === "phone",
    });
    test("downloads original full portfolio, factor and exact-version strategy and opens offline", async ({
      page,
      browser,
    }) => {
      const monitor = watch(page);
      const metadata = await page.request.get("/app/api/v1/meta");
      const meta: Schemas["MetaData"] = (await metadata.json()).data;
      const generation = meta.generation?.generation_id;
      if (!generation || !meta.viewer)
        throw new Error("Root must publish original owned report fixtures");
      const meResponse = await page.request.get("/app/api/v1/collaboration/me");
      const me: Schemas["CollaborationMe"] = (await meResponse.json()).data;
      expect(me.available).toBe(true);
      expect(me.username).toBe(meta.viewer);
      const jobsResponse = await page.request.get("/app/api/v1/backtests/portfolio/runs");
      const jobs: Schemas["PortfolioJobsData"] = (await jobsResponse.json()).data;
      const portfolio = jobs.jobs.find(
        (job) => job.status === "completed" && job.result_hash !== null,
      );
      if (!portfolio?.result_hash) throw new Error("Root original portfolio worker seal required");
      await page.goto(`#/backtest?job=${portfolio.job_id}`);
      const html = page.getByRole("link", { name: "HTML 报告", exact: true });
      await expect(html).toHaveAttribute(
        "href",
        new RegExp(`report\\.html\\?result_hash=${portfolio.result_hash}$`),
      );
      const [year, month, day] = portfolio.start_date.split("-").map(Number);
      await offlineFile(browser, html, /组合|回测/, `${year}年${month}月${day}日`);
      const factorsResponse = await page.request.get(
        `/app/api/v1/factors/results?generation_id=${generation}`,
      );
      const factors: Schemas["FactorResultListData"] = (await factorsResponse.json()).data;
      const factor = factors.results.find(
        (item) => item.status === "succeeded" && item.display_status === "available",
      );
      if (!factor) throw new Error("Root original factor sealed research fixture required");
      const factorName = factor.factor_name_zh;
      if (!factorName) throw new Error("Root original published factor name required");
      await page.goto("#/factors");
      await page
        .getByRole("table", { name: "因子列表", exact: true })
        .getByText(factorName, { exact: true })
        .click();
      const factorLink = page
        .getByRole("region", { name: "检验结果", exact: true })
        .getByRole("link", { name: "导出只读页面", exact: true });
      await expect(factorLink).toHaveAttribute(
        "href",
        new RegExp(`/factors/results/${factor.job_id}/report\\?generation_id=${generation}$`),
      );
      await offlineFile(browser, factorLink, /因子/, factor.factor_id);
      const templateResponse = await page.request.get("/app/api/v1/strategy-templates");
      const templates: Schemas["StrategyTemplateCatalogData"] = (await templateResponse.json())
        .data;
      const template = templates.templates.find(
        (item) =>
          item.latest_run?.owner_id === meta.viewer &&
          item.latest_run.head.version === item.head.version &&
          item.latest_run.complete_result_hash.length === 64,
      );
      if (!template?.latest_run)
        throw new Error("Root original exact-version strategy worker seal required");
      await page.goto("#/strategies");
      await page
        .getByRole("table", { name: "我的策略", exact: true })
        .getByText(template.name, { exact: true })
        .click();
      const strategyLink = page
        .getByRole("dialog", { name: template.name, exact: true })
        .getByRole("link", { name: "导出只读页面", exact: true });
      await expect(strategyLink).toHaveAttribute(
        "href",
        new RegExp(
          `/template-results/${template.latest_run.job_id}/report\\.html\\?result_hash=${template.latest_run.complete_result_hash}$`,
        ),
      );
      await offlineFile(browser, strategyLink, /策略/, template.latest_run.input_hash);
      await expectNoHorizontalOverflow(page, `${mode} original report entry`);
      expect(monitor.problems).toEqual([]);
    });
  });
}
