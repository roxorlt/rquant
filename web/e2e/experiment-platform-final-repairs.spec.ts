import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { experimentFixture as fixture } from "../src/pages/experiments/formal.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

// Original in-process sealed results and actual typed API responses. These HTTP
// transitions verify UI only; no market source, native execution or PIT claim.
async function api(
  page: Page,
  baseURL: string | undefined,
  options: { available?: boolean; writer?: boolean } = {},
) {
  if (!baseURL) throw new Error("an actual configured baseURL is required");
  const origin = new URL(baseURL).origin;
  const problems: string[] = [];
  const expectedFailures = new Map<string, number>();
  const seenFailures: string[] = [];
  let denial: 401 | 403 | null = null;
  let deniedReads = 0;
  let legacyReads = 0;
  const family = fixture.family.data;
  const base = "/app/api/v1/experiments";
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("console", (message) => {
    if (message.type() !== "error") return;
    const expected = expectedFailures.get(message.location().url);
    if (
      expected &&
      message
        .text()
        .startsWith(`Failed to load resource: the server responded with a status of ${expected} `)
    ) {
      seenFailures.push(message.location().url);
      return;
    }
    problems.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", (request) => problems.push(`request failed: ${request.url()}`));
  page.on("request", (request) => {
    if (!request.url().startsWith(`${origin}/`) && !request.url().startsWith("data:"))
      problems.push(`external request: ${request.url()}`);
  });
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const reply = (json: unknown) => route.fulfill({ status: 200, json });
    if (url.pathname === "/app/api/v1/meta")
      return reply(metaEnvelope({ viewer: "alice", generationId: fixture.generation_id }));
    if (url.pathname === `${base}/capabilities`) {
      if (denial) {
        deniedReads += 1;
        expectedFailures.set(url.href, denial);
        return route.fulfill({ status: denial, json: { detail: "当前账号无法读取" } });
      }
      return reply({
        ...fixture.capabilities,
        data: {
          ...fixture.capabilities.data,
          available: options.available ?? true,
          can_search: options.writer ?? true,
        },
      });
    }
    if (url.pathname === `${base}/mine`) return reply(fixture.mine);
    const familyPath = `${base}/families/${encodeURIComponent(family.family_id)}`;
    if (url.pathname === familyPath) return reply(fixture.family);
    if (url.pathname === `${familyPath}/heatmap`) return reply(fixture.heatmap);
    const item = family.items.find(
      (item) =>
        url.pathname === `${base}/results/${item.experiment_id}` ||
        url.pathname === `${base}/results/${item.experiment_id}/statistics`,
    );
    if (item) {
      expect(url.searchParams.get("generation_id")).toBe(fixture.generation_id);
      if (url.pathname.endsWith("/statistics"))
        return reply(fixture.statistics[item.experiment_id]);
      expect(url.searchParams.get("result_hash")).toBe(item.result_hash);
      return reply(fixture.results[item.experiment_id]);
    }
    if (url.pathname === `${base}/compare`) return reply(fixture.comparison);
    if (url.pathname === base && request.method() === "GET") {
      legacyReads += 1;
      const data: Schemas["ExperimentListData"] = {
        available: true,
        items: [
          {
            experiment_id: "e".repeat(64),
            hypothesis_family: "旧共享研究",
            registered_at: "2026-09-24T07:20:00Z",
            status: "succeeded",
            completed_at: "2026-09-24T07:25:00Z",
            trade_count: 12,
            net_return_pct: 7.5,
            max_drawdown_pct: 3.25,
            win_rate_pct: 60,
          },
        ],
        retained_count: 1,
        truncated: false,
        oldest_registered_at: "2026-09-24T07:20:00Z",
        next_cursor: null,
      };
      return reply({ data, serving: fixture.mine.serving });
    }
    problems.push(`unexpected API: ${request.method()} ${url.pathname}`);
    return route.fulfill({ status: 404, json: { detail: "unexpected synthetic endpoint" } });
  });
  return {
    problems,
    seenFailures,
    deny: (status: 401 | 403) => {
      denial = status;
    },
    deniedReads: () => deniedReads,
    legacyReads: () => legacyReads,
  };
}

test("M8补修：原归一净值与全N参数指标可读", async ({ page, baseURL }, info) => {
  const proof = await api(page, baseURL);
  await page.goto("./#/experiments");
  const mine = page.getByRole("table", { name: "我的实验", exact: true });
  await expect(mine.getByRole("row")).toHaveCount(fixture.mine.data.items.length + 1);
  await expect(mine.getByRole("columnheader", { name: "策略 / 版本" })).toBeVisible();
  await expect(mine.getByRole("columnheader", { name: "净收益" })).toBeVisible();
  await mine.getByRole("button", { name: "仓位实验 · 1", exact: true }).click();
  const drawer = page.getByRole("dialog", { name: "仓位实验", exact: true });
  await expect(drawer.getByRole("img", { name: "实验与基准净值" }).locator("canvas")).toHaveCount(
    1,
  );
  const matrix = drawer.getByRole("table", { name: "完整参数与指标" });
  await expect(matrix.getByRole("row")).toHaveCount(fixture.family.data.planned_count + 1);
  const first = matrix.getByRole("row").nth(1);
  const metrics = fixture.family.data.items[0]?.metrics;
  if (!metrics?.length) throw new Error("actual original row metrics are missing");
  await first
    .locator("summary")
    .filter({ hasText: /^全部指标$/ })
    .focus();
  await page.keyboard.press("Enter");
  for (const metric of metrics)
    await expect(first.getByText(metric.label, { exact: true })).toBeVisible();
  await first
    .locator("summary")
    .filter({ hasText: /^全部参数$/ })
    .click();
  await expect(first.getByText("单股上限", { exact: true })).toBeVisible();
  await drawer
    .locator("summary")
    .filter({ hasText: /^逐日净值$/ })
    .focus();
  await page.keyboard.press("Enter");
  const daily = drawer.getByRole("table", { name: "实验1逐日净值", exact: true });
  const result = fixture.results[fixture.family.data.items[0]?.experiment_id ?? ""];
  if (!result) throw new Error("actual original complete result is missing");
  await expect(daily.getByRole("row")).toHaveCount(result.data.curves.length + 1);
  for (const [index, point] of result.data.curves.entries()) {
    expect(point.nav).toBeLessThan(2);
    const cells = daily
      .getByRole("row")
      .nth(index + 1)
      .getByRole("cell");
    await expect(cells.nth(0)).toHaveText(point.trade_date);
    await expect(cells.nth(1)).toHaveText(
      point.nav.toLocaleString("zh-CN", { minimumFractionDigits: 4, maximumFractionDigits: 4 }),
    );
  }
  await expectNoHorizontalOverflow(page, "M8 normalized full metrics");
  await page.screenshot({ path: info.outputPath("m8-normalized-full-metrics.png") });
  expect(findJargon(await page.locator("body").innerText())).toEqual([]);
  const firstPoint = result.data.curves[0];
  if (!firstPoint) throw new Error("original daily value is missing");
  const precision = daily.getByRole("row").nth(1).getByRole("cell").nth(1).locator(".tip-anchor");
  if (info.project.use.isMobile) await precision.tap();
  else await precision.focus();
  await expect(page.getByRole("tooltip")).toHaveText(`完整净值：${firstPoint.nav}`);
  expect(proof.problems).toEqual([]);
});

for (const status of [401, 403] as const) {
  test(`M8补修：cached${status}撤下本人视图并保留原请求`, async ({ page, baseURL }) => {
    await page.clock.install({ time: new Date("2026-10-05T08:00:05Z") });
    const proof = await api(page, baseURL);
    const key = "rquant:experiment-request:v1:alice";
    const body: Schemas["ExperimentCancelWrite"] = {
      kind: "cancel_experiment_family",
      command_id: "00000000-0000-0000-0000-000000780041",
      requested_at: "2026-10-05T08:00:00Z",
      family_id: fixture.family.data.family_id,
    };
    await page.addInitScript(
      ({ key, body }) => sessionStorage.setItem(key, JSON.stringify({ owner: "alice", body })),
      { key, body },
    );
    await page.goto("./#/experiments");
    const mine = page.getByRole("table", { name: "我的实验", exact: true });
    await mine.getByRole("checkbox", { name: "选择仓位实验第1项" }).check();
    await mine.getByRole("button", { name: "仓位实验 · 1", exact: true }).click();
    await expect(page.getByRole("img", { name: "实验与基准净值" })).toBeVisible();
    proof.deny(status);
    // Public hash navigation unmounts only the page. The query client and the
    // previously successful capability remain cached while the mount refetch fails.
    await page.evaluate(() => {
      window.location.hash = "#/m8-proof-absent-page";
    });
    await expect(page.getByRole("heading", { name: "没有这个页面" })).toBeVisible();
    await page.clock.fastForward(61_000);
    await page.evaluate(() => {
      window.location.hash = "#/experiments";
    });
    await expect(
      page.getByRole("alert").filter({ hasText: "当前账号无法查看本人实验" }),
    ).toBeVisible();
    await expect(page.getByRole("table", { name: "我的实验", exact: true })).toHaveCount(0);
    await expect(page.getByRole("img", { name: "实验与基准净值" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "新建实验" })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "核对原请求" })).toHaveCount(0);
    expect(
      await page.evaluate((key) => JSON.parse(sessionStorage.getItem(key) ?? "{}"), key),
    ).toEqual({ owner: "alice", body });
    expect(proof.deniedReads()).toBeGreaterThan(0);
    expect(proof.legacyReads()).toBe(0);
    await expectNoHorizontalOverflow(page, "M8 cached permission withdrawal");
    expect(proof.problems).toEqual([]);
  });
}

for (const [label, available, writer] of [
  ["私有可用", true, true],
  ["writer关闭", true, false],
  ["私有不可用", false, false],
] as const) {
  test(`M8补修：${label}时旧共享入口独立可达`, async ({ page, baseURL }) => {
    const proof = await api(page, baseURL, { available, writer });
    await page.goto("./#/experiments");
    if (available) {
      await expect(page.getByRole("table", { name: "我的实验", exact: true })).toBeVisible();
      if (writer) await expect(page.getByRole("button", { name: "新建实验" })).toBeEnabled();
      else await expect(page.getByRole("button", { name: "新建实验" })).toBeDisabled();
      expect(proof.legacyReads()).toBe(0);
      await page.getByRole("button", { name: "旧共享记录", exact: true }).click();
    }
    const legacy = page.getByRole("table", { name: "实验记录", exact: true });
    await expect(legacy.getByText("旧共享研究", { exact: true })).toBeVisible();
    await expect(page.getByRole("table", { name: "我的实验", exact: true })).toHaveCount(0);
    await expect(page.getByRole("button", { name: "仓位实验 · 1", exact: true })).toHaveCount(0);
    expect(proof.legacyReads()).toBe(1);
    if (available) {
      await page.getByRole("button", { name: "我的实验", exact: true }).click();
      await expect(page.getByRole("table", { name: "我的实验", exact: true })).toBeVisible();
      await expect(page.getByRole("table", { name: "实验记录", exact: true })).toHaveCount(0);
    }
    await expectNoHorizontalOverflow(page, "M8 separate shared history");
    expect(proof.problems).toEqual([]);
  });
}
