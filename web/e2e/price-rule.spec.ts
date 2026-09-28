import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

type RuleEnvelope = Schemas["Envelope_PriceAlertRuleListData_"];
type ListEnvelope = Schemas["Envelope_ManualWatchlistListData_"];
type ExactEnvelope = Schemas["Envelope_ManualWatchlistExactData_"];
type Save = Schemas["SavePriceAlertRuleRequest"];

for (const width of [1440, 390]) {
  test(`价格规则创建、续查和发布 ${width}px`, async ({ page }, testInfo) => {
    await page.setViewportSize({ width, height: 844 });
    const initial = metaEnvelope();
    const oldGeneration = initial.serving.generation_id as string;
    const nextGeneration = "b".repeat(64);
    const now = Date.now();
    const built = new Date(now - 10_000).toISOString();
    const expiresAt = new Date(now + 60 * 60_000).toISOString();
    let published = false;
    let sent: Save | null = null;
    let posts = 0;
    let newBuilt = "";

    const meta = (next: boolean) => {
      const value = metaEnvelope({ generationId: next ? nextGeneration : oldGeneration });
      value.data.server_time = new Date(now + (next ? 5_000 : 0)).toISOString();
      if (value.data.generation) value.data.generation.built_at = next ? newBuilt : built;
      value.serving.built_at = next ? newBuilt : built;
      return value;
    };
    const list = (next: boolean): RuleEnvelope => ({
      serving: meta(next).serving,
      data: {
        availability: "ready",
        available_at: next ? newBuilt : built,
        evaluation_running: false,
        message: "",
        items:
          next && sent
            ? [
                {
                  rule_id: sent.rule.rule_id,
                  version: 1,
                  deleted: false,
                  ts_code: sent.ts_code,
                  membership_version: sent.membership_version,
                  name: sent.rule.name,
                  priority: sent.rule.priority,
                  enabled: sent.rule.enabled,
                  comparison: sent.rule.comparison,
                  threshold: String(sent.rule.threshold),
                  valid_from: sent.rule.valid_from,
                  valid_until: sent.rule.valid_until,
                  scope_status: "valid",
                  updated_at: newBuilt,
                },
              ]
            : [],
      },
    });
    const watchlist = (next: boolean): ListEnvelope => ({
      serving: meta(next).serving,
      data: {
        availability: "ready",
        available_at: next ? newBuilt : built,
        message: "",
        items: [
          {
            ts_code: "600001.SH",
            version: 2,
            source: "detail",
            price_levels: [],
            expires_at: expiresAt,
            updated_at: built,
          },
        ],
      },
    });
    const exact = (next: boolean): ExactEnvelope => ({
      serving: meta(next).serving,
      data: {
        availability: "ready",
        available_at: next ? newBuilt : built,
        ts_code: "600001.SH",
        status: "active",
        version: 2,
        source: "detail",
        message: "",
        price_levels: [],
        expires_at: expiresAt,
        updated_at: built,
      },
    });
    await page.route("**/api/v1/meta", (route) => route.fulfill({ json: meta(published) }));
    await page.route("**/api/v1/monitor/rules", (route) =>
      route.fulfill({ json: list(published) }),
    );
    await page.route("**/api/v1/watchlist", (route) =>
      route.fulfill({ json: watchlist(published) }),
    );
    await page.route("**/api/v1/watchlist/600001.SH", (route) =>
      route.fulfill({ json: exact(published) }),
    );
    await page.route("**/api/v1/monitor/rules/commands", (route) => {
      const body = route.request().postDataJSON() as Save;
      expect(route.request().headers()["x-rquant-csrf"]).toBe("1");
      if (sent) expect(body).toEqual(sent);
      sent = body;
      posts += 1;
      newBuilt = new Date(Date.parse(body.requested_at) + 2_000).toISOString();
      return route.fulfill({
        json: {
          command_id: body.command_id,
          kind: body.kind,
          rule_id: body.rule.rule_id,
          status: "saved_syncing",
          version: 1,
          reason: null,
          message: "已保存，正在同步。",
        },
      });
    });

    await page.goto("./#/monitor");
    const panel = page.getByRole("region", { name: "价格提醒规则" });
    await expect(panel.getByText("价格提醒尚未运行")).toBeVisible();
    const create = panel.getByRole("button", { name: "新建规则" });
    await expect(create).toBeEnabled();
    if (width === 1440) {
      await create.focus();
      await create.press("Enter");
    } else await create.click();
    await panel.getByLabel("名称").fill("跌破提醒");
    await panel.getByLabel("价格").fill("12.50");
    await panel.getByRole("button", { name: "保存规则" }).click();
    await expect(panel.getByText("已保存，正在同步")).toBeVisible();
    expect(posts).toBe(1);
    expect(sent).toMatchObject({
      kind: "save_price_alert_rule",
      generation_id: oldGeneration,
      ts_code: "600001.SH",
      membership_version: 2,
      expected_version: null,
    });
    await page.reload();
    await expect(panel.getByText("已保存，正在同步")).toBeVisible();
    await expect.poll(() => posts).toBeGreaterThanOrEqual(2);
    await expect(panel.getByRole("button", { name: "新建规则" })).toBeEnabled();
    published = true;
    await panel.getByRole("button", { name: "刷新规则" }).click();
    const rule = panel.getByRole("listitem", { name: "跌破提醒" });
    await expect(rule).toContainText("12.50");
    await expect(panel.getByText("规则已保存。")).toBeVisible();
    await expect(panel.getByText("价格提醒尚未运行")).toBeVisible();
    await expectNoHorizontalOverflow(page, `price rule ${width}px`);
    expect(findJargon(await page.locator("main").innerText())).toEqual([]);
    const captureDir = process.env.RQ_E2E_CAPTURE_DIR;
    await testInfo.attach(`price-rule-${width}px`, {
      body: await page.screenshot({
        fullPage: true,
        path: captureDir ? `${captureDir}/price-rule-${width}.png` : undefined,
      }),
      contentType: "image/png",
    });
  });
}

test("确定未保存后可在同代重新尝试，390px", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  const initial = metaEnvelope();
  const generation = initial.serving.generation_id as string;
  const now = Date.now();
  const built = new Date(now - 10_000).toISOString();
  const expiresAt = new Date(now + 60 * 60_000).toISOString();
  const meta = () => {
    const value = metaEnvelope();
    value.data.server_time = new Date(now).toISOString();
    if (value.data.generation) value.data.generation.built_at = built;
    value.serving.built_at = built;
    return value;
  };
  const sent: Save[] = [];
  await page.route("**/api/v1/meta", (route) => route.fulfill({ json: meta() }));
  await page.route("**/api/v1/monitor/rules", (route) =>
    route.fulfill({
      json: {
        serving: meta().serving,
        data: {
          availability: "ready",
          available_at: built,
          evaluation_running: false,
          message: "",
          items: [],
        },
      } satisfies RuleEnvelope,
    }),
  );
  await page.route("**/api/v1/watchlist", (route) =>
    route.fulfill({
      json: {
        serving: meta().serving,
        data: {
          availability: "ready",
          available_at: built,
          message: "",
          items: [
            {
              ts_code: "600001.SH",
              version: 2,
              source: "detail",
              price_levels: [],
              expires_at: expiresAt,
              updated_at: built,
            },
          ],
        },
      } satisfies ListEnvelope,
    }),
  );
  await page.route("**/api/v1/watchlist/600001.SH", (route) =>
    route.fulfill({
      json: {
        serving: meta().serving,
        data: {
          availability: "ready",
          available_at: built,
          ts_code: "600001.SH",
          status: "active",
          version: 2,
          source: "detail",
          message: "",
          price_levels: [],
          expires_at: expiresAt,
          updated_at: built,
        },
      } satisfies ExactEnvelope,
    }),
  );
  await page.route("**/api/v1/monitor/rules/commands", (route) => {
    const body = route.request().postDataJSON() as Save;
    sent.push(body);
    return route.fulfill({
      json: {
        command_id: body.command_id,
        kind: body.kind,
        rule_id: body.rule.rule_id,
        status: sent.length === 1 ? "failed" : "saved_syncing",
        version: sent.length === 1 ? null : 1,
        reason: null,
        message: sent.length === 1 ? "未保存" : "已保存，正在同步。",
      },
    });
  });

  await page.goto("./#/monitor");
  const panel = page.getByRole("region", { name: "价格提醒规则" });
  await expect(panel.getByRole("button", { name: "新建规则" })).toBeEnabled();
  await panel.getByRole("button", { name: "新建规则" }).click();
  await panel.getByLabel("名称").fill("失败后重试");
  await panel.getByLabel("价格").fill("12.50");
  await panel.getByRole("button", { name: "保存规则" }).click();
  const recovery = panel.getByRole("listitem", { name: "失败后重试" });
  await expect(recovery.getByText("未保存，可重试")).toBeVisible();
  await page.reload();
  await expect(recovery.getByRole("button", { name: "重新尝试" })).toBeEnabled();
  await recovery.getByRole("button", { name: "重新尝试" }).click();
  await expect(recovery.getByText("已保存，正在同步")).toBeVisible();
  expect(sent).toHaveLength(2);
  expect(sent[1]?.command_id).not.toBe(sent[0]?.command_id);
  expect(sent[1]?.generation_id).toBe(generation);
  await expect(panel.getByText("价格提醒尚未运行")).toBeVisible();
  await expectNoHorizontalOverflow(page, "failed price rule retry 390px");
});
