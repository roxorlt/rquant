import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { API_NOW } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

// Root runs this against the real compiled UI and offline owner-published fixture.
// No browser response is populated with invented health facts.
for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test.describe(`${viewport.name} system health`, () => {
    test.use({
      viewport: { width: viewport.width, height: viewport.height },
      hasTouch: viewport.name === "phone",
    });

    test("shows six layers, original source links and readable missing states", async ({
      page,
    }) => {
      const watcher = watch(page);
      await page.clock.setFixedTime(new Date(API_NOW));
      const response = await page.request.get("./api/v1/health");
      expect(response.status()).toBe(200);
      const envelope = (await response.json()) as Schemas["Envelope_HealthData_"];
      const layers = envelope.data.layers;
      expect(layers?.map((item) => item.key)).toEqual([
        "host",
        "market",
        "strategy",
        "orders",
        "risk",
        "comparison",
      ]);
      await page.goto("./#/health");
      await expect(page.getByRole("heading", { level: 1, name: "系统健康" })).toBeVisible();
      for (const layer of layers ?? []) {
        const card = page.getByRole("region", { name: layer.name, exact: true });
        await expect(card).toBeVisible();
        for (const link of layer.links) {
          await expect(card.getByRole("link", { name: link.label, exact: true })).toHaveAttribute(
            "href",
            new RegExp(`${link.href}$`),
          );
        }
        for (const item of layer.metrics.filter((metric) => !metric.available)) {
          await expect(
            card.locator(".health-metrics li").filter({ hasText: item.name }).first(),
          ).toContainText("—");
        }
      }
      expect(findJargon(await page.locator("main").innerText())).toEqual([]);
      await expectNoHorizontalOverflow(page, "six health layers");
      const refresh = page.getByRole("button", { name: "刷新", exact: true });
      await refresh.focus();
      const refreshed = page.waitForResponse(
        (item) => item.url().endsWith("/api/v1/health") && item.status() === 200,
      );
      await refresh.press("Enter");
      await refreshed;
      await expect(refresh).toBeEnabled();
      expect(watcher.problems).toEqual([]);
    });

    test("keeps real account exposure separate and opens verified heartbeat details with keyboard", async ({
      page,
    }) => {
      const watcher = watch(page);
      await page.clock.setFixedTime(new Date(API_NOW));
      const response = await page.request.get("./api/v1/health");
      expect(response.status()).toBe(200);
      const envelope = (await response.json()) as Schemas["Envelope_HealthData_"];
      const exposure = envelope.data.layers?.find((item) => item.key === "risk")?.exposure ?? [];
      const scopes = new Map(exposure.map((row) => [row.scope_key, row.scope_label]));
      expect(
        scopes.size,
        "fixture must publish two genuine accounts in one complete owner graph",
      ).toBeGreaterThanOrEqual(2);
      const service = envelope.data.services.find(
        (item) => item.detail?.available && item.detail.observations.length,
      );
      expect(
        service,
        "fixture must start and read a service with a verified same-read witness",
      ).toBeDefined();
      if (!service) throw new Error("missing verified service fixture");
      await page.goto("./#/health");
      for (const label of scopes.values()) {
        const table = page.getByRole("table", { name: `${label}暴露`, exact: true });
        await expect(table).toBeVisible();
        for (const row of exposure.filter((item) => item.scope_label === label)) {
          await expect(table.getByRole("cell", { name: row.name, exact: true })).toBeVisible();
        }
      }
      for (const layer of envelope.data.layers ?? []) {
        if (!["orders", "risk", "comparison"].includes(layer.key)) continue;
        const card = page.getByRole("region", { name: layer.name, exact: true });
        for (const item of layer.metrics) {
          const row = card.locator(".health-metrics li").filter({
            has: page.getByRole("button", {
              name: `${item.scope_label}${item.name}详情`,
              exact: true,
            }),
          });
          await expect(row).toHaveCount(1);
          await expect(row.locator(".health-metric-scope")).toHaveText(item.scope_label);
          await expect(row.locator(".health-metric-scope")).toBeVisible();
          expect(await row.innerText()).not.toContain("account_id");
        }
      }
      const services = page.getByRole("table", { name: "运行服务" });
      const row = services.getByRole("row").filter({ hasText: service.name }).first();
      await row.focus();
      await row.press("Enter");
      const drawer = page.getByRole("dialog", { name: service.name });
      await expect(drawer).toBeVisible();
      for (const observation of service.detail?.observations ?? []) {
        await expect(drawer).toContainText(observation.label);
        await expect(drawer).toContainText(String(observation.value));
      }
      await expectNoHorizontalOverflow(page, "heartbeat detail");
      await page.keyboard.press("Escape");
      await expect(drawer).toHaveCount(0);
      const metric = envelope.data.layers?.flatMap((item) => item.metrics)[0];
      expect(metric, "fixture must retain an original metric").toBeDefined();
      if (!metric) throw new Error("missing original metric fixture");
      const details = page.getByRole("button", { name: `${metric.name}详情` }).first();
      if (viewport.name === "phone") await details.tap();
      else await details.focus();
      await expect(page.getByRole("tooltip")).toContainText(metric.source_generation_id);
      await expectNoHorizontalOverflow(page, "health source tip");
      expect(watcher.problems).toEqual([]);
    });
  });
}
