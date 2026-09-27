import { expect, test } from "@playwright/test";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

const identityA = "a".repeat(64);
const identityB = "b".repeat(64);

function summary(identity = identityA, count = 1234) {
  const fields = [
    ["pe_ttm", "市盈率", "倍"],
    ["pb", "市净率", "倍"],
    ["dv_ttm", "股息率", "%"],
    ["roe", "净资产收益率", "%"],
    ["or_yoy", "营收同比", "%"],
    ["netprofit_yoy", "归母净利同比", "%"],
  ] as const;
  return {
    status: "ready",
    decision_date: "2026-09-23",
    waiting_for_today: true,
    source: { identity, updated_at: "2026-09-24T06:00:00Z" },
    record_count: count,
    coverage_note: "全市场覆盖尚未核验",
    fields: fields.map(([key, label, unit]) => ({
      key,
      label,
      unit,
      known_count: count - 34,
      unknown_count: 34,
      reasons: [{ label: "披露尚未可见", count: 34 }],
    })),
  };
}

for (const viewport of [
  { name: "desktop", width: 1440, height: 900 },
  { name: "phone", width: 390, height: 844 },
] as const) {
  test(`${viewport.name} financial summary is readable and source-bound`, async ({ page }) => {
    const observer = watch(page);
    await page.setViewportSize({ width: viewport.width, height: viewport.height });
    const identities: string[] = [];
    await page.route("**/api/v1/data/fundamentals/summary**", async (route) => {
      const expected = new URL(route.request().url()).searchParams.get("expected_identity") ?? "";
      identities.push(expected);
      if (expected === identityA) {
        await route.fulfill({ status: 409, json: { detail: "changed" } });
      } else {
        await route.fulfill({
          json: summary(
            identities.length === 1 ? identityA : identityB,
            identities.length === 1 ? 1234 : 1235,
          ),
        });
      }
    });
    await page.goto("./#/datacenter");
    const finance = page.getByRole("button", { name: "财务", exact: true });
    await finance.focus();
    await page.keyboard.press("Enter");
    const panel = page.getByRole("region", { name: "财务概况" });
    await expect(panel.getByText("1,234")).toBeVisible();
    await expect(panel.getByText("记录核对日")).toBeVisible();
    await expect(panel.getByText("副本同步时间")).toBeVisible();
    await expect(panel.getByText("全市场覆盖尚未核验")).toBeVisible();
    await expect(panel.getByRole("list", { name: "财务字段记录数" }).locator("li")).toHaveCount(6);
    await expectNoHorizontalOverflow(page, "financial summary");
    await page.screenshot({ path: `test-results/financial-${viewport.name}.png`, fullPage: true });
    await panel
      .getByRole("list", { name: "财务字段记录数" })
      .locator(".tip-anchor")
      .first()
      .focus();
    await expect(page.getByRole("tooltip")).toContainText("最近四季");

    await panel.getByRole("button", { name: "刷新财务数据" }).click();
    await expect(panel.getByText("1,235")).toBeVisible();
    expect(identities).toEqual(["", identityA, ""]);
    expect(await panel.innerText()).not.toContain(identityB);
    expect(
      observer.problems.filter(
        (problem) =>
          !(problem.startsWith("HTTP 409:") && problem.includes("/fundamentals/summary")) &&
          !(
            problem.startsWith("console error: Failed to load resource:") &&
            problem.includes("409 (Conflict)")
          ),
      ),
    ).toEqual([]);
  });
}
