import { execSync } from "node:child_process";
import { expect, test } from "@playwright/test";
import { REPLAY_ROOT, REPO_ROOT, SERVING_ROOT, UV_RUN } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

for (const width of [1440, 390]) {
  test(`数据审计同源结果 ${width}px`, async ({ page }) => {
    test.skip(Boolean(REPLAY_ROOT), "回放副本只读，使用合成研究元数据验证审计链路");
    execSync(
      `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama --publish-next --audit`,
      {
        cwd: REPO_ROOT,
        env: {
          ...process.env,
          RQUANT_DISABLE_DOTENV: "1",
          TUSHARE_TOKEN_MAIN: "0000000000000000000000000000000000000000",
          DATA_DIR: SERVING_ROOT,
          DUCKDB_PATH: `${SERVING_ROOT}/audit-fixture.duckdb`,
          PARQUET_DIR: `${SERVING_ROOT}/parquet`,
          LOG_DIR: `${SERVING_ROOT}/logs`,
        },
      },
    );
    await expect
      .poll(async () => {
        const response = await page.request.get("./api/v1/data/health");
        return response.ok() ? (await response.json()).data.source_state : null;
      })
      .toBe("ready");

    const observer = watch(page);
    await page.setViewportSize({ width, height: 844 });
    await page.goto("./#/datacenter");
    const minute = page.getByRole("button", { name: /股票分钟线/ });
    await minute.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByText("审计失败")).toBeVisible();
    await expect(page.getByText("上次完成")).toBeVisible();
    const issues = page.getByRole("table", { name: "审计问题" });
    await expect(issues).toContainText("分钟线缺少日线");
    await expect(issues).toContainText("待处理");
    await expect(page.getByText("发现 2 条")).toBeVisible();
    await expect(page.getByText("全部 1 条")).toBeVisible();
    if (width === 390) {
      await page.getByRole("button", { name: "返回目录" }).click();
    }
    const limitUp = page.getByRole("button", { name: /东方财富涨停池/ });
    await limitUp.focus();
    await page.keyboard.press("Enter");
    await expect(page.getByRole("table", { name: "审计问题" })).toContainText("涨停池日期异常");
    await expect(page.getByText("全部 1 条")).toBeVisible();
    await expectNoHorizontalOverflow(page, "data audit");
    const body = await page.locator("main").innerText();
    expect(body).not.toContain("/private/path");
    expect(body).not.toContain("scope_key");
    expect(body).not.toContain("stage1-v3");
    expect(observer.problems).toEqual([]);
  });
}
