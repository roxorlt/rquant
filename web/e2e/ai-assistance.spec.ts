import { expect, type Locator, type Page, test } from "@playwright/test";
import type { AIGenerateRequest } from "../src/api/aiAssistance.ts";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

async function closeDrawer(drawer: Locator): Promise<void> {
  await drawer
    .getByRole("button", { name: /close|关闭/i })
    .first()
    .click();
  await expect(drawer).not.toBeVisible();
}

async function citation(page: Page, anchor: Locator, phone: boolean): Promise<void> {
  if (phone) await anchor.click();
  else await anchor.focus();
  const tip = page.getByRole("tooltip");
  await expect(tip).toBeVisible();
  await expect(tip).toContainText("12.50%");
  await expect(tip.getByRole("link", { name: "查看原文" })).toHaveAttribute(
    "href",
    /^https:\/\/finance\.eastmoney\.com\/a\/600002\.html$/,
  );
  if (phone) await anchor.click();
  else await anchor.blur();
}

for (const mode of ["desktop", "phone"] as const) {
  test.describe(mode, () => {
    test.use({
      viewport: mode === "phone" ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
      hasTouch: mode === "phone",
      isMobile: mode === "phone",
    });
    test("uses original owners for assistant, pool edits, complete history, sealed interpretation and news", async ({
      page,
    }) => {
      const monitor = watch(page);
      const paid: AIGenerateRequest[] = [];
      const executions: Schemas["ExecuteScreenQuery"][] = [];
      page.on("request", (request) => {
        if (request.method() !== "POST") return;
        if (new URL(request.url()).pathname.endsWith("/ai/requests"))
          paid.push(request.postDataJSON());
        if (new URL(request.url()).pathname.endsWith("/screen/query/execute"))
          executions.push(request.postDataJSON());
      });
      const jobsResponse = await page.request.get("/app/api/v1/backtests/portfolio/runs");
      expect(jobsResponse.ok()).toBeTruthy();
      const jobs: Schemas["PortfolioJobsData"] = (await jobsResponse.json()).data;
      const sealed = jobs.jobs.find(
        (job) => job.status === "completed" && job.result_hash !== null,
      );
      expect(
        sealed,
        "Root must run the real original worker and finalizer before browser start",
      ).toBeDefined();
      if (!sealed) throw new Error("original sealed seed missing");
      const capabilityResponse = await page.request.get("/app/api/v1/ai/capabilities");
      const before: Schemas["AICapabilities"] = (await capabilityResponse.json()).data;
      expect(before.can_generate).toBe(true);
      await page.goto("#/overview");
      await expect(page.getByRole("heading", { name: "总览", exact: true })).toBeVisible();
      await expect(page.getByLabel("选择候选股票")).toHaveValue("");
      await page.getByLabel("选择候选股票").selectOption("600002.SH");
      await expect(page.getByRole("region", { name: "已披露事实" })).toContainText("12.50%");
      await expect(page.getByRole("region", { name: "预测与展望" })).toContainText("15.00%");
      await citation(
        page,
        page
          .getByRole("region", { name: "已披露事实" })
          .getByRole("button", { name: "查看摘要依据" })
          .last(),
        mode === "phone",
      );
      await expect(page.getByText("已完成 3 · 待处理 0")).toBeVisible();
      await expectNoHorizontalOverflow(page, `${mode} original news`);

      const topButton = page.getByRole("button", { name: "AI 助手", exact: true });
      await topButton.click();
      let drawer = page.getByRole("dialog", { name: "AI 助手", exact: true });
      await expect(drawer).toBeVisible();
      await drawer.getByRole("textbox", { name: "选股描述" }).fill("排除 ST，按流通市值从小到大");
      await drawer.getByRole("button", { name: "生成建议", exact: true }).click();
      await drawer.getByRole("button", { name: "应用到条件", exact: true }).click();
      await expect(drawer.getByRole("spinbutton", { name: "第 1 项权重" })).toHaveValue("1");
      await drawer.getByRole("button", { name: "撤销应用", exact: true }).click();
      await expect(drawer.getByRole("spinbutton", { name: "第 1 项权重" })).toHaveCount(0);
      await drawer.getByRole("button", { name: "继续查看原请求", exact: true }).click();
      await drawer.getByRole("button", { name: "应用到条件", exact: true }).click();
      await drawer.getByRole("spinbutton", { name: "第 1 项权重" }).fill("80");
      await drawer.getByRole("button", { name: "执行筛选", exact: true }).click();
      await expect(drawer.getByRole("table", { name: "助手筛选结果" })).toContainText("600001");
      expect(paid).toHaveLength(1);
      expect(executions).toHaveLength(1);
      const command = executions[0];
      expect(command?.definition.ranking?.conditions[0]?.weight).toBe(80);
      await expectNoHorizontalOverflow(page, `${mode} original screen result`);
      await page.reload();
      await topButton.click();
      drawer = page.getByRole("dialog", { name: "AI 助手", exact: true });
      await drawer.getByRole("button", { name: "继续查看筛选", exact: true }).click();
      await expect(drawer.getByRole("table", { name: "助手筛选结果" })).toContainText("600001");
      expect(executions).toHaveLength(1);
      expect(paid).toHaveLength(1);
      await drawer.getByLabel("开始日期").fill(sealed.start_date);
      await drawer.getByLabel("结束日期").fill(sealed.end_date);
      await drawer.getByRole("button", { name: "准备完整区间", exact: true }).click();
      await expect(drawer.getByText("完整交易日", { exact: true })).toBeVisible();
      await drawer.getByRole("button", { name: "核对并确认回测", exact: true }).click();
      const confirm = page.getByRole("dialog", { name: "确认组合回测", exact: true });
      await expect(confirm).toContainText("每日");
      await confirm.getByRole("button", { name: "确认回测", exact: true }).click();
      await expect(drawer.getByRole("link", { name: "查看运行与结果" })).toHaveAttribute(
        "href",
        /job=/,
      );
      await closeDrawer(drawer);
      await expect(topButton).toBeFocused();

      await page.goto("#/pools");
      await page.getByRole("button", { name: "查看 研究样本池条件", exact: true }).click();
      await page.getByRole("button", { name: "用一句话改池子", exact: true }).click();
      const editor = page.getByRole("dialog", { name: "编辑规则", exact: true });
      await editor
        .getByRole("textbox", { name: "修改描述" })
        .fill("添加流通市值低于两百亿元的条件");
      await editor.getByRole("button", { name: "解析并预览", exact: true }).click();
      await expect(editor.getByRole("region", { name: "建议预览" })).toContainText("新增");
      await editor.getByRole("button", { name: "应用到草稿", exact: true }).click();
      await expect(editor.getByRole("spinbutton", { name: /市值/ })).toHaveValue("200");
      await editor.getByRole("button", { name: "预览变更", exact: true }).click();
      const savedResponse = page.waitForResponse(
        (response) =>
          response.request().method() === "POST" &&
          new URL(response.url()).pathname.endsWith("/pools/editor/commands"),
      );
      await editor.getByRole("button", { name: "保存规则", exact: true }).click();
      expect((await savedResponse).ok()).toBeTruthy();
      await expect(editor.getByRole("status").filter({ hasText: "池子已保存" })).toContainText(
        "池子已保存",
      );
      await editor.getByRole("button", { name: "返回画布", exact: true }).click();
      await expect(editor).not.toBeVisible();
      const poolResponse = await page.request.get("/app/api/v1/pools/editor");
      const pools: Schemas["PoolEditorData"] = (await poolResponse.json()).data;
      const pool = pools.pools.find((item) => item.key === "user/ai-owned-pool");
      expect(pool?.depends_on).toBe("n-shape-pool1");
      expect(pool?.delay_days).toBe(2);
      expect(pool?.include_columns).toEqual(["CLOSE[0]"]);
      expect(pool?.ranking?.top_n).toBe(2);
      expect(pool?.rule_calls.map((rule) => rule.name)).toEqual(["not_st", "circ_mv_lt"]);

      await page.goto(`#/backtest?tab=portfolio&job=${sealed.job_id}`);
      const interpretation = page
        .locator("section.panel")
        .filter({ has: page.getByRole("heading", { name: "AI 解读", exact: true }) });
      await interpretation.getByRole("button", { name: "生成解读", exact: true }).click();
      for (const name of ["概要", "分年表现", "风险", "下一步建议"])
        await expect(interpretation.getByRole("region", { name, exact: true })).toBeVisible();
      await interpretation
        .getByRole("button", { name: /^查看.*依据$/ })
        .first()
        .focus();
      if (mode === "phone")
        await interpretation
          .getByRole("button", { name: /^查看.*依据$/ })
          .first()
          .click();
      await expect(page.getByRole("tooltip")).toContainText("封存原值");
      await page.getByRole("heading", { name: "AI 解读", exact: true }).click();
      await page.getByRole("button", { name: "我的", exact: true }).click();
      await page.getByRole("menuitem", { name: "AI 用量", exact: true }).click();
      const usage = page.getByRole("dialog", { name: "AI 用量", exact: true });
      await expect(usage.getByRole("table", { name: "每日 AI 用量" })).toBeVisible();
      await expect(usage).toContainText("本人调用");
      await closeDrawer(usage);
      expect(paid.map((request) => request.purpose)).toEqual([
        "screen",
        "pool_edit",
        "interpretation",
      ]);
      expect(new Set(paid.map((request) => request.request_id)).size).toBe(3);
      const after: Schemas["AICapabilities"] = (
        await (await page.request.get("/app/api/v1/ai/capabilities")).json()
      ).data;
      expect(after.remaining_calls).toBe((before.remaining_calls ?? 0) - 3);
      await page.reload();
      for (const name of ["概要", "分年表现", "风险", "下一步建议"])
        await expect(interpretation.getByRole("region", { name, exact: true })).toBeVisible();
      await expect(
        interpretation.getByRole("button", { name: "生成解读", exact: true }),
      ).toHaveCount(0);
      await interpretation.getByRole("button", { name: "刷新解读", exact: true }).click();
      await expect(interpretation.getByRole("region", { name: "概要", exact: true })).toBeVisible();
      expect(paid).toHaveLength(3);
      const originalInterpretation = paid.find((request) => request.purpose === "interpretation");
      if (!originalInterpretation) throw new Error("original interpretation request missing");
      const cachedRequest: AIGenerateRequest = {
        ...originalInterpretation,
        request_id: await page.evaluate(() => crypto.randomUUID()),
      };
      let cacheResponse = await page.request.post("/app/api/v1/ai/requests", {
        headers: { "X-Rquant-Csrf": "1" },
        data: cachedRequest,
      });
      if (cacheResponse.status() === 429) {
        expect((await cacheResponse.json()).detail).toBe("操作太频繁，请一分钟后再试。");
        const retryAfter = Number(cacheResponse.headers()["retry-after"]);
        expect(retryAfter).toBe(60);
        await new Promise((resolve) => setTimeout(resolve, retryAfter * 1_000));
        cacheResponse = await page.request.post("/app/api/v1/ai/requests", {
          headers: { "X-Rquant-Csrf": "1" },
          data: cachedRequest,
        });
      }
      expect(cacheResponse.ok()).toBe(true);
      const cached: Schemas["AIRequestView"] = (await cacheResponse.json()).data;
      expect(cached.request_id).toBe(cachedRequest.request_id);
      expect(cached.state).toBe("completed");
      expect(cached.result?.purpose).toBe("interpretation");
      if (cached.result?.purpose !== "interpretation")
        throw new Error("original full sealed interpretation cache missing");
      expect(cached.result.interpretation.binding.result.job_id).toBe(sealed.job_id);
      expect(cached.result.interpretation.binding.result.result_sha256).toBe(sealed.result_hash);
      const afterCache: Schemas["AICapabilities"] = (
        await (await page.request.get("/app/api/v1/ai/capabilities")).json()
      ).data;
      expect(afterCache.remaining_calls).toBe(after.remaining_calls);
      await test.info().attach("original-full-result-cache-reuse", {
        body: JSON.stringify({
          cachedRequest,
          cached,
          remaining_calls: afterCache.remaining_calls,
        }),
        contentType: "application/json",
      });
      await expectNoHorizontalOverflow(page, `${mode} complete AI workflow`);
      expect(findJargon(await page.locator("body").innerText())).toEqual([]);
      expect(monitor.problems).toEqual([]);
    });
  });
}
