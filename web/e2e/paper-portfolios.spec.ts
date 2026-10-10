import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import {
  paperAccount,
  paperDetail,
  paperEnvelope,
  paperGeneration,
  paperHistory,
} from "../src/pages/paper/paperPortfolio.fixture.ts";
import { metaEnvelope } from "../src/test/fixtures.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

type Command =
  | Schemas["SavePaperPortfolioConfiguration"]
  | Schemas["SetPaperAccountPaused"]
  | Schemas["RunPaperPortfolioResearch"];
type Receipt = Schemas["PaperPortfolioCommandData"];
const base = "/app/api/v1/paper-portfolios";
const start = paperDetail.data.configuration.configured_at;

// The exported baseline is from the real synthetic Web API. State variants here
// verify only React transport/interaction; native-paper-root-02 proves authority.
async function fixture(page: Page, baseURL: string | undefined, uncertain = false) {
  if (!baseURL) throw new Error("paper fixture needs the actual baseURL");
  const allowedOrigin = new URL(baseURL).origin;
  const problems: string[] = [];
  const operations: Command[] = [];
  const retries: Command[] = [];
  const previews: Command[] = [];
  let generation = paperGeneration;
  let viewer = "alice";
  let empty = false;
  let unavailable = false;
  const detail = structuredClone(paperDetail.data);
  let last: Receipt | null = null;
  let releaseCatalog: (() => void) | null = null;
  let holdCatalog = false;
  const first = paperHistory.data.records[0];
  if (!first || !detail.account) throw new Error("missing actual baseline record/account");
  let summary: Schemas["PaperResearchSummary"] | null = null;
  page.on("pageerror", (error) => problems.push(error.message));
  page.on("console", (message) => {
    if (message.type() === "error") problems.push(message.text());
  });
  page.on("requestfailed", (request) => problems.push(`failed request: ${request.url()}`));
  page.on("request", (request) => {
    if (new URL(request.url()).origin !== allowedOrigin && !request.url().startsWith("data:"))
      problems.push(`external request: ${request.url()}`);
  });
  await page.clock.setFixedTime(new Date("2026-07-31T01:32:05Z"));
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    const json = (data: unknown) => route.fulfill({ json: paperEnvelope(data, generation) });
    if (path === "/app/api/v1/meta")
      return route.fulfill({ json: metaEnvelope({ generationId: generation, viewer }) });
    if (request.method() === "GET" && path === "/app/api/v1/collaboration/me") {
      const meta = metaEnvelope({ generationId: generation, viewer });
      const envelope: Schemas["Envelope_CollaborationMe_"] = {
        serving: { ...meta.serving, generation_id: null },
        data: {
          available: false,
          mode: "legacy",
          username: meta.data.viewer,
          role: null,
          revision: null,
          state_sha256: null,
          can_manage_users: false,
          can_research: false,
          can_read_audit: false,
          message: "协作权限尚未启用。",
        },
      };
      return route.fulfill({ json: envelope });
    }
    if (path === "/app/api/v1/health") return json({ available: false, units: [] });
    if (request.method() === "POST") {
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      const decoded = request.postDataJSON();
      const command: Command = path.endsWith("/pause/confirm") ? decoded.request : decoded;
      expect(Object.keys(command)).not.toContain("owner_id");
      expect(Object.keys(command)).not.toContain("path");
      expect(command.account_id).toBe(paperAccount);
      if (path.endsWith("/pause/prepare")) {
        previews.push(command);
        return json({
          command,
          confirmation_id: "synthetic-confirmation",
          expires_at: "2026-07-31T01:37:05Z",
        });
      }
      if (path.endsWith("/recover")) {
        retries.push(command);
        expect(command).toEqual(operations.at(-1));
        if (last?.status === "waiting_application") {
          detail.operator = {
            ...detail.operator,
            paused: command.kind === "set_paper_account_paused" && command.paused,
            status: "applied",
            sequence: last.sequence ?? 2,
            control_fingerprint: "e".repeat(64),
          };
          last = { ...last, status: "applied", message: "已应用。" };
        } else if (last?.status === "waiting_publication" || last?.status === "uncertain") {
          last = { ...last, status: "published", message: "已保存。" };
        }
        return json(last);
      }
      operations.push(command);
      if (uncertain) {
        last = {
          command_id: command.command_id,
          account_id: paperAccount,
          status: "uncertain",
          message: "结果待确认，请继续查看。",
        };
        return json(last);
      }
      if (
        path.endsWith("/configuration") &&
        command.kind === "save_paper_portfolio_configuration"
      ) {
        last = {
          command_id: command.command_id,
          account_id: paperAccount,
          status: "waiting_publication",
          configuration_version: 2,
          message: "规则已保存，等待发布。",
        };
      } else if (path.endsWith("/pause/confirm") && command.kind === "set_paper_account_paused") {
        expect(decoded.confirmation_id).toBe("synthetic-confirmation");
        expect(command).toEqual(previews.at(-1));
        last = {
          command_id: command.command_id,
          account_id: paperAccount,
          status: "waiting_application",
          sequence: 2,
          message: "已提交，等待应用。",
        };
      } else if (
        (path.endsWith("/reconcile") || path.endsWith("/band")) &&
        command.kind === "run_paper_portfolio_research"
      ) {
        last = {
          command_id: command.command_id,
          account_id: paperAccount,
          status: "submitted",
          job_id: command.command_id,
          message: "研究已提交，等待封存。",
        };
        summary = {
          task_name: command.task_name,
          job_id: command.command_id,
          account_id: paperAccount,
          configuration_fingerprint: detail.configuration.fingerprint,
          configuration_version: 1,
          status: "submitted",
          accepted_at: command.requested_at,
          reason: "结果尚未封存",
          sealed: null,
        };
        detail.recent_research = [summary];
      } else {
        problems.push(`unexpected POST ${path}`);
        return route.fulfill({ status: 404 });
      }
      return json(last);
    }
    if (path === base) {
      if (holdCatalog)
        await new Promise<void>((resolve) => {
          releaseCatalog = resolve;
        });
      return json({
        availability: empty ? "empty" : "populated",
        available_at: start,
        accounts: empty ? [] : [detail],
      });
    }
    if (path === `${base}/${paperAccount}`)
      return json(
        unavailable
          ? { ...detail, account: null, status: "unavailable", reason: "估值缺数据" }
          : detail,
      );
    if (path === `${base}/${paperAccount}/history`) {
      const cursor = new URL(request.url()).searchParams.get("cursor");
      if (cursor) expect(cursor).toBe("synthetic-original-cursor");
      return json({
        ...paperHistory.data,
        total_orders: 201,
        records: [cursor ? { ...first, sequence: 1 } : { ...first, sequence: 201 }],
        next_cursor: cursor ? null : "synthetic-original-cursor",
      });
    }
    if (summary && path === `${base}/${paperAccount}/research/${summary.job_id}`)
      return json(summary);
    if (summary && path === `${base}/${paperAccount}/research/${summary.job_id}/download`)
      return route.fulfill({
        contentType: "application/zip",
        body: Buffer.from("UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA==", "base64"),
      });
    problems.push(`unhandled ${request.method()} ${path}`);
    return route.fulfill({ status: 404, json: { detail: "unhandled fixture path" } });
  });
  return {
    problems,
    operations,
    retries,
    previews,
    changeGeneration() {
      generation = "b".repeat(64);
    },
    changeViewer() {
      viewer = "bob";
      empty = true;
    },
    setEmpty(value: boolean) {
      empty = value;
    },
    setUnavailable() {
      unavailable = true;
    },
    hold() {
      holdCatalog = true;
    },
    release() {
      holdCatalog = false;
      releaseCatalog?.();
    },
    addBand() {
      detail.backtests = [
        {
          job_id: "11111111-1111-4111-8111-111111111111",
          completed_at: start,
          name: "同版已封存回测",
        },
      ];
      detail.nav = [
        {
          trade_date: "2026-07-30",
          configuration_fingerprint: detail.configuration.fingerprint,
          calendar_source_identity: "a".repeat(64),
          ledger_revision: 1,
          ledger_head_fingerprint: "b".repeat(64),
          material_fingerprint: "c".repeat(64),
          close_at: "2026-07-30T07:00:00Z",
          published_at: "2026-07-30T07:00:00Z",
          status: "unavailable",
          reason: "收盘估值缺数据",
          nav: null,
          cash: null,
          daily_return: null,
          normalized_nav: null,
        },
      ];
    },
    completeBand() {
      unavailable = false;
      detail.can_band = true;
      detail.nav = detail.nav.map((row) => ({
        ...row,
        status: "complete",
        reason: null,
        nav: "995",
        cash: "195",
        daily_return: "-0.005",
        normalized_nav: "0.995",
      }));
    },
    seal() {
      if (!summary || !detail.account) throw new Error("no original synthetic research operation");
      summary = {
        ...summary,
        status: "succeeded",
        reason: null,
        sealed: {
          task_name: "paper_reconcile",
          job_id: summary.job_id,
          account_id: paperAccount,
          configuration_fingerprint: detail.configuration.fingerprint,
          configuration_version: 1,
          spec_hash: "a".repeat(64),
          manifest_hash: "b".repeat(64),
          complete_result_hash: "c".repeat(64),
          result_hash: "d".repeat(64),
          completed_at: "2026-07-31T01:32:05Z",
          band: null,
          reconcile: {
            contract: "paper-reconcile-result/v1",
            input_hash: "a".repeat(64),
            configuration_fingerprint: detail.configuration.fingerprint,
            ledger_revision: 1,
            head_fingerprint: "b".repeat(64),
            copy_sha256: "c".repeat(64),
            expected_fingerprint: "d".repeat(64),
            actual_fingerprint: "d".repeat(64),
            status: "consistent",
            difference_count: 0,
            differences: [],
            truncated: false,
            account: detail.account,
          },
        },
      };
      detail.recent_research = [summary];
      generation = "c".repeat(64);
    },
  };
}

test("settings, original application, readonly result and keyboard history", async ({
  page,
  baseURL,
}, testInfo) => {
  const state = await fixture(page, baseURL);
  await page.goto("./#/paper");
  const card = page.getByRole("button", { name: "查看模拟账户 1" });
  await card.focus();
  await page.keyboard.press("Enter");
  const action = page.getByRole("button", { name: "设置仓位", exact: true });
  await action.click();
  const rules = page.getByRole("dialog", { name: "仓位与回撤" });
  await rules.getByRole("checkbox", { name: "回撤限制", exact: true }).check();
  const tip = rules.getByRole("button", { name: "回撤限制说明" });
  if (testInfo.project.name === "phone") await tip.tap();
  else {
    await tip.focus();
    await tip.hover();
  }
  await expect(page.getByRole("tooltip")).toContainText("新的净值序列");
  await expect(rules.getByRole("checkbox", { name: "回撤限制", exact: true })).toBeChecked();
  await rules.getByLabel("触发回撤（%）").fill("100");
  await rules.getByRole("button", { name: "保存规则" }).click();
  await expect(rules.getByRole("alert")).toContainText("须小于 100%");
  expect(state.operations).toHaveLength(0);
  await rules.getByLabel("触发回撤（%）").fill("8.5");
  await rules.getByRole("button", { name: "保存规则" }).click();
  await expect(rules).toContainText("规则已保存，等待发布。");
  await expect(rules.getByRole("button", { name: "保存规则" })).toBeDisabled();
  await rules.getByRole("button", { name: "继续查看" }).click();
  await expect(rules).toContainText("已保存。");
  await page.keyboard.press("Escape");
  await expect(action).toBeFocused();
  await page.getByRole("button", { name: "暂停新入场", exact: true }).click();
  const confirm = page.getByRole("dialog", { name: "暂停新入场" });
  await expect(confirm.getByRole("button", { name: "确认执行" })).toBeDisabled();
  await confirm.getByRole("textbox").fill("自选策略");
  await confirm.getByRole("button", { name: "确认执行" }).click();
  await expect(page.getByText("已提交，等待应用。", { exact: true })).toBeVisible();
  await expect(page.getByText("运行中", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "继续查看" }).click();
  await expect(page.getByText("已暂停", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "只读对账", exact: true }).click();
  await expect(page.getByText("研究已提交，等待封存。", { exact: true })).toBeVisible();
  await page.reload();
  await page.getByRole("button", { name: "查看模拟账户 1" }).click();
  await page.getByRole("button", { name: "看结果" }).click();
  await expect(page.getByRole("dialog", { name: "模拟盘研究结果" })).toContainText("结果等待完成");
  await page.keyboard.press("Escape");
  state.seal();
  await page.reload();
  await page.getByRole("button", { name: "查看模拟账户 1" }).click();
  await page.getByRole("button", { name: "看结果" }).click();
  await expect(page.getByRole("dialog", { name: "模拟盘研究结果" })).toContainText(
    "对账完成，无差异。",
  );
  const download = page.waitForEvent("download");
  await page.getByRole("button", { name: "下载结果" }).click();
  expect((await download).suggestedFilename()).toBe("模拟盘研究结果.zip");
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "完整历史", exact: true }).click();
  await page.getByRole("button", { name: "下一页", exact: true }).click();
  const row = page.getByRole("table", { name: "完整模拟指令" }).getByRole("row").nth(1);
  await row.focus();
  await page.keyboard.press("Enter");
  await expect(page.getByRole("dialog", { name: "模拟指令详情" })).toContainText("800 / 800");
  await page.keyboard.press("Escape");
  await expect(row).toBeFocused();
  await page.getByRole("button", { name: "账户列表", exact: true }).click();
  await expect(card).toBeFocused();
  await expectNoHorizontalOverflow(page, "paper account/history");
  expect(state.problems).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("accounts.png"), fullPage: true });
});

test("unknown request survives reload and a generation change then clears for another owner", async ({
  page,
  baseURL,
}) => {
  const state = await fixture(page, baseURL, true);
  await page.goto("./#/paper");
  await page.getByRole("button", { name: "查看模拟账户 1" }).click();
  await page.getByRole("button", { name: "设置仓位", exact: true }).click();
  await page.getByRole("button", { name: "保存规则" }).click();
  await expect(page.getByText("结果待确认，请继续查看。", { exact: true })).toBeVisible();
  state.changeGeneration();
  await page.reload();
  await expect(page.getByRole("button", { name: "继续查看" })).toBeVisible();
  await page.getByRole("button", { name: "继续查看" }).click();
  await expect(page.getByText("已保存。", { exact: true })).toBeVisible();
  expect(state.retries[0]).toEqual(state.operations[0]);
  expect(state.operations).toHaveLength(1);
  state.changeViewer();
  await page.reload();
  await expect(page.getByText("还没有模拟账户")).toBeVisible();
  await expect(page.getByText("已保存。", { exact: true })).toHaveCount(0);
  await expectNoHorizontalOverflow(page, "paper original recovery");
  expect(state.problems).toEqual([]);
});

test("loading, empty and genuine valuation/NAV gaps stay explicit; band uses an exact source", async ({
  page,
  baseURL,
}, testInfo) => {
  const state = await fixture(page, baseURL);
  state.hold();
  await page.goto("./#/paper");
  await expect(page.getByLabel("模拟账户加载中")).toBeVisible();
  state.setEmpty(true);
  state.release();
  await expect(page.getByText("还没有模拟账户")).toBeVisible();
  state.setEmpty(false);
  state.setUnavailable();
  state.addBand();
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.getByRole("button", { name: "查看模拟账户 1" }).click();
  await expect(page.getByText("账户估值暂不可用")).toBeVisible();
  await expect(page.getByText("1 天缺数据")).toBeVisible();
  await page.getByText("查看净值数据", { exact: true }).click();
  await expect(page.getByRole("table", { name: "逐日净值数据" })).toContainText("2026-07-30");
  await expect(page.getByRole("table", { name: "逐日净值数据" })).not.toContainText("0.0000");
  state.completeBand();
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.getByRole("button", { name: "计算回测区间", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "计算回测区间", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "计算回测区间" });
  await expect(dialog.getByLabel("同版回测")).toHaveValue("11111111-1111-4111-8111-111111111111");
  await dialog.getByRole("button", { name: "提交计算" }).click();
  expect(state.operations[0]).toMatchObject({
    task_name: "paper_backtest_band",
    backtest_job_id: "11111111-1111-4111-8111-111111111111",
  });
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "计算回测区间", exact: true })).toBeFocused();
  await expectNoHorizontalOverflow(page, "paper valuation/NAV gaps");
  expect(state.problems).toEqual([]);
  await page.screenshot({ path: testInfo.outputPath("gaps.png"), fullPage: true });
});
