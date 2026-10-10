import { expect, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

for (const mode of ["desktop", "phone"] as const) {
  test.describe(`${mode} collaboration`, () => {
    test.use({
      viewport: mode === "phone" ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
      hasTouch: mode === "phone",
      isMobile: mode === "phone",
    });
    test("uses issued confirmation, original lost-reply recovery, real audit and real account switch", async ({
      page,
    }) => {
      const admin = process.env.RQ_C15_ADMIN_BASE;
      const viewer = process.env.RQ_C15_VIEWER_BASE;
      if (!admin || !viewer)
        throw new Error("Root must install the two original, trusted fixture proxies");
      let active = admin;
      let loseReply = false;
      let lost = false;
      const commands: Schemas["CollaborationRoleSubmit"][] = [];
      const lookups: Schemas["CollaborationRoleLookup"][] = [];
      const failures: string[] = [];
      page.on("pageerror", (error) => failures.push(error.message));
      page.on("request", (request) => {
        if (request.method() !== "POST") return;
        const pathname = new URL(request.url()).pathname;
        if (pathname.endsWith("/collaboration/roles/commands"))
          commands.push(request.postDataJSON());
        if (pathname.endsWith("/collaboration/roles/lookup")) lookups.push(request.postDataJSON());
      });
      await page.route("**/app/api/v1/**", async (route) => {
        const original = new URL(route.request().url());
        const actual = new URL(original.pathname.slice("/app/".length) + original.search, active);
        const headers = { ...route.request().headers() };
        if (route.request().method() === "POST") headers.origin = actual.origin;
        delete headers.host;
        const response = await route.fetch({ url: actual.href, headers });
        if (loseReply && original.pathname.endsWith("/collaboration/roles/commands")) {
          expect(response.status()).toBe(200);
          loseReply = false;
          lost = true;
          await route.abort("connectionreset");
        } else await route.fulfill({ response });
      });
      const meResponse = await page.request.get(new URL("api/v1/collaboration/me", admin).href);
      expect(meResponse.ok()).toBe(true);
      const current: Schemas["CollaborationMe"] = (await meResponse.json()).data;
      expect(current.available).toBe(true);
      expect(current.role).toBe("admin");
      const rolesResponse = await page.request.get(
        new URL("api/v1/collaboration/users", admin).href,
      );
      const roles: Schemas["RoleState"] = (await rolesResponse.json()).data;
      const target = roles.users.find(
        (entry) => entry.username !== current.username && entry.role === "viewer",
      );
      if (!target) throw new Error("Root fixture needs an explicit, non-admin viewer role");
      await page.goto("#/overview");
      const mine = page.getByRole("button", { name: "我的" });
      if (mode === "desktop") {
        await mine.focus();
        await page.keyboard.press("Enter");
      } else await mine.click();
      await page.getByRole("menuitem", { name: "用户与权限", exact: true }).click();
      await expect(page.getByRole("heading", { name: "用户与权限", exact: true })).toBeVisible();
      const role = page.getByLabel(`${target.username} 的角色`, { exact: true });
      await role.selectOption("researcher");
      const change = page.getByRole("button", {
        name: `修改 ${target.username} 的角色`,
        exact: true,
      });
      await change.click();
      let dialog = page.getByRole("dialog", { name: "修改角色" });
      await expect(dialog).toBeVisible();
      await expect(dialog).toContainText("查看者改为研究者");
      await dialog.getByRole("textbox").fill(`${target.username}x`);
      await expect(dialog.getByRole("button", { name: "确认修改", exact: true })).toBeDisabled();
      await page.keyboard.press("Escape");
      await expect(dialog).not.toBeVisible();
      await expect(change).toBeFocused();
      expect(commands).toHaveLength(0);
      await change.click();
      dialog = page.getByRole("dialog", { name: "修改角色" });
      await dialog.getByRole("textbox").fill(target.username);
      loseReply = true;
      await dialog.getByRole("button", { name: "确认修改", exact: true }).click();
      await expect(page.getByRole("button", { name: "查看原操作", exact: true })).toBeVisible();
      await expect.poll(() => lost).toBe(true);
      expect(commands).toHaveLength(1);
      const original = commands[0];
      if (!original) throw new Error("original command missing");
      await page.reload();
      await expect(page.getByText("角色已更新。", { exact: true })).toBeVisible();
      expect(
        lookups.some((entry) => JSON.stringify(entry.command) === JSON.stringify(original.command)),
      ).toBe(true);
      expect(commands).toHaveLength(1);
      const finalResponse = await page.request.get(
        new URL("api/v1/collaboration/users", admin).href,
      );
      const final: Schemas["RoleState"] = (await finalResponse.json()).data;
      expect(final.users.find((entry) => entry.username === target.username)?.role).toBe(
        "researcher",
      );
      await expect(role).toHaveValue("researcher");
      await expectNoHorizontalOverflow(page, `${mode} roles`);
      await page.getByRole("button", { name: "我的" }).click();
      await page.getByRole("menuitem", { name: "操作记录", exact: true }).click();
      await page.getByLabel("操作人", { exact: true }).fill(current.username ?? "");
      await page.getByRole("combobox", { name: "操作", exact: true }).selectOption("set_user_role");
      await page.getByRole("button", { name: "筛选", exact: true }).click();
      const auditResponse = await page.request.get(
        new URL(
          `api/v1/collaboration/audit?limit=20&actor_id=${encodeURIComponent(current.username ?? "")}&command_kind=set_user_role`,
          admin,
        ).href,
      );
      const audit: Schemas["CommandAuditPage"] = (await auditResponse.json()).data;
      const index = audit.items.findIndex(
        (item) => item.command_id === original.command.command_id,
      );
      expect(index).toBeGreaterThanOrEqual(0);
      const table = page.getByRole("table", { name: "操作记录", exact: true });
      await expect(table).toBeVisible();
      await table
        .getByRole("row")
        .nth(index + 1)
        .click();
      const detail = page.getByRole("dialog", { name: "操作详情" });
      await expect(detail).toContainText(original.command.command_id);
      await page.keyboard.press("Escape");
      await expect(detail).not.toBeVisible();
      await expectNoHorizontalOverflow(page, `${mode} audit`);
      // Restore through the same real, issued UI command before testing another account.
      await page.goto("#/users");
      await page.getByLabel(`${target.username} 的角色`, { exact: true }).selectOption(target.role);
      await page
        .getByRole("button", { name: `修改 ${target.username} 的角色`, exact: true })
        .click();
      dialog = page.getByRole("dialog", { name: "修改角色" });
      await dialog.getByRole("textbox").fill(target.username);
      await dialog.getByRole("button", { name: "确认修改", exact: true }).click();
      await expect(page.getByText("角色已更新。", { exact: true })).toBeVisible();
      await expect(page.getByRole("row").filter({ has: role }).getByRole("cell").nth(1)).toHaveText(
        "查看者",
      );
      await expect(role).toHaveValue(target.role);
      await page
        .getByLabel(`${target.username} 的角色`, { exact: true })
        .selectOption("researcher");
      await page
        .getByRole("button", { name: `修改 ${target.username} 的角色`, exact: true })
        .click();
      await expect(page.getByRole("dialog", { name: "修改角色" })).toBeVisible();
      active = viewer;
      await expect(page.getByText("当前账号不能管理用户。", { exact: true })).toBeVisible({
        timeout: 25_000,
      });
      await expect(page.getByRole("dialog", { name: "修改角色" })).not.toBeVisible();
      await expect(page.getByRole("table", { name: "用户与权限" })).not.toBeVisible();
      await expect(page.getByRole("button", { name: "AI 助手", exact: true })).toBeDisabled();
      expect(commands).toHaveLength(2);
      await page.getByRole("button", { name: "我的" }).click();
      await expect(
        page.getByRole("menuitem", { name: "用户与权限", exact: true }),
      ).not.toBeVisible();
      await page.getByRole("menuitem", { name: "操作记录", exact: true }).click();
      await expect(page.getByLabel("操作人", { exact: true })).toBeDisabled();
      await expect(page.getByText(original.command.command_id, { exact: true })).not.toBeVisible();
      await expectNoHorizontalOverflow(page, `${mode} own audit`);
      await page.unrouteAll({ behavior: "wait" });
      expect(failures).toEqual([]);
    });
    test("filters original commands and Shanghai times in UTC and Shanghai browsers", async ({
      browser,
    }) => {
      const admin = process.env.RQ_C15_ADMIN_BASE;
      if (!admin) throw new Error("Root must install the original trusted admin fixture proxy");
      for (const timezoneId of ["UTC", "Asia/Shanghai"]) {
        const context = await browser.newContext({
          baseURL: admin,
          timezoneId,
          viewport: mode === "phone" ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
          hasTouch: mode === "phone",
          isMobile: mode === "phone",
        });
        try {
          const page = await context.newPage();
          const failures: string[] = [];
          const auditRequests: URL[] = [];
          page.on("pageerror", (error) => failures.push(error.message));
          page.on("request", (request) => {
            const url = new URL(request.url());
            if (request.method() === "GET" && url.pathname.endsWith("/collaboration/audit"))
              auditRequests.push(url);
          });
          await page.goto("#/audit");
          await expect(page.getByRole("table", { name: "操作记录", exact: true })).toBeVisible();
          await page.getByLabel("操作人", { exact: true }).fill("");
          for (const [kind, label] of [
            ["submit_portfolio_backtest", "运行组合回测"],
            ["submit_factor_run", "运行因子研究"],
            ["run_strategy_template", "运行策略回测"],
          ] as const) {
            await page.getByRole("combobox", { name: "操作", exact: true }).selectOption(kind);
            const responsePromise = page.waitForResponse((response) => {
              const url = new URL(response.url());
              return (
                response.request().method() === "GET" &&
                url.pathname.endsWith("/collaboration/audit") &&
                url.searchParams.get("command_kind") === kind &&
                !url.searchParams.has("actor_id")
              );
            });
            await page.getByRole("button", { name: "筛选", exact: true }).click();
            const response = await responsePromise;
            expect(response.status()).toBe(200);
            const original: Schemas["CommandAuditPage"] = (await response.json()).data;
            expect(original.items.length).toBeGreaterThan(0);
            expect(original.items.every((item) => item.command_kind === kind)).toBe(true);
            const table = page.getByRole("table", { name: "操作记录", exact: true });
            await expect(table).toContainText(label);
            await expect(table).not.toContainText(kind);
          }
          const start = page.getByLabel("开始时间（上海）", { exact: true });
          const end = page.getByLabel("结束时间（上海）", { exact: true });
          await start.fill("2026-10-06T09:00");
          await end.fill("2026-10-06T10:00");
          const timedPromise = page.waitForResponse((response) => {
            const url = new URL(response.url());
            return (
              response.request().method() === "GET" &&
              url.pathname.endsWith("/collaboration/audit") &&
              url.searchParams.has("time_from")
            );
          });
          await page.getByRole("button", { name: "筛选", exact: true }).click();
          const timed = await timedPromise;
          expect(timed.status()).toBe(200);
          const timeQuery = new URL(timed.url()).searchParams;
          expect(timeQuery.get("time_from")).toBe("2026-10-06T01:00:00.000Z");
          expect(timeQuery.get("time_until")).toBe("2026-10-06T02:00:00.000Z");
          expect(timeQuery.has("cursor")).toBe(false);
          await end.fill("2026-10-06T09:00");
          const count = auditRequests.length;
          await page.getByRole("button", { name: "筛选", exact: true }).click();
          await expect(page.getByRole("alert")).toContainText("开始时间须早于结束时间。");
          expect(auditRequests).toHaveLength(count);
          await start.fill("");
          await end.fill("");
          const clearedPromise = page.waitForResponse((response) => {
            const url = new URL(response.url());
            return (
              response.request().method() === "GET" &&
              url.pathname.endsWith("/collaboration/audit") &&
              url.searchParams.get("command_kind") === "run_strategy_template" &&
              !url.searchParams.has("time_from") &&
              !url.searchParams.has("time_until")
            );
          });
          await page.getByRole("button", { name: "筛选", exact: true }).click();
          const cleared = await clearedPromise;
          expect(cleared.status()).toBe(200);
          expect(new URL(cleared.url()).searchParams.has("cursor")).toBe(false);
          await expect(page.getByRole("table", { name: "操作记录", exact: true })).toContainText(
            "运行策略回测",
          );
          await expectNoHorizontalOverflow(page, `${mode} audit ${timezoneId}`);
          expect(failures).toEqual([]);
        } finally {
          await context.close();
        }
      }
    });
  });
}
