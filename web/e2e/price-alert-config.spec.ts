import { expect, type Page, test } from "@playwright/test";
import type { MetaEnvelope, Schemas } from "../src/api/client.ts";
import { API_NOW } from "./env.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

type List = Schemas["Envelope_PriceAlertRuleListData_"];
type Command = Schemas["PriceAlertRuleCommandRequest"];
type Item = Schemas["PriceAlertRuleItem"];
const PATH = "**/api/v1/monitor/price-rules";
const JOURNAL = "rquant.price-rule-command.v1";

async function listing(page: Page) {
  const meta = (await (await page.request.get("api/v1/meta")).json()) as MetaEnvelope;
  if (meta.serving.built_at === null) throw new Error("synthetic generation is missing");
  // Pin this routed business scenario independently of the shared API's elapsed clock.
  meta.serving = {
    ...meta.serving,
    state: "ready",
    age_seconds: (Date.parse(API_NOW) - Date.parse(meta.serving.built_at)) / 1_000,
    message: null,
  };
  meta.data.server_time = API_NOW;
  await page.route("**/api/v1/meta", (route) => route.fulfill({ json: meta }));
  const state: List = {
    serving: meta.serving,
    data: {
      availability: "ready",
      message: "",
      available_at: meta.data.server_time,
      can_write: true,
      write_message: "",
      members: [{ ts_code: "600001.SH", version: 1, expires_at: null }],
      priority_options: [
        { value: "P0", label: "紧急" },
        { value: "P1", label: "重要" },
        { value: "P2", label: "普通" },
        { value: "P3", label: "提示" },
      ],
      items: [
        {
          rule_id: "browser-rule-a",
          version: 1,
          ts_code: "600001.SH",
          membership_version: 1,
          name: "突破提醒",
          priority: "P2",
          priority_label: "普通",
          enabled: true,
          comparison: "gte",
          threshold: "10.123456",
          valid_from: "09:30:01.123456",
          valid_until: "14:57:02",
          updated_at: meta.data.server_time,
          scope_status: "bound",
          scope_message: "行情评估接通后才会提醒。",
          status_label: "未运行",
        },
      ],
    },
  };
  return { meta, state };
}
function updatedItem(body: Command, version: number, meta: MetaEnvelope): Item {
  if (!body.rule || !body.ts_code || !body.membership_version)
    throw new Error("synthetic save lacks a full rule");
  return {
    ...body.rule,
    rule_id: body.rule_id,
    version,
    ts_code: body.ts_code,
    membership_version: body.membership_version,
    priority_label: { P0: "紧急", P1: "重要", P2: "普通", P3: "提示" }[body.rule.priority],
    updated_at: meta.data.server_time,
    status_label: "未运行",
    scope_status: body.rule.enabled ? "bound" : "disabled",
    scope_message: body.rule.enabled ? "行情评估接通后才会提醒。" : "规则已停用。",
  };
}

for (const width of [1440, 390])
  test.describe(`到价规则 ${width}px`, () => {
    test.use({
      viewport: { width, height: 844 },
      hasTouch: width === 390,
      isMobile: width === 390,
    });
    test("新建、精确编辑、启停和删除，键盘焦点与手机布局", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const { meta, state } = await listing(page);
      const seen: Command[] = [];
      await page.route(PATH, (route) => route.fulfill({ json: state }));
      // These routed responses exercise browser behavior; real Web/CAS proof is separate.
      await page.route(`${PATH}/commands`, async (route) => {
        const body = route.request().postDataJSON() as Command;
        seen.push(body);
        const version = (body.expected_version ?? 0) + 1;
        if (body.action === "save") {
          state.data.items = state.data.items.filter((item) => item.rule_id !== body.rule_id);
          state.data.items.push(updatedItem(body, version, meta));
        } else if (body.action === "delete")
          state.data.items = state.data.items.filter((item) => item.rule_id !== body.rule_id);
        else
          state.data.items = state.data.items.map((item) =>
            item.rule_id === body.rule_id
              ? {
                  ...item,
                  version,
                  enabled: body.enabled ?? false,
                  scope_status: body.enabled ? "bound" : "disabled",
                }
              : item,
          );
        await route.fulfill({
          json: {
            command_id: body.command_id,
            rule_id: body.rule_id,
            action: body.action,
            status: "published",
            version,
            message: "配置已核对。",
          },
        });
      });
      await page.goto("./#/monitor");
      const panel = page.getByRole("region", { name: "到价规则" });
      const edit = panel.getByRole("button", { name: "编辑 突破提醒" });
      await expect(edit).toBeVisible();
      await edit.focus();
      await edit.press("Enter");
      const drawer = page.getByRole("dialog", { name: "编辑到价规则" });
      await expect(drawer.getByLabel("阈值价格")).toHaveValue("10.123456");
      await expect(drawer.getByLabel("开始时间")).toHaveValue("09:30:01.123456");
      await drawer.press("Escape");
      await expect(edit).toBeFocused();
      const create = panel.getByRole("button", { name: "新建规则" });
      await expect(create).toBeEnabled();
      await create.click();
      const fresh = page.getByRole("dialog", { name: "新建到价规则" });
      await fresh.getByLabel("规则名称").fill("回落提醒");
      await fresh.getByLabel("阈值价格").fill("8.987654");
      if (width === 390) {
        await fresh.getByText("启用规则", { exact: true }).tap();
        await expect(page.getByRole("tooltip")).toContainText("开关只保存启用设置");
        await fresh.getByLabel("阈值价格").click();
      }
      await expectNoHorizontalOverflow(page, `price rule drawer ${width}px`);
      await testInfo.attach(`price-rule-drawer-${width}px`, {
        body: await page.screenshot({ fullPage: true }),
        contentType: "image/png",
      });
      await fresh.getByRole("button", { name: "保存规则" }).click();
      await expect(panel).toContainText("已保存");
      expect(seen[0]?.rule?.threshold).toBe("8.987654");
      expect(seen[0]).not.toHaveProperty("owner_id");
      await fresh.getByRole("button", { name: "取消编辑" }).click();
      await expectNoHorizontalOverflow(page, `price rules ${width}px`);
      await panel.getByRole("button", { name: "编辑 回落提醒" }).click();
      const second = page.getByRole("dialog", { name: "编辑到价规则" });
      await second.getByLabel("阈值价格").fill("9.123456");
      await second.getByRole("button", { name: "保存规则" }).click();
      await expect.poll(() => seen.length).toBe(2);
      expect(seen[1]).toMatchObject({ action: "save", expected_version: 1 });
      await second.getByRole("button", { name: "取消编辑" }).click();
      await expect(panel.getByRole("table", { name: "到价规则" })).toContainText("9.12");
      await expect(panel.getByRole("switch", { name: "启停 回落提醒" })).toBeEnabled();
      await panel.getByRole("switch", { name: "启停 回落提醒" }).click();
      await expect.poll(() => seen.length).toBe(3);
      expect(seen[2]).toMatchObject({ action: "set_enabled", expected_version: 2, enabled: false });
      await expect(panel.getByRole("switch", { name: "启停 回落提醒" })).toHaveAttribute(
        "aria-checked",
        "false",
      );
      await panel.getByRole("button", { name: "删除 回落提醒" }).click();
      await page
        .getByRole("dialog", { name: "删除到价规则？" })
        .getByRole("button", { name: "删除规则" })
        .click();
      await expect.poll(() => seen.length).toBe(4);
      expect(seen[3]).toMatchObject({ action: "delete", expected_version: 3 });
      await expect(panel).toContainText("已删除");
      await expect(panel).not.toContainText("正在监控");
      await expectNoHorizontalOverflow(page, `price rules final ${width}px`);
      await testInfo.attach(`price-rules-${width}px`, {
        body: await page.screenshot({ fullPage: true }),
        contentType: "image/png",
      });
      expect(watcher.problems).toEqual([]);
    });

    test("未知回执刷新后只恢复原请求，等待同步与失效状态准确", async ({ page }, testInfo) => {
      const watcher = watch(page);
      const { meta, state } = await listing(page);
      const sent: Command[] = [];
      const resumed: Command[] = [];
      await page.route(PATH, (route) => route.fulfill({ json: state }));
      await page.route(`${PATH}/commands`, async (route) => {
        sent.push(route.request().postDataJSON() as Command);
        await route.fulfill({ json: { detail: "合成无效回执" } });
      });
      await page.route(`${PATH}/commands/resume`, async (route) => {
        const body = route.request().postDataJSON() as Command;
        resumed.push(body);
        state.data.items[0] = {
          ...state.data.items[0]!,
          scope_status: "expired",
          scope_message: "盯盘已到期，可停用或删除规则。",
        };
        await route.fulfill({
          json: {
            command_id: body.command_id,
            rule_id: body.rule_id,
            action: body.action,
            status: "saved_syncing",
            version: 2,
            message: "设置已写入，等待同步。",
          },
        });
      });
      await page.goto("./#/monitor");
      await page.getByRole("button", { name: "编辑 突破提醒" }).click();
      const drawer = page.getByRole("dialog", { name: "编辑到价规则" });
      await drawer.getByRole("button", { name: "保存规则" }).click();
      await expect(page.getByRole("region", { name: "到价规则" })).toContainText("状态待核对");
      expect(sent).toHaveLength(1);
      const stored = await page.evaluate(
        ({ key, owner, id }) => localStorage.getItem(`${key}:${owner}:${id}`),
        {
          key: JOURNAL,
          owner: encodeURIComponent(meta.data.viewer ?? ""),
          id: sent[0]!.command_id,
        },
      );
      expect(JSON.parse(stored ?? "{}").body).toEqual(sent[0]);
      await page.reload();
      const panel = page.getByRole("region", { name: "到价规则" });
      await expect(panel).toContainText("设置已写入，等待同步");
      expect(resumed.length).toBeGreaterThan(0);
      expect(resumed.every((body) => JSON.stringify(body) === JSON.stringify(sent[0]))).toBe(true);
      expect(sent).toHaveLength(1);
      await expect(panel).not.toContainText("已保存");
      await expectNoHorizontalOverflow(page, `price rules recovery ${width}px`);
      await testInfo.attach(`price-rule-recovery-${width}px`, {
        body: await page.screenshot({ fullPage: true }),
        contentType: "image/png",
      });
      expect(watcher.problems).toEqual([]);
    });
  });
