import { expect, type Locator, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { expectNoHorizontalOverflow, watch } from "./watch.ts";

type Command =
  | Schemas["RequestPromotionReview"]
  | Schemas["PreparePromotionApproval"]
  | Schemas["ApprovePromotion-Input"]
  | Schemas["RunStrategyWalkForward"];
type Data = Schemas["StrategyPromotionData"];
type Result = Schemas["StrategyPromotionCommandData"];
const pendingKey = "rquant.strategy-promotion.pending.v1";
const commandsPath = "/app/api/v1/strategy-promotions/commands";

async function readData(page: Page, parent: Schemas["StrategyTemplateItem"]): Promise<Data> {
  const response = await page.request.get(
    `/app/api/v1/strategy-promotions/${encodeURIComponent(parent.strategy_id)}?source_kind=template&version=${parent.head.version}`,
  );
  expect(response.status()).toBe(200);
  const envelope: Schemas["Envelope_StrategyPromotionData_"] = await response.json();
  expect(envelope.serving.state).toBe("ready");
  expect(envelope.data.strategy_id).toBe(parent.strategy_id);
  return envelope.data;
}
async function openParent(page: Page, parent: Schemas["StrategyTemplateItem"]): Promise<Locator> {
  await page.goto("#/strategies");
  await page
    .getByRole("table", { name: "我的策略", exact: true })
    .getByText(parent.name, { exact: true })
    .click();
  const drawer = page.getByRole("dialog", { name: parent.name, exact: true });
  await expect(drawer).toBeVisible();
  const panel = drawer.getByRole("region", { name: "阶段评估", exact: true });
  await expect(panel.getByRole("combobox", { name: "验证版本", exact: true })).toBeVisible();
  return panel;
}
async function originalSettled(panel: Locator): Promise<void> {
  const lookup = panel.getByRole("button", { name: "查看原操作", exact: true });
  if (await lookup.isVisible()) await lookup.click();
  await expect(lookup).toHaveCount(0);
}

for (const mode of ["desktop", "phone"] as const) {
  test.describe(`${mode} original strategy promotion`, () => {
    test.use({
      viewport: mode === "phone" ? { width: 390, height: 844 } : { width: 1440, height: 1000 },
      hasTouch: mode === "phone",
      isMobile: mode === "phone",
    });
    test("reads the real six-fold and forward chain, then manually approves another sealed exact version", async ({
      page,
    }) => {
      const fixtureNow = process.env.RQ_E2E_NOW;
      if (!fixtureNow || !Number.isFinite(Date.parse(fixtureNow)))
        throw new Error("Root must provide the complete macro's exact final synthetic clock");
      await page.clock.setFixedTime(new Date(fixtureNow));
      const monitor = watch(page);
      const consoleErrors: { index: number; text: string; url: string }[] = [];
      const errorResponses: Promise<{
        index: number;
        method: string;
        status: number;
        url: string;
        detail: unknown;
      }>[] = [];
      page.on("console", (message) => {
        if (message.type() !== "error") return;
        const index = monitor.problems.length - 1;
        if (monitor.problems[index] === `console error: ${message.text()}`)
          consoleErrors.push({ index, text: message.text(), url: message.location().url });
      });
      page.on("response", (response) => {
        if (response.status() < 400) return;
        const index = monitor.problems.length - 1;
        errorResponses.push(
          response.json().then(
            (body: { detail?: unknown }) => ({
              index,
              method: response.request().method(),
              status: response.status(),
              url: response.url(),
              detail: body.detail,
            }),
            () => ({
              index,
              method: response.request().method(),
              status: response.status(),
              url: response.url(),
              detail: null,
            }),
          ),
        );
      });
      const sent: Command[] = [];
      const looked: Command[] = [];
      page.on("request", (request) => {
        if (request.method() !== "POST") return;
        const path = new URL(request.url()).pathname;
        if (path === commandsPath) sent.push(request.postDataJSON());
        if (path === `${commandsPath}/lookup`) looked.push(request.postDataJSON());
      });
      const metaResponse = await page.request.get("/app/api/v1/meta");
      expect(metaResponse.status()).toBe(200);
      const meta: Schemas["MetaData"] = (await metaResponse.json()).data;
      if (!meta.viewer || !meta.generation?.generation_id)
        throw new Error("Root must publish the original owned generation");
      const roleResponse = await page.request.get("/app/api/v1/collaboration/me");
      expect(roleResponse.status()).toBe(200);
      const me: Schemas["CollaborationMe"] = (await roleResponse.json()).data;
      expect(me.username).toBe(meta.viewer);
      expect(me.available).toBe(true);
      expect(me.role).toBe("admin");
      const catalogResponse = await page.request.get("/app/api/v1/strategy-templates");
      expect(catalogResponse.status()).toBe(200);
      const catalog: Schemas["StrategyTemplateCatalogData"] = (await catalogResponse.json()).data;
      const parent = catalog.templates[0];
      if (!parent) throw new Error("Root must publish the original authored parent");
      const source = await readData(page, parent);
      expect(source.availability).toBe("populated");
      const final = source.states.find(
        (item) => item.owner_id === meta.viewer && item.state.stage === "monitor_approved",
      );
      if (!final)
        throw new Error("Root must actually finish the original complete promotion worker macro");
      expect(final.state.revision).toBe(3);
      expect(final.state.paper_approval_hash).toMatch(/^[a-f0-9]{64}$/);
      const sealed = source.candidates.find(
        (item) => item.target.strategy_id === final.state.target.strategy_id,
      );
      if (!sealed) throw new Error("Original exact candidate result is missing");
      expect(sealed.has_sealed_reference).toBe(true);
      expect(sealed.parent_count).toBe(4);
      expect(sealed.template_parent?.head).toEqual(parent.head);
      for (const hash of [
        sealed.input_hash,
        sealed.spec_hash,
        sealed.manifest_hash,
        sealed.result_hash,
      ])
        expect(hash).toMatch(/^[a-f0-9]{64}$/);
      const fold = source.walk_forward.find((item) => item.target_key === final.target_key);
      expect(fold?.fold_count).toBe(6);
      expect(fold?.submitted).toBe(true);
      const paper = source.paper_accounts.find((item) => item.target_key === final.target_key);
      expect(paper?.band_jobs.length).toBeGreaterThan(0);
      const finalReview = source.reviews.find(
        (item) =>
          item.target.strategy_id === final.state.target.strategy_id &&
          item.to_stage === "monitor_approved",
      );
      expect(finalReview?.gates.every((gate) => gate.status === "satisfied")).toBe(true);
      expect(
        Number(finalReview?.gates.find((gate) => gate.key === "forward_open_days")?.value),
      ).toBe(20);
      const paperReview = source.reviews.find(
        (item) =>
          item.target.strategy_id === final.state.target.strategy_id &&
          item.to_stage === "paper_candidate",
      );
      expect(paperReview?.gates.find((gate) => gate.key === "full_parent_bh")?.status).toBe(
        "satisfied",
      );
      expect(paperReview?.gates.find((gate) => gate.key === "six_folds")?.status).toBe("satisfied");
      expect(
        Number(paperReview?.gates.find((gate) => gate.key === "unique_outer")?.value),
      ).toBeGreaterThan(0);

      let panel = await openParent(page, parent);
      const version = panel.getByRole("combobox", { name: "验证版本", exact: true });
      await version.selectOption(JSON.stringify(sealed));
      await expect(panel.getByLabel("当前阶段", { exact: true })).toHaveText("当前：监控批准");
      await expect(panel.getByRole("button", { name: "评估下一阶段", exact: true })).toBeDisabled();
      await expect(panel.getByRole("button", { name: "批准晋级", exact: true })).toBeDisabled();
      expect(sent).toEqual([]);
      const rules = panel.getByText("晋级规则", { exact: true }).locator("..");
      if (mode === "phone") await rules.tap();
      else await rules.focus();
      await expect(page.getByRole("tooltip")).toContainText("人工批准");
      if (mode === "phone") await rules.tap();
      else await version.focus();
      await expect(page.getByRole("tooltip")).toHaveCount(0);
      await expectNoHorizontalOverflow(page, `${mode} full original evidence`);
      const another = source.candidates.find(
        (item) =>
          item.target.owner_id === meta.viewer &&
          item.has_sealed_reference &&
          item.is_current &&
          !source.states.some(
            (state) =>
              state.target_key !== final.target_key &&
              state.state.target.strategy_id === item.target.strategy_id,
          ) &&
          item.target.strategy_id !== final.state.target.strategy_id,
      );
      if (!another)
        throw new Error("Root must retain another actually sealed exploratory child version");
      await version.selectOption(JSON.stringify(another));
      await expect(panel.getByLabel("当前阶段", { exact: true })).toHaveText("当前：探索");
      await panel.getByRole("button", { name: "评估下一阶段", exact: true }).click();
      await expect(panel.getByRole("table", { name: "阶段证据", exact: true })).toContainText(
        "已满足",
      );
      const reviewed = sent.find((item) => item.kind === "request_promotion_review");
      expect(reviewed).toMatchObject({
        kind: "request_promotion_review",
        target: another.target,
        expected_revision: 0,
        selection: another.selection,
      });
      expect(reviewed?.target.strategy_id).not.toBe(parent.strategy_id);
      await expect
        .poll(async () =>
          (await readData(page, parent)).reviews.some(
            (item) => item.command_id === reviewed?.command_id,
          ),
        )
        .toBe(true);
      await page.reload();
      panel = await openParent(page, parent);
      await originalSettled(panel);
      await panel
        .getByRole("combobox", { name: "验证版本", exact: true })
        .selectOption(JSON.stringify(another));
      await expect(panel.getByLabel("当前阶段", { exact: true })).toHaveText("当前：探索");
      const approvalButton = panel.getByRole("button", { name: "批准晋级", exact: true });
      await expect(approvalButton).toBeEnabled();
      await approvalButton.click();
      let dialog = page.getByRole("dialog", { name: "批准晋级", exact: true });
      await expect(dialog).toBeVisible();
      await expect(dialog.getByRole("button", { name: "确认晋级", exact: true })).toBeDisabled();
      await dialog.getByRole("button", { name: /^取\s*消$/, exact: true }).click();
      await expect(dialog).toHaveCount(0);
      await expect(approvalButton).toBeFocused();
      expect(sent.filter((item) => item.kind === "approve_promotion")).toEqual([]);
      await expect(panel.getByLabel("当前阶段", { exact: true })).toHaveText("当前：探索");

      let actualApproval: Result | undefined;
      let lost = false;
      let lostUrl = "";
      await page.route(`**${commandsPath}`, async (route) => {
        const body: Command = route.request().postDataJSON();
        if (body.kind !== "approve_promotion" || lost) {
          await route.continue();
          return;
        }
        const response = await route.fetch();
        expect(response.status()).toBe(200);
        actualApproval = (
          (await response.json()) as Schemas["Envelope_StrategyPromotionCommandData_"]
        ).data;
        expect(actualApproval.approval?.after.stage).toBe("comparable");
        expect(actualApproval.approval?.after.target).toEqual(another.target);
        lost = true;
        lostUrl = route.request().url();
        await route.abort("failed");
      });
      await approvalButton.click();
      dialog = page.getByRole("dialog", { name: "批准晋级", exact: true });
      await expect(dialog).toBeVisible();
      const name = dialog.getByRole("textbox");
      await name.fill(`${another.target.name}x`);
      await expect(dialog.getByRole("button", { name: "确认晋级", exact: true })).toBeDisabled();
      await name.fill(another.target.name);
      await page.keyboard.press("Tab");
      await dialog.getByRole("button", { name: "确认晋级", exact: true }).click();
      await expect(panel.getByText("结果待确认，请查看原操作。", { exact: true })).toBeVisible();
      const original = sent.find((item) => item.kind === "approve_promotion");
      if (original?.kind !== "approve_promotion")
        throw new Error("The exact original approval body must remain available");
      expect(actualApproval?.original_request.command_id).toBe(original.command_id);
      expect(original.preparation.actor_id).toBe(meta.viewer);
      expect(original.preparation.role_state_hash).toBe(me.state_sha256);
      expect(original.preparation.review.target).toEqual(another.target);
      const preserved = await page.evaluate((key) => sessionStorage.getItem(key), pendingKey);
      expect(JSON.parse(preserved ?? "null").body).toEqual(original);
      expect(sent.filter((item) => item.kind === "approve_promotion")).toHaveLength(1);
      const countBeforeReload = sent.length;
      await page.reload();
      panel = await openParent(page, parent);
      await expect
        .poll(() => looked.some((item) => item.command_id === original.command_id))
        .toBe(true);
      const recoveredBody = looked.find((item) => item.command_id === original.command_id);
      expect(recoveredBody).toEqual(original);
      await originalSettled(panel);
      await panel
        .getByRole("combobox", { name: "验证版本", exact: true })
        .selectOption(JSON.stringify(another));
      await expect(panel.getByLabel("当前阶段", { exact: true })).toHaveText("当前：可比");
      expect(sent).toHaveLength(countBeforeReload);
      const restored = await readData(page, parent);
      expect(
        restored.states.find((item) => item.state.target.strategy_id === another.target.strategy_id)
          ?.state.revision,
      ).toBe(1);
      expect(
        restored.states.find(
          (item) => item.state.target.strategy_id === final.state.target.strategy_id,
        )?.state,
      ).toEqual(final.state);
      await expectNoHorizontalOverflow(page, `${mode} original UUID recovery`);
      expect(lost).toBe(true);
      const allowed = new Set<number>();
      const expectedLoss = monitor.problems
        .map((item, index) => ({ item, index }))
        .filter(({ item }) => item === `request failed: ${lostUrl}`);
      expect(expectedLoss).toHaveLength(1);
      const lostRequest = expectedLoss[0];
      if (!lostRequest) throw new Error("The deliberately lost original response was not recorded");
      allowed.add(lostRequest.index);
      const lostConsole = consoleErrors.filter(
        (item) => item.url === lostUrl && item.text === "Failed to load resource: net::ERR_FAILED",
      );
      expect(lostConsole.length).toBeLessThanOrEqual(1);
      for (const item of lostConsole) allowed.add(item.index);

      const currentMetaResponse = await page.request.get("/app/api/v1/meta");
      expect(currentMetaResponse.status()).toBe(200);
      const currentMeta: Schemas["MetaData"] = (await currentMetaResponse.json()).data;
      expect(currentMeta.viewer).toBe(meta.viewer);
      expect(currentMeta.generation?.generation_id).not.toBe(original.generation_id);
      const transitionUrls = new Set<string>();
      for (const response of await Promise.all(errorResponses)) {
        const url = new URL(response.url);
        if (
          response.method !== "GET" ||
          response.status !== 409 ||
          url.origin !== new URL(lostUrl).origin ||
          !["/app/api/v1/strategy-templates", "/app/api/v1/strategy-templates/sources"].includes(
            url.pathname,
          ) ||
          url.searchParams.get("generation_id") !== original.generation_id ||
          response.detail !== "数据已更新，请重新查看策略。"
        )
          continue;
        expect(monitor.problems[response.index]).toBe(`HTTP 409: ${response.url}`);
        transitionUrls.add(response.url);
        allowed.add(response.index);
      }
      for (const item of consoleErrors) {
        if (
          transitionUrls.has(item.url) &&
          item.text ===
            "Failed to load resource: the server responded with a status of 409 (Conflict)"
        )
          allowed.add(item.index);
      }
      expect(monitor.problems.filter((_item, index) => !allowed.has(index))).toEqual([]);
    });
  });
}
