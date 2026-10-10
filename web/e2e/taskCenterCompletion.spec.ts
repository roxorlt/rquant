import { expect, type Page, test } from "@playwright/test";
import type { Schemas } from "../src/api/client.ts";
import { findJargon } from "../src/test/jargon.ts";
import {
  taskClock,
  taskGeneration,
  taskMeta,
  taskScenarioOverview,
  taskSchedulingCapabilities,
  taskUnitCapabilities,
} from "./taskCenter.fixture.ts";
import { expectNoHorizontalOverflow } from "./watch.ts";

type Command =
  | Schemas["PrepareUnitRun"]
  | Schemas["RequestUnitRun"]
  | Schemas["SetLabSchedulingPaused"];
type Result = Schemas["TaskControlCommandData"];
const readonlyUnit = "rquant-backup.service";
const writerUnit = "rquant-daily.service";
const invocation = "d".repeat(32);

async function fixture(page: Page, baseURL: string | undefined) {
  if (!baseURL) throw new Error("task fixture requires the actual baseURL");
  const origin = new URL(baseURL).origin;
  const problems: string[] = [];
  const commands: Command[] = [];
  const lookups: Command[] = [];
  const resumes: Command[] = [];
  const grants: string[] = [];
  const logs: URL[] = [];
  const expectedHttpErrors = new Map<string, number>();
  let generation = taskGeneration;
  let viewer = "alice";
  let metaViewer = viewer;
  let metaGeneration = generation;
  let lost = false;
  let rejectNext = false;
  let disabled = false;
  let missing = false;
  let emptyLogs = false;
  let revoked = false;
  const overview = taskScenarioOverview();
  const receipts = new Map<string, Result>();
  const confirmations = new Map<string, Schemas["PrepareUnitRun"]>();
  const state = overview.data.scheduling;
  if (!state) throw new Error("actual baseline has no scheduling state");

  page.on("pageerror", (error) => problems.push(error.message));
  page.on("console", (message) => {
    if (message.type() !== "error") return;
    const status = expectedHttpErrors.get(message.location().url);
    if (status && message.text().includes(`status of ${status}`)) return;
    problems.push(`console error: ${message.text()}`);
  });
  page.on("requestfailed", (request) => problems.push(`failed request: ${request.url()}`));
  page.on("request", (request) => {
    if (!request.url().startsWith("data:") && new URL(request.url()).origin !== origin)
      problems.push(`external request: ${request.url()}`);
  });
  page.on("response", (response) => {
    if (response.status() >= 400 && expectedHttpErrors.get(response.url()) !== response.status())
      problems.push(`HTTP ${response.status()}: ${response.url()}`);
  });
  await page.clock.install({ time: new Date(taskClock) });
  await page.route("**/api/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname.replace(/^\/app/, "");
    const json = (value: unknown) => route.fulfill({ json: value });
    if (path === "/api/v1/meta") {
      if (viewer !== metaViewer || generation !== metaGeneration) {
        const response = await page.waitForResponse(
          (response) =>
            new URL(response.url()).pathname.replace(/^\/app/, "") === "/api/v1/collaboration/me",
        );
        await response.finished();
        metaViewer = viewer;
        metaGeneration = generation;
      }
      const meta = structuredClone(taskMeta);
      meta.data.viewer = viewer;
      meta.serving.generation_id = generation;
      if (meta.data.generation) meta.data.generation.generation_id = generation;
      return json(meta);
    }
    if (request.method() === "GET" && path === "/api/v1/collaboration/me") {
      const envelope: Schemas["Envelope_CollaborationMe_"] = {
        serving: { ...taskMeta.serving, generation_id: null },
        data: {
          available: false,
          mode: "legacy",
          username: viewer,
          role: null,
          revision: null,
          state_sha256: null,
          can_manage_users: false,
          can_research: false,
          can_read_audit: false,
          message: "协作权限尚未启用。",
        },
      };
      return json(envelope);
    }
    if (path === "/api/v1/health")
      return json({ serving: overview.serving, data: { available: false, units: [] } });
    if (path === "/api/v1/tasks/overview")
      return json({
        serving: { ...overview.serving, generation_id: generation },
        data: {
          ...overview.data,
          scheduled: missing
            ? {
                ...overview.data.scheduled,
                source_state: "unavailable",
                source_label: "任务状态暂不可用",
                items: [],
                remaining_seconds: null,
              }
            : overview.data.scheduled,
        },
      });
    if (path === "/api/v1/tasks/control-capabilities") {
      grants.push(url.searchParams.get("generation_id") ?? "");
      if (revoked) {
        expectedHttpErrors.set(request.url(), 403);
        return route.fulfill({ status: 403, json: { detail: "Forbidden" } });
      }
      const authorized = !disabled && viewer === "alice";
      const caps: Schemas["TaskControlCapabilitiesData"] = {
        ...taskUnitCapabilities,
        generation_id: generation,
        units:
          authorized && !missing
            ? [
                {
                  unit: readonlyUnit,
                  can_request: true,
                  requires_confirmation: false,
                  reason: "执行前会再次核验。",
                },
                {
                  unit: writerUnit,
                  can_request: true,
                  requires_confirmation: true,
                  reason: "写入前须准备并确认。",
                },
              ]
            : [],
        can_control_scheduling: authorized && !missing,
        can_recover_units: authorized,
        can_recover_scheduling: authorized,
        scheduling: { ...taskSchedulingCapabilities.scheduling, ...state },
      };
      return json(caps);
    }
    if (path === "/api/v1/tasks/services/log-capabilities")
      return json({ units: viewer === "alice" && !revoked ? [readonlyUnit, writerUnit] : [] });
    if (path.includes("/logs")) {
      logs.push(url);
      const pageData: Schemas["JournalPage"] = {
        service_label: "备份数据",
        scope: "本机本次开机以来的服务日志（含手动运行）",
        invocation_id: url.searchParams.get("invocation_id"),
        entries: emptyLogs
          ? []
          : [
              { at: taskClock, level: "信息", text: "任务已开始" },
              { at: taskClock, level: "信息", text: "任务已完成" },
            ],
        next_cursor: null,
      };
      return json(pageData);
    }
    if (request.method() === "POST") {
      expect(request.headers()["x-rquant-csrf"]).toBe("1");
      const body: Command = request.postDataJSON();
      expect(Object.keys(body)).not.toContain("owner_id");
      expect(Object.keys(body)).not.toContain("actor");
      expect(body.command_id).toMatch(/^[a-f0-9]{8}-[a-f0-9-]{27}$/);
      const recover = path.endsWith("/lookup") || path.endsWith("/resume");
      if (recover) {
        const original = commands.find((item) => item.command_id === body.command_id);
        expect(body).toEqual(original);
        (path.endsWith("/lookup") ? lookups : resumes).push(body);
        const previous = receipts.get(body.command_id);
        if (!previous) throw new Error("original synthetic receipt is missing");
        if (body.kind === "request_unit_run" && !lost) {
          const row = overview.data.scheduled.items.find((item) => item.service_unit === body.unit);
          if (!row) throw new Error("original unit result row is missing");
          row.result_label = "已完成";
          row.ended_at = taskClock;
          row.duration_seconds = 1;
          const completed: Result = {
            ...previous,
            status: "succeeded",
            message: "本次运行已完成。",
            can_resume: false,
            invocation_id: invocation,
            ended_at: taskClock,
            duration_seconds: 1,
          };
          receipts.set(body.command_id, completed);
          return json(completed);
        }
        return json(previous);
      }
      commands.push(body);
      if (rejectNext) {
        rejectNext = false;
        expectedHttpErrors.set(request.url(), 422);
        return route.fulfill({ status: 422, json: { detail: "synthetic-private-error" } });
      }
      let result: Result;
      if (body.kind === "prepare_unit_run") {
        expect(path).toBe(`/api/v1/tasks/units/${writerUnit}/run/prepare`);
        expect(body.run.unit).toBe(writerUnit);
        const id = "00000000-0000-4000-8000-000000000004";
        confirmations.set(id, body);
        result = {
          command_id: body.command_id,
          original_request: body,
          status: "prepared",
          can_resume: false,
          message: "准备已完成，请确认本次运行。",
          confirmation_id: id,
          confirmation_expires_at: new Date(Date.parse(taskClock) + 300_000).toISOString(),
        };
      } else if (body.kind === "request_unit_run") {
        expect(path).toBe(`/api/v1/tasks/units/${body.unit}/run`);
        expect([readonlyUnit, writerUnit]).toContain(body.unit);
        if (body.unit === writerUnit) {
          const prepare = body.confirmation_id
            ? confirmations.get(body.confirmation_id)
            : undefined;
          if (!prepare) throw new Error("writer did not use original preparation");
          expect({
            command_id: body.command_id,
            requested_at: body.requested_at,
            generation_id: body.generation_id,
            unit: body.unit,
          }).toEqual(prepare.run);
        }
        const row = overview.data.scheduled.items.find((item) => item.service_unit === body.unit);
        if (!row) throw new Error("unit result row is missing");
        if (!lost) {
          row.started_at = taskClock;
          row.invocation_id = invocation;
          row.origin_label = "手动运行";
          row.result_label = "运行中";
        }
        result = {
          command_id: body.command_id,
          original_request: body,
          status: lost ? "unknown" : "started",
          can_resume: true,
          message: lost ? "结果待确认，请核验原请求。" : "本次运行已开始。",
          started_at: lost ? null : taskClock,
          invocation_id: lost ? null : invocation,
        };
      } else {
        expect(path).toBe("/api/v1/tasks/scheduling/commands");
        expect(body.expected_version).toBe(state.desired_version);
        expect(body).not.toHaveProperty("job_id");
        state.desired_version = body.expected_version + 1;
        state.desired_paused = body.paused;
        state.draining_count = body.paused ? 1 : 0;
        state.note = body.paused ? "正在暂停，等待当前分片收尾。" : "正在恢复调度。";
        result = {
          command_id: body.command_id,
          original_request: body,
          status: "submitted",
          can_resume: true,
          desired_version: state.desired_version,
          message: "请求已受理，等待调度应用。",
        };
      }
      receipts.set(body.command_id, result);
      return json(result);
    }
    problems.push(`unhandled request: ${request.method()} ${path}`);
    return route.fulfill({ status: 404, json: { detail: "Unknown synthetic request" } });
  });
  return {
    problems,
    commands,
    lookups,
    resumes,
    grants,
    logs,
    setLost(value: boolean) {
      lost = value;
    },
    rejectNext() {
      rejectNext = true;
    },
    setViewer(value: string) {
      viewer = value;
    },
    setGeneration(value: string) {
      generation = value;
    },
    setMissing() {
      missing = true;
    },
    setDisabled() {
      disabled = true;
    },
    setEmptyLogs() {
      emptyLogs = true;
    },
    setRevoked() {
      revoked = true;
    },
    applyScheduling() {
      state.applied_version = state.desired_version;
      state.applied_paused = state.desired_paused;
      state.draining_count = 0;
      state.note = state.applied_paused ? "研究调度已暂停。" : "研究调度正常。";
    },
  };
}

async function openTasks(page: Page) {
  await page.goto("./#/tasks");
  await expect(page.getByRole("heading", { name: "任务与调度", exact: true })).toBeVisible();
}

async function tip(page: Page, name: string) {
  const control = page.getByRole("button", { name, exact: true });
  if (test.info().project.name === "phone") await control.tap();
  else {
    await control.hover();
    await expect(page.getByRole("tooltip")).toBeVisible();
    await page.mouse.move(1, 1);
    await control.focus();
  }
  await expect(page.getByRole("tooltip")).toBeVisible();
  await page.getByRole("heading", { name: "任务与调度", exact: true }).click();
}

test("four regions, readonly confirmation, writer preparation and invocation logs", async ({
  page,
  baseURL,
}, info) => {
  const state = await fixture(page, baseURL);
  await openTasks(page);
  for (const name of ["定时任务区", "运行服务区", "资源概况"])
    await expect(page.getByRole("region", { name, exact: true })).toBeVisible();
  await expect(page.getByRole("heading", { name: "研究任务", exact: true })).toBeVisible();
  expect(findJargon(await page.locator("main").innerText())).toEqual([]);
  await expect(page.getByRole("region", { name: "资源概况", exact: true })).toContainText("0.0%");
  await tip(page, "备份数据运行说明");
  expect(state.commands).toHaveLength(0);
  const run = page.getByRole("button", { name: "立即运行备份数据", exact: true });
  await run.focus();
  await page.keyboard.press("Enter");
  const first = page.getByRole("dialog", { name: "运行备份数据", exact: true });
  await expect(first).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(run).toBeFocused();
  expect(state.commands).toHaveLength(0);
  await run.click();
  await page.getByRole("button", { name: "确认运行", exact: true }).click();
  await expect(page.getByText("本次运行已开始。", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "核验备份数据原请求", exact: true }).click();
  await expect(page.getByText("本次运行已完成。", { exact: true })).toBeVisible();
  const logButton = page.getByRole("button", { name: "查看备份数据的运行日志", exact: true });
  await logButton.click();
  await expect(page.getByRole("list", { name: "服务日志" })).toContainText("任务已完成");
  expect(state.logs[0]?.searchParams.get("invocation_id")).toBe(invocation);
  await page.keyboard.press("Escape");
  await expect(logButton).toBeFocused();
  const writer = page.getByRole("button", { name: "立即运行日线更新", exact: true });
  await writer.click();
  const dialog = page.getByRole("dialog", { name: "运行日线更新", exact: true });
  await expect(dialog.getByRole("button", { name: "确认运行", exact: true })).toBeDisabled();
  await dialog.getByRole("button", { name: /取\s*消/ }).click();
  await expect(writer).toBeFocused();
  expect(state.commands.filter((item) => item.kind === "request_unit_run")).toHaveLength(1);
  await writer.click();
  await dialog.getByRole("textbox").fill("日线更新");
  await dialog.getByRole("button", { name: "确认运行", exact: true }).click();
  await expect(page.getByRole("button", { name: "核验日线更新原请求", exact: true })).toBeVisible();
  expect(state.commands.filter((item) => item.kind === "prepare_unit_run")).toHaveLength(2);
  expect(state.commands.filter((item) => item.kind === "request_unit_run")).toHaveLength(2);
  await expectNoHorizontalOverflow(page, "task controls and results");
  await page.screenshot({ path: info.outputPath("task-controls.png"), fullPage: true });
  expect(state.problems).toEqual([]);
});

test("global pause drains before applied and resumes with a new CAS version", async ({
  page,
  baseURL,
}, info) => {
  const state = await fixture(page, baseURL);
  await openTasks(page);
  await tip(page, "全局调度说明");
  const pause = page.getByRole("button", { name: "暂停研究调度", exact: true });
  const pauseDialog = page.getByRole("dialog", { name: "暂停研究调度", exact: true });
  await pause.focus();
  await page.keyboard.press("Enter");
  await expect(pauseDialog).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(pauseDialog).toBeHidden();
  await expect(pause).toBeFocused();
  expect(state.commands).toHaveLength(0);
  await pause.click();
  await page.getByRole("button", { name: "确认暂停", exact: true }).click();
  await expect(page.getByText("请求已受理，等待调度应用。", { exact: true })).toBeVisible();
  await expect(page.getByText("还有 1 项在收尾", { exact: true })).toBeVisible();
  await expect(page.getByText("研究调度已暂停。", { exact: true })).toHaveCount(0);
  state.applyScheduling();
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.getByText("研究调度已暂停。", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "恢复研究调度", exact: true }).click();
  await page.getByRole("button", { name: "确认恢复", exact: true }).click();
  state.applyScheduling();
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.getByText("研究调度正常。", { exact: true })).toBeVisible();
  expect(
    state.commands.map((item) =>
      item.kind === "set_lab_scheduling_paused" ? [item.expected_version, item.paused] : item.kind,
    ),
  ).toEqual([
    [0, true],
    [1, false],
  ]);
  await expectNoHorizontalOverflow(page, "global scheduling");
  await page.screenshot({ path: info.outputPath("scheduling.png"), fullPage: true });
  expect(state.problems).toEqual([]);
});

test("unknown original UUID survives missing source and same-owner generation, then clears on owner change", async ({
  page,
  baseURL,
}) => {
  const state = await fixture(page, baseURL);
  state.setLost(true);
  await openTasks(page);
  await page.getByRole("button", { name: "立即运行备份数据", exact: true }).click();
  await page.getByRole("button", { name: "确认运行", exact: true }).click();
  await expect(page.getByText("结果待确认，请核验原请求。", { exact: true })).toBeVisible();
  state.setMissing();
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await page.getByRole("button", { name: "核验备份数据原请求", exact: true }).click();
  const generation = "b".repeat(64);
  state.setGeneration(generation);
  await page.clock.runFor(15_100);
  await expect.poll(() => state.grants.includes(generation)).toBe(true);
  await page.getByRole("button", { name: "核验备份数据原请求", exact: true }).click();
  await page.getByRole("button", { name: "恢复备份数据原请求", exact: true }).click();
  expect(state.lookups).toEqual([state.commands[0], state.commands[0]]);
  expect(state.resumes).toEqual([state.commands[0]]);
  expect(state.commands[0]?.generation_id).toBe(taskGeneration);
  state.setViewer("bob");
  await page.clock.runFor(15_100);
  await expect(page.getByRole("button", { name: "核验备份数据原请求", exact: true })).toHaveCount(
    0,
  );
  await expect(page.getByText("结果待确认，请核验原请求。", { exact: true })).toHaveCount(0);
  expect(state.commands).toHaveLength(1);
  expect(state.problems).toEqual([]);
});

test("definite rejection releases the control and revoked capability clears private recovery", async ({
  page,
  baseURL,
}) => {
  const state = await fixture(page, baseURL);
  state.rejectNext();
  await openTasks(page);
  const run = page.getByRole("button", { name: "立即运行备份数据", exact: true });
  await run.click();
  await page.getByRole("button", { name: "确认运行", exact: true }).click();
  await expect(page.getByText("请求已拒绝，请刷新状态后再操作。", { exact: true })).toBeVisible();
  await expect(run).toBeEnabled();
  await expect(page.getByRole("button", { name: "核验备份数据原请求", exact: true })).toHaveCount(
    0,
  );
  state.setLost(true);
  await run.click();
  await page.getByRole("button", { name: "确认运行", exact: true }).click();
  await expect(page.getByRole("button", { name: "核验备份数据原请求", exact: true })).toBeVisible();
  state.setRevoked();
  await page.getByRole("button", { name: "刷新", exact: true }).click();
  await expect(page.getByRole("button", { name: "核验备份数据原请求", exact: true })).toHaveCount(
    0,
  );
  await expect(page.locator("main")).not.toContainText("synthetic-private-error");
  expect(state.commands).toHaveLength(2);
  expect(state.problems).toEqual([]);
});

test("writer server expiry refuses confirmation with no start and restores the opener", async ({
  page,
  baseURL,
}) => {
  const state = await fixture(page, baseURL);
  await openTasks(page);
  const run = page.getByRole("button", { name: "立即运行日线更新", exact: true });
  await run.click();
  const dialog = page.getByRole("dialog", { name: "运行日线更新", exact: true });
  await dialog.getByRole("textbox").fill("日线更新");
  await expect(dialog.getByRole("button", { name: "确认运行", exact: true })).toBeEnabled();
  await page.clock.setFixedTime(new Date(Date.parse(taskClock) + 300_000));
  await page.clock.runFor(1000);
  await expect(dialog.getByText("确认已过期，请关闭后重新发起。", { exact: true })).toBeVisible();
  await expect(dialog.getByRole("button", { name: "确认运行", exact: true })).toBeDisabled();
  await page.keyboard.press("Escape");
  await expect(run).toBeFocused();
  expect(state.commands).toHaveLength(1);
  expect(state.commands[0]?.kind).toBe("prepare_unit_run");
  expect(state.problems).toEqual([]);
});

test("CPU zero differs from unknown, original TTL expires, and empty logs do not imply a missing service", async ({
  page,
  baseURL,
}, info) => {
  const state = await fixture(page, baseURL);
  state.setDisabled();
  state.setEmptyLogs();
  await openTasks(page);
  await expect(
    page.getByRole("table", { name: "资源分组", exact: true }).getByRole("row", { name: /盘中/ }),
  ).toContainText("0.0%");
  await expect(
    page.getByRole("table", { name: "资源分组", exact: true }).getByRole("row", { name: /维护/ }),
  ).not.toContainText("0.0%");
  await expect(page.getByRole("button", { name: "立即运行备份数据", exact: true })).toHaveCount(0);
  // A disabled write capability does not remove the separately allowed log read.
  const logButton = page.getByRole("button", { name: "查看备份数据的运行日志", exact: true });
  await logButton.click();
  await expect(page.getByText("所选范围还没有可显示的日志。", { exact: true })).toBeVisible();
  expect(state.logs).toHaveLength(1);
  await page.keyboard.press("Escape");
  await expect(logButton).toBeFocused();
  await page.clock.fastForward(81_000);
  await expect(page.getByText("资源状态已过期", { exact: true })).toBeVisible();
  await expect(page.getByText("定时任务状态已过期", { exact: true })).toBeVisible();
  await expectNoHorizontalOverflow(page, "unknown CPU and expired source");
  await page.screenshot({ path: info.outputPath("cpu-expired.png"), fullPage: true });
  expect(state.problems).toEqual([]);
});
