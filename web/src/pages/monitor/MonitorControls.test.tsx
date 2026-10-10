import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { HttpResponse, http } from "msw";
import { vi } from "vitest";
import type { Schemas } from "@/api/client";
import type { TaskControlRequest } from "@/api/taskControls";
import { AppProviders } from "@/app/App";
import { testQueryClient } from "@/test/queryClient";
import { server } from "@/test/server";
import { MonitorControls } from "./MonitorControls";
import { monitorRecoveryKey } from "./monitorControlRecovery";

const generation = "a".repeat(64);
const installation = "e".repeat(64);
beforeEach(() => window.sessionStorage.clear());
const capabilities: Schemas["TaskControlCapabilitiesData"] = {
  generation_id: generation,
  notifier_mode: {
    available: true,
    mode: "shadow",
    revision: 2,
    installation_sha256: installation,
    can_request: true,
    can_set_live: true,
    note: "已核对当前通知配置。",
  },
  monitor_builtins: [
    {
      builtin_id: "pool2_levels",
      label: "档位提醒",
      enabled: true,
      revision: 2,
      installation_sha256: installation,
      can_request: true,
    },
  ],
  units: [
    {
      unit: "rquant-notify-test.service",
      can_request: true,
      requires_confirmation: true,
      reason: "每 10 分钟最多一次。",
      next_allowed_at: null,
    },
  ],
  can_control_scheduling: false,
  can_recover_scheduling: false,
  can_recover_units: true,
  scheduling: { available: false, note: "未配置" },
  note: "已核对",
};
const runtime: Schemas["MonitorRuntimeData"] = {
  state: "ready",
  source_label: "已核对",
  source_note: "当前原通知记录",
  observed_at: null,
  mode: "shadow",
  mode_label: "仅记录",
  applied_revision: 2,
  applied_command_id: "previous-mode-command",
  monitor_installation_sha256: installation,
  channels: [],
  builtins: [
    {
      builtin_id: "pool2_levels",
      label: "档位提醒",
      enabled: true,
      state: "ready",
      state_label: "正常",
      source_note: "原有效报价",
      observed_at: null,
      evaluated_at: "2026-09-24T02:00:00Z",
      source_valid_until: null,
      last_triggered_at: null,
      matched_count: 0,
      channels: ["pushdeer"],
      applied_revision: 2,
      applied_command_id: "previous-builtin-command",
      monitor_installation_sha256: installation,
    },
  ],
};

function installCapabilities(value = capabilities) {
  server.use(http.get("*/api/v1/tasks/control-capabilities", () => HttpResponse.json(value)));
}

function answer(body: TaskControlRequest, status: Schemas["TaskControlCommandData"]["status"]) {
  return {
    command_id: body.command_id,
    original_request: body,
    status,
    message: status === "prepared" ? "请核对后确认。" : "已保存，正在同步。",
    can_resume: false,
    confirmation_id: status === "prepared" ? "c".repeat(64) : null,
    confirmation_expires_at:
      status === "prepared" ? new Date(Date.now() + 120_000).toISOString() : null,
    desired_revision: ["submitted", "succeeded"].includes(status) ? 3 : null,
    desired_installation_sha256: ["submitted", "succeeded"].includes(status) ? installation : null,
  } satisfies Schemas["TaskControlCommandData"];
}

function controls(props: Partial<Parameters<typeof MonitorControls>[0]> = {}) {
  const client = testQueryClient();
  const onRefresh = vi.fn();
  const component = (extra: Partial<Parameters<typeof MonitorControls>[0]> = {}) => (
    <AppProviders queryClient={client}>
      <MonitorControls
        viewer="alice"
        identityKnown={true}
        generationId={generation}
        refreshKey={0}
        data={runtime}
        onRefresh={onRefresh}
        {...props}
        {...extra}
      />
    </AppProviders>
  );
  return { ...render(component()), onRefresh, component };
}

describe("通知快捷控制", () => {
  it("hides private facts and disables writes before meta confirms an actor", async () => {
    installCapabilities();
    const calls = vi.fn();
    server.use(
      http.post("*/api/v1/tasks/*", () => {
        calls();
        return new HttpResponse(null, { status: 503 });
      }),
    );
    controls({
      viewer: null,
      generationId: null,
      identityKnown: false,
      data: { ...runtime, mode_label: "私有模式内容" },
    });
    expect(screen.queryByText("私有模式内容")).toBeNull();
    expect(screen.getByRole("button", { name: "切换通知模式" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "立即运行测试推送" })).toBeNull();
    expect(calls).not.toHaveBeenCalled();
  });

  it("refuses every new operation when initial request storage is unavailable", async () => {
    installCapabilities();
    const calls = vi.fn();
    const storage = vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => {
      throw new Error("synthetic unavailable session storage");
    });
    try {
      server.use(
        http.post("*/api/v1/tasks/*", () => {
          calls();
          return new HttpResponse(null, { status: 503 });
        }),
      );
      controls();
      expect(await screen.findByRole("button", { name: "切换为正式推送" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "暂停档位提醒" })).toBeDisabled();
      expect(screen.getByRole("button", { name: "立即运行测试推送" })).toBeDisabled();
      expect(calls).not.toHaveBeenCalled();
    } finally {
      storage.mockRestore();
    }
  });

  it("keeps the saved heavy UUID recoverable when saving the confirmed body fails before start", async () => {
    installCapabilities();
    const prepared: TaskControlRequest[] = [],
      started: TaskControlRequest[] = [],
      lookedUp: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/units/:unit/run/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        prepared.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
      http.post("*/api/v1/tasks/units/:unit/run", async ({ request }) => {
        started.push((await request.json()) as TaskControlRequest);
        return HttpResponse.json(answer(started[0] as TaskControlRequest, "succeeded"));
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        lookedUp.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
    );
    const user = userEvent.setup();
    controls();
    await user.click(await screen.findByRole("button", { name: "立即运行测试推送" }));
    const dialog = await screen.findByRole("dialog", { name: "运行测试推送" });
    const original = Storage.prototype.setItem;
    const failing = vi.spyOn(Storage.prototype, "setItem").mockImplementation(function (
      this: Storage,
      key,
      value,
    ) {
      if (key === monitorRecoveryKey && value.includes('"request_unit_run"'))
        throw new Error("synthetic quota exhausted at confirmed body");
      original.call(this, key, value);
    });
    try {
      await user.type(within(dialog).getByRole("textbox"), "测试推送");
      await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
      expect(await screen.findByText("原请求无法保存，请刷新后重试。")).toBeInTheDocument();
      expect(started).toHaveLength(0);
      expect(prepared).toHaveLength(1);
      const lookup = await screen.findByRole("button", { name: "核验测试推送原请求" });
      await waitFor(() => expect(lookup).toBeEnabled());
      await user.click(lookup);
      await waitFor(() => expect(lookedUp).toHaveLength(1));
      expect(lookedUp[0]).toEqual(prepared[0]);
      expect(started).toHaveLength(0);
    } finally {
      failing.mockRestore();
    }
  });

  it("finishes submitted mode only after the matching original owner application receipt", async () => {
    installCapabilities();
    let appliedBody: TaskControlRequest | undefined;
    server.use(
      http.post("*/api/v1/tasks/notifications/mode/prepare", async ({ request }) =>
        HttpResponse.json(answer((await request.json()) as TaskControlRequest, "prepared")),
      ),
      http.post("*/api/v1/tasks/notifications/mode", async ({ request }) => {
        appliedBody = (await request.json()) as TaskControlRequest;
        return HttpResponse.json(answer(appliedBody, "submitted"));
      }),
    );
    const user = userEvent.setup(),
      view = controls();
    await user.click(await screen.findByRole("button", { name: "切换为正式推送" }));
    const dialog = await screen.findByRole("dialog", { name: "切换通知模式" });
    await user.type(within(dialog).getByRole("textbox"), "正式推送");
    await user.click(within(dialog).getByRole("button", { name: "确认切换" }));
    await waitFor(() => expect(appliedBody?.kind).toBe("set_notifier_delivery_mode"));
    expect(screen.getByRole("button", { name: "暂停档位提醒" })).toBeDisabled();
    installCapabilities({
      ...capabilities,
      notifier_mode: {
        ...capabilities.notifier_mode,
        revision: 3,
        mode: "live",
      },
    });
    const applied = {
      ...runtime,
      mode: "live",
      mode_label: "正式推送",
      applied_revision: 3,
    } as const;
    view.rerender(
      view.component({
        refreshKey: 1,
        data: { ...applied, applied_command_id: "another-command" },
      }),
    );
    await screen.findByRole("button", { name: "切换为仅记录" });
    expect(screen.getByRole("button", { name: "暂停档位提醒" })).toBeDisabled();
    view.rerender(
      view.component({ data: { ...applied, applied_command_id: appliedBody?.command_id } }),
    );
    await waitFor(() => expect(screen.getByRole("button", { name: "切换为仅记录" })).toBeEnabled());
    expect(screen.getByText("通知模式已应用。")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "暂停档位提醒" })).toBeEnabled();
  });

  it("finishes submitted builtin only after the exact same installation command and revision", async () => {
    installCapabilities();
    let body: TaskControlRequest | undefined;
    server.use(
      http.post("*/api/v1/tasks/monitor/builtins/commands", async ({ request }) => {
        body = (await request.json()) as TaskControlRequest;
        return HttpResponse.json(answer(body, "submitted"));
      }),
    );
    const user = userEvent.setup(),
      view = controls();
    const pause = await screen.findByRole("button", { name: "暂停档位提醒" });
    await waitFor(() => expect(pause).toBeEnabled());
    await user.click(pause);
    await waitFor(() => expect(body).toBeDefined());
    expect(await screen.findByRole("button", { name: "切换为正式推送" })).toBeDisabled();
    installCapabilities({
      ...capabilities,
      monitor_builtins: capabilities.monitor_builtins.map((row) => ({
        ...row,
        revision: 3,
        enabled: false,
      })),
    });
    const applied = {
      ...runtime,
      builtins: (runtime.builtins ?? []).map((row) => ({
        ...row,
        enabled: false,
        state: "disabled",
        applied_revision: 3,
        applied_command_id: body?.command_id,
        monitor_installation_sha256: "f".repeat(64),
      })),
    } as Schemas["MonitorRuntimeData"];
    view.rerender(view.component({ refreshKey: 1, data: { ...applied, builtins: undefined } }));
    expect(await screen.findByRole("button", { name: "切换为正式推送" })).toBeDisabled();
    expect(screen.queryByText("规则已应用。")).not.toBeInTheDocument();
    view.rerender(view.component({ data: applied }));
    await screen.findByRole("button", { name: "启用档位提醒" });
    expect(await screen.findByRole("button", { name: "切换为正式推送" })).toBeDisabled();
    view.rerender(
      view.component({
        data: {
          ...applied,
          builtins: (applied.builtins ?? []).map((row) => ({
            ...row,
            monitor_installation_sha256: installation,
          })),
        },
      }),
    );
    await waitFor(() => expect(screen.getByRole("button", { name: "启用档位提醒" })).toBeEnabled());
    expect(screen.getByRole("button", { name: "切换为正式推送" })).toBeEnabled();
    expect(screen.getByText("规则已应用。")).toBeInTheDocument();
  });

  it("retains the exact builtin body before POST through unknown meta and same actor remount", async () => {
    installCapabilities();
    const written: TaskControlRequest[] = [],
      recovered: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/monitor/builtins/commands", async ({ request }) => {
        written.push((await request.json()) as TaskControlRequest);
        expect(window.sessionStorage.length).toBeGreaterThan(0);
        return new HttpResponse(null, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        recovered.push(body);
        return HttpResponse.json(answer(body, "submitted"));
      }),
    );
    const user = userEvent.setup(),
      first = controls();
    const pause = await screen.findByRole("button", { name: "暂停档位提醒" });
    await waitFor(() => expect(pause).toBeEnabled());
    await user.click(pause);
    await screen.findByRole("button", { name: "核验档位提醒原请求" });
    first.rerender(first.component({ viewer: null, generationId: null, identityKnown: false }));
    expect(screen.queryByRole("button", { name: "核验档位提醒原请求" })).toBeNull();
    expect(screen.getByRole("button", { name: "切换通知模式" })).toBeDisabled();
    first.unmount();
    const second = controls({ viewer: null, generationId: null, identityKnown: false });
    expect(screen.queryByText("仅记录")).toBeNull();
    second.rerender(
      second.component({ viewer: "alice", generationId: generation, identityKnown: true }),
    );
    const lookup = await screen.findByRole("button", { name: "核验档位提醒原请求" });
    await waitFor(() => expect(lookup).toBeEnabled());
    await user.click(lookup);
    await waitFor(() => expect(recovered).toHaveLength(1));
    expect(recovered[0]).toEqual(written[0]);
    expect(written).toHaveLength(1);
  });

  it("restores a heavy unknown request after one meta failure and a same actor reload", async () => {
    installCapabilities();
    const written: TaskControlRequest[] = [],
      recovered: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/units/:unit/run/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        written.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
      http.post("*/api/v1/tasks/units/:unit/run", async ({ request }) => {
        written.push((await request.json()) as TaskControlRequest);
        return new HttpResponse(null, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        recovered.push(body);
        return HttpResponse.json(answer(body, "unknown"));
      }),
    );
    const user = userEvent.setup(),
      first = controls();
    await user.click(await screen.findByRole("button", { name: "立即运行测试推送" }));
    const dialog = await screen.findByRole("dialog", { name: "运行测试推送" });
    await user.type(within(dialog).getByRole("textbox"), "测试推送");
    await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
    await screen.findByRole("button", { name: "核验测试推送原请求" });
    first.rerender(first.component({ viewer: null, generationId: null, identityKnown: false }));
    expect(screen.queryByRole("button", { name: "核验测试推送原请求" })).toBeNull();
    first.rerender(first.component());
    expect(await screen.findByRole("button", { name: "核验测试推送原请求" })).toBeInTheDocument();
    first.unmount();
    const second = controls({ viewer: null, generationId: null, identityKnown: false });
    second.rerender(
      second.component({ viewer: "alice", generationId: generation, identityKnown: true }),
    );
    const lookup = await screen.findByRole("button", { name: "核验测试推送原请求" });
    await waitFor(() => expect(lookup).toBeEnabled());
    await user.click(lookup);
    await waitFor(() => expect(recovered).toHaveLength(1));
    expect(written).toHaveLength(2);
    expect(recovered[0]).toEqual(written[1]);
  });

  it("prepares the exact second UUID, waits for typed confirmation, and cancels without applying", async () => {
    installCapabilities();
    const sent: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/notifications/mode/prepare", async ({ request }) => {
        expect(request.headers.get("X-Rquant-Csrf")).toBe("1");
        const body = (await request.json()) as TaskControlRequest;
        sent.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
    );
    const user = userEvent.setup();
    controls();
    const start = await screen.findByRole("button", { name: "切换为正式推送" });
    await user.click(start);
    const dialog = await screen.findByRole("dialog", { name: "切换通知模式" });
    expect(sent).toHaveLength(1);
    const prepare = sent[0];
    expect(prepare?.kind).toBe("prepare_notifier_delivery_mode");
    if (prepare?.kind !== "prepare_notifier_delivery_mode") throw new Error("missing prepare");
    expect(prepare.command_id).not.toBe(prepare.run.command_id);
    expect(prepare.run.mode).toBe("live");
    expect(prepare.run.expected_revision).toBe(2);
    expect(within(dialog).getByRole("button", { name: "确认切换" })).toBeDisabled();
    await user.click(within(dialog).getByRole("button", { name: /^取\s*消$/ }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull(), { timeout: 5000 });
    await waitFor(() => expect(start).toHaveFocus(), { timeout: 5000 });
    expect(sent).toHaveLength(1);
  });

  it("looks up an unknown mode result with the same original request instead of creating another UUID", async () => {
    installCapabilities();
    const prepared: TaskControlRequest[] = [];
    const applied: TaskControlRequest[] = [];
    const lookedUp: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/notifications/mode/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        prepared.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
      http.post("*/api/v1/tasks/notifications/mode", async ({ request }) => {
        applied.push((await request.json()) as TaskControlRequest);
        return new HttpResponse(null, { status: 503 });
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        lookedUp.push(body);
        return HttpResponse.json(answer(body, "succeeded"));
      }),
    );
    const user = userEvent.setup();
    controls();
    await user.click(await screen.findByRole("button", { name: "切换为正式推送" }));
    const dialog = await screen.findByRole("dialog", { name: "切换通知模式" });
    await user.type(within(dialog).getByRole("textbox"), "正式推送");
    await user.click(within(dialog).getByRole("button", { name: "确认切换" }));
    await user.click(await screen.findByRole("button", { name: "核验通知模式原请求" }));
    await waitFor(() => expect(lookedUp).toHaveLength(1));
    expect(applied).toHaveLength(1);
    expect(lookedUp[0]).toEqual(applied[0]);
    expect(prepared).toHaveLength(1);
    if (prepared[0]?.kind !== "prepare_notifier_delivery_mode") throw new Error("missing prepare");
    expect(applied[0]?.command_id).toBe(prepared[0].run.command_id);
    expect(applied[0]).toMatchObject({ confirmation_id: "c".repeat(64), mode: "live" });
  });

  it("clears an open private confirmation when viewer or generation changes", async () => {
    installCapabilities();
    server.use(
      http.post("*/api/v1/tasks/notifications/mode/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        return HttpResponse.json(answer(body, "prepared"));
      }),
    );
    const user = userEvent.setup();
    const view = controls();
    await user.click(await screen.findByRole("button", { name: "切换为正式推送" }));
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    view.rerender(view.component({ viewer: "bob", generationId: "b".repeat(64) }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(screen.queryByRole("button", { name: "核验通知模式原请求" })).toBeNull();
    expect(screen.getByRole("button", { name: "切换通知模式" })).toBeDisabled();
  });

  it("uses the actual manual unit's two-step confirmation and cooldown", async () => {
    installCapabilities();
    const calls: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/units/:unit/run/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        calls.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
      http.post("*/api/v1/tasks/units/:unit/run", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        calls.push(body);
        return HttpResponse.json(answer(body, "succeeded"));
      }),
    );
    const user = userEvent.setup();
    const view = controls();
    await user.click(await screen.findByRole("button", { name: "立即运行测试推送" }));
    const dialog = await screen.findByRole("dialog", { name: "运行测试推送" });
    expect(within(dialog).getByRole("button", { name: "确认运行" })).toBeDisabled();
    expect(dialog).not.toHaveTextContent("会写入数据");
    await user.type(within(dialog).getByRole("textbox"), "测试推送");
    await user.click(within(dialog).getByRole("button", { name: "确认运行" }));
    await waitFor(() => expect(calls).toHaveLength(2));
    if (calls[0]?.kind !== "prepare_unit_run") throw new Error("missing unit prepare");
    expect(calls[1]?.command_id).toBe(calls[0].run.command_id);
    expect(calls[1]).toMatchObject({
      unit: "rquant-notify-test.service",
      confirmation_id: "c".repeat(64),
    });
    installCapabilities({
      ...capabilities,
      units: [
        {
          ...capabilities.units[0],
          can_request: false,
          reason: "10 分钟内已请求过测试。",
        } as Schemas["TaskUnitControlChoice"],
      ],
    });
    view.rerender(view.component({ refreshKey: 1 }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "立即运行测试推送" })).toBeDisabled(),
    );
  });

  it("refuses mode and builtin writes when current capability is unavailable", async () => {
    installCapabilities({
      ...capabilities,
      notifier_mode: {
        available: false,
        can_request: false,
        can_set_live: false,
        note: "配置尚未安装。",
      },
      monitor_builtins: capabilities.monitor_builtins.map((item) => ({
        ...item,
        can_request: false,
      })),
      units: [],
    });
    controls();
    expect(await screen.findByRole("button", { name: "切换通知模式" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "暂停档位提醒" })).toBeDisabled();
    expect(screen.queryByRole("button", { name: "立即运行测试推送" })).toBeNull();
  });

  it("binds builtin CAS and rejects a changed response identity while retaining the original UUID", async () => {
    installCapabilities();
    const sent: TaskControlRequest[] = [],
      recovered: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/monitor/builtins/commands", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        sent.push(body);
        if (body.kind !== "set_monitor_builtin_enabled") throw new Error("wrong builtin request");
        return HttpResponse.json(answer({ ...body, builtin_id: "surge" }, "succeeded"));
      }),
      http.post("*/api/v1/tasks/controls/lookup", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        recovered.push(body);
        return HttpResponse.json(answer(body, "succeeded"));
      }),
    );
    const user = userEvent.setup();
    controls();
    const pause = await screen.findByRole("button", { name: "暂停档位提醒" });
    await waitFor(() => expect(pause).toBeEnabled());
    await user.click(pause);
    await user.click(await screen.findByRole("button", { name: "核验档位提醒原请求" }));
    await waitFor(() => expect(recovered).toHaveLength(1));
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      kind: "set_monitor_builtin_enabled",
      builtin_id: "pool2_levels",
      expected_revision: 2,
      enabled: false,
    });
    expect(recovered[0]).toEqual(sent[0]);
    expect(sent[0]).not.toHaveProperty("owner_id");
  });

  it("clears private pending operations after a permission refusal", async () => {
    installCapabilities();
    server.use(
      http.post(
        "*/api/v1/tasks/notifications/mode/prepare",
        () => new HttpResponse(null, { status: 403 }),
      ),
    );
    const user = userEvent.setup();
    controls();
    await user.click(await screen.findByRole("button", { name: "切换为正式推送" }));
    await waitFor(() =>
      expect(screen.getByRole("button", { name: "切换为正式推送" })).toBeDisabled(),
    );
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(screen.queryByRole("button", { name: "核验通知模式原请求" })).toBeNull();
    expect(screen.getByRole("button", { name: "暂停档位提醒" })).toBeDisabled();
  });

  it("retires a prepared preview on a new generation and uses two fresh UUIDs", async () => {
    installCapabilities();
    const prepares: TaskControlRequest[] = [];
    server.use(
      http.post("*/api/v1/tasks/notifications/mode/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        prepares.push(body);
        return HttpResponse.json(answer(body, "prepared"));
      }),
    );
    const user = userEvent.setup(),
      view = controls();
    await user.click(await screen.findByRole("button", { name: "切换为正式推送" }));
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    const nextGeneration = "b".repeat(64);
    installCapabilities({ ...capabilities, generation_id: nextGeneration });
    view.rerender(view.component({ generationId: nextGeneration }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    const start = await screen.findByRole("button", { name: "切换为正式推送" });
    await waitFor(() => expect(start).toBeEnabled());
    await user.click(start);
    expect(await screen.findByRole("dialog")).toBeInTheDocument();
    expect(prepares).toHaveLength(2);
    const old = prepares[0],
      current = prepares[1];
    if (
      old?.kind !== "prepare_notifier_delivery_mode" ||
      current?.kind !== "prepare_notifier_delivery_mode"
    )
      throw new Error("missing prepares");
    expect(current.run.generation_id).toBe(nextGeneration);
    expect(current.command_id).not.toBe(old.command_id);
    expect(current.run.command_id).not.toBe(old.run.command_id);
  });

  it("refuses an expired server confirmation without applying the mode", async () => {
    installCapabilities();
    server.use(
      http.post("*/api/v1/tasks/notifications/mode/prepare", async ({ request }) => {
        const body = (await request.json()) as TaskControlRequest;
        return HttpResponse.json({
          ...answer(body, "prepared"),
          confirmation_expires_at: new Date(Date.now() - 1).toISOString(),
        });
      }),
    );
    const user = userEvent.setup();
    controls();
    await user.click(await screen.findByRole("button", { name: "切换为正式推送" }));
    const dialog = await screen.findByRole("dialog");
    await user.type(within(dialog).getByRole("textbox"), "正式推送");
    expect(within(dialog).getByRole("button", { name: "确认切换" })).toBeDisabled();
    expect(within(dialog).getByRole("alert")).toHaveTextContent("确认已过期");
  });
});
