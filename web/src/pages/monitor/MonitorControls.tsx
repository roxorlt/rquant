import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import type { Schemas } from "@/api/client";
import {
  type BuiltinControlRequest,
  type NotifierModeRequest,
  type NotifierPrepareRequest,
  recoverTaskControl,
  submitTaskControl,
  type TaskControlRequest,
  type TaskControlResult,
  useTaskControlCapabilities,
} from "@/api/taskControls";
import { Button, ConfirmDialog, Panel, StatusBadge, Tip } from "@/ui";
import { TaskUnitControls } from "../tasks/TaskUnitControls";
import {
  restoreTaskControlFocus,
  type TaskPending,
  taskRequestError,
} from "../tasks/taskControlRecovery";
import { BuiltinRules } from "./BuiltinRules";
import {
  MonitorControlMemory,
  MonitorControlPersistenceError,
  monitorApplication,
  settledMonitorRequest,
} from "./monitorControlRecovery";

export function MonitorControls({
  viewer,
  identityKnown = viewer !== null,
  generationId,
  refreshKey,
  data,
  loading = false,
  onRefresh,
}: {
  viewer: string | null;
  identityKnown?: boolean;
  generationId: string | null;
  refreshKey: number;
  data: Schemas["MonitorRuntimeData"] | undefined;
  loading?: boolean;
  onRefresh: () => void;
}) {
  const capabilities = useTaskControlCapabilities(
    identityKnown ? viewer : null,
    identityKnown ? generationId : null,
    refreshKey,
  );
  const [memory] = useState(() => new MonitorControlMemory());
  useSyncExternalStore(memory.subscribeRecords, memory.recordsSnapshot);
  const [draft, setDraft] = useState<NotifierModeRequest | null>(null);
  const [confirmation, setConfirmation] = useState<TaskControlResult | null>(null);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [revoked, setRevoked] = useState(false);
  const [storageNotice, setStorageNotice] = useState<string | null>(null);
  const region = useRef<HTMLDivElement | null>(null);
  const trigger = useRef<HTMLButtonElement | null>(null);
  const operation = useRef(0);
  const wasOpen = useRef(false);
  const controller = useRef<AbortController | null>(null);
  const identity = useRef({ viewer, generationId, identityKnown });
  identity.current = { viewer, generationId, identityKnown };
  const sameIdentity = identityKnown && memory.isCurrent(viewer, generationId);
  const pending = Object.fromEntries(sameIdentity && viewer ? memory.entries(viewer) : []);
  const privateData = sameIdentity ? data : undefined;
  const controls =
    sameIdentity && !capabilities.isError && capabilities.data?.generation_id === generationId
      ? capabilities.data
      : undefined;
  const mode = controls?.notifier_mode;
  const choices = controls?.monitor_builtins ?? [];
  const testChoice = controls?.units.find((item) => item.unit === "rquant-notify-test.service");
  const modePending = pending.mode;
  const unresolved = Object.values(pending).some(
    (item) => !settledMonitorRequest(item, privateData, controls),
  );
  const otherUnresolved = Object.entries(pending).some(
    ([key, item]) =>
      key !== "rquant-notify-test.service" && !settledMonitorRequest(item, privateData, controls),
  );
  const target = mode?.mode === "live" ? "shadow" : "live";
  const targetLabel = target === "live" ? "正式推送" : "仅记录";
  const canSetMode =
    !!viewer &&
    generationId !== null &&
    !revoked &&
    memory.storageAvailable &&
    mode?.available === true &&
    mode.can_request &&
    mode.revision != null &&
    (target !== "live" || mode.can_set_live);
  const canRecover =
    !!viewer &&
    !revoked &&
    controls !== undefined &&
    (controls.can_recover_units ||
      mode?.can_request === true ||
      choices.some((item) => item.can_request));
  const current = (token: number, owner: string, generation: string) =>
    operation.current === token &&
    identity.current.identityKnown &&
    identity.current.viewer === owner &&
    identity.current.generationId === generation;

  useEffect(
    () => () => {
      operation.current += 1;
      controller.current?.abort();
    },
    [],
  );
  useEffect(() => {
    operation.current += 1;
    controller.current?.abort();
    if (identityKnown) memory.confirmActor(viewer, generationId);
    else memory.suspend();
    setDraft(null);
    setConfirmation(null);
    setOpen(false);
    setBusy(false);
    setRevoked(false);
    setStorageNotice(null);
  }, [viewer, generationId, identityKnown, memory]);
  useEffect(() => {
    if (!capabilities.isError && capabilities.data?.generation_id === generationId)
      setRevoked(false);
  }, [capabilities.data, capabilities.isError, generationId]);

  function keep(key: string, value: TaskPending | null) {
    if (!viewer) return;
    memory.put(viewer, key, value);
  }

  function revoke() {
    operation.current += 1;
    controller.current?.abort();
    if (viewer) memory.clear(viewer);
    setDraft(null);
    setConfirmation(null);
    setOpen(false);
    setBusy(false);
    setRevoked(true);
    onRefresh();
  }

  async function send(
    key: string,
    body: TaskControlRequest,
    action: "submit" | "lookup" | "resume",
  ) {
    if (!identityKnown || !viewer || generationId === null || busy || revoked) return;
    const token = ++operation.current,
      owner = viewer,
      generation = generationId;
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    const record: TaskPending = {
      body,
      result: pending[key]?.body.command_id === body.command_id ? pending[key].result : null,
      message: "操作结果待确认，请核验原请求。",
    };
    setBusy(true);
    try {
      keep(key, record);
      const result =
        action === "submit"
          ? await submitTaskControl(body, request.signal)
          : await recoverTaskControl(body, action, request.signal);
      if (!current(token, owner, generation)) return;
      keep(key, { body, result, message: result.message });
      if (
        body.kind === "prepare_notifier_delivery_mode" &&
        result.status === "prepared" &&
        result.confirmation_id &&
        result.confirmation_expires_at
      ) {
        if (body.generation_id === generation) {
          setDraft({
            ...body.run,
            kind: "set_notifier_delivery_mode",
            confirmation_id: result.confirmation_id,
          });
          setConfirmation(result);
          setOpen(true);
        } else keep(key, null);
      } else if (
        body.kind !== "prepare_notifier_delivery_mode" &&
        result.status !== "unknown" &&
        result.status !== "not_found"
      )
        onRefresh();
    } catch (error) {
      if (!current(token, owner, generation)) return;
      if (error instanceof MonitorControlPersistenceError) {
        setStorageNotice(error.message);
        return;
      }
      const outcome = taskRequestError(error);
      if (outcome === "revoked") revoke();
      else if (outcome === "refused") {
        keep(key, { ...record, refused: true, message: "请求已拒绝，请刷新后再操作。" });
        setOpen(false);
        onRefresh();
      } else keep(key, record);
    } finally {
      if (current(token, owner, generation)) setBusy(false);
    }
  }

  function prepare(element: HTMLButtonElement) {
    if (
      !canSetMode ||
      unresolved ||
      busy ||
      !mode ||
      mode.revision == null ||
      generationId === null
    )
      return;
    trigger.current = element;
    const run: Schemas["NotifierModeDraft"] = {
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generationId,
      expected_revision: mode.revision,
      mode: target,
    };
    const body: NotifierPrepareRequest = {
      kind: "prepare_notifier_delivery_mode",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generationId,
      run,
    };
    void send("mode", body, "submit");
  }

  function toggle(choice: Schemas["MonitorBuiltinControlView"]) {
    if (
      !identityKnown ||
      !memory.storageAvailable ||
      revoked ||
      !choice.can_request ||
      generationId === null ||
      busy ||
      unresolved
    )
      return;
    const body: BuiltinControlRequest = {
      kind: "set_monitor_builtin_enabled",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generationId,
      builtin_id: choice.builtin_id,
      enabled: !choice.enabled,
      expected_revision: choice.revision,
    };
    void send(choice.builtin_id, body, "submit");
  }

  function cancel() {
    setOpen(false);
    setDraft(null);
    setConfirmation(null);
    if (
      modePending?.body.kind === "prepare_notifier_delivery_mode" &&
      modePending.result?.status === "prepared"
    )
      keep("mode", null);
  }
  const dialogOpen = sameIdentity && open && draft?.generation_id === generationId;
  const effectiveMode = privateData?.state === "ready" ? privateData.mode_label : "未确认";
  useEffect(() => {
    if (wasOpen.current && !dialogOpen) restoreTaskControlFocus(trigger.current, region.current);
    wasOpen.current = dialogOpen;
  }, [dialogOpen]);

  return (
    <div className="monitor-controls" ref={region}>
      <Panel title="通知设置">
        <div className="monitor-control-bar">
          <Tip content={privateData?.source_note ?? mode?.note ?? "通知配置暂无法核验。"}>
            <span className="monitor-mode">
              当前：<strong>{effectiveMode}</strong>
            </span>
          </Tip>
          <Button
            size="sm"
            variant="ghost"
            aria-label={mode?.available ? `切换为${targetLabel}` : "切换通知模式"}
            disabled={!canSetMode || busy || unresolved}
            onClick={(event) => prepare(event.currentTarget)}
          >
            {mode?.available ? `切换为${targetLabel}` : "切换模式"}
          </Button>
          <Tip content={mode?.note ?? "通知配置尚未开放。"} interactive>
            <Button size="sm" variant="ghost" aria-label="通知模式说明">
              ?
            </Button>
          </Tip>
          {viewer && generationId && testChoice ? (
            <TaskUnitControls
              key={`${viewer}:${generationId}`}
              unit={testChoice.unit}
              name="测试推送"
              viewer={viewer}
              generationId={generationId}
              choice={
                revoked || !memory.storageAvailable || otherUnresolved
                  ? { ...testChoice, can_request: false }
                  : testChoice
              }
              canRecover={!revoked && controls?.can_recover_units === true}
              memory={memory}
              onRefresh={onRefresh}
              onRevoked={revoke}
            />
          ) : null}
        </div>
        {Object.entries(pending)
          .filter(([key]) => key !== "rquant-notify-test.service")
          .map(([key, item]) => {
            const name =
              key === "mode"
                ? "通知模式"
                : (choices.find((choice) => choice.builtin_id === key)?.label ?? "规则");
            const applied = monitorApplication(item, privateData, controls);
            return (
              <div className="monitor-control-pending" key={key}>
                <span className="hint" role="status">
                  {applied ? (key === "mode" ? "通知模式已应用。" : "规则已应用。") : item.message}
                </span>
                {!settledMonitorRequest(item, privateData, controls) &&
                item.result?.status !== "prepared" ? (
                  <>
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busy || !canRecover}
                      aria-label={`核验${name}原请求`}
                      onClick={() => void send(key, item.body, "lookup")}
                    >
                      核验原请求
                    </Button>
                    {item.result?.can_resume ? (
                      <Button
                        size="sm"
                        variant="ghost"
                        disabled={busy || !canRecover}
                        aria-label={`恢复${name}原请求`}
                        onClick={() => void send(key, item.body, "resume")}
                      >
                        继续核验
                      </Button>
                    ) : null}
                  </>
                ) : null}
              </div>
            );
          })}
        {revoked || capabilities.isError ? (
          <StatusBadge state="warn" label="注意" reason="操作权限暂无法核验，请刷新后重试。" />
        ) : null}
        {storageNotice || !memory.storageAvailable ? (
          <StatusBadge
            state="warn"
            label="注意"
            reason={storageNotice ?? "原请求无法保存，请刷新后重试。"}
          />
        ) : null}
      </Panel>
      <BuiltinRules
        data={privateData}
        loading={loading}
        choices={revoked || !memory.storageAvailable ? [] : choices}
        busyIds={busy ? choices.map((choice) => choice.builtin_id) : []}
        blockedIds={unresolved ? choices.map((choice) => choice.builtin_id) : []}
        onToggle={toggle}
      />
      <ConfirmDialog
        key={`${viewer}:${generationId}:${confirmation?.command_id}`}
        open={dialogOpen}
        level="high"
        title="切换通知模式"
        description={
          draft?.mode === "live"
            ? "将向已配置的通道提交符合条件的提醒。请核对后确认。"
            : "新提醒将只记录，不向通道提交。请核对后确认。"
        }
        confirmName={draft?.mode === "live" ? "正式推送" : "仅记录"}
        expiresAt={
          confirmation?.confirmation_expires_at
            ? new Date(confirmation.confirmation_expires_at)
            : undefined
        }
        confirmLabel="确认切换"
        busy={busy}
        disabled={!canSetMode || draft === null || draft.expected_revision !== mode?.revision}
        onConfirm={() => {
          if (draft) {
            const body = draft;
            setOpen(false);
            setDraft(null);
            void send("mode", body, "submit");
          }
        }}
        onCancel={cancel}
        afterClose={() => restoreTaskControlFocus(trigger.current, region.current)}
      />
    </div>
  );
}
