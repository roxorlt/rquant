import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import {
  recoverTaskControl,
  submitTaskControl,
  type TaskControlRequest,
  type TaskControlResult,
  type UnitControlChoice,
  type UnitPrepareRequest,
  type UnitRunRequest,
} from "@/api/taskControls";
import { Button, ConfirmDialog, Tip } from "@/ui";
import {
  restoreTaskControlFocus,
  settledTask,
  type TaskControlMemory,
  type TaskPending,
  taskRequestError,
} from "./taskControlRecovery";

export function TaskUnitControls({
  unit,
  name,
  viewer,
  generationId,
  choice,
  canRecover,
  memory,
  onRefresh,
  onRevoked,
}: {
  unit: string;
  name: string;
  viewer: string;
  generationId: string;
  choice: UnitControlChoice | undefined;
  canRecover: boolean;
  memory: TaskControlMemory;
  onRefresh: () => void;
  onRevoked: () => void;
}) {
  const [pending, setPending] = useState<TaskPending | null>(() => memory.get(viewer, unit));
  const epoch = useSyncExternalStore(memory.subscribe, memory.snapshot);
  const [draft, setDraft] = useState<UnitRunRequest | null>(null);
  const [confirmation, setConfirmation] = useState<TaskControlResult | null>(null);
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const region = useRef<HTMLDivElement | null>(null);
  const trigger = useRef<HTMLButtonElement | null>(null);
  const wasOpen = useRef(false);
  const operation = useRef(0);
  const controller = useRef<AbortController | null>(null);
  const identity = useRef({ viewer, generationId });
  identity.current = { viewer, generationId };
  const current = (token: number, owner: string, generation: string) =>
    operation.current === token &&
    identity.current.viewer === owner &&
    identity.current.generationId === generation;

  useEffect(() => {
    return () => {
      operation.current += 1;
      controller.current?.abort();
    };
  }, []);
  useEffect(() => {
    if (wasOpen.current && !open) restoreTaskControlFocus(trigger.current, region.current);
    wasOpen.current = open;
  }, [open]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: a new source generation or revoked permission epoch must abort private operations.
  useEffect(() => {
    operation.current += 1;
    controller.current?.abort();
    setPending(memory.get(viewer, unit));
    setDraft(null);
    setConfirmation(null);
    setOpen(false);
    setBusy(false);
    setNotice(null);
  }, [viewer, generationId, memory, unit, epoch]);

  function keep(value: TaskPending | null) {
    const record = value === null ? null : { ...value, unitName: name };
    memory.put(viewer, unit, record);
    setPending(record);
  }

  async function send(
    body: TaskControlRequest,
    mode: "submit" | "lookup" | "resume",
  ): Promise<void> {
    const token = ++operation.current;
    const owner = viewer,
      generation = generationId;
    controller.current?.abort();
    const request = new AbortController();
    controller.current = request;
    const record: TaskPending = {
      body,
      result: pending?.body.command_id === body.command_id ? pending.result : null,
      message: "运行结果待确认，请核验原请求。",
    };
    keep(record);
    setBusy(true);
    setNotice(null);
    try {
      const result =
        mode === "submit"
          ? await submitTaskControl(body, request.signal)
          : await recoverTaskControl(body, mode, request.signal);
      if (!current(token, owner, generation)) return;
      keep({ body, result, message: result.message });
      if (
        body.kind === "prepare_unit_run" &&
        result.status === "prepared" &&
        result.confirmation_id &&
        result.confirmation_expires_at &&
        body.generation_id === generation
      ) {
        setDraft({
          ...body.run,
          kind: "request_unit_run",
          confirmation_id: result.confirmation_id,
        });
        setConfirmation(result);
        setOpen(true);
      }
      if (
        body.kind === "request_unit_run" &&
        result.status !== "unknown" &&
        result.status !== "not_found"
      )
        onRefresh();
    } catch (error) {
      if (!current(token, owner, generation)) return;
      const outcome = taskRequestError(error);
      if (outcome === "revoked") {
        keep(null);
        setOpen(false);
        onRevoked();
      } else if (outcome === "refused") {
        keep({ ...record, refused: true, message: "请求已拒绝，请刷新状态后再操作。" });
        setOpen(false);
        onRefresh();
      } else keep({ ...record, message: "运行结果待确认，请核验原请求。" });
    } finally {
      if (current(token, owner, generation)) setBusy(false);
    }
  }

  function start(element: HTMLButtonElement) {
    if (!choice?.can_request || busy || (pending && !settledTask(pending))) return;
    trigger.current = element;
    const body: UnitRunRequest = {
      kind: "request_unit_run",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generationId,
      unit,
      confirmation_id: null,
    };
    setDraft(body);
    setNotice(null);
    if (choice.requires_confirmation) {
      const prepare: UnitPrepareRequest = {
        kind: "prepare_unit_run",
        command_id: crypto.randomUUID(),
        requested_at: new Date().toISOString(),
        generation_id: generationId,
        run: {
          command_id: body.command_id,
          requested_at: body.requested_at,
          generation_id: generationId,
          unit,
        },
      };
      void send(prepare, "submit");
    } else {
      setConfirmation(null);
      setOpen(true);
    }
  }

  function close() {
    setOpen(false);
    setDraft(null);
    setConfirmation(null);
    if (pending?.body.kind === "prepare_unit_run" && pending.result?.status === "prepared")
      keep(null);
  }
  const unresolved = pending !== null && !settledTask(pending);
  if (choice === undefined && pending === null && notice === null) return null;
  return (
    <div className="tasks-unit-controls" ref={region}>
      <div className="tasks-unit-actions">
        <Button
          size="sm"
          variant="ghost"
          aria-label={`立即运行${name}`}
          disabled={!choice?.can_request || busy || unresolved}
          onClick={(event) => start(event.currentTarget)}
        >
          立即运行
        </Button>
        <Tip content={choice?.reason ?? "任务操作尚未开放。"} interactive>
          <Button size="sm" variant="ghost" aria-label={`${name}运行说明`}>
            ?
          </Button>
        </Tip>
      </div>
      {notice ? (
        <span className="hint" role="status">
          {notice}
        </span>
      ) : null}
      {pending ? (
        <div className="tasks-control-pending">
          <span className="hint" role="status">
            {pending.message}
          </span>
          {unresolved && canRecover ? (
            <>
              <Button
                size="sm"
                variant="ghost"
                disabled={busy}
                aria-label={`核验${name}原请求`}
                onClick={() => void send(pending.body, "lookup")}
              >
                核验原请求
              </Button>
              {pending.result?.can_resume ? (
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy}
                  aria-label={`恢复${name}原请求`}
                  onClick={() => void send(pending.body, "resume")}
                >
                  继续核验
                </Button>
              ) : null}
            </>
          ) : null}
        </div>
      ) : null}
      <ConfirmDialog
        open={open}
        level={confirmation === null ? "heavy" : "high"}
        title={`运行${name}`}
        description={
          confirmation === null
            ? "将请求运行一次。任务开始和完成后会显示实际结果。"
            : "本次任务会写入数据。请核对后确认。"
        }
        confirmName={name}
        expiresAt={
          confirmation?.confirmation_expires_at
            ? new Date(confirmation.confirmation_expires_at)
            : undefined
        }
        confirmLabel="确认运行"
        busy={busy}
        disabled={draft === null || !choice?.can_request}
        onConfirm={() => {
          if (draft) {
            const body = draft;
            setOpen(false);
            setDraft(null);
            void send(body, "submit");
          }
        }}
        onCancel={close}
        afterClose={() => restoreTaskControlFocus(trigger.current, region.current)}
      />
    </div>
  );
}
