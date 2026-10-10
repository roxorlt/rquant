import { useEffect, useRef, useState, useSyncExternalStore } from "react";
import {
  recoverTaskControl,
  type SchedulingRequest,
  type SchedulingView,
  submitTaskControl,
} from "@/api/taskControls";
import { Button, ConfirmDialog, Tip } from "@/ui";
import {
  restoreTaskControlFocus,
  type TaskControlMemory,
  type TaskPending,
  taskRequestError,
} from "./taskControlRecovery";

export function LabSchedulingControls({
  state,
  viewer,
  generationId,
  canControl,
  canRecover,
  memory,
  onRefresh,
  onRevoked,
}: {
  state: SchedulingView;
  viewer: string;
  generationId: string;
  canControl: boolean;
  canRecover: boolean;
  memory: TaskControlMemory;
  onRefresh: () => void;
  onRevoked: () => void;
}) {
  const [pending, setPending] = useState<TaskPending | null>(() =>
    memory.get(viewer, "scheduling"),
  );
  const epoch = useSyncExternalStore(memory.subscribe, memory.snapshot);
  const [confirm, setConfirm] = useState<boolean | null>(null);
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
    token === operation.current &&
    owner === identity.current.viewer &&
    generation === identity.current.generationId;
  useEffect(
    () => () => {
      operation.current += 1;
      controller.current?.abort();
    },
    [],
  );
  useEffect(() => {
    if (wasOpen.current && confirm === null)
      restoreTaskControlFocus(trigger.current, region.current);
    wasOpen.current = confirm !== null;
  }, [confirm]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: a new source generation or revoked permission epoch must abort private operations.
  useEffect(() => {
    operation.current += 1;
    controller.current?.abort();
    setPending(memory.get(viewer, "scheduling"));
    setConfirm(null);
    setBusy(false);
    setNotice(null);
  }, [viewer, generationId, memory, epoch]);
  function keep(value: TaskPending | null) {
    memory.put(viewer, "scheduling", value);
    setPending(value);
  }

  const body = pending?.body.kind === "set_lab_scheduling_paused" ? pending.body : null;
  const applied =
    body !== null &&
    state.available &&
    pending?.result?.desired_version != null &&
    pending.result.status !== "rejected" &&
    state.applied_version === pending.result.desired_version &&
    state.applied_paused === body.paused;
  const refused = pending?.refused === true || pending?.result?.status === "rejected";
  const unresolved = pending !== null && !applied && !refused;

  async function send(request: SchedulingRequest, mode: "submit" | "lookup" | "resume") {
    const token = ++operation.current,
      owner = viewer,
      generation = generationId;
    controller.current?.abort();
    const abort = new AbortController();
    controller.current = abort;
    const record: TaskPending = {
      body: request,
      result: pending?.body.command_id === request.command_id ? pending.result : null,
      message: "调度结果待确认，请核验原请求。",
    };
    keep(record);
    setBusy(true);
    setNotice(null);
    try {
      const result =
        mode === "submit"
          ? await submitTaskControl(request, abort.signal)
          : await recoverTaskControl(request, mode, abort.signal);
      if (!current(token, owner, generation)) return;
      keep({ body: request, result, message: result.message });
      onRefresh();
    } catch (error) {
      if (!current(token, owner, generation)) return;
      const outcome = taskRequestError(error);
      if (outcome === "revoked") {
        keep(null);
        setConfirm(null);
        onRevoked();
      } else if (outcome === "refused") {
        keep({ ...record, refused: true, message: "请求已拒绝，请刷新调度状态。" });
        onRefresh();
      } else keep(record);
    } finally {
      if (current(token, owner, generation)) setBusy(false);
    }
  }

  function start(paused: boolean) {
    setConfirm(null);
    if (!canControl || state.desired_version == null || unresolved || busy) return;
    const request: SchedulingRequest = {
      kind: "set_lab_scheduling_paused",
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      generation_id: generationId,
      expected_version: state.desired_version,
      paused,
    };
    void send(request, "submit");
  }
  return (
    <div className="tasks-scheduling-controls" ref={region}>
      <div className="tasks-scheduling-state">
        <strong>{state.note}</strong>
        <Tip
          content="暂停新研究分片。当前分片继续收尾，单项暂停保持。状态应用后会显示结果。"
          interactive
        >
          <Button size="sm" variant="ghost" aria-label="全局调度说明">
            ?
          </Button>
        </Tip>
        {state.draining_count != null && state.draining_count > 0 ? (
          <span className="hint">还有 {state.draining_count} 项在收尾</span>
        ) : null}
      </div>
      {canControl || pending !== null ? (
        <div className="tasks-unit-actions">
          <Button
            size="sm"
            variant="ghost"
            disabled={
              !canControl || !state.available || busy || unresolved || state.desired_paused === true
            }
            onClick={(event) => {
              trigger.current = event.currentTarget;
              setConfirm(true);
            }}
          >
            暂停研究调度
          </Button>
          <Button
            size="sm"
            variant="ghost"
            disabled={
              !canControl || !state.available || busy || unresolved || state.desired_paused !== true
            }
            onClick={(event) => {
              trigger.current = event.currentTarget;
              setConfirm(false);
            }}
          >
            恢复研究调度
          </Button>
        </div>
      ) : null}
      {notice ? (
        <span className="hint" role="status">
          {notice}
        </span>
      ) : null}
      {pending && !applied ? (
        <div className="tasks-control-pending">
          <span className="hint" role="status">
            {pending.message}
          </span>
          {body && unresolved && canRecover ? (
            <>
              <Button
                size="sm"
                variant="ghost"
                disabled={busy}
                onClick={() => void send(body, "lookup")}
              >
                核验调度原请求
              </Button>
              {pending.result?.can_resume ? (
                <Button
                  size="sm"
                  variant="ghost"
                  disabled={busy}
                  onClick={() => void send(body, "resume")}
                >
                  继续核验调度
                </Button>
              ) : null}
            </>
          ) : null}
        </div>
      ) : null}
      <ConfirmDialog
        open={confirm !== null}
        level="heavy"
        title={confirm === true ? "暂停研究调度" : "恢复研究调度"}
        description={
          confirm === true
            ? "暂停新分片。当前分片和数据请求继续收尾。"
            : "恢复新分片。单项任务的暂停保持。"
        }
        confirmLabel={confirm === true ? "确认暂停" : "确认恢复"}
        busy={busy}
        disabled={!canControl || unresolved}
        onConfirm={() => {
          if (confirm !== null) start(confirm);
        }}
        onCancel={() => setConfirm(null)}
        afterClose={() => restoreTaskControlFocus(trigger.current, region.current)}
      />
    </div>
  );
}
