import { useEffect, useState } from "react";
import { ApiError } from "@/api/client";
import {
  type LabControlReceipt,
  type LabControlRequest,
  type ResearchJobItem,
  submitLabControl,
} from "@/api/endpoints";
import { Button, ConfirmDialog } from "@/ui";

type Action = LabControlRequest["action"];
type Pending = {
  body: LabControlRequest;
  status: LabControlReceipt["status"];
  message: string;
};

const JOURNAL = "rquant.lab-job-control.v1";
const CHANGE = "rquant:lab-job-control";
const LABEL: Record<Action, string> = {
  pause: "暂停",
  resume: "恢复",
  cancel: "取消",
  retry: "重试",
};
const ACTIONS: Action[] = ["pause", "resume", "cancel", "retry"];
const UNKNOWN = "提交状态待确认，请查询或重试原请求。";
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

function key(viewer: string, jobId: string): string {
  return `${JOURNAL}:${viewer}:${jobId}`;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function readPending(storageKey: string, jobId: string): Pending | null {
  try {
    const raw = window.localStorage.getItem(storageKey);
    if (raw === null || raw.length > 2048) return null;
    const parsed: unknown = JSON.parse(raw);
    if (!isObject(parsed) || !isObject(parsed.body)) return null;
    const body = parsed.body;
    if (
      body.job_id !== jobId ||
      typeof body.command_id !== "string" ||
      !UUID.test(body.command_id) ||
      typeof body.requested_at !== "string" ||
      !Number.isFinite(Date.parse(body.requested_at)) ||
      !ACTIONS.includes(body.action as Action) ||
      !Number.isInteger(body.expected_version) ||
      Number(body.expected_version) < 0 ||
      typeof parsed.status !== "string" ||
      !["submitted", "pending", "processing", "unknown", "conflict", "failed"].includes(
        parsed.status,
      ) ||
      typeof parsed.message !== "string" ||
      parsed.message.length > 80
    ) {
      return null;
    }
    return parsed as Pending;
  } catch {
    return null;
  }
}

function savePending(storageKey: string, jobId: string, value: Pending | null): boolean {
  try {
    if (value === null) window.localStorage.removeItem(storageKey);
    else window.localStorage.setItem(storageKey, JSON.stringify(value));
    window.dispatchEvent(new Event(CHANGE));
    return (
      value === null || readPending(storageKey, jobId)?.body.command_id === value.body.command_id
    );
  } catch {
    return false;
  }
}

export function LabJobControls({
  row,
  viewer,
  onRefresh,
  onFailedRefresh,
  rearmReadyForCommand,
  onRevoked,
}: {
  row: ResearchJobItem;
  viewer: string;
  onRefresh: () => void;
  onFailedRefresh: (commandId: string) => void;
  rearmReadyForCommand: string | null;
  onRevoked: () => void;
}) {
  const storageKey = key(viewer, row.job_id);
  const [pending, setPending] = useState<Pending | null>(() => readPending(storageKey, row.job_id));
  const [confirm, setConfirm] = useState<Action | null>(null);
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    const sync = () => setPending(readPending(storageKey, row.job_id));
    sync();
    window.addEventListener("storage", sync);
    window.addEventListener(CHANGE, sync);
    return () => {
      window.removeEventListener("storage", sync);
      window.removeEventListener(CHANGE, sync);
    };
  }, [storageKey, row.job_id]);

  async function send(record: Pending): Promise<void> {
    setBusy(true);
    try {
      const receipt = await submitLabControl(record.body);
      const next = { ...record, status: receipt.status, message: receipt.message };
      if (savePending(storageKey, row.job_id, next)) setPending(next);
      else setNotice("无法保存操作状态，请勿重复发起，请稍后刷新。");
      if (receipt.status === "conflict") onRefresh();
    } catch (error) {
      const next = { ...record, status: "unknown" as const, message: UNKNOWN };
      savePending(storageKey, row.job_id, next);
      setPending(next);
      if (error instanceof ApiError && [401, 403].includes(error.status)) onRevoked();
    } finally {
      setBusy(false);
    }
  }

  async function start(action: Action): Promise<void> {
    setConfirm(null);
    if (
      busy ||
      pending !== null ||
      row.job_version == null ||
      !row.available_actions?.includes(action)
    ) {
      return;
    }
    const saved = readPending(storageKey, row.job_id);
    if (saved !== null) {
      setPending(saved);
      return;
    }
    const body: LabControlRequest = {
      command_id: crypto.randomUUID(),
      requested_at: new Date().toISOString(),
      job_id: row.job_id,
      action,
      expected_version: row.job_version,
    };
    const record: Pending = { body, status: "pending", message: UNKNOWN };
    if (!savePending(storageKey, row.job_id, record)) {
      setNotice("无法保存操作请求，请稍后重试。");
      return;
    }
    setPending(record);
    setNotice(null);
    await send(record);
  }

  const changed = pending !== null && row.job_version !== pending.body.expected_version;
  const failedReady =
    pending?.status === "failed" && pending.body.command_id === rearmReadyForCommand;
  return (
    <div className="tasks-job-controls">
      {pending ? (
        <div className="tasks-control-pending">
          <span role="status">
            {pending.status === "failed"
              ? failedReady
                ? "任务状态已刷新，请核对后再操作。"
                : pending.message
              : changed
                ? "任务状态已更新，请核对结果。"
                : pending.message}
          </span>
          {pending.status === "failed" && !failedReady ? (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => onFailedRefresh(pending.body.command_id)}
            >
              刷新任务
            </Button>
          ) : null}
          {pending.status !== "conflict" && pending.status !== "failed" ? (
            <Button
              size="sm"
              variant="ghost"
              disabledReason={busy ? "正在核对这次请求" : undefined}
              aria-label={`查询或重试${row.strategy_name}`}
              onClick={() => void send(pending)}
            >
              查询 / 重试
            </Button>
          ) : null}
          {(pending.status === "failed" && failedReady) ||
          (pending.status !== "failed" && (changed || pending.status === "conflict")) ? (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => {
                if (savePending(storageKey, row.job_id, null)) setPending(null);
              }}
            >
              已核对
            </Button>
          ) : null}
        </div>
      ) : (
        <fieldset className="tasks-control-actions">
          <legend className="sr-only">{row.strategy_name}操作</legend>
          {ACTIONS.filter((action) => row.available_actions?.includes(action)).map((action) => (
            <Button
              key={action}
              size="sm"
              variant="ghost"
              disabledReason={busy ? "正在提交这次请求" : undefined}
              aria-label={`${LABEL[action]}${row.strategy_name}`}
              onClick={() =>
                action === "cancel" || action === "retry" ? setConfirm(action) : void start(action)
              }
            >
              {LABEL[action]}
            </Button>
          ))}
        </fieldset>
      )}
      {notice ? <span role="alert">{notice}</span> : null}
      <ConfirmDialog
        open={confirm !== null}
        level="heavy"
        title={confirm === "cancel" ? "确认取消研究任务" : "确认重试研究任务"}
        description={
          confirm === "cancel"
            ? "取消后无法继续当前任务。"
            : "将重新尝试失败任务，可能再次占用研究资源。"
        }
        confirmLabel={confirm === "cancel" ? "确认取消" : "确认重试"}
        busy={busy}
        onConfirm={() => {
          if (confirm !== null) void start(confirm);
        }}
        onCancel={() => setConfirm(null)}
      />
    </div>
  );
}
