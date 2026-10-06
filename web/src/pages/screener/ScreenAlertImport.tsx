import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  createScreenAlertDraft,
  fetchScreenAlertDraft,
  type ScreenAlertDraft,
  type ScreenAlertDraftRequest,
  type ScreenExecutionView,
} from "@/api/screen";
import { formatShanghaiDateTime } from "@/format/time";
import { Button, Tip } from "@/ui";
import { screenPoolSaveStorage } from "./screenPoolSaveSession";

const PREFIX = "rquant.screen-alert-draft.v1:";
const HASH = /^[0-9a-f]{64}$/;
const DRAFT_ID = /^[0-9a-f]{24}$/;
interface Intent {
  schema: 1;
  request: ScreenAlertDraftRequest;
  draftId: string | null;
}
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function readIntent(storage: Storage, key: string): Intent | null {
  const raw = storage.getItem(key);
  if (raw === null) return null;
  if (raw.length > 4096) throw new Error("intent exceeds bound");
  const value: unknown = JSON.parse(raw);
  if (
    !record(value) ||
    value.schema !== 1 ||
    !record(value.request) ||
    Object.keys(value).sort().join() !== "draftId,request,schema"
  )
    throw new Error("intent changed");
  const request = value.request;
  if (
    Object.keys(request).sort().join() !== "command_hash,command_id,execution_id" ||
    typeof request.command_id !== "string" ||
    request.command_id.length < 1 ||
    request.command_id.length > 128 ||
    typeof request.execution_id !== "string" ||
    request.execution_id.length < 1 ||
    request.execution_id.length > 128 ||
    typeof request.command_hash !== "string" ||
    !HASH.test(request.command_hash) ||
    !(value.draftId === null || (typeof value.draftId === "string" && DRAFT_ID.test(value.draftId)))
  )
    throw new Error("intent changed");
  return value as unknown as Intent;
}

export function ScreenAlertImport({
  ownerScope,
  execution,
  blockedReason,
  onOpen,
}: {
  ownerScope: string | null;
  execution: ScreenExecutionView | null;
  blockedReason?: string | null;
  onOpen: (draftId: string) => void;
}) {
  const [intent, setIntent] = useState<Intent | null>(null);
  const [draft, setDraft] = useState<ScreenAlertDraft | null>(null);
  const [busy, setBusy] = useState(false);
  const [storageReady, setStorageReady] = useState(true);
  const [message, setMessage] = useState<string | null>(null);
  const active = useRef<{
    epoch: number;
    controller: AbortController | null;
    intent: Intent | null;
    busy: boolean;
  }>({ epoch: 0, controller: null, intent: null, busy: false });
  const key = ownerScope === null ? null : `${PREFIX}${ownerScope}`;
  useEffect(() => {
    const state = active.current;
    state.epoch += 1;
    state.controller?.abort();
    state.controller = null;
    state.busy = false;
    state.intent = null;
    setBusy(false);
    setDraft(null);
    setIntent(null);
    setMessage(null);
    setStorageReady(true);
    if (key !== null) {
      try {
        const saved = readIntent(screenPoolSaveStorage(), key);
        state.intent = saved;
        setIntent(saved);
        if (saved) setMessage("草稿待确认，请恢复原请求。");
      } catch {
        setStorageReady(false);
        setMessage("上次草稿无法核对，请检查浏览器存储。");
      }
    }
    return () => {
      state.epoch += 1;
      state.controller?.abort();
      state.busy = false;
    };
  }, [key]);

  function persist(value: Intent): boolean {
    if (key === null || !storageReady) return false;
    try {
      const storage = screenPoolSaveStorage();
      const raw = JSON.stringify(value);
      storage.setItem(key, raw);
      if (storage.getItem(key) !== raw) throw new Error("intent not durable");
      active.current.intent = value;
      setIntent(value);
      return true;
    } catch {
      setStorageReady(false);
      setMessage("草稿待确认，请检查浏览器存储。");
      return false;
    }
  }

  async function resolve(original: Intent) {
    const state = active.current;
    if (state.busy || ownerScope === null || !storageReady) return;
    const epoch = state.epoch;
    const controller = new AbortController();
    state.controller = controller;
    state.busy = true;
    setBusy(true);
    setMessage(null);
    try {
      const data =
        original.draftId === null
          ? await createScreenAlertDraft(original.request, controller.signal)
          : await fetchScreenAlertDraft(original.draftId, controller.signal);
      if (controller.signal.aborted || epoch !== state.epoch) return;
      const confirmed = data.alert_draft;
      if (
        !data.available ||
        data.owner_scope_tag !== ownerScope ||
        !confirmed ||
        !DRAFT_ID.test(confirmed.draft_id) ||
        (original.draftId !== null && confirmed.draft_id !== original.draftId) ||
        confirmed.origin.execution_id !== original.request.execution_id ||
        confirmed.origin.command_hash !== original.request.command_hash ||
        confirmed.origin.draft_id !== confirmed.draft_id ||
        confirmed.capabilities.consumer_state !== "awaiting_consumer" ||
        !HASH.test(confirmed.content_hash ?? "") ||
        !Number.isFinite(Date.parse(confirmed.expires_at)) ||
        Date.parse(confirmed.expires_at) <= Date.now()
      )
        throw new Error("draft reply unconfirmed");
      if (!persist({ ...original, draftId: confirmed.draft_id })) return;
      setDraft(confirmed);
      setMessage("提醒草稿已生成，尚未生效。");
      onOpen(confirmed.draft_id);
    } catch (caught) {
      if (controller.signal.aborted || epoch !== state.epoch) return;
      if (original.draftId !== null && caught instanceof ApiError && caught.status === 404) {
        try {
          screenPoolSaveStorage().removeItem(key ?? "");
          active.current.intent = null;
          setIntent(null);
          setDraft(null);
          setMessage("草稿已过期或不可用，请重新带入条件。");
        } catch {
          setStorageReady(false);
          setMessage("草稿待确认，请检查浏览器存储。");
        }
      } else setMessage("草稿待确认，请恢复原请求。");
    } finally {
      if (epoch === state.epoch) {
        state.busy = false;
        state.controller = null;
        setBusy(false);
      }
    }
  }

  const executionReady =
    execution?.status === "succeeded" &&
    HASH.test(execution.command_hash) &&
    HASH.test(execution.artifact_sha256 ?? "") &&
    HASH.test(execution.member_rank_sha256 ?? "") &&
    execution.total != null &&
    execution.base_count != null &&
    execution.unknown_count != null &&
    execution.completed_at != null;
  const pending = intent !== null && draft === null;
  const reason = !storageReady
    ? "浏览器存储不可用，请先核对原请求。"
    : ownerScope === null
      ? "本人选股记录暂不可用。"
      : pending
        ? "先恢复原草稿请求。"
        : (blockedReason ?? (!executionReady ? "先确认完整筛选结果。" : undefined));
  function create() {
    if (reason || execution === null || active.current.busy) return;
    const original: Intent = {
      schema: 1,
      draftId: null,
      request: {
        command_id: `screen-alert-${crypto.randomUUID()}`,
        execution_id: execution.execution_id,
        command_hash: execution.command_hash,
      },
    };
    if (persist(original)) void resolve(original);
  }
  return (
    <div className="screen-query-recovery">
      <div className="screen-query-actions">
        <Tip content="带入完整选股条件与排名；确认保存后才会提醒。">
          <Button disabled={busy} disabledReason={reason ?? undefined} onClick={create}>
            {busy ? "正在核对…" : "设置提醒"}
          </Button>
        </Tip>
        {pending ? (
          <Button
            disabled={busy || !storageReady}
            onClick={() => {
              if (active.current.intent) void resolve(active.current.intent);
            }}
          >
            恢复原请求
          </Button>
        ) : null}
        {draft ? <Button onClick={() => onOpen(draft.draft_id)}>继续设置</Button> : null}
      </div>
      {message ? <p role="status">{message}</p> : null}
      {draft ? (
        <Tip content={`${formatShanghaiDateTime(draft.expires_at)} 到期，仅本人可见。`}>
          <span>{draft.conditions.length} 条条件已带入</span>
        </Tip>
      ) : null}
    </div>
  );
}
