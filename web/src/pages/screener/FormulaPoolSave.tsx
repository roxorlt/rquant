import { useState } from "react";
import { ApiError } from "@/api/client";
import {
  type FormulaPoolSaveRequest,
  submitFormulaPoolSave,
  useFormulaPools,
} from "@/api/formulaPools";
import { useCurrentGeneration, useCurrentMeta } from "@/api/useMeta";
import { formatCount } from "@/format/number";
import { Button } from "@/ui";

const LEGACY_JOURNAL_KEY = "rquant-formula-pool-save-v1";
const JOURNAL_KEY_PREFIX = "rquant-formula-pool-save-v2:";
const SAFE_NAME = /^[\w\u4e00-\u9fff-]+$/u;

type Journal = {
  viewer: string;
  request: FormulaPoolSaveRequest;
  status: "pending" | "succeeded";
  poolName: string | null;
  version: string | null;
};

function parseJournal(raw: string | null): Journal | null {
  try {
    if (!raw) return null;
    const value: unknown = JSON.parse(raw);
    if (typeof value !== "object" || value === null || !("request" in value)) return null;
    const request = value.request;
    if (typeof request !== "object" || request === null) return null;
    if (
      !("viewer" in value) ||
      typeof value.viewer !== "string" ||
      !("status" in value) ||
      (value.status !== "pending" && value.status !== "succeeded") ||
      !("poolName" in value) ||
      (value.poolName !== null && typeof value.poolName !== "string") ||
      !("version" in value) ||
      (value.version !== null && typeof value.version !== "string") ||
      !("base_name" in request) ||
      typeof request.base_name !== "string" ||
      !("display_name" in request) ||
      typeof request.display_name !== "string" ||
      !("task_id" in request) ||
      typeof request.task_id !== "string" ||
      !("command_id" in request) ||
      typeof request.command_id !== "string" ||
      !("requested_at" in request) ||
      typeof request.requested_at !== "string" ||
      !("expected_version" in request) ||
      request.expected_version !== null
    )
      return null;
    return {
      viewer: value.viewer,
      status: value.status,
      poolName: value.poolName,
      version: value.version,
      request: {
        base_name: request.base_name,
        display_name: request.display_name,
        task_id: request.task_id,
        command_id: request.command_id,
        requested_at: request.requested_at,
        expected_version: null,
      },
    };
  } catch {
    return null;
  }
}

function journalKey(viewer: string): string {
  return `${JOURNAL_KEY_PREFIX}${encodeURIComponent(viewer)}`;
}

function readJournal(viewer: string | null): Journal | null {
  if (viewer === null) return null;
  try {
    const current = parseJournal(window.localStorage.getItem(journalKey(viewer)));
    if (current?.viewer === viewer) return current;
    const legacy = parseJournal(window.localStorage.getItem(LEGACY_JOURNAL_KEY));
    return legacy?.viewer === viewer ? legacy : null;
  } catch {
    return null;
  }
}

function writeJournal(viewer: string, journal: Journal | null): void {
  try {
    if (journal) window.localStorage.setItem(journalKey(viewer), JSON.stringify(journal));
    else window.localStorage.removeItem(journalKey(viewer));
    if (parseJournal(window.localStorage.getItem(LEGACY_JOURNAL_KEY))?.viewer === viewer) {
      window.localStorage.removeItem(LEGACY_JOURNAL_KEY);
    }
  } catch {
    // The open page retains the request when browser storage is disabled.
  }
}

export function readFormulaPoolSaveTaskId(viewer: string | null): string | null {
  return readJournal(viewer)?.request.task_id ?? null;
}

type SaveCandidate = {
  taskId: string;
  formula: string;
  tradeDate: string;
  matchCount: number;
  unknownCount: number;
};

export function FormulaPoolSave({ candidate }: { candidate: SaveCandidate | null }) {
  const meta = useCurrentMeta();
  const generation = useCurrentGeneration();
  const viewer = meta.data?.data.viewer ?? null;
  const pools = useFormulaPools();
  const [name, setName] = useState("");
  const [journal, setJournal] = useState<Journal | null>(() => readJournal(viewer));
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const active = viewer !== null && journal?.viewer === viewer ? journal : null;
  const pending = active?.status === "pending";
  const saved = active?.status === "succeeded";
  const validName =
    name.trim().length >= 1 && name.trim().length <= 80 && SAFE_NAME.test(name.trim());
  const visible =
    saved &&
    active.poolName !== null &&
    active.version !== null &&
    generation !== undefined &&
    pools.serving?.generation_id === generation &&
    pools.data?.availability === "ready" &&
    pools.data.pools.some(
      (pool) => pool.pool_name === active.poolName && pool.version === active.version,
    );
  const readBlocked =
    pools.error !== null ||
    (pools.data !== undefined &&
      (pools.data.availability === "unavailable" ||
        pools.data.availability === "not_published" ||
        pools.serving?.generation_id !== generation));

  function remember(value: Journal | null): void {
    setJournal(value);
    const owner = value?.viewer ?? viewer;
    if (owner !== null) writeJournal(owner, value);
  }

  async function submit(value: Journal): Promise<void> {
    setBusy(true);
    setNotice(null);
    remember(value);
    try {
      const receipt = await submitFormulaPoolSave(value.request);
      if (
        receipt.status === "succeeded" &&
        receipt.command_id === value.request.command_id &&
        receipt.pool_name === `user/${value.request.base_name}` &&
        typeof receipt.version === "string" &&
        /^[0-9a-f]{64}$/.test(receipt.version)
      ) {
        remember({
          ...value,
          status: "succeeded",
          poolName: receipt.pool_name,
          version: receipt.version,
        });
        pools.refetch();
      } else if (receipt.status === "succeeded") {
        setNotice("保存回执暂不完整，请用原请求核对。");
      } else if (!["pending", "processing", "ambiguous"].includes(receipt.status)) {
        remember(null);
        setName(value.request.display_name);
        setNotice(
          receipt.status === "conflict" ? "这个名称已被使用，请换一个名称。" : receipt.message,
        );
      }
    } catch (caught) {
      if (
        caught instanceof ApiError &&
        caught.status >= 400 &&
        caught.status < 500 &&
        caught.status !== 408
      ) {
        remember(null);
        setName(value.request.display_name);
        setNotice(caught.status === 409 ? "这个名称已被使用，请换一个名称。" : caught.message);
      }
    } finally {
      setBusy(false);
    }
  }

  function save(): void {
    if (candidate === null || !viewer || !validName || active || busy) return;
    const trimmed = name.trim();
    void submit({
      viewer,
      request: {
        base_name: trimmed,
        display_name: trimmed,
        task_id: candidate.taskId,
        expected_version: null,
        command_id: crypto.randomUUID(),
        requested_at: new Date().toISOString(),
      },
      status: "pending",
      poolName: null,
      version: null,
    });
  }

  if (active === null && candidate === null && notice === null) return null;

  return (
    <section className="formula-pool-save" aria-label="保存公式池">
      <div className="formula-market-recent-heading">
        <h4>保存为池子</h4>
        {active === null && candidate !== null ? (
          <span className="formula-pool-save-date num">{candidate.tradeDate}</span>
        ) : null}
      </div>
      {active ? (
        <div className="formula-pool-save-state" role="status">
          <strong>
            {pending
              ? "保存状态待确认"
              : visible
                ? "已保存，可在池子画布查看"
                : readBlocked
                  ? "已保存，暂无法确认池子"
                  : "已保存，等待池子发布"}
          </strong>
          <span>{active.request.display_name}</span>
          {pending ? (
            <Button size="sm" disabled={busy} onClick={() => void submit(active)}>
              {busy ? "正在核对…" : "继续核对"}
            </Button>
          ) : !visible ? (
            <Button size="sm" onClick={() => pools.refetch()} disabled={pools.isFetching}>
              {readBlocked ? "重试读取" : "检查发布"}
            </Button>
          ) : null}
          {saved ? (
            <Button
              size="sm"
              variant="ghost"
              onClick={() => {
                remember(null);
                setName("");
                setNotice(null);
              }}
            >
              保存另一个
            </Button>
          ) : null}
        </div>
      ) : candidate !== null ? (
        <>
          <div className="formula-pool-save-proof">
            <code className="mono">{candidate.formula}</code>
            <span>
              命中 <b className="num">{formatCount(candidate.matchCount)}</b> · 未能判断{" "}
              <b className="num">{formatCount(candidate.unknownCount)}</b>
            </span>
          </div>
          <div className="formula-pool-save-compose">
            <label className="field">
              <span className="lbl">池子名称</span>
              <input
                className="inp"
                value={name}
                maxLength={80}
                onChange={(event) => setName(event.target.value)}
                autoComplete="off"
              />
            </label>
            <Button
              variant="primary"
              disabledReason={
                !viewer
                  ? "请先登录"
                  : !validName
                    ? "请输入 1–80 个汉字、字母、数字、横线或下划线"
                    : undefined
              }
              onClick={save}
            >
              保存为池子
            </Button>
          </div>
        </>
      ) : null}
      {notice ? (
        <>
          <p className="formula-market-notice" role="status">
            {notice}
          </p>
          {active === null && candidate === null ? (
            <p className="hint">刷新最近运行，结果可读后可重新保存。</p>
          ) : null}
        </>
      ) : null}
    </section>
  );
}
