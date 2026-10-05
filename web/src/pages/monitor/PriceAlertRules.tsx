import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import {
  type PriceRuleCommand,
  type PriceRuleItem,
  postPriceRule,
  usePriceRules,
} from "@/api/priceAlertRules";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  PageSkeleton,
  Panel,
  SideDrawer,
  Switch,
  Tip,
} from "@/ui";
import {
  PriceAlertRuntimeFacts,
  PriceAlertRuntimeStatus,
  priceText,
  usePriceAlertRuntimeFacts,
} from "./PriceAlertRuntimeFacts";
import {
  type EditingIdentity,
  matchesEditingSession,
  PRICE_RULE_EVENT,
  PriceAlertRuleCommandSession,
  type PriceRuleEntry,
  type PriceRuleLock,
  unresolved,
} from "./priceAlertRuleCommandSession";
import "./priceAlertRules.css";

interface Draft {
  identity: EditingIdentity;
  generation: string;
  binding: string;
  initial: string;
  fields: NonNullable<PriceRuleCommand["rule"]>;
}
const STATUS_TEXT: Record<PriceRuleEntry["status"], string> = {
  unknown: "状态待核对",
  uncertain: "状态待核对",
  not_found: "暂未查到原操作",
  pending: "等待处理",
  processing: "正在处理",
  saved_syncing: "设置已写入，等待同步",
  published: "已保存",
  superseded: "规则已更新",
  conflict: "规则已更新，请刷新",
  capacity: "规则已满",
  scope_invalid: "盯盘已更新，请重新选择股票",
  failed: "操作未完成",
  rejected: "操作未受理",
};
function operationText(entry: PriceRuleEntry): string {
  if (entry.status !== "published") return STATUS_TEXT[entry.status];
  return entry.body.action === "delete"
    ? "已删除"
    : entry.body.action === "set_enabled"
      ? entry.body.enabled
        ? "已启用"
        : "已停用"
      : "已保存";
}
function uniqueId(): string {
  return `web-${crypto.randomUUID()}`;
}
function storage(): Storage | null {
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}
function browserLock(): PriceRuleLock | null {
  if (navigator.locks === undefined) return null;
  return async <T,>(name: string, action: () => Promise<T>): Promise<T> =>
    await navigator.locks.request(name, { mode: "exclusive" }, action);
}
function draftKey(binding: string, fields: Draft["fields"]): string {
  return JSON.stringify([binding, fields]);
}
function wallTimeKey(value: string): string | null {
  const match = /^(?:([01]\d|2[0-3])):([0-5]\d)(?::([0-5]\d)(?:\.(\d{1,6}))?)?$/.exec(value);
  return match
    ? `${match[1]}${match[2]}${match[3] ?? "00"}${(match[4] ?? "").padEnd(6, "0")}`
    : null;
}

export function PriceAlertRules() {
  const result = usePriceRules();
  const runtimeFacts = usePriceAlertRuntimeFacts(result.owner, result.generation);
  const session = useMemo(
    () => new PriceAlertRuleCommandSession(storage(), result.owner, postPriceRule, browserLock()),
    [result.owner],
  );
  const commands = useSyncExternalStore(session.subscribe, session.snapshot, session.snapshot);
  const [draft, setDraft] = useState<Draft | null>(null);
  const draftRef = useRef<Draft | null>(draft);
  draftRef.current = draft;
  const [sending, setSending] = useState(false);
  const [discard, setDiscard] = useState(false);
  const discardClose = useRef<Draft | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<PriceRuleItem | null>(null);
  const [error, setError] = useState<string | null>(null);
  const editTargets = useRef(new Map<string, HTMLDivElement>());
  const ownerRef = useRef(result.owner);
  ownerRef.current = result.owner;
  const createRef = useRef<HTMLSpanElement>(null);
  const [, setTimePulse] = useState(0);
  const anchor = useRef({ server: 0, local: 0, value: "" });
  if (result.serverTime !== null && result.serverTime !== anchor.current.value) {
    anchor.current = {
      server: Date.parse(result.serverTime),
      local: performance.now(),
      value: result.serverTime,
    };
  }
  const now = anchor.current.server + performance.now() - anchor.current.local;
  const ready = result.data?.availability === "ready";
  const members = (result.data?.members ?? []).filter(
    (member) => member.expires_at === null || Date.parse(member.expires_at) > now,
  );
  const rows = (result.data?.items ?? []).map((row) => {
    const expired = result.data?.members.find(
      (member) =>
        member.ts_code === row.ts_code &&
        member.version === row.membership_version &&
        member.expires_at !== null &&
        Date.parse(member.expires_at) <= now,
    );
    return expired
      ? {
          ...row,
          scope_status: "expired" as const,
          scope_message: "盯盘已到期，可停用或删除规则。",
        }
      : row;
  });
  const writable =
    ready && result.data?.can_write === true && commands.storageAvailable && Number.isFinite(now);
  const openDraft = draft?.identity.owner === result.owner ? draft : null;
  const pendingFor = (id: string) =>
    commands.entries.find((entry) => entry.body.rule_id === id && unresolved(entry));
  const draftPending = openDraft === null ? undefined : pendingFor(openDraft.identity.ruleId);
  const changedGeneration = openDraft !== null && openDraft.generation !== result.generation;

  useEffect(() => {
    session.setActive(true);
    const changed = () => session.refresh();
    window.addEventListener("storage", changed);
    window.addEventListener(PRICE_RULE_EVENT, changed);
    void session.resumePending();
    return () => {
      session.setActive(false);
      window.removeEventListener("storage", changed);
      window.removeEventListener(PRICE_RULE_EVENT, changed);
    };
  }, [session]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: 账号变化必须清空旧草稿；此依赖只作为重置触发器。
  useEffect(() => {
    setDraft(null);
    setSending(false);
    setDeleteTarget(null);
    discardClose.current = null;
    setDiscard(false);
    setError(null);
  }, [result.owner]);
  useEffect(() => {
    if (result.generation !== null) void session.resumePending();
  }, [result.generation, session]);
  useEffect(() => {
    const expiries = (result.data?.members ?? [])
      .map((member) => (member.expires_at === null ? Infinity : Date.parse(member.expires_at)))
      .filter((at) => at > now);
    const next = Math.min(...expiries);
    if (!Number.isFinite(next)) return;
    const timer = window.setTimeout(
      () => setTimePulse((value) => value + 1),
      Math.min(2_147_483_647, Math.max(1, next - now + 1)),
    );
    return () => window.clearTimeout(timer);
  }, [now, result.data]);

  function close() {
    const closing = draftRef.current?.identity;
    discardClose.current = null;
    setDraft(null);
    setSending(false);
    setDiscard(false);
    setError(null);
    window.requestAnimationFrame(() => {
      if (!closing || closing.owner !== ownerRef.current || draftRef.current !== null) return;
      const trigger =
        closing.readVersion === null
          ? null
          : editTargets.current.get(closing.ruleId)?.querySelector("button");
      (trigger ?? createRef.current?.querySelector("button"))?.focus();
    });
  }
  function requestClose() {
    const current = draftRef.current;
    if (
      current &&
      draftKey(current.binding, current.fields) !== current.initial &&
      !pendingFor(current.identity.ruleId)
    )
      setDiscard(true);
    else close();
  }
  function open(item?: PriceRuleItem) {
    if (result.owner === null || result.generation === null) return;
    const fields: Draft["fields"] = item
      ? {
          name: item.name,
          priority: item.priority,
          enabled: item.enabled,
          comparison: item.comparison,
          threshold: item.threshold,
          valid_from: item.valid_from,
          valid_until: item.valid_until,
        }
      : {
          name: "到价提醒",
          priority: "P2",
          enabled: true,
          comparison: "gte",
          threshold: "",
          valid_from: "09:30",
          valid_until: "14:57",
        };
    const binding = item
      ? `${item.ts_code}:${item.membership_version}`
      : members[0]
        ? `${members[0].ts_code}:${members[0].version}`
        : "";
    setDraft({
      identity: {
        owner: result.owner,
        sessionId: uniqueId(),
        ruleId: item?.rule_id ?? uniqueId(),
        readVersion: item?.version ?? null,
      },
      generation: result.generation,
      binding,
      fields,
      initial: draftKey(binding, fields),
    });
    setSending(false);
    setError(null);
  }
  function editField<K extends keyof Draft["fields"]>(key: K, value: Draft["fields"][K]) {
    setDraft((current) =>
      current ? { ...current, fields: { ...current.fields, [key]: value } } : current,
    );
    setError(null);
  }
  function base(
    ruleId: string,
    version: number | null,
    generation: string,
  ): Pick<
    PriceRuleCommand,
    "command_id" | "requested_at" | "generation_id" | "rule_id" | "expected_version"
  > {
    return {
      command_id: uniqueId(),
      requested_at: new Date(now).toISOString(),
      generation_id: generation,
      rule_id: ruleId,
      expected_version: version,
    };
  }
  async function save() {
    const current = draftRef.current;
    if (!current || !writable || changedGeneration || draftPending) return;
    const [code, version] = current.binding.split(":");
    const from = wallTimeKey(current.fields.valid_from);
    const until = wallTimeKey(current.fields.valid_until);
    if (from === null || until === null || from >= until) {
      setError("请填写有效时间，开始须早于结束。");
      return;
    }
    if (
      !code ||
      !version ||
      !current.fields.name.trim() ||
      !/^[+]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(current.fields.threshold.trim()) ||
      !/[1-9]/.test(current.fields.threshold.split(/[eE]/)[0] ?? "")
    ) {
      setError("请填写股票、名称和有效价格。");
      return;
    }
    const member = members.find((value) => `${value.ts_code}:${value.version}` === current.binding);
    const original = rows.find((value) => value.rule_id === current.identity.ruleId);
    if (
      !member &&
      (current.fields.enabled ||
        original === undefined ||
        current.binding !== `${original.ts_code}:${original.membership_version}`)
    ) {
      setError("请选择当前有效的盯盘股票。");
      return;
    }
    setSending(true);
    const started = await session.start({
      ...base(current.identity.ruleId, current.identity.readVersion, current.generation),
      action: "save",
      ts_code: code,
      membership_version: Number(version),
      rule: { ...current.fields },
    });
    if (matchesEditingSession(current.identity, draftRef.current?.identity ?? null)) {
      setSending(false);
      if (started)
        setDraft((value) =>
          value ? { ...value, initial: draftKey(value.binding, value.fields) } : value,
        );
    }
    result.refresh();
  }
  async function setEnabled(row: PriceRuleItem, enabled: boolean) {
    if (!writable || result.generation === null || pendingFor(row.rule_id)) return;
    await session.start({
      ...base(row.rule_id, row.version, result.generation),
      action: "set_enabled",
      enabled,
    });
    result.refresh();
  }
  async function remove() {
    const row = deleteTarget;
    if (!row || !writable || result.generation === null || pendingFor(row.rule_id)) return;
    setDeleteTarget(null);
    await session.start({ ...base(row.rule_id, row.version, result.generation), action: "delete" });
    result.refresh();
  }
  const columns: DataColumn<PriceRuleItem>[] = [
    { id: "name", header: "规则", value: (row) => row.name, wrap: true },
    { id: "stock", header: "股票", value: (row) => row.ts_code },
    { id: "priority", header: "级别", value: (row) => row.priority_label, secondary: true },
    {
      id: "condition",
      header: "触发条件",
      value: (row) => row.threshold,
      secondary: true,
      cell: (row) => (
        <Tip content={`完整价格 ${row.threshold}`}>
          <span>
            {row.comparison === "gte" ? "不低于" : "不高于"} {priceText(row.threshold)}
          </span>
        </Tip>
      ),
    },
    {
      id: "hours",
      header: "时段",
      value: (row) => `${row.valid_from}–${row.valid_until}`,
      secondary: true,
    },
    {
      id: "status",
      header: "状态",
      value: (row) => row.status_label,
      secondary: true,
      cell: (row) => <PriceAlertRuntimeStatus facts={runtimeFacts} rule={row} />,
    },
    {
      id: "enabled",
      header: "启用",
      value: (row) => (row.enabled ? 1 : 0),
      cell: (row) => (
        <Tip content={pendingFor(row.rule_id) ? "请先核对原操作。" : row.scope_message}>
          <Switch
            checked={row.enabled}
            label={`启停 ${row.name}`}
            onChange={(enabled) => void setEnabled(row, enabled)}
            disabled={
              !writable ||
              pendingFor(row.rule_id) !== undefined ||
              (!row.enabled && row.scope_status !== "disabled" && row.scope_status !== "bound")
            }
          />
        </Tip>
      ),
    },
    {
      id: "actions",
      header: "操作",
      value: () => null,
      cell: (row) => (
        <div
          className="price-rule-actions"
          ref={(target) => {
            if (target === null) editTargets.current.delete(row.rule_id);
            else editTargets.current.set(row.rule_id, target);
          }}
        >
          <Button
            size="sm"
            variant="ghost"
            aria-label={`编辑 ${row.name}`}
            onClick={() => open(row)}
          >
            编辑
          </Button>
          <Button
            size="sm"
            variant="ghost"
            aria-label={`删除 ${row.name}`}
            disabled={!writable || pendingFor(row.rule_id) !== undefined}
            onClick={() => setDeleteTarget(row)}
          >
            删除
          </Button>
        </div>
      ),
    },
  ];
  const fieldDisabled = sending || draftPending !== undefined;
  const oldBinding =
    openDraft &&
    !members.some((member) => `${member.ts_code}:${member.version}` === openDraft.binding)
      ? openDraft.binding
      : null;
  return (
    <>
      <Panel
        title="告警规则"
        label="告警规则"
        actions={
          <>
            <Button size="sm" variant="ghost" onClick={result.refresh}>
              刷新规则
            </Button>
            <span ref={createRef}>
              <Button
                size="sm"
                variant="primary"
                onClick={() => open()}
                disabledReason={
                  !writable
                    ? result.data?.write_message || "规则暂不可用，请稍后重试。"
                    : members.length === 0
                      ? "先从个股详情加入手动盯盘。"
                      : undefined
                }
              >
                新建规则
              </Button>
            </span>
          </>
        }
      >
        {result.loading ? (
          <PageSkeleton label="到价规则加载中" />
        ) : !ready ? (
          <EmptyState
            title={result.data?.message || "规则暂不可用，请稍后重试。"}
            hint={
              <Button size="sm" onClick={result.refresh}>
                重试
              </Button>
            }
          />
        ) : rows.length === 0 ? (
          <EmptyState
            title="还没有到价规则"
            hint={
              members.length ? "选择盯盘股票后即可新建。" : "从个股详情加入手动盯盘后即可新建。"
            }
          />
        ) : (
          <div className="price-rule-table">
            <DataTable
              rows={rows}
              columns={columns}
              rowKey={(row) => row.rule_id}
              label="到价规则"
            />
          </div>
        )}
        {commands.message ? (
          <p className="price-rule-note" role="alert">
            {commands.message}
          </p>
        ) : null}
        {commands.entries.length ? (
          <ul className="price-rule-operations" aria-label="规则操作记录">
            {commands.entries.map((entry) => (
              <li key={entry.body.command_id}>
                <span>
                  {entry.body.rule?.name ??
                    rows.find((row) => row.rule_id === entry.body.rule_id)?.name ??
                    "到价规则"}
                </span>
                <span role="status">{operationText(entry)}</span>
                {unresolved(entry) ? (
                  <Button
                    size="sm"
                    disabled={commands.busy.includes(entry.body.command_id)}
                    onClick={() => {
                      void session.advance(entry.body.command_id).then(result.refresh);
                    }}
                  >
                    继续核对
                  </Button>
                ) : ["conflict", "capacity", "scope_invalid", "failed", "rejected"].includes(
                    entry.status,
                  ) ? (
                  <Button size="sm" variant="ghost" onClick={result.refresh}>
                    刷新规则
                  </Button>
                ) : null}
              </li>
            ))}
          </ul>
        ) : null}
      </Panel>
      <PriceAlertRuntimeFacts facts={runtimeFacts} />
      <SideDrawer
        open={openDraft !== null}
        title={openDraft?.identity.readVersion === null ? "新建到价规则" : "编辑到价规则"}
        onClose={requestClose}
        footer={
          <div className="price-rule-footer">
            <Button onClick={requestClose} aria-label="取消编辑">
              取消
            </Button>
            <Button
              variant="primary"
              onClick={() => void save()}
              aria-label="保存规则"
              disabled={fieldDisabled || !writable || changedGeneration}
            >
              保存
            </Button>
          </div>
        }
      >
        {openDraft ? (
          <div className="price-rule-form">
            {changedGeneration ? <p role="alert">规则已更新，请关闭后重新打开。</p> : null}
            {draftPending ? <p role="status">{operationText(draftPending)}</p> : null}
            {error ? (
              <p className="crit-text" role="alert">
                {error}
              </p>
            ) : null}
            <label className="field">
              <span className="lbl">股票</span>
              <select
                className="inp"
                value={openDraft.binding}
                disabled={fieldDisabled}
                onChange={(event) =>
                  setDraft((current) =>
                    current ? { ...current, binding: event.target.value } : current,
                  )
                }
              >
                <option value="">请选择盯盘股票</option>
                {oldBinding ? (
                  <option value={oldBinding} disabled>
                    {oldBinding.split(":")[0]}（请重新选择）
                  </option>
                ) : null}
                {members.map((member) => (
                  <option key={member.ts_code} value={`${member.ts_code}:${member.version}`}>
                    {member.ts_code}
                  </option>
                ))}
              </select>
            </label>
            <label className="field">
              <span className="lbl">规则名称</span>
              <input
                className="inp"
                value={openDraft.fields.name}
                maxLength={80}
                disabled={fieldDisabled}
                onChange={(event) => editField("name", event.target.value)}
              />
            </label>
            <div className="price-rule-pair">
              <label className="field">
                <span className="lbl">级别</span>
                <select
                  className="inp"
                  value={openDraft.fields.priority}
                  disabled={fieldDisabled}
                  onChange={(event) =>
                    editField("priority", event.target.value as Draft["fields"]["priority"])
                  }
                >
                  {(result.data?.priority_options ?? []).map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span className="lbl">价格条件</span>
                <select
                  className="inp"
                  value={openDraft.fields.comparison}
                  disabled={fieldDisabled}
                  onChange={(event) => editField("comparison", event.target.value as "gte" | "lte")}
                >
                  <option value="gte">不低于</option>
                  <option value="lte">不高于</option>
                </select>
              </label>
            </div>
            <label className="field">
              <span className="lbl">阈值价格</span>
              <input
                className="inp num"
                inputMode="decimal"
                value={openDraft.fields.threshold}
                maxLength={1024}
                disabled={fieldDisabled}
                onChange={(event) => editField("threshold", event.target.value)}
              />
            </label>
            <div className="price-rule-pair">
              <label className="field">
                <span className="lbl">开始时间</span>
                <input
                  className="inp"
                  inputMode="numeric"
                  value={openDraft.fields.valid_from}
                  disabled={fieldDisabled}
                  onChange={(event) => editField("valid_from", event.target.value)}
                />
              </label>
              <label className="field">
                <span className="lbl">结束时间</span>
                <input
                  className="inp"
                  inputMode="numeric"
                  value={openDraft.fields.valid_until}
                  disabled={fieldDisabled}
                  onChange={(event) => editField("valid_until", event.target.value)}
                />
              </label>
            </div>
            <div className="price-rule-intent">
              <Tip content="按上海时间填写。开始须早于结束，可保留秒和小数秒。">
                <span>有效时段</span>
              </Tip>
              <Tip content="开关只保存启用设置；行情评估接通后才会提醒。">
                <span>启用规则</span>
              </Tip>
              <Switch
                checked={openDraft.fields.enabled}
                label="启用规则"
                disabled={fieldDisabled}
                onChange={(enabled) => editField("enabled", enabled)}
              />
            </div>
          </div>
        ) : null}
      </SideDrawer>
      <ConfirmDialog
        open={discard}
        level="heavy"
        title="放弃未保存的修改？"
        description="本次草稿会丢失。已提交的操作仍可继续核对。"
        confirmLabel="放弃修改"
        onCancel={() => {
          discardClose.current = null;
          setDiscard(false);
        }}
        onConfirm={() => {
          discardClose.current = draftRef.current;
          setDiscard(false);
        }}
        afterClose={() => {
          if (
            !openDraft ||
            discardClose.current !== openDraft ||
            draftRef.current !== openDraft ||
            openDraft.identity.owner !== ownerRef.current
          )
            return;
          discardClose.current = null;
          close();
        }}
      />
      <ConfirmDialog
        open={deleteTarget !== null}
        level="heavy"
        title="删除到价规则？"
        description={`删除“${deleteTarget?.name ?? ""}”后，它将从规则列表移除。`}
        confirmLabel="删除规则"
        onCancel={() => setDeleteTarget(null)}
        onConfirm={() => void remove()}
      />
    </>
  );
}
