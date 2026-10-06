import { useCallback, useEffect, useRef, useState } from "react";
import {
  type ConditionCommand,
  type ConditionItem,
  type ConditionRule,
  type ConditionTrigger,
  isConditionReceipt,
  postConditionRule,
  readConditionCommand,
  useConditionRules,
} from "@/api/conditionAlerts";
import { fetchScreenAlertDraft, type ScreenAlertDraft } from "@/api/screen";
import { formatCount } from "@/format/number";
import { type DataColumn, DataTable } from "@/table/DataTable";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  PageSkeleton,
  Panel,
  type ParameterValue,
  RelativeTime,
  SideDrawer,
  StatusBadge,
  Tip,
} from "@/ui";
import { type ScreenConditionDraft, ScreenConditionEditor } from "../shared/ScreenConditionEditor";
import "../screener/screener.css";
import "./priceAlertRules.css";

interface Draft {
  owner: string;
  generation: string;
  version: number | null;
  fields: ConditionRule;
  initial: string;
}
interface Confirmation {
  body: ConditionCommand;
  label: string;
  count: number | null;
  expires: Date;
  owner: string;
}
const PREFIX = "rquant.condition-command.v1:";
function parameter(value: unknown): value is ParameterValue {
  return (
    value === null ||
    typeof value === "string" ||
    typeof value === "number" ||
    (Array.isArray(value) && value.every((item: unknown) => typeof item === "string"))
  );
}
function editable(conditions: ConditionRule["conditions"]): ScreenConditionDraft[] {
  return conditions.map((call, id) => {
    const args: Record<string, ParameterValue> = {};
    for (const [key, value] of Object.entries(call.args ?? {})) {
      if (!parameter(value)) throw new Error("原条件无法完整带入，请刷新。");
      args[key] = value;
    }
    return { id, key: call.name, args };
  });
}
function baseRule(): ConditionRule {
  return {
    schema_version: 1,
    rule_id: `condition-${crypto.randomUUID()}`,
    name: "",
    priority: "P2",
    enabled: false,
    conditions: [{ name: "not_st", args: {} }],
    ranking: null,
    scope: { kind: "market", universe_policy: "trusted_current" },
    frequency: { kind: "every_evaluation" },
    governance: { channels: ["pushdeer"], dedup_window_seconds: 60, notify_recovery: false },
    trading_hours: {
      timezone: "Asia/Shanghai",
      windows: [
        { start: "09:30:00", end: "11:30:00" },
        { start: "13:00:00", end: "14:57:00" },
      ],
    },
    source_policy: {
      condition_semantics_version: "screen-registry/v1",
      daily_anchor: "previous_closed_session",
      intraday_contract_id: "intraday-pit",
      minimum_intraday_contract_version: 3,
    },
  };
}
function statusTone(label: ConditionItem["status_label"]) {
  return label === "正常"
    ? "ok"
    : label === "注意"
      ? "warn"
      : label === "异常"
        ? "crit"
        : label === "等待开盘"
          ? "waiting"
          : "idle";
}

function editorIssue(rule: ConditionRule): string | null {
  if (!rule.name.trim()) return "填写规则名称。";
  if (!rule.conditions.length) return "至少保留一条条件。";
  if (!rule.governance.channels.length) return "选择通知通道。";
  if (
    !Number.isInteger(rule.governance.dedup_window_seconds) ||
    rule.governance.dedup_window_seconds < 0 ||
    rule.governance.dedup_window_seconds > 3600
  )
    return "重复提醒间隔为 0 至 3600 秒。";
  if (
    rule.frequency.kind === "per_symbol_minutes" &&
    (!Number.isInteger(rule.frequency.minutes) ||
      rule.frequency.minutes < 1 ||
      rule.frequency.minutes > 60)
  )
    return "提醒间隔为 1 至 60 分钟。";
  if (
    rule.ranking &&
    (!Number.isInteger(rule.ranking.top_n) ||
      rule.ranking.top_n < 1 ||
      rule.ranking.top_n > 100 ||
      rule.ranking.conditions.some(
        (rank) => !Number.isFinite(rank.weight) || rank.weight < 0 || rank.weight > 100,
      ) ||
      rule.ranking.conditions.reduce((sum, rank) => sum + rank.weight, 0) <= 0 ||
      new Set(rule.ranking.conditions.map((rank) => rank.metric)).size !==
        rule.ranking.conditions.length)
  )
    return "检查排名数量、指标和权重。";
  const validTime = /^(?:[01]\d|2[0-3]):[0-5]\d(?::[0-5]\d(?:\.\d{1,6})?)?$/;
  if (
    rule.trading_hours.windows.some(
      (window) =>
        !validTime.test(window.start) || !validTime.test(window.end) || window.start >= window.end,
    )
  )
    return "检查有效时间，开始须早于结束。";
  return null;
}

export function conditionDeliveryLabel(
  trigger: Pick<ConditionTrigger, "delivery_state" | "provider_receipt" | "targets">,
): string {
  const receipts = (trigger.targets ?? []).map((target) => target.provider_receipt);
  if (trigger.delivery_state === "succeeded") {
    if (
      trigger.provider_receipt?.startsWith("shadow:") ||
      receipts.some((receipt) => receipt?.startsWith("shadow:"))
    )
      return "已记录";
    return receipts.length > 0 && receipts.every((receipt) => receipt !== null)
      ? "已提交"
      : "待核对";
  }
  return { failed: "发送失败", unknown: "待核对", pending: "等待发送", cancelled: "已取消" }[
    trigger.delivery_state
  ];
}

export function ConditionAlertRules() {
  const result = useConditionRules();
  const [draft, setDraft] = useState<Draft | null>(null);
  const [pending, setPending] = useState<ConditionCommand | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [storageReady, setStorageReady] = useState(true);
  const [busy, setBusy] = useState(false);
  const [confirm, setConfirm] = useState<Confirmation | null>(null);
  const [deleting, setDeleting] = useState<ConditionItem | null>(null);
  const [discard, setDiscard] = useState(false);
  const [imported, setImported] = useState<ScreenAlertDraft | null>(null);
  const [hash, setHash] = useState(() => window.location.hash);
  const [blockKey, setBlockKey] = useState("not_st");
  const focusReturn = useRef<{
    element: HTMLButtonElement;
    owner: string | null;
    epoch: number;
    ruleId?: string;
  } | null>(null);
  const editEntries = useRef(new Map<string, HTMLDivElement>());
  const pageActive = useRef(true);
  useEffect(() => {
    pageActive.current = true;
    return () => {
      pageActive.current = false;
      focusReturn.current = null;
    };
  }, []);
  const state = useRef({
    owner: result.owner,
    epoch: 0,
    busy: false,
    editorOpen: false,
    pending: null as ConditionCommand | null,
  });
  state.current.owner = result.owner;
  const key = result.owner === null ? null : `${PREFIX}${result.owner}`;
  const current = draft?.owner === result.owner ? draft : null;
  state.current.editorOpen = current !== null;
  const ready = result.data?.availability === "ready";
  const writable =
    ready &&
    result.data?.can_write === true &&
    storageReady &&
    result.owner !== null &&
    result.generation !== null;
  const changed = current !== null && current.generation !== result.generation;
  const canEnable = result.data?.can_enable === true;
  const scopeOption = result.data?.scopes.find(
    (option) =>
      current !== null && JSON.stringify(option.scope) === JSON.stringify(current.fields.scope),
  );
  const anchor = useRef({ server: 0, local: 0, value: "" });
  if (result.serverTime !== null && result.serverTime !== anchor.current.value)
    anchor.current = {
      server: Date.parse(result.serverTime),
      local: performance.now(),
      value: result.serverTime,
    };
  const now = useCallback(
    () => new Date(anchor.current.server + performance.now() - anchor.current.local),
    [],
  );

  async function advance(body: ConditionCommand, resume: boolean, epoch: number) {
    if (state.current.busy) return;
    state.current.busy = true;
    setBusy(true);
    try {
      const reply = await postConditionRule(body, resume);
      if (epoch !== state.current.epoch) return;
      if (!isConditionReceipt(reply, body)) throw new Error("unconfirmed reply");
      setMessage(reply.message);
      if (
        !["pending", "processing", "saved_syncing", "uncertain", "not_found"].includes(reply.status)
      ) {
        if (key !== null && localStorage.getItem(key) === JSON.stringify(body))
          localStorage.removeItem(key);
        state.current.pending = null;
        setPending(null);
        if (["published", "superseded"].includes(reply.status)) setDraft(null);
      }
      result.refresh();
    } catch {
      if (epoch === state.current.epoch) setMessage("状态待核对，请继续核对原操作。");
    } finally {
      if (epoch === state.current.epoch) {
        state.current.busy = false;
        setBusy(false);
      }
    }
  }
  const advanceRef = useRef(advance);
  advanceRef.current = advance;
  useEffect(() => {
    const active = state.current;
    active.epoch += 1;
    active.busy = false;
    active.pending = null;
    setDraft(null);
    setPending(null);
    setConfirm(null);
    setDeleting(null);
    setDiscard(false);
    setImported(null);
    setMessage(null);
    setBusy(false);
    setStorageReady(true);
    function restore() {
      if (key === null) return;
      try {
        const raw = localStorage.getItem(key);
        const original = raw === null ? null : readConditionCommand(raw);
        if (raw !== null && original === null) throw new Error("invalid original request");
        active.pending = original;
        setPending(original);
        if (original && !active.busy) void advanceRef.current(original, true, active.epoch);
      } catch {
        setStorageReady(false);
        setMessage("原操作无法核对，请检查浏览器存储。");
      }
    }
    restore();
    window.addEventListener("storage", restore);
    return () => {
      active.epoch += 1;
      window.removeEventListener("storage", restore);
    };
  }, [key]);

  useEffect(() => {
    const update = () => setHash(window.location.hash);
    window.addEventListener("hashchange", update);
    return () => window.removeEventListener("hashchange", update);
  }, []);

  useEffect(() => {
    setImported(null);
    const draftId = new URLSearchParams(hash.split("?")[1] ?? "").get("conditionDraft");
    if (
      result.owner === null ||
      result.generation === null ||
      !draftId ||
      !/^[0-9a-f]{24}$/.test(draftId)
    )
      return;
    const epoch = state.current.epoch;
    const controller = new AbortController();
    void fetchScreenAlertDraft(draftId, controller.signal)
      .then((data) => {
        if (controller.signal.aborted || epoch !== state.current.epoch) return;
        const actual = data.alert_draft;
        if (
          !data.available ||
          !actual ||
          actual.draft_id !== draftId ||
          actual.origin.draft_id !== draftId ||
          Date.parse(actual.expires_at) <= now().getTime()
        )
          throw new Error("draft unavailable");
        editable(actual.conditions);
        setImported(actual);
        setMessage("选股条件已带入，请确认规则。");
      })
      .catch(() => {
        if (!controller.signal.aborted && epoch === state.current.epoch)
          setMessage("草稿已过期或不可用，请重新带入条件。");
      });
    return () => controller.abort();
  }, [result.owner, result.generation, hash, now]);

  function open(item?: ConditionItem) {
    if (
      !writable ||
      result.owner === null ||
      result.generation === null ||
      state.current.pending !== null
    )
      return;
    try {
      const fields: ConditionRule = item
        ? structuredClone(item.rule)
        : imported
          ? {
              ...baseRule(),
              name: imported.suggested_name,
              conditions: imported.conditions,
              ranking: imported.ranking,
              scope: imported.preferred_scope,
              source_policy: imported.source_policy,
              origin: imported.origin,
            }
          : baseRule();
      editable(fields.conditions);
      setDraft({
        owner: result.owner,
        generation: result.generation,
        version: item?.version ?? null,
        fields,
        initial: JSON.stringify(fields),
      });
      setMessage(null);
    } catch {
      setMessage("原条件无法完整带入，请刷新。");
    }
  }
  function patch(value: Partial<ConditionRule>) {
    setDraft((old) =>
      old && old.owner === result.owner ? { ...old, fields: { ...old.fields, ...value } } : old,
    );
  }
  function close() {
    if (current && JSON.stringify(current.fields) !== current.initial) {
      setDiscard(true);
      return;
    }
    setDraft(null);
  }
  const restoreEditorFocus = useCallback(() => {
    const target = focusReturn.current;
    if (!target) return true;
    if (
      !pageActive.current ||
      target.owner !== state.current.owner ||
      target.epoch !== state.current.epoch
    ) {
      focusReturn.current = null;
      return true;
    }
    if (state.current.editorOpen) return false;
    const visible = [...document.querySelectorAll('[role="dialog"]')].some((dialog) =>
      dialog
        .getAttribute("aria-labelledby")
        ?.split(/\s+/)
        .some((id) =>
          ["新建条件规则", "编辑条件规则"].includes(
            document.getElementById(id)?.textContent?.trim() ?? "",
          ),
        ),
    );
    if (visible) return false;
    const entry = target.ruleId
      ? editEntries.current.get(target.ruleId)?.querySelector<HTMLButtonElement>("button")
      : target.element;
    focusReturn.current = null;
    if (entry?.isConnected && !entry.disabled) entry.focus();
    return true;
  }, []);
  useEffect(() => {
    const target = focusReturn.current;
    if (current !== null || !target) return;
    if (target.owner !== result.owner) {
      focusReturn.current = null;
      return;
    }
    // Observe real removal for both interrupted opening and completed closing.
    const observer = new MutationObserver(() => {
      if (restoreEditorFocus()) observer.disconnect();
    });
    observer.observe(document.body, { childList: true, subtree: true });
    if (restoreEditorFocus()) observer.disconnect();
    return () => observer.disconnect();
  }, [current, result.owner, restoreEditorFocus]);
  function afterEditorChange(open: boolean) {
    if (!open) queueMicrotask(() => restoreEditorFocus());
  }
  async function send(body: ConditionCommand) {
    if (!writable || key === null || state.current.busy || state.current.pending !== null) return;
    const epoch = state.current.epoch;
    try {
      const save = async () => {
        if (epoch !== state.current.epoch || localStorage.getItem(key) !== null)
          throw new Error("previous request pending");
        const raw = JSON.stringify(body);
        if (readConditionCommand(raw) === null) throw new Error("invalid original request");
        localStorage.setItem(key, raw);
        if (localStorage.getItem(key) !== raw) throw new Error("request was not durable");
        state.current.pending = body;
        setPending(body);
      };
      if (navigator.locks) await navigator.locks.request(key, { mode: "exclusive" }, save);
      else await save();
      if (epoch === state.current.epoch) await advance(body, false, epoch);
    } catch {
      if (epoch === state.current.epoch) {
        setStorageReady(false);
        setMessage("原操作无法核对，请检查浏览器存储。");
      }
    }
  }
  function command(
    item: ConditionItem,
    action: "delete" | "set_enabled",
    enabled?: boolean,
  ): ConditionCommand | null {
    if (result.generation === null || !Number.isFinite(now().getTime())) return null;
    return {
      command_id: `condition-web-${crypto.randomUUID()}`,
      requested_at: now().toISOString(),
      generation_id: result.generation,
      expected_version: item.version,
      rule_id: item.rule_id,
      action,
      ...(enabled === undefined ? {} : { enabled }),
    };
  }
  function confirmOrSend(body: ConditionCommand) {
    if (
      (body.rule?.enabled || body.enabled === true) &&
      (body.rule?.scope.kind === "market" ||
        result.data?.items.find((item) => item.rule_id === body.rule_id)?.rule.scope.kind ===
          "market")
    ) {
      const option = result.data?.scopes.find((item) => item.scope.kind === "market");
      if (!option?.available || !canEnable) return;
      setConfirm({
        body,
        label: option.label,
        count: option.member_count ?? null,
        expires: new Date(now().getTime() + 120_000),
        owner: result.owner ?? "",
      });
    } else void send(body);
  }
  function save() {
    if (!current || changed || !writable || pending || editorIssue(current.fields) !== null) return;
    const body: ConditionCommand = {
      command_id: `condition-web-${crypto.randomUUID()}`,
      requested_at: now().toISOString(),
      generation_id: current.generation,
      expected_version: current.version,
      rule_id: current.fields.rule_id,
      action: "save",
      rule: current.fields,
    };
    confirmOrSend(body);
  }
  const rows = result.data?.items ?? [];
  const columns: DataColumn<ConditionItem>[] = [
    {
      id: "name",
      header: "规则",
      value: (row) => row.rule.name,
      wrap: true,
      cell: (row) => (
        <Tip
          content={`${row.rule.conditions.length} 条条件；${result.data?.scopes.find((option) => JSON.stringify(option.scope) === JSON.stringify(row.rule.scope))?.label ?? "原范围待核对"}`}
        >
          <span>{row.rule.name}</span>
        </Tip>
      ),
    },
    {
      id: "status",
      header: "状态",
      value: (row) => row.status_label,
      cell: (row) => (
        <StatusBadge
          state={statusTone(row.status_label)}
          label={row.status_label}
          reason={row.scope_message || undefined}
        />
      ),
    },
    {
      id: "count",
      header: "匹配",
      value: (row) => row.matched_count ?? null,
      numeric: true,
      cell: (row) => (
        <Tip
          content={
            row.unknown_count == null
              ? "尚无完整评估回证。"
              : `待核对 ${formatCount(row.unknown_count)} 只`
          }
        >
          <span className="num">
            {row.matched_count == null ? "—" : formatCount(row.matched_count)}
          </span>
        </Tip>
      ),
    },
    {
      id: "time",
      header: "最近评估",
      value: (row) => row.evaluated_at ?? null,
      secondary: true,
      cell: (row) => (row.evaluated_at ? <RelativeTime at={row.evaluated_at} /> : "—"),
    },
    {
      id: "actions",
      header: "操作",
      value: () => null,
      cell: (row) => (
        <div
          className="price-rule-actions"
          ref={(node) => {
            if (node) editEntries.current.set(row.rule_id, node);
            else editEntries.current.delete(row.rule_id);
          }}
        >
          <Button
            size="sm"
            aria-label={`编辑条件 ${row.rule.name}`}
            disabled={busy || pending !== null}
            disabledReason={!writable ? result.data?.write_message || "规则暂不可用。" : undefined}
            onClick={(event) => {
              focusReturn.current = {
                element: event.currentTarget,
                owner: result.owner,
                epoch: state.current.epoch,
                ruleId: row.rule_id,
              };
              open(row);
            }}
          >
            编辑
          </Button>
          <Button
            size="sm"
            disabled={busy || pending !== null}
            disabledReason={
              !writable
                ? "规则暂不可用。"
                : !row.rule.enabled && (!canEnable || row.scope_status !== "bound")
                  ? result.data?.enable_message || row.scope_message
                  : undefined
            }
            onClick={() => {
              const body = command(row, "set_enabled", !row.rule.enabled);
              if (body) confirmOrSend(body);
            }}
          >
            {row.rule.enabled ? "停用" : "启用"}
          </Button>
          <Button
            size="sm"
            variant="ghost"
            disabled={!writable || busy || pending !== null}
            aria-label={`删除条件 ${row.rule.name}`}
            onClick={() => setDeleting(row)}
          >
            删除
          </Button>
        </div>
      ),
    },
  ];
  const formReason = !writable
    ? result.data?.write_message || "规则暂不可用。"
    : pending
      ? "先核对原操作。"
      : changed
        ? "数据已更新，请重新打开规则。"
        : current === null || editorIssue(current.fields) !== null
          ? current === null
            ? "打开规则后保存。"
            : (editorIssue(current.fields) ?? undefined)
          : current.fields.enabled && (!canEnable || !scopeOption?.available)
            ? result.data?.enable_message || "范围暂不可用。"
            : undefined;
  let conditions: ScreenConditionDraft[] = [];
  if (current) conditions = editable(current.fields.conditions);
  return (
    <>
      <Panel
        title="条件提醒"
        label="条件提醒"
        actions={
          <>
            <Button size="sm" variant="ghost" onClick={result.refresh}>
              刷新条件规则
            </Button>
            <Button
              size="sm"
              variant="primary"
              disabled={busy || pending !== null}
              disabledReason={
                !writable ? result.data?.write_message || "规则暂不可用。" : undefined
              }
              onClick={(event) => {
                focusReturn.current = {
                  element: event.currentTarget,
                  owner: result.owner,
                  epoch: state.current.epoch,
                };
                open();
              }}
            >
              {imported ? "继续设置条件提醒" : "新建条件规则"}
            </Button>
          </>
        }
      >
        {result.loading ? (
          <PageSkeleton label="条件规则加载中" />
        ) : !ready ? (
          <EmptyState
            title={result.data?.message || "条件规则暂不可用"}
            hint={<Button onClick={result.refresh}>重试</Button>}
          />
        ) : rows.length ? (
          <div className="price-rule-table">
            <DataTable
              rows={rows}
              columns={columns}
              rowKey={(row) => row.rule_id}
              label="条件规则"
            />
          </div>
        ) : (
          <EmptyState title="还没有条件规则" hint="从选股结果带入条件，或新建规则。" />
        )}
        {message ? (
          <p className="price-rule-note" role="status">
            {message}
          </p>
        ) : null}
        {pending ? (
          <Button
            size="sm"
            disabled={busy}
            onClick={() => void advance(pending, true, state.current.epoch)}
          >
            继续核对原操作
          </Button>
        ) : null}
        {(result.data?.triggers ?? []).length ? (
          <DataTable
            rows={result.data?.triggers ?? []}
            rowKey={(row) => row.event.event_id}
            label="条件触发回执"
            columns={[
              { id: "rule", header: "触发规则", value: (row) => row.event.rule_name },
              {
                id: "stock",
                header: "股票",
                value: (row) => row.event.ts_code,
                cell: (row) => <span className="mono">{row.event.ts_code}</span>,
              },
              {
                id: "kind",
                header: "触发",
                value: (row) => (row.event.trigger_kind === "matched" ? "满足条件" : "已恢复"),
              },
              {
                id: "delivery",
                header: "通知",
                value: (row) => row.delivery_state,
                cell: (row) => (
                  <Tip
                    content={(row.targets ?? [])
                      .map(
                        (target) =>
                          `${target.channel === "pushdeer" ? "PushDeer" : "PushPlus"}：${conditionDeliveryLabel({ delivery_state: target.state, targets: [target] })}。提供方接收不代表手机已显示。`,
                      )
                      .join("；")}
                  >
                    <span>{conditionDeliveryLabel(row)}</span>
                  </Tip>
                ),
              },
              {
                id: "at",
                header: "时间",
                value: (row) => row.event.event_time,
                secondary: true,
                cell: (row) => <RelativeTime at={row.event.event_time} />,
              },
            ]}
          />
        ) : null}
      </Panel>
      <SideDrawer
        open={current !== null}
        title={current?.version === null ? "新建条件规则" : "编辑条件规则"}
        onClose={close}
        afterOpenChange={afterEditorChange}
        footer={
          <div className="price-rule-footer">
            <Button onClick={close}>取消编辑条件</Button>
            <Button variant="primary" disabled={busy} disabledReason={formReason} onClick={save}>
              保存条件规则
            </Button>
          </div>
        }
      >
        {current ? (
          <form
            className="price-rule-form"
            onSubmit={(event) => {
              event.preventDefault();
              save();
            }}
          >
            <label className="field">
              <span className="lbl">规则名称</span>
              <input
                className="inp"
                aria-label="条件规则名称"
                value={current.fields.name}
                maxLength={80}
                onChange={(event) => patch({ name: event.target.value })}
              />
            </label>
            <div className="price-rule-pair">
              <label className="field">
                <span className="lbl">范围</span>
                <select
                  className="inp"
                  aria-label="条件范围"
                  value={scopeOption ? JSON.stringify(scopeOption.scope) : "original"}
                  onChange={(event) => {
                    const option = result.data?.scopes.find(
                      (item) => JSON.stringify(item.scope) === event.target.value,
                    );
                    if (option) patch({ scope: option.scope });
                  }}
                >
                  {!scopeOption ? <option value="original">原范围待核对</option> : null}
                  {result.data?.scopes.map((option) => (
                    <option key={JSON.stringify(option.scope)} value={JSON.stringify(option.scope)}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>
              <label className="field">
                <span className="lbl">优先级</span>
                <select
                  className="inp"
                  aria-label="条件优先级"
                  value={current.fields.priority}
                  onChange={(event) => {
                    const priority = event.target.value;
                    if (
                      priority === "P0" ||
                      priority === "P1" ||
                      priority === "P2" ||
                      priority === "P3"
                    )
                      patch({ priority });
                  }}
                >
                  <option value="P0">紧急</option>
                  <option value="P1">重要</option>
                  <option value="P2">普通</option>
                  <option value="P3">提示</option>
                </select>
              </label>
            </div>
            <ScreenConditionEditor
              conditions={conditions}
              blocks={result.data?.blocks ?? []}
              allowRsi
              onRemove={(id) =>
                patch({ conditions: current.fields.conditions.filter((_, index) => index !== id) })
              }
              onUpdate={(id, name, value) =>
                patch({
                  conditions: current.fields.conditions.map((call, index) =>
                    index === id ? { ...call, args: { ...call.args, [name]: value } } : call,
                  ),
                })
              }
            />
            <div className="price-rule-pair">
              <select
                className="inp"
                aria-label="添加的条件"
                value={blockKey}
                onChange={(event) => setBlockKey(event.target.value)}
              >
                {result.data?.blocks.map((block) => (
                  <option key={block.key} value={block.key}>
                    {block.label}
                  </option>
                ))}
              </select>
              <Button
                disabled={conditions.length >= 26}
                onClick={() => {
                  const block = result.data?.blocks.find((item) => item.key === blockKey);
                  if (block)
                    patch({
                      conditions: [
                        ...current.fields.conditions,
                        {
                          name: block.key,
                          args: Object.fromEntries(
                            block.parameters.map((item) => [item.key, item.initial ?? null]),
                          ),
                        },
                      ],
                    });
                }}
              >
                添加条件
              </Button>
            </div>
            <fieldset>
              <legend className="lbl">排名</legend>
              <label>
                <input
                  type="checkbox"
                  checked={current.fields.ranking != null}
                  onChange={(event) => {
                    const metric = result.data?.ranking_metrics[0]?.value;
                    patch({
                      ranking:
                        event.target.checked && metric
                          ? { top_n: 100, conditions: [{ metric, weight: 1, ascending: false }] }
                          : null,
                    });
                  }}
                />
                按排名保留
              </label>
              {current.fields.ranking ? (
                <>
                  <label className="field">
                    <span className="lbl">保留数量</span>
                    <input
                      className="inp num"
                      aria-label="排名保留数量"
                      type="number"
                      min={1}
                      max={100}
                      value={current.fields.ranking.top_n}
                      onChange={(event) => {
                        if (current.fields.ranking)
                          patch({
                            ranking: {
                              ...current.fields.ranking,
                              top_n: Number(event.target.value),
                            },
                          });
                      }}
                    />
                  </label>
                  {current.fields.ranking.conditions.map((rank, index) => (
                    <div className="price-rule-pair" key={rank.metric}>
                      <label>
                        <span className="lbl">排名指标</span>
                        <select
                          className="inp"
                          aria-label={`排名指标 ${index + 1}`}
                          value={rank.metric}
                          onChange={(event) => {
                            if (current.fields.ranking)
                              patch({
                                ranking: {
                                  ...current.fields.ranking,
                                  conditions: current.fields.ranking.conditions.map((item, at) =>
                                    at === index ? { ...item, metric: event.target.value } : item,
                                  ),
                                },
                              });
                          }}
                        >
                          {result.data?.ranking_metrics.map((metric) => (
                            <option value={metric.value} key={metric.value}>
                              {metric.label}
                            </option>
                          ))}
                        </select>
                      </label>
                      <div>
                        <label>
                          <span className="lbl">权重</span>
                          <input
                            className="inp num"
                            type="number"
                            min={0}
                            max={100}
                            step={0.1}
                            aria-label={`排名权重 ${index + 1}`}
                            value={rank.weight}
                            onChange={(event) => {
                              if (current.fields.ranking)
                                patch({
                                  ranking: {
                                    ...current.fields.ranking,
                                    conditions: current.fields.ranking.conditions.map((item, at) =>
                                      at === index
                                        ? { ...item, weight: Number(event.target.value) }
                                        : item,
                                    ),
                                  },
                                });
                            }}
                          />
                        </label>
                        <label>
                          <input
                            type="checkbox"
                            checked={rank.ascending}
                            onChange={(event) => {
                              if (current.fields.ranking)
                                patch({
                                  ranking: {
                                    ...current.fields.ranking,
                                    conditions: current.fields.ranking.conditions.map((item, at) =>
                                      at === index
                                        ? { ...item, ascending: event.target.checked }
                                        : item,
                                    ),
                                  },
                                });
                            }}
                          />
                          越小越优先
                        </label>
                        <Button
                          size="sm"
                          disabled={(current.fields.ranking?.conditions.length ?? 0) <= 1}
                          onClick={() => {
                            if (current.fields.ranking)
                              patch({
                                ranking: {
                                  ...current.fields.ranking,
                                  conditions: current.fields.ranking.conditions.filter(
                                    (_, at) => at !== index,
                                  ),
                                },
                              });
                          }}
                        >
                          删除排名
                        </Button>
                      </div>
                    </div>
                  ))}
                  <Button
                    disabled={current.fields.ranking.conditions.length >= 4}
                    onClick={() => {
                      const metric = result.data?.ranking_metrics.find(
                        (item) =>
                          !current.fields.ranking?.conditions.some(
                            (rank) => rank.metric === item.value,
                          ),
                      );
                      if (current.fields.ranking && metric)
                        patch({
                          ranking: {
                            ...current.fields.ranking,
                            conditions: [
                              ...current.fields.ranking.conditions,
                              { metric: metric.value, weight: 1, ascending: false },
                            ],
                          },
                        });
                    }}
                  >
                    添加排名
                  </Button>
                </>
              ) : null}
            </fieldset>
            <label className="field">
              <span className="lbl">提醒频率</span>
              <select
                className="inp"
                aria-label="提醒频率"
                value={current.fields.frequency.kind}
                onChange={(event) => {
                  const kind = event.target.value;
                  if (kind === "every_evaluation") patch({ frequency: { kind } });
                  if (kind === "per_symbol_minutes") patch({ frequency: { kind, minutes: 5 } });
                  if (kind === "bar_close") patch({ frequency: { kind, bar_size: "1min" } });
                }}
              >
                <option value="every_evaluation">每次评估</option>
                <option value="per_symbol_minutes">每只股票间隔</option>
                <option value="bar_close">每分钟收线</option>
              </select>
            </label>
            {current.fields.frequency.kind === "per_symbol_minutes" ? (
              <label className="field">
                <span className="lbl">间隔（分钟）</span>
                <input
                  className="inp num"
                  aria-label="提醒间隔分钟"
                  type="number"
                  min={1}
                  max={60}
                  value={current.fields.frequency.minutes}
                  onChange={(event) =>
                    patch({
                      frequency: {
                        kind: "per_symbol_minutes",
                        minutes: Number(event.target.value),
                      },
                    })
                  }
                />
              </label>
            ) : null}
            <fieldset>
              <legend className="lbl">有效时间</legend>
              {current.fields.trading_hours.windows.map((window, index) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: 时间输入均受控，删除时按当前时段位置整体重绑。
                <div className="price-rule-pair" key={`${index}-${current.fields.rule_id}`}>
                  {(["start", "end"] as const).map((key) => (
                    <label key={key}>
                      <span className="lbl">{key === "start" ? "开始" : "结束"}</span>
                      <input
                        className="inp"
                        type="text"
                        inputMode="numeric"
                        placeholder="09:30:00"
                        maxLength={15}
                        aria-label={`${key === "start" ? "开始" : "结束"}时间 ${index + 1}`}
                        value={window[key]}
                        onChange={(event) =>
                          patch({
                            trading_hours: {
                              ...current.fields.trading_hours,
                              windows: current.fields.trading_hours.windows.map((item, at) =>
                                at === index ? { ...item, [key]: event.target.value } : item,
                              ),
                            },
                          })
                        }
                      />
                    </label>
                  ))}
                  <Button
                    size="sm"
                    disabled={current.fields.trading_hours.windows.length <= 1}
                    onClick={() =>
                      patch({
                        trading_hours: {
                          ...current.fields.trading_hours,
                          windows: current.fields.trading_hours.windows.filter(
                            (_, at) => at !== index,
                          ),
                        },
                      })
                    }
                  >
                    删除时段
                  </Button>
                </div>
              ))}
              <Button
                disabled={current.fields.trading_hours.windows.length >= 2}
                onClick={() =>
                  patch({
                    trading_hours: {
                      ...current.fields.trading_hours,
                      windows: [
                        ...current.fields.trading_hours.windows,
                        { start: "13:00:00", end: "14:57:00" },
                      ],
                    },
                  })
                }
              >
                添加时段
              </Button>
            </fieldset>
            <fieldset>
              <legend className="lbl">通知通道</legend>
              {(["pushdeer", "pushplus"] as const).map((channel) => (
                <label key={channel}>
                  <input
                    type="checkbox"
                    checked={current.fields.governance.channels.includes(channel)}
                    onChange={(event) =>
                      patch({
                        governance: {
                          ...current.fields.governance,
                          channels: event.target.checked
                            ? [...current.fields.governance.channels, channel]
                            : current.fields.governance.channels.filter((item) => item !== channel),
                        },
                      })
                    }
                  />
                  {channel === "pushdeer" ? "PushDeer" : "PushPlus"}
                </label>
              ))}
            </fieldset>
            <label className="field">
              <span className="lbl">重复提醒间隔（秒）</span>
              <input
                className="inp num"
                aria-label="重复提醒间隔秒"
                type="number"
                min={0}
                max={3600}
                value={current.fields.governance.dedup_window_seconds}
                onChange={(event) =>
                  patch({
                    governance: {
                      ...current.fields.governance,
                      dedup_window_seconds: Number(event.target.value),
                    },
                  })
                }
              />
            </label>
            <label>
              <input
                type="checkbox"
                checked={current.fields.governance.notify_recovery}
                onChange={(event) =>
                  patch({
                    governance: {
                      ...current.fields.governance,
                      notify_recovery: event.target.checked,
                    },
                  })
                }
              />
              条件恢复时提醒
            </label>
            <Tip
              content={
                !canEnable || !scopeOption?.available
                  ? result.data?.enable_message || scopeOption?.message || "数据暂不可用。"
                  : "开启后，收到完整评估回证才显示运行状态。"
              }
            >
              <label>
                <input
                  type="checkbox"
                  aria-label="启用条件规则"
                  checked={current.fields.enabled}
                  disabled={!current.fields.enabled && (!canEnable || !scopeOption?.available)}
                  onChange={(event) => patch({ enabled: event.target.checked })}
                />
                启用规则
              </label>
            </Tip>
          </form>
        ) : null}
      </SideDrawer>
      <ConfirmDialog
        open={confirm !== null}
        level="high"
        title="启用全市场条件提醒？"
        description={
          confirm
            ? `${confirm.label} · ${confirm.count == null ? "股票数量待核对" : `${formatCount(confirm.count)} 只股票`}`
            : ""
        }
        confirmName={confirm?.label}
        expiresAt={confirm?.expires}
        now={now}
        confirmLabel="确认启用"
        busy={busy}
        disabled={
          !writable ||
          !canEnable ||
          confirm?.owner !== result.owner ||
          confirm?.body.generation_id !== result.generation
        }
        onCancel={() => setConfirm(null)}
        onConfirm={() => {
          if (
            confirm &&
            confirm.owner === result.owner &&
            confirm.body.generation_id === result.generation &&
            canEnable
          ) {
            const body = confirm.body;
            setConfirm(null);
            void send(body);
          }
        }}
      />
      <ConfirmDialog
        open={deleting !== null}
        level="heavy"
        title="删除条件规则？"
        description={deleting?.rule.name ?? ""}
        busy={busy}
        disabled={!writable || pending !== null}
        confirmLabel="删除规则"
        onCancel={() => setDeleting(null)}
        onConfirm={() => {
          if (deleting) {
            const body = command(deleting, "delete");
            setDeleting(null);
            if (body) void send(body);
          }
        }}
      />
      <ConfirmDialog
        open={discard}
        level="heavy"
        title="放弃未保存的条件？"
        description="关闭后不会保存本次修改。"
        confirmLabel="放弃修改"
        onCancel={() => setDiscard(false)}
        onConfirm={() => {
          setDiscard(false);
          setDraft(null);
        }}
      />
    </>
  );
}
