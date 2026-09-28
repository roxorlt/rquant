import { useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import type { Schemas } from "@/api/client";
import { useManualWatchlist } from "@/api/manualWatchlist";
import {
  type PriceRuleCommandDraft,
  type PriceRuleCommandEntry,
  PriceRuleCommandSession,
  priceRuleProvedNoEffect,
  submitPriceRuleCommand,
} from "@/api/priceAlertRuleCommand";
import {
  type PriceRuleItem,
  usePriceAlertRules,
  verifyPriceRuleBasis,
  verifyPriceRuleOwner,
} from "@/api/priceAlertRules";
import { formatPrice } from "@/format/number";
import { Button, ConfirmDialog, EmptyState, Panel, StatusBadge, Switch, Tip } from "@/ui";
import "./priceRule.css";

type Priority = Schemas["PriceAlertRule"]["priority"];
type Comparison = Schemas["PriceAlertRule"]["comparison"];

interface Editor {
  scope: object;
  generationId: string;
  ruleId: string;
  expectedVersion: number | null;
  tsCode: string;
  name: string;
  priority: Priority;
  comparison: Comparison;
  threshold: string;
  validFrom: string;
  validUntil: string;
  enabled: boolean;
}

function nextId(): string {
  return `web-${Array.from(crypto.getRandomValues(new Uint8Array(16)), (value) => value.toString(16).padStart(2, "0")).join("")}`;
}

async function browserLock(name: string, task: () => Promise<void>): Promise<void> {
  if (!navigator.locks?.request) throw new Error("browser lock unavailable");
  await navigator.locks.request(name, { mode: "exclusive" }, task);
}

function editorFor(item: PriceRuleItem, generationId: string, scope: object): Editor | null {
  if (
    item.deleted ||
    item.ts_code === null ||
    item.name === null ||
    item.priority === null ||
    item.comparison === null ||
    item.threshold === null ||
    item.valid_from === null ||
    item.valid_until === null ||
    item.enabled === null
  )
    return null;
  return {
    scope,
    generationId,
    ruleId: item.rule_id,
    expectedVersion: item.version,
    tsCode: item.ts_code,
    name: item.name,
    priority: item.priority,
    comparison: item.comparison,
    threshold: item.threshold,
    validFrom: item.valid_from.slice(0, 5),
    validUntil: item.valid_until.slice(0, 5),
    enabled: item.enabled,
  };
}

function operation(
  entry: PriceRuleCommandEntry | undefined,
): { label: string; terminal: boolean } | null {
  if (!entry) return null;
  if (entry.status === "saved_syncing" || entry.status === "published")
    return { label: "已保存，正在同步", terminal: false };
  if (priceRuleProvedNoEffect(entry))
    return { label: entry.status === "capacity" ? "规则数量已满" : "数据已变化", terminal: true };
  if (entry.status === "failed") return { label: "未保存，可重试", terminal: true };
  if (entry.status === "pending" || entry.status === "processing")
    return { label: "正在处理", terminal: false };
  return { label: "状态待核对", terminal: false };
}

function RuleForm({
  editor,
  stocks,
  generationId,
  submitting,
  onChange,
  onSave,
  onCancel,
}: {
  editor: Editor;
  stocks: readonly { ts_code: string; version: number }[];
  generationId: string | null;
  submitting: boolean;
  onChange: (editor: Editor) => void;
  onSave: () => void;
  onCancel: () => void;
}) {
  const stock = stocks.find((item) => item.ts_code === editor.tsCode);
  const threshold = Number(editor.threshold);
  const valid =
    generationId === editor.generationId &&
    stock !== undefined &&
    editor.name.trim().length > 0 &&
    editor.name.trim().length <= 80 &&
    /^\d+(?:\.\d{1,2})?$/.test(editor.threshold) &&
    Number.isFinite(threshold) &&
    threshold > 0 &&
    /^\d{2}:\d{2}$/.test(editor.validFrom) &&
    /^\d{2}:\d{2}$/.test(editor.validUntil) &&
    editor.validFrom < editor.validUntil;
  return (
    <form
      className="price-rule-editor"
      onSubmit={(event) => {
        event.preventDefault();
        if (valid) onSave();
      }}
    >
      <div className="price-rule-editor-head">
        <strong>{editor.expectedVersion === null ? "新建价格规则" : "编辑价格规则"}</strong>
        <Button size="sm" variant="ghost" onClick={onCancel}>
          取消编辑
        </Button>
      </div>
      <div className="price-rule-fields">
        <label className="field">
          <span className="lbl">名称</span>
          <input
            className="inp"
            maxLength={80}
            value={editor.name}
            onChange={(event) => onChange({ ...editor, name: event.target.value })}
          />
        </label>
        <label className="field">
          <span className="lbl">股票</span>
          <select
            className="inp"
            value={editor.tsCode}
            onChange={(event) => onChange({ ...editor, tsCode: event.target.value })}
          >
            {stocks.map((item) => (
              <option value={item.ts_code} key={item.ts_code}>
                {item.ts_code}
              </option>
            ))}
          </select>
        </label>
        <label className="field">
          <span className="lbl">条件</span>
          <select
            className="inp"
            value={editor.comparison}
            onChange={(event) =>
              onChange({ ...editor, comparison: event.target.value as Comparison })
            }
          >
            <option value="gte">达到或高于</option>
            <option value="lte">达到或低于</option>
          </select>
        </label>
        <label className="field">
          <span className="lbl">价格</span>
          <input
            className="inp num"
            inputMode="decimal"
            value={editor.threshold}
            onChange={(event) => onChange({ ...editor, threshold: event.target.value })}
          />
        </label>
        <label className="field">
          <span className="lbl">级别</span>
          <select
            className="inp"
            value={editor.priority}
            onChange={(event) => onChange({ ...editor, priority: event.target.value as Priority })}
          >
            <option value="P0">紧急</option>
            <option value="P1">高</option>
            <option value="P2">普通</option>
            <option value="P3">低</option>
          </select>
        </label>
        <label className="field">
          <span className="lbl">开始</span>
          <input
            className="inp"
            type="time"
            value={editor.validFrom}
            onChange={(event) => onChange({ ...editor, validFrom: event.target.value })}
          />
        </label>
        <label className="field">
          <span className="lbl">结束</span>
          <input
            className="inp"
            type="time"
            value={editor.validUntil}
            onChange={(event) => onChange({ ...editor, validUntil: event.target.value })}
          />
        </label>
      </div>
      <div className="price-rule-editor-foot">
        <Button type="submit" size="sm" variant="primary" disabled={!valid || submitting}>
          {submitting ? "正在保存" : "保存规则"}
        </Button>
      </div>
    </form>
  );
}

export function PriceRulePanel({
  viewer,
  generationId,
  fresh,
  refreshMeta,
}: {
  viewer: string | null;
  generationId: string | null;
  fresh: boolean;
  refreshMeta: () => void;
}) {
  const rules = usePriceAlertRules(viewer, generationId, fresh);
  const watchlist = useManualWatchlist();
  const [editor, setEditor] = useState<Editor | null>(null);
  const [submittingScope, setSubmittingScope] = useState<object | null>(null);
  const [deleting, setDeleting] = useState<{ scope: object; item: PriceRuleItem } | null>(null);
  const activeScope = useRef<object | null>(null);
  const scopeKey = JSON.stringify([viewer, generationId]);
  const scoped = useMemo(() => {
    const token = { scopeKey };
    return {
      token,
      session: new PriceRuleCommandSession({
        storage: (() => {
          try {
            return typeof navigator.locks?.request === "function" ? window.localStorage : null;
          } catch {
            return null;
          }
        })(),
        viewer,
        post: submitPriceRuleCommand,
        verifyNew: (draft) =>
          viewer === null ? Promise.resolve("stale" as const) : verifyPriceRuleBasis(viewer, draft),
        verifyOwner: () =>
          viewer === null ? Promise.resolve(false) : verifyPriceRuleOwner(viewer),
        nextId,
        now: () => new Date().toISOString(),
        withLock: browserLock,
        isCurrent: () => activeScope.current === token,
      }),
    };
  }, [viewer, scopeKey]);
  activeScope.current = scoped.token;
  const session = scoped.session;
  const activeEditor = editor?.scope === scoped.token ? editor : null;
  const activeDeleting = deleting?.scope === scoped.token ? deleting.item : null;
  const submitting = submittingScope === scoped.token;
  useEffect(() => {
    activeScope.current = scoped.token;
    setEditor(null);
    setDeleting(null);
    return () => {
      if (activeScope.current === scoped.token) activeScope.current = null;
    };
  }, [scoped]);
  const command = useSyncExternalStore(session.subscribe, session.snapshot, session.snapshot);
  useEffect(() => {
    if (viewer !== null) void session.resumePending();
  }, [session, viewer]);
  useEffect(() => {
    if (rules.state !== "ready" || !rules.serving) return;
    for (const [id, entry] of Object.entries(command.entries)) {
      const item = rules.items.find((row) => row.rule_id === id);
      if (entry.status === "saved_syncing" || entry.status === "published")
        session.resolveProjected(id, item, rules.serving.generation_id, rules.serving.built_at);
    }
  }, [session, command.entries, rules.items, rules.serving, rules.state]);

  const canWrite =
    fresh &&
    viewer !== null &&
    generationId !== null &&
    rules.state === "ready" &&
    command.storageAvailable;
  const stockReady =
    watchlist.state === "ready" &&
    watchlist.viewer === viewer &&
    watchlist.generationId === generationId;
  const stocks = stockReady ? watchlist.items : [];
  const visible = rules.items.filter((item) => !item.deleted);
  const recovery = Object.entries(command.entries).filter(
    ([id]) => !visible.some((item) => item.rule_id === id),
  );

  function refresh(): void {
    refreshMeta();
    rules.retry();
    watchlist.retry();
  }

  function startSave(): void {
    if (
      !activeEditor ||
      submitting ||
      !canWrite ||
      !stockReady ||
      generationId !== activeEditor.generationId
    )
      return;
    const stock = stocks.find((item) => item.ts_code === activeEditor.tsCode);
    if (!stock) return;
    const draft: PriceRuleCommandDraft = {
      kind: "save_price_alert_rule",
      generation_id: generationId,
      ts_code: stock.ts_code,
      membership_version: stock.version,
      expected_version: activeEditor.expectedVersion,
      rule: {
        rule_id: activeEditor.ruleId,
        name: activeEditor.name.trim(),
        priority: activeEditor.priority,
        enabled: activeEditor.enabled,
        comparison: activeEditor.comparison,
        threshold: activeEditor.threshold,
        valid_from: `${activeEditor.validFrom}:00`,
        valid_until: `${activeEditor.validUntil}:00`,
      },
    };
    const priorCommandId = session.snapshot().entries[activeEditor.ruleId]?.body.command_id;
    setSubmittingScope(scoped.token);
    void session
      .start(draft)
      .then(() => {
        if (activeScope.current !== scoped.token) return;
        const currentCommandId = session.snapshot().entries[activeEditor.ruleId]?.body.command_id;
        if (currentCommandId && currentCommandId !== priorCommandId)
          setEditor((current) => (current === activeEditor ? null : current));
      })
      .finally(() => setSubmittingScope((current) => (current === scoped.token ? null : current)));
  }

  function startToggle(item: PriceRuleItem, enabled: boolean): void {
    if (!canWrite || generationId === null) return;
    void session.start({
      kind: "set_price_alert_rule_enabled",
      generation_id: generationId,
      rule_id: item.rule_id,
      expected_version: item.version,
      enabled,
    });
  }

  function confirmDelete(): void {
    if (!activeDeleting || !canWrite || generationId === null) return;
    void session.start({
      kind: "delete_price_alert_rule",
      generation_id: generationId,
      rule_id: activeDeleting.rule_id,
      expected_version: activeDeleting.version,
    });
    setDeleting(null);
  }

  function retryFailed(entry: PriceRuleCommandEntry): void {
    if (
      entry.status !== "failed" ||
      !canWrite ||
      entry.body.generation_id !== generationId ||
      (entry.body.kind === "save_price_alert_rule" && !stockReady)
    )
      return;
    const { command_id: _commandId, requested_at: _requestedAt, ...draft } = entry.body;
    void session.start(draft);
  }

  return (
    <Panel
      title="价格提醒规则"
      label="价格提醒规则"
      actions={
        <>
          <Button size="sm" variant="ghost" onClick={refresh}>
            刷新规则
          </Button>
          <Button
            size="sm"
            variant="primary"
            disabled={!canWrite || !stockReady || stocks.length === 0}
            disabledReason={
              !canWrite || !stockReady
                ? "规则或手动名单暂不可用，请刷新。"
                : stocks.length === 0
                  ? "请先添加手动盯盘股票。"
                  : undefined
            }
            onClick={() => {
              if (generationId && stocks[0])
                setEditor({
                  scope: scoped.token,
                  generationId,
                  ruleId: nextId(),
                  expectedVersion: null,
                  tsCode: stocks[0].ts_code,
                  name: "",
                  priority: "P2",
                  comparison: "gte",
                  threshold: "",
                  validFrom: "09:30",
                  validUntil: "14:57",
                  enabled: true,
                });
            }}
          >
            新建规则
          </Button>
        </>
      }
    >
      <div className="price-rule-overview">
        <StatusBadge
          state="idle"
          label="价格提醒尚未运行"
          reason="规则可以先保存；价格评估与通知尚未接通。"
        />
        {command.message &&
        viewer !== null &&
        (rules.state === "ready" || Object.keys(command.entries).length > 0) ? (
          <p role="status" className="price-rule-message">
            {command.message}
          </p>
        ) : null}
      </div>
      {activeEditor ? (
        <RuleForm
          editor={activeEditor}
          stocks={stocks}
          generationId={generationId}
          submitting={submitting}
          onChange={setEditor}
          onSave={startSave}
          onCancel={() => setEditor(null)}
        />
      ) : null}
      {rules.state === "loading" ? <p className="hint">正在读取规则</p> : null}
      {rules.state === "not_ready" ? (
        <EmptyState
          title="规则尚未就绪，请稍后刷新。"
          hint={
            <Button size="sm" onClick={refresh}>
              重试
            </Button>
          }
        />
      ) : null}
      {rules.state === "unavailable" ? (
        <EmptyState
          title="规则暂不可用，请稍后重试。"
          hint={
            <Button size="sm" onClick={refresh}>
              重试
            </Button>
          }
        />
      ) : null}
      {rules.state === "ready" && visible.length === 0 && recovery.length === 0 ? (
        <EmptyState title="还没有价格规则" hint="从手动盯盘股票新建规则。" />
      ) : null}
      {visible.length > 0 ? (
        <div className="price-rule-column-head" aria-hidden="true">
          <span>开关</span>
          <span>规则</span>
          <span>条件</span>
          <span>时段</span>
          <span>状态</span>
          <span />
        </div>
      ) : null}
      {visible.length > 0 || recovery.length > 0 ? (
        <ul className="price-rule-list" aria-label="价格规则">
          {visible.map((item) => {
            const entry = command.entries[item.rule_id];
            const op = operation(entry);
            const busy = command.busyRuleIds.includes(item.rule_id);
            const blocked =
              !!entry &&
              entry.status !== "failed" &&
              (!op?.terminal || entry.body.generation_id === generationId);
            const validScope = item.scope_status === "valid";
            const name = item.name ?? item.ts_code ?? "未命名规则";
            const complete =
              item.version >= 1 &&
              item.membership_version !== null &&
              editorFor(item, generationId ?? "", scoped.token) !== null;
            const editable =
              complete &&
              validScope &&
              stockReady &&
              stocks.some(
                (stock) =>
                  stock.ts_code === item.ts_code && stock.version === item.membership_version,
              );
            return (
              <li className="price-rule-row" aria-label={name} key={item.rule_id}>
                <div className="price-rule-switch">
                  <Switch
                    checked={item.enabled === true}
                    label={`${name}规则开关`}
                    disabled={!canWrite || !validScope || !complete || blocked || busy}
                    onChange={(enabled) => startToggle(item, enabled)}
                  />
                </div>
                <div className="price-rule-identity">
                  <strong>{name}</strong>
                  <span className="mono">{item.ts_code ?? "—"}</span>
                </div>
                <div className="price-rule-condition">
                  <span className="hint price-rule-label">条件</span>
                  {item.comparison === "gte"
                    ? "达到或高于"
                    : item.comparison === "lte"
                      ? "达到或低于"
                      : "—"}{" "}
                  <span className="num">
                    {item.threshold === null ? "—" : formatPrice(Number(item.threshold))}
                  </span>
                </div>
                <div className="price-rule-time">
                  <span className="hint price-rule-label">时段</span>
                  {item.valid_from?.slice(0, 5) ?? "—"}–{item.valid_until?.slice(0, 5) ?? "—"}
                </div>
                <div className="price-rule-state">
                  {op ? (
                    <StatusBadge
                      state={op.terminal ? "warn" : "waiting"}
                      label={op.label}
                      reason={
                        op.terminal
                          ? entry?.status === "failed"
                            ? "可重新尝试，提交前会核对最新数据。"
                            : "刷新后可按最新数据重试。"
                          : "规则操作正在核对，显示的仍是已发布配置。"
                      }
                    />
                  ) : (
                    <StatusBadge
                      state={validScope ? "idle" : "warn"}
                      label={
                        validScope
                          ? "未运行"
                          : item.scope_status === "expired"
                            ? "名单已到期"
                            : "名单已变化"
                      }
                      reason={validScope ? "价格提醒尚未运行。" : "请检查手动盯盘名单。"}
                    />
                  )}
                </div>
                <div className="price-rule-actions">
                  {entry?.status === "failed" ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={
                        !canWrite ||
                        busy ||
                        entry.body.generation_id !== generationId ||
                        (entry.body.kind === "save_price_alert_rule" && !stockReady)
                      }
                      onClick={() => retryFailed(entry)}
                    >
                      重新尝试
                    </Button>
                  ) : entry && !op?.terminal ? (
                    <Button
                      size="sm"
                      variant="ghost"
                      disabled={busy}
                      onClick={() => {
                        if (entry.status === "published") refresh();
                        else void session.advance(item.rule_id);
                      }}
                    >
                      继续核对
                    </Button>
                  ) : null}
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={!canWrite || !editable || blocked || busy}
                    aria-label={`编辑${name}`}
                    onClick={() => {
                      const next = editorFor(item, generationId ?? "", scoped.token);
                      if (next) setEditor(next);
                    }}
                  >
                    编辑
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={!canWrite || !complete || blocked || busy}
                    aria-label={`删除${name}`}
                    onClick={() => setDeleting({ scope: scoped.token, item })}
                  >
                    删除
                  </Button>
                  <Tip content={`规则 ${item.rule_id} · 版本 ${item.version}`}>
                    <button type="button" className="price-rule-info" aria-label={`${name}详情`}>
                      ⓘ
                    </button>
                  </Tip>
                </div>
              </li>
            );
          })}
          {recovery.map(([id, entry]) => {
            const op = operation(entry);
            const label =
              entry.body.kind === "save_price_alert_rule" ? entry.body.rule.name : "待核对规则";
            return (
              <li className="price-rule-row price-rule-recovery" aria-label={label} key={id}>
                <div className="price-rule-identity">
                  <strong>{label}</strong>
                  <span className="hint">待核对操作</span>
                </div>
                <div className="price-rule-state">
                  <StatusBadge
                    state={op?.terminal ? "warn" : "waiting"}
                    label={op?.label ?? "状态待核对"}
                  />
                </div>
                {entry.status === "failed" ? (
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={
                      !canWrite ||
                      command.busyRuleIds.includes(id) ||
                      entry.body.generation_id !== generationId ||
                      (entry.body.kind === "save_price_alert_rule" && !stockReady)
                    }
                    onClick={() => retryFailed(entry)}
                  >
                    重新尝试
                  </Button>
                ) : !op?.terminal ? (
                  <Button
                    size="sm"
                    variant="ghost"
                    disabled={command.busyRuleIds.includes(id)}
                    onClick={() => {
                      if (entry.status === "published") refresh();
                      else void session.advance(id);
                    }}
                  >
                    继续核对
                  </Button>
                ) : (
                  <Button size="sm" variant="ghost" onClick={refresh}>
                    刷新规则
                  </Button>
                )}
              </li>
            );
          })}
        </ul>
      ) : null}
      <ConfirmDialog
        key={scopeKey}
        open={activeDeleting !== null}
        level="heavy"
        title="删除价格规则"
        description={`删除「${activeDeleting?.name ?? "这条规则"}」后，已发布的配置需等待页面数据更新。`}
        confirmLabel="删除规则"
        onConfirm={confirmDelete}
        onCancel={() => setDeleting(null)}
      />
    </Panel>
  );
}
