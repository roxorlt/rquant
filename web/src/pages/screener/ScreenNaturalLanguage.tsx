import { useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import {
  fetchScreenNlPreview,
  type ScreenBlock,
  type ScreenCatalogData,
  type ScreenNlPreviewData,
} from "@/api/screen";
import { Button, Panel, type ParameterValue, Tip } from "@/ui";

type ConditionDescription = { label: string; parameters: string[] };
export type EditableScreenCondition = { key: string; args: Record<string, ParameterValue> };
const RECENT_DESCRIPTIONS_KEY = "rquant.screen.recent-descriptions.v1";
const RECENT_LIMIT = 5;
const DESCRIPTION_LIMIT = 500;

function readRecentDescriptions(): string[] {
  try {
    const raw = window.sessionStorage.getItem(RECENT_DESCRIPTIONS_KEY);
    if (!raw || raw.length > 16_384) return [];
    const stored: unknown = JSON.parse(raw);
    if (!Array.isArray(stored)) return [];
    const descriptions: string[] = [];
    for (const item of stored.slice(0, RECENT_LIMIT)) {
      if (
        typeof item === "string" &&
        item.trim().length > 0 &&
        item.length <= DESCRIPTION_LIMIT &&
        !descriptions.includes(item)
      ) {
        descriptions.push(item);
      }
    }
    return descriptions;
  } catch {
    return [];
  }
}

function offsetLabel(offset: number): string {
  return offset === 1 ? "前一交易日" : `前 ${offset} 个交易日`;
}

function valueLabel(value: unknown, parameter: ScreenBlock["parameters"][number]): string | null {
  if (value === null || value === "") return "未设置";
  if (Array.isArray(value)) {
    const labels = value.map((item) => valueLabel(item, parameter));
    return labels.every((label) => label !== null) ? labels.join("、") : null;
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) return null;
    if (parameter.key === "offset") {
      return value === 0 ? "所选交易日" : offsetLabel(value);
    }
    return new Intl.NumberFormat("zh-CN", { maximumFractionDigits: 4 }).format(
      value * (parameter.scale || 1),
    );
  }
  if (typeof value !== "string") return null;
  const option = parameter.options?.find((item) => item.value === value);
  if (option) return option.label;
  if (parameter.custom_ma) {
    const derived = /^(MA|RSI)(\d{1,3})\[(\d{1,2})\]$/.exec(value);
    if (derived) {
      const offset = Number(derived[3]);
      const label = `${derived[2]} 日${derived[1] === "MA" ? "均线" : " RSI"}`;
      return offset === 0 ? label : `${offsetLabel(offset)}${label}`;
    }
  }
  const shifted = /^([A-Z_][A-Z0-9_]*)\[(\d{1,2})\]$/.exec(value);
  if (shifted) {
    const base = parameter.options?.find((item) => item.value === `${shifted[1]}[0]`);
    if (base) {
      const offset = Number(shifted[2]);
      return offset === 0 ? base.label : `${offsetLabel(offset)}${base.label}`;
    }
  }
  return null;
}

function editableValue(value: unknown): ParameterValue | undefined {
  if (value === null || typeof value === "string") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value) && value.every((item) => typeof item === "string")) return value;
  return undefined;
}

function describeConditions(
  conditions: ScreenNlPreviewData["conditions"],
  blocks: ScreenBlock[],
): { conditions: EditableScreenCondition[]; descriptions: ConditionDescription[] } | null {
  if (conditions.length === 0 || conditions.length > 26) return null;
  const byKey = new Map(blocks.map((block) => [block.key, block]));
  const descriptions: ConditionDescription[] = [];
  const editable: EditableScreenCondition[] = [];
  for (const condition of conditions) {
    const block = byKey.get(condition.key);
    const rawArgs = condition.args ?? {};
    if (!block || Object.keys(rawArgs).some((key) => !block.parameters.some((p) => p.key === key)))
      return null;
    const parameters: string[] = [];
    const args: Record<string, ParameterValue> = {};
    for (const parameter of block.parameters) {
      if (parameter.required && rawArgs[parameter.key] == null) return null;
      const value = editableValue(
        rawArgs[parameter.key] === undefined ? parameter.initial : rawArgs[parameter.key],
      );
      if (value === undefined) return null;
      if (parameter.input === "multi_choice" && value !== null && !Array.isArray(value))
        return null;
      if (parameter.input !== "multi_choice" && Array.isArray(value)) return null;
      if (parameter.input === "integer" && value !== null && !Number.isInteger(value)) return null;
      if (
        (parameter.input === "number" || parameter.input === "integer") &&
        value !== null &&
        typeof value !== "number"
      )
        return null;
      const label = valueLabel(value, parameter);
      if (label === null) return null;
      args[parameter.key] = value;
      parameters.push(`${parameter.label} ${label}`);
    }
    descriptions.push({ label: block.label, parameters });
    editable.push({ key: condition.key, args });
  }
  return { conditions: editable, descriptions };
}

function errorText(error: unknown): string {
  if (!(error instanceof ApiError)) return "暂无法生成，请稍后重试。";
  if (error.status === 401 || error.status === 403) return "请先登录，再试一次。";
  if (error.status === 409) return "选股数据已更新，请刷新后再试。";
  if (error.status === 422) {
    return error.message === "请先选择想筛选的日期。"
      ? "请先选择想筛选的日期。"
      : "没能确定条件，请说清筛选范围和数值。";
  }
  if (error.status === 429) return "请求太频繁，请稍后再试。";
  return "暂无法生成，请稍后重试。";
}

export function ScreenNaturalLanguage({
  available,
  sourceKind,
  sourceIdentity,
  tradeDate,
  conditionRevision,
  successfulRunRevision,
  blocks,
  onApply,
  onUndo,
  onConflict,
}: {
  available: boolean;
  sourceKind: ScreenCatalogData["source_kind"] | null;
  sourceIdentity: string | null;
  tradeDate: string | null;
  conditionRevision: number;
  successfulRunRevision: number;
  blocks: ScreenBlock[];
  onApply: (conditions: EditableScreenCondition[]) => void;
  onUndo: () => void;
  onConflict: () => void;
}) {
  const [instruction, setInstruction] = useState("");
  const [suggestion, setSuggestion] = useState<{
    conditions: EditableScreenCondition[];
    descriptions: ConditionDescription[];
  } | null>(null);
  const [applied, setApplied] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [recentDescriptions, setRecentDescriptions] = useState(readRecentDescriptions);
  const recentRef = useRef(recentDescriptions);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const controllerRef = useRef<AbortController | null>(null);
  const requestId = useRef(0);
  const context = JSON.stringify({
    available,
    sourceKind,
    sourceIdentity,
    tradeDate,
    conditionRevision,
    blocks,
  });
  const contextRef = useRef(context);
  const runRevisionRef = useRef(successfulRunRevision);

  useEffect(() => {
    if (runRevisionRef.current === successfulRunRevision) return;
    runRevisionRef.current = successfulRunRevision;
    setApplied(false);
  }, [successfulRunRevision]);

  useEffect(() => {
    if (contextRef.current === context) return;
    contextRef.current = context;
    requestId.current += 1;
    controllerRef.current?.abort();
    setSuggestion(null);
    setApplied(false);
    setLoading(false);
    setError(null);
  }, [context]);

  useEffect(() => () => controllerRef.current?.abort(), []);

  const discardPending = () => {
    requestId.current += 1;
    controllerRef.current?.abort();
    setSuggestion(null);
    setLoading(false);
    setError(null);
  };

  const rememberDescription = (description: string) => {
    if (!description.trim() || description.length > DESCRIPTION_LIMIT) return;
    const next = [description, ...recentRef.current.filter((item) => item !== description)].slice(
      0,
      RECENT_LIMIT,
    );
    recentRef.current = next;
    setRecentDescriptions(next);
    try {
      window.sessionStorage.setItem(RECENT_DESCRIPTIONS_KEY, JSON.stringify(next));
    } catch {
      // The current tab still keeps the in-memory list when storage is unavailable.
    }
  };

  const recallDescription = (description: string) => {
    discardPending();
    setInstruction(description);
    textareaRef.current?.focus();
  };

  const generate = async () => {
    const trimmed = instruction.trim();
    if (!available || !sourceKind || !sourceIdentity || !tradeDate || !trimmed || applied) return;
    const id = ++requestId.current;
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    const requestedContext = context;
    const timeout = window.setTimeout(() => controller.abort(), 20_000);
    setLoading(true);
    setError(null);
    setSuggestion(null);
    try {
      const data = await fetchScreenNlPreview(
        {
          source_kind: sourceKind,
          source_identity: sourceIdentity,
          trade_date: tradeDate,
          instruction: trimmed,
        },
        controller.signal,
      );
      if (id !== requestId.current || requestedContext !== contextRef.current) return;
      if (
        data.source_kind !== sourceKind ||
        data.source_identity !== sourceIdentity ||
        data.trade_date !== tradeDate
      ) {
        setError("选股数据已更新，请刷新后再试。");
        onConflict();
        return;
      }
      const preview = describeConditions(data.conditions, blocks);
      if (preview === null) {
        setError("条件资料已变化，请刷新后再试。");
        return;
      }
      setSuggestion(preview);
      rememberDescription(instruction);
    } catch (caught) {
      if (id !== requestId.current || requestedContext !== contextRef.current) return;
      if (caught instanceof ApiError && caught.status === 409) onConflict();
      setError(controller.signal.aborted ? "生成超时，请稍后重试。" : errorText(caught));
    } finally {
      window.clearTimeout(timeout);
      if (id === requestId.current) setLoading(false);
    }
  };

  const apply = () => {
    if (!suggestion || contextRef.current !== context) {
      setSuggestion(null);
      setError("选股数据已更新，请刷新后再试。");
      return;
    }
    onApply(suggestion.conditions);
    setSuggestion(null);
    setApplied(true);
  };

  const seen = new Map<string, number>();

  return (
    <Panel title="用一句话描述">
      {!available ? (
        <p className="screen-nl-note">暂不能生成，仍可手动添加条件</p>
      ) : !sourceKind || !sourceIdentity || !tradeDate ? (
        <p className="screen-nl-note">选股数据正在更新，请刷新后再试。</p>
      ) : (
        <div className="screen-nl">
          <div className="screen-nl-compose">
            <label className="field">
              <span className="lbl">选股描述</span>
              <textarea
                ref={textareaRef}
                className="inp"
                rows={2}
                maxLength={DESCRIPTION_LIMIT}
                placeholder="例如：排除 ST，流通市值低于 80 亿"
                value={instruction}
                onChange={(event) => {
                  setInstruction(event.target.value);
                  discardPending();
                }}
              />
            </label>
            <Button
              variant="primary"
              disabledReason={
                loading
                  ? "正在生成，请稍候。"
                  : applied
                    ? "先核对或撤销已应用的条件。"
                    : !instruction.trim()
                      ? "先写一句选股描述。"
                      : undefined
              }
              onClick={() => void generate()}
            >
              {loading ? "正在生成…" : "生成条件"}
            </Button>
          </div>
          <section className="screen-nl-recent" aria-label="最近描述">
            <Tip content="仅保留当前标签页成功生成的描述。">
              <strong className="screen-nl-recent-title">最近描述</strong>
            </Tip>
            {recentDescriptions.length === 0 ? (
              <span className="screen-nl-note">暂无</span>
            ) : (
              <div className="screen-nl-recent-list">
                {recentDescriptions.map((description) => (
                  <button
                    key={description}
                    type="button"
                    className="screen-nl-recent-item"
                    title={description}
                    onClick={() => recallDescription(description)}
                  >
                    {description}
                  </button>
                ))}
              </div>
            )}
          </section>
          {loading ? (
            <p className="screen-nl-note" role="status">
              正在生成条件…
            </p>
          ) : null}
          {error ? (
            <p className="screen-notice error" role="alert">
              {error}
            </p>
          ) : null}
          {suggestion ? (
            <section className="screen-nl-preview" aria-label="建议条件">
              <div className="screen-nl-preview-head">
                <strong>建议条件</strong>
                <span>{suggestion.descriptions.length} 条</span>
              </div>
              <ol>
                {suggestion.descriptions.map((description, index) => {
                  const signature = JSON.stringify(description);
                  const occurrence = seen.get(signature) ?? 0;
                  seen.set(signature, occurrence + 1);
                  return (
                    <li key={`${signature}:${occurrence}`}>
                      <span className="screen-nl-index num">{index + 1}</span>
                      <span>
                        <strong>{description.label}</strong>
                        {description.parameters.length ? (
                          <small className="screen-nl-parameter-list">
                            {description.parameters.join(" · ")}
                          </small>
                        ) : null}
                      </span>
                    </li>
                  );
                })}
              </ol>
              <div className="screen-nl-preview-actions">
                <Tip content="先核对建议，再手动运行筛选。">
                  <span className="screen-nl-help">如何生效</span>
                </Tip>
                <Button variant="primary" size="sm" onClick={apply}>
                  应用到条件
                </Button>
              </div>
            </section>
          ) : null}
          {applied ? (
            <div className="screen-nl-applied" role="status">
              <span>已加入条件，请核对后运行筛选。</span>
              <Button
                size="sm"
                onClick={() => {
                  onUndo();
                  setApplied(false);
                }}
              >
                撤销应用
              </Button>
            </div>
          ) : null}
        </div>
      )}
    </Panel>
  );
}
