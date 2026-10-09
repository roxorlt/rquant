import { useEffect, useRef, useState } from "react";
import { useAiCapabilities } from "@/api/aiAssistance";
import type { Schemas } from "@/api/client";
import { useAiGeneration } from "@/app/aiAssistanceSession";

type ScreenRankingPlan = Schemas["ScreenRankingPlan"];

import type { ScreenBlock, ScreenNlPreviewData, ScreenNlPreviewRequest } from "@/api/screen";
import { Button, Panel, type ParameterValue, Tip } from "@/ui";

type ConditionDescription = { label: string; parameters: string[] };
export type EditableScreenCondition = { key: string; args: Record<string, ParameterValue> };
const RECENT_DESCRIPTIONS_KEY = "rquant.screen.recent-descriptions.v1";
const RECENT_LIMIT = 5;
const DESCRIPTION_LIMIT = 500;

function readRecentDescriptions(scope: string | null): string[] {
  try {
    if (scope === null) return [];
    const raw = window.sessionStorage.getItem(`${RECENT_DESCRIPTIONS_KEY}:${scope}`);
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

export function describeConditions(
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
      if (
        parameter.input === "choice" &&
        !parameter.custom_ma &&
        value !== null &&
        parameter.options?.length &&
        !parameter.options.some((option) => option.value === String(value))
      )
        return null;
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

export function ScreenNaturalLanguage({
  ownerScope = null,
  description = "",
  onDescriptionChange,
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
  ownerScope?: string | null;
  description?: string;
  onDescriptionChange?: (value: string) => void;
  available: boolean;
  sourceKind: ScreenNlPreviewRequest["source_kind"] | null;
  sourceIdentity: string | null;
  tradeDate: string | null;
  conditionRevision: number;
  successfulRunRevision: number;
  blocks: ScreenBlock[];
  onApply: (conditions: EditableScreenCondition[], ranking?: ScreenRankingPlan | null) => void;
  onUndo: () => void;
  onConflict: () => void;
}) {
  const [instruction, setInstruction] = useState(description);
  const [suggestion, setSuggestion] = useState<
    | (NonNullable<ReturnType<typeof describeConditions>> & { ranking: ScreenRankingPlan | null })
    | null
  >(null);
  const [applied, setApplied] = useState(false);
  const [recent, setRecent] = useState(() => readRecentDescriptions(ownerScope));
  const area = useRef<HTMLTextAreaElement>(null);
  const request = useAiGeneration(ownerScope, "screen");
  const capabilities = useAiCapabilities(ownerScope);
  const context = JSON.stringify({ sourceKind, sourceIdentity, tradeDate, conditionRevision });
  const requestedContext = useRef<string | null>(null);
  const recorded = useRef("");
  const conflict = useRef(onConflict);
  conflict.current = onConflict;
  const reported = useRef("");
  useEffect(() => {
    setInstruction(description);
  }, [description]);
  useEffect(() => {
    setRecent(readRecentDescriptions(ownerScope));
    setSuggestion(null);
    setApplied(false);
  }, [ownerScope]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: A changed condition context discards the old suggestion.
  useEffect(() => {
    setSuggestion(null);
    setApplied(false);
  }, [context]);
  // biome-ignore lint/correctness/useExhaustiveDependencies: A successful run ends the pending apply state.
  useEffect(() => {
    setApplied(false);
  }, [successfulRunRevision]);
  useEffect(() => {
    const result = request.view?.result;
    if (result?.purpose !== "screen" || requestedContext.current !== context) return;
    const definition = result.definition;
    if (
      definition.source_kind !== sourceKind ||
      definition.source_identity !== sourceIdentity ||
      definition.trade_date !== tradeDate
    ) {
      setSuggestion(null);
      const key = `${request.view?.request_id}:${sourceIdentity}:${tradeDate}`;
      if (reported.current !== key) {
        reported.current = key;
        conflict.current();
      }
      return;
    }
    const preview = describeConditions(
      definition.conditions.map((call) => ({ key: call.name, args: call.args })),
      blocks,
    );
    if (preview) {
      setSuggestion({ ...preview, ranking: definition.ranking ?? null });
      if (
        request.view &&
        recorded.current !== request.view.request_id &&
        request.original?.purpose === "screen" &&
        ownerScope
      ) {
        recorded.current = request.view.request_id;
        const instruction = request.original.instruction;
        const next = [
          instruction,
          ...readRecentDescriptions(ownerScope).filter((value) => value !== instruction),
        ].slice(0, RECENT_LIMIT);
        setRecent(next);
        try {
          sessionStorage.setItem(`${RECENT_DESCRIPTIONS_KEY}:${ownerScope}`, JSON.stringify(next));
        } catch {
          /* The saved original request remains available. */
        }
      }
    } else setSuggestion(null);
  }, [
    request.view,
    request.original,
    sourceKind,
    sourceIdentity,
    tradeDate,
    blocks,
    context,
    ownerScope,
  ]);
  const reason = !ownerScope
    ? "请先登录。"
    : !available
      ? "暂不能生成，仍可手动添加条件"
      : capabilities.data?.can_generate !== true
        ? (capabilities.data?.message ?? "正在查看助手状态。")
        : undefined;
  async function generate() {
    if (
      reason ||
      !ownerScope ||
      !sourceKind ||
      !sourceIdentity ||
      !tradeDate ||
      !instruction.trim()
    )
      return;
    const body = {
      purpose: "screen" as const,
      request_id: crypto.randomUUID(),
      instruction: instruction.trim(),
      source_kind: sourceKind,
      source_identity: sourceIdentity,
      trade_date: tradeDate,
      include_ranking: true,
    };
    setSuggestion(null);
    setApplied(false);
    requestedContext.current = context;
    await request.generate(body);
  }
  if (!available && !request.original)
    return (
      <Panel title="用一句话描述">
        <p className="screen-nl-note">暂不能生成，仍可手动添加条件</p>
      </Panel>
    );
  return (
    <Panel title="用一句话描述">
      {reason ? <p className="screen-nl-note">{reason}</p> : null}
      <div className="screen-nl">
        <label className="field">
          <span className="lbl">选股描述</span>
          <textarea
            ref={area}
            className="inp"
            rows={2}
            maxLength={500}
            value={instruction}
            onChange={(event) => {
              requestedContext.current = null;
              setSuggestion(null);
              setInstruction(event.target.value);
              onDescriptionChange?.(event.target.value);
            }}
            onKeyDown={(event) => {
              if (event.key === "Enter" && (event.metaKey || event.ctrlKey)) {
                event.preventDefault();
                void generate();
              }
            }}
            placeholder="例如：非 ST，市值较小，近期放量上涨"
          />
        </label>
        <div className="row">
          {request.original ? (
            <>
              <Button
                size="sm"
                onClick={() => {
                  requestedContext.current = context;
                  if (
                    (request.view?.state === "reserved" || request.errorStatus === 404) &&
                    request.original
                  ) {
                    void request.generate(request.original);
                  } else {
                    void request.lookup();
                  }
                }}
                disabled={request.busy}
              >
                {request.view?.state === "reserved" || request.errorStatus === 404
                  ? "继续生成原请求"
                  : "继续查看原请求"}
              </Button>
              <Button
                size="sm"
                variant="ghost"
                disabled={request.busy || (!request.absent && request.view?.state !== "completed")}
                onClick={() => {
                  request.reset();
                  setSuggestion(null);
                  setApplied(false);
                }}
              >
                新建描述
              </Button>
            </>
          ) : (
            <Button
              onClick={() => void generate()}
              disabledReason={reason}
              disabled={
                !instruction.trim() || request.busy || !sourceKind || !sourceIdentity || !tradeDate
              }
            >
              生成建议
            </Button>
          )}
          <Tip content="只生成建议。条件和排名可编辑，确认后才会实际筛选。⌘ / Ctrl + Enter 生成。">
            <span className="screen-help" role="img" aria-label="生成建议说明">
              ⓘ
            </span>
          </Tip>
        </div>
        {request.busy ? (
          <p role="status" className="hint">
            正在生成建议…
          </p>
        ) : null}
        {request.error ? (
          <p role="alert" className="hint">
            {request.error}
          </p>
        ) : null}
        {suggestion ? (
          <section className="screen-nl-preview" aria-label="建议条件">
            {suggestion.descriptions.map((item, index) => (
              // biome-ignore lint/suspicious/noArrayIndexKey: The immutable suggestion owns these stateless rows until it is discarded.
              <div key={`${item.label}:${index}`}>
                <b>{item.label}</b>
                <span className="hint"> {item.parameters.join(" · ")}</span>
              </div>
            ))}
            <p className="hint">排名 {suggestion.ranking?.conditions.length ?? 0} 项</p>
            <Button
              onClick={() => {
                onApply(suggestion.conditions, suggestion.ranking);
                setSuggestion(null);
                setApplied(true);
              }}
            >
              应用到条件
            </Button>
          </section>
        ) : null}
        {applied ? (
          <div className="row">
            <span role="status">已加入条件，请核对后运行筛选。</span>
            <Button
              size="sm"
              variant="ghost"
              onClick={() => {
                onUndo();
                setApplied(false);
              }}
            >
              撤销应用
            </Button>
          </div>
        ) : null}
        <section aria-label="最近描述">
          <span className="hint">最近描述</span>
          <div className="row">
            {recent.map((value) => (
              <Button
                key={value}
                size="sm"
                variant="ghost"
                onClick={() => {
                  requestedContext.current = null;
                  setSuggestion(null);
                  setInstruction(value);
                  onDescriptionChange?.(value);
                  area.current?.focus();
                }}
              >
                {value}
              </Button>
            ))}
          </div>
        </section>
      </div>
    </Panel>
  );
}
