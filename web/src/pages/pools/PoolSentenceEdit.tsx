import { useEffect, useRef, useState } from "react";
import { useAiCapabilities } from "@/api/aiAssistance";
import type { EditablePool, PoolNlPreview, PoolRuleChange } from "@/api/poolEditor";
import type { ScreenBlock } from "@/api/screen";
import { useCurrentMeta } from "@/api/useMeta";
import { useAiGeneration } from "@/app/aiAssistanceSession";
import { Button, Tip } from "@/ui";

type RuleCall = PoolNlPreview["rule_calls"][number];

function valueLabel(value: unknown, parameter: ScreenBlock["parameters"][number]): string {
  if (value == null || value === "") return "未设置";
  if (Array.isArray(value))
    return value
      .map(
        (item) => parameter.options?.find((option) => option.value === item)?.label ?? "原有选项",
      )
      .join("、");
  const option = parameter.options?.find((item) => item.value === String(value));
  if (option) return option.label;
  if (typeof value === "number") return String(value * (parameter.scale || 1));
  return "原有数据项";
}

function ruleDescription(rule: RuleCall | null, block: ScreenBlock | undefined): string {
  if (!rule || !block) return "";
  const values = block.parameters.map(
    (parameter) => `${parameter.label} ${valueLabel(rule.args[parameter.key], parameter)}`,
  );
  return values.join(" · ");
}

function changeDescription(change: PoolRuleChange, block: ScreenBlock | undefined): string {
  if (change.kind !== "parameter_changed" || !change.before || !change.after || !block)
    return ruleDescription(change.after ?? change.before ?? null, block);
  const changed = block.parameters
    .filter(
      (parameter) =>
        JSON.stringify(change.before?.args[parameter.key]) !==
        JSON.stringify(change.after?.args[parameter.key]),
    )
    .map(
      (parameter) =>
        `${parameter.label} ${valueLabel(change.before?.args[parameter.key], parameter)} → ${valueLabel(change.after?.args[parameter.key], parameter)}`,
    );
  return changed.length ? changed.join(" · ") : "参数已调整";
}

export function PoolSentenceEdit(props: Parameters<typeof PoolSentenceEditBody>[0]) {
  const viewer = useCurrentMeta().data?.data.viewer ?? null;
  return <PoolSentenceEditBody key={viewer} {...props} />;
}

function PoolSentenceEditBody({
  pool,
  generationId,
  verifiedVersion,
  available,
  busy,
  rulesChanged,
  ruleRevision,
  metadataRevision,
  blocks,
  startOpen,
  onApply,
  onUndo,
}: {
  pool: EditablePool;
  generationId: string | null;
  verifiedVersion: string | null;
  available: boolean;
  busy: boolean;
  rulesChanged: boolean;
  ruleRevision: number;
  metadataRevision: number;
  blocks: ScreenBlock[];
  startOpen: boolean;
  onApply: (rules: PoolNlPreview["rule_calls"]) => void;
  onUndo: () => void;
}) {
  const [open, setOpen] = useState(startOpen);
  const [instruction, setInstruction] = useState("");
  const [suggestion, setSuggestion] = useState<PoolNlPreview | null>(null);
  const [applied, setApplied] = useState(false);
  const viewer = useCurrentMeta().data?.data.viewer ?? null;
  const request = useAiGeneration(viewer, `pool:${pool.key}`);
  const capability = useAiCapabilities(viewer, open);
  const loading = request.busy;
  const [error, setError] = useState<string | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const requestedBase = useRef<string | null>(null);
  const ruleBase = `${generationId}:${verifiedVersion}:${ruleRevision}`;
  const base = `${ruleBase}:${metadataRevision}`;
  const baseRef = useRef(base);
  const ruleBaseRef = useRef(ruleBase);
  const blockMap = new Map(blocks.map((block) => [block.key, block]));
  const seenChanges = new Map<string, number>();

  useEffect(() => {
    if (open) textareaRef.current?.focus();
  }, [open]);

  useEffect(() => {
    if (baseRef.current === base) return;
    const ruleBaseChanged = ruleBaseRef.current !== ruleBase;
    baseRef.current = base;
    ruleBaseRef.current = ruleBase;
    requestedBase.current = null;
    setSuggestion(null);
    if (ruleBaseChanged) setApplied(false);
    setError(null);
  }, [base, ruleBase]);

  const unavailableReason =
    !generationId || verifiedVersion !== pool.version
      ? "规则正在更新，请重新打开后再试。"
      : rulesChanged && !applied
        ? "已有未保存的条件修改。请先保存或重新打开，再使用一句话修改。"
        : applied
          ? "先核对或撤销已应用的建议。"
          : busy
            ? "当前保存尚未完成，请稍后再试。"
            : undefined;

  const discardSuggestion = () => {
    requestedBase.current = null;
    setSuggestion(null);
    setError(null);
  };

  const requestPreview = async () => {
    requestedBase.current = base;
    setError(null);
    setSuggestion(null);
    if (request.original) {
      if (request.view?.state === "reserved" || request.errorStatus === 404) {
        await request.generate(request.original);
      } else {
        await request.lookup();
      }
      return;
    }
    const trimmed = instruction.trim();
    if (
      !available ||
      !generationId ||
      unavailableReason ||
      !viewer ||
      !capability.data?.can_generate ||
      !trimmed ||
      trimmed.length > 500
    )
      return;
    await request.generate({
      purpose: "pool_edit",
      request_id: crypto.randomUUID(),
      pool_key: pool.key,
      generation_id: generationId,
      expected_version: pool.version,
      instruction: trimmed,
    });
  };

  useEffect(() => {
    const result = request.view?.result;
    if (result?.purpose !== "pool_edit" || requestedBase.current !== base) return;
    const current = result.base;
    const fullBase = (value: EditablePool) =>
      JSON.stringify([
        value.key,
        value.version,
        value.display_name,
        value.description,
        value.depends_on,
        value.delay_days,
        value.rule_calls,
        value.include_columns,
        value.ranking,
      ]);
    if (
      result.base_generation_id !== generationId ||
      fullBase(current) !== fullBase(pool) ||
      rulesChanged
    ) {
      setSuggestion(null);
      setError("规则已更新，请重新打开后再试。");
      return;
    }
    if (!result.changes.length || !result.rule_calls.length) {
      setError("没有识别到变化，请说清要调整的条件。");
      return;
    }
    setSuggestion({
      pool_key: current.key,
      base_version: current.version,
      base_generation_id: result.base_generation_id,
      rule_calls: result.rule_calls,
      changes: result.changes,
      message: result.message,
    });
  }, [request.view, base, generationId, pool, rulesChanged]);

  const apply = () => {
    if (
      !suggestion ||
      baseRef.current !== base ||
      rulesChanged ||
      suggestion.base_generation_id !== generationId ||
      suggestion.base_version !== verifiedVersion
    ) {
      setSuggestion(null);
      setError("规则已更新，请重新打开后再试。");
      return;
    }
    onApply(suggestion.rule_calls);
    setSuggestion(null);
    setApplied(true);
    setError(null);
  };

  return (
    <section className="pool-sentence-edit" aria-label="一句话修改">
      <div className="pool-editor-section-head">
        <h3>一句话修改</h3>
        <Button
          size="sm"
          aria-expanded={open}
          onClick={() => {
            if (open) discardSuggestion();
            setOpen((current) => !current);
          }}
        >
          {open ? "收起" : "用一句话改池子"}
        </Button>
      </div>
      {open ? (
        !available && !request.original ? (
          <p className="pools-note" role="status">
            暂不能生成，仍可手动编辑
          </p>
        ) : (
          <div className="pool-sentence-body">
            <label className="field" htmlFor="pool-sentence-instruction">
              <span className="lbl">修改描述</span>
              <textarea
                ref={textareaRef}
                className="inp pool-sentence-input"
                id="pool-sentence-instruction"
                rows={3}
                maxLength={500}
                placeholder="例如：把放量倍数改为 3，再排除 ST"
                value={instruction}
                onChange={(event) => {
                  setInstruction(event.target.value);
                  discardSuggestion();
                }}
              />
            </label>
            <div className="pool-sentence-actions">
              <Button
                variant="primary"
                size="sm"
                disabledReason={
                  loading
                    ? "正在生成，请稍候。"
                    : request.original
                      ? undefined
                      : (unavailableReason ??
                        (capability.data?.can_generate
                          ? !instruction.trim()
                            ? "先写一句修改描述。"
                            : undefined
                          : (capability.data?.message ?? "调用尚未启用。")))
                }
                onClick={() => void requestPreview()}
              >
                {loading
                  ? "正在查看…"
                  : request.original
                    ? request.view?.state === "reserved" || request.errorStatus === 404
                      ? "继续生成原请求"
                      : "继续查看原请求"
                    : "解析并预览"}
              </Button>
              {request.original && (request.view?.state === "completed" || request.absent) ? (
                <Button
                  size="sm"
                  variant="ghost"
                  onClick={() => {
                    request.reset();
                    discardSuggestion();
                  }}
                >
                  新建描述
                </Button>
              ) : null}
              <Tip content="建议先应用到草稿，核对规则后再保存。">
                <span className="pools-info">如何生效</span>
              </Tip>
            </div>
            {unavailableReason && !applied ? (
              <p className="pools-note">{unavailableReason}</p>
            ) : null}
            {error || request.error ? (
              <p className="pool-editor-error" role="alert">
                {error ??
                  (request.errorStatus === 409 ? "规则已更新，请重新打开后再试。" : request.error)}
              </p>
            ) : null}
            {suggestion ? (
              <section className="pool-sentence-preview" aria-label="建议预览">
                <div className="pool-sentence-preview-head">
                  <strong>建议变更</strong>
                  <span>{suggestion.changes.length} 项</span>
                </div>
                <ul>
                  {suggestion.changes.map((change) => {
                    const block = blockMap.get(change.after?.name ?? change.before?.name ?? "");
                    const description = changeDescription(change, block);
                    const signature = JSON.stringify(change);
                    const occurrence = seenChanges.get(signature) ?? 0;
                    seenChanges.set(signature, occurrence + 1);
                    return (
                      <li key={`${signature}:${occurrence}`}>
                        <span className={`pool-sentence-kind pool-sentence-${change.kind}`}>
                          {change.kind === "added"
                            ? "新增"
                            : change.kind === "removed"
                              ? "移除"
                              : "改参数"}
                        </span>
                        <span className="pool-sentence-change">
                          <strong>{block?.label ?? change.label}</strong>
                          {description ? <small>{description}</small> : null}
                        </span>
                      </li>
                    );
                  })}
                </ul>
                {suggestion.message ? <p className="pools-note">{suggestion.message}</p> : null}
                <div className="pool-sentence-preview-foot">
                  <span>
                    命中数 <strong>暂无法估算</strong>
                  </span>
                  <Button size="sm" variant="primary" onClick={apply}>
                    应用到草稿
                  </Button>
                </div>
              </section>
            ) : null}
            {applied ? (
              <div className="pool-sentence-applied" role="status">
                <span>已应用到草稿，请核对条件。</span>
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
        )
      ) : null}
    </section>
  );
}
