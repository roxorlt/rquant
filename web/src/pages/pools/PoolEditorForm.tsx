import { useEffect, useMemo, useRef, useState } from "react";
import type { PublishedPool } from "@/api/endpoints";
import type {
  BuiltinPoolCopySource,
  EditableCanvas,
  EditablePool,
  PoolNlPreview,
} from "@/api/poolEditor";
import { isPoolRankingMetric, type PoolRankingMetric } from "@/api/poolEditor";
import {
  catalogUsableForGeneration,
  isFundamentalScreenField,
  type ScreenBlock,
  type ScreenOption,
  useScreenCatalog,
} from "@/api/screen";
import { Button, ParamControl, type ParameterValue, SideDrawer, Tip } from "@/ui";
import type { PublicationStage } from "./editorPublication";
import type { EditorSessionSnapshot, PoolEditorSession, SaveInput } from "./editorSession";
import { PoolSentenceEdit } from "./PoolSentenceEdit";

type RuleDraft = { id: number; key: string; args: Record<string, ParameterValue> };
type RankDraft = { id: number; metric: PoolRankingMetric; ascending: boolean; weight: string };
type Mode =
  | { kind: "create"; parentKey: string | null }
  | { kind: "edit"; pool: EditablePool }
  | { kind: "copy"; source: BuiltinPoolCopySource };

function initialRules(pool: Pick<EditablePool, "rule_calls"> | null): RuleDraft[] {
  return (
    pool?.rule_calls.map((rule, index) => ({
      id: index + 1,
      key: rule.name,
      args: rule.args as Record<string, ParameterValue>,
    })) ?? []
  );
}

function ruleKey(rule: { name: string; args: Record<string, unknown> }): string {
  return JSON.stringify([
    rule.name,
    Object.entries(rule.args).sort(([left], [right]) => left.localeCompare(right)),
  ]);
}

function reconcileSuggestion(
  original: RuleDraft[],
  candidate: PoolNlPreview["rule_calls"],
): RuleDraft[] {
  const available = [...original];
  const matched = new Map<number, RuleDraft>();
  let nextId = Math.max(0, ...original.map((rule) => rule.id)) + 1;
  candidate.forEach((rule, index) => {
    const match = available.findIndex(
      (item) => ruleKey({ name: item.key, args: item.args }) === ruleKey(rule),
    );
    if (match >= 0) {
      const [old] = available.splice(match, 1);
      if (old) matched.set(index, old);
    }
  });
  candidate.forEach((rule, index) => {
    if (matched.has(index)) return;
    const match = available.findIndex((item) => item.key === rule.name);
    if (match >= 0) {
      const [old] = available.splice(match, 1);
      if (old) matched.set(index, old);
    }
  });
  return candidate.map((rule, index) => {
    const old = matched.get(index);
    return {
      id: old?.id ?? nextId++,
      key: rule.name,
      args: rule.args as Record<string, ParameterValue>,
    };
  });
}

function initialArgs(block: ScreenBlock): Record<string, ParameterValue> {
  return Object.fromEntries(
    block.parameters.map((parameter) => [
      parameter.key,
      parameter.key === "period" && parameter.input === "choice"
        ? Number(parameter.initial)
        : parameter.initial,
    ]),
  );
}

function validParameter(
  value: ParameterValue,
  parameter: ScreenBlock["parameters"][number],
): boolean {
  if (isFundamentalScreenField(value)) return false;
  if (value === null || value === "") return !parameter.required;
  if (parameter.input === "multi_choice") {
    return (
      Array.isArray(value) &&
      (!parameter.required || value.length > 0) &&
      value.every((item) => parameter.options?.some((option) => option.value === item))
    );
  }
  if (parameter.input === "choice" || parameter.input === "field") {
    return (
      (typeof value === "string" ||
        (parameter.key === "period" && typeof value === "number" && Number.isInteger(value))) &&
      parameter.options?.some((option) => option.value === String(value)) === true
    );
  }
  if (parameter.input === "operand" && typeof value === "string") {
    return parameter.options?.some((option) => option.value === value) === true;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) return false;
  if (parameter.input === "integer" && !Number.isInteger(value)) return false;
  if (parameter.minimum != null && value < parameter.minimum) return false;
  if (parameter.maximum != null && value > parameter.maximum) return false;
  return true;
}

function preservedOption(
  value: ParameterValue,
  original: ParameterValue | undefined,
  parameter: ScreenBlock["parameters"][number],
): ScreenOption | null {
  if (
    isFundamentalScreenField(value) ||
    typeof value !== "string" ||
    value !== original ||
    !["operand", "choice", "field"].includes(parameter.input) ||
    parameter.options?.some((option) => option.value === value)
  )
    return null;
  const match = /^([A-Z_][A-Z0-9_]*)\[(\d+)\]$/.exec(value);
  const label = match
    ? parameter.options?.find((option) => option.value === `${match[1]}[0]`)?.label
    : null;
  const offset = match ? Number(match[2]) : 0;
  return {
    value,
    label: label
      ? `${offset === 1 ? "前一交易日" : `前 ${offset} 个交易日`}${label}`
      : "原有数据项",
  };
}

function parameterText(
  value: ParameterValue,
  parameter: ScreenBlock["parameters"][number],
  extraOption: ScreenOption | null = null,
): string {
  if (value === null || value === "") return "未设置";
  if (Array.isArray(value)) {
    return value
      .map(
        (item) => parameter.options?.find((option) => option.value === item)?.label ?? "未识别选项",
      )
      .join("、");
  }
  if (typeof value === "string")
    return (
      parameter.options?.find((option) => option.value === value)?.label ??
      extraOption?.label ??
      "未识别选项"
    );
  if (parameter.key === "period" && parameter.input === "choice")
    return (
      parameter.options?.find((option) => option.value === String(value))?.label ?? `${value} 日`
    );
  return String(value * (parameter.scale || 1));
}

function requestLabel(
  snapshot: EditorSessionSnapshot,
  publication: PublicationStage,
): string | null {
  const journal = snapshot.journal;
  if (!journal) return null;
  if (journal.saveStatus === "failed")
    return journal.saveConflict ? "保存未完成，规则已更新" : "保存失败，请检查后重试";
  if (journal.saveStatus === "ambiguous" || journal.saveStatus === "unknown")
    return "保存状态待确认";
  if (journal.saveStatus !== "succeeded") return "正在核对保存结果";
  if (journal.canvasName === null) return "池子已保存";
  if (journal.attachStatus === "failed") return "池子已保存，画布挂接失败";
  if (journal.attachStatus === "ambiguous" || journal.attachStatus === "unknown")
    return "池子已保存，画布状态待确认";
  if (journal.attachStatus !== "succeeded") return `池子已保存，正在加入「${journal.canvasName}」`;
  return publication === "published" || publication === "result"
    ? `已加入「${journal.canvasName}」`
    : "加入请求已完成，等待画布更新";
}

export function PoolEditorForm({
  mode,
  publishedPools,
  canvases,
  currentCanvas,
  generationId,
  verifiedVersion,
  attachmentVersion,
  nlPreviewAvailable,
  startWithNl,
  session,
  snapshot,
  publicationStage,
  onClose,
  onRestart,
}: {
  mode: Mode;
  publishedPools: PublishedPool[];
  canvases: EditableCanvas[];
  currentCanvas: string | null;
  generationId: string | null;
  verifiedVersion: string | null;
  attachmentVersion: string | null;
  nlPreviewAvailable: boolean;
  startWithNl: boolean;
  session: PoolEditorSession;
  snapshot: EditorSessionSnapshot;
  publicationStage: PublicationStage;
  onClose: () => void;
  onRestart: () => void;
}) {
  const editing = mode.kind === "edit" ? mode.pool : null;
  const copying = mode.kind === "copy" ? mode.source : null;
  const firstPoolMode = mode.kind === "create" && mode.parentKey === null;
  const catalog = useScreenCatalog();
  const [name, setName] = useState(
    editing?.display_name ??
      (copying
        ? `${copying.display_name.replace(/[^\w\u4e00-\u9fff-]/gu, "").slice(0, 78)}副本`
        : ""),
  );
  const [description, setDescription] = useState(
    editing?.description ?? copying?.description ?? "",
  );
  const [parent, setParent] = useState(
    mode.kind === "create"
      ? (mode.parentKey ?? "")
      : (editing?.depends_on ?? copying?.depends_on ?? ""),
  );
  const [delay, setDelay] = useState(editing?.delay_days ?? copying?.delay_days ?? 1);
  const [canvas, setCanvas] = useState(
    currentCanvas && canvases.some((item) => item.name === currentCanvas) ? currentCanvas : "",
  );
  const [chosenBlock, setChosenBlock] = useState("");
  const [rules, setRules] = useState<RuleDraft[]>(() => initialRules(editing ?? copying));
  const [rankRows, setRankRows] = useState<RankDraft[]>(
    () =>
      (editing?.ranking ?? copying?.ranking)?.conditions.map((row, index) => ({
        id: index + 1,
        metric: row.metric,
        ascending: row.ascending,
        weight: String(row.weight),
      })) ?? [],
  );
  const [topN, setTopN] = useState(String((editing?.ranking ?? copying?.ranking)?.top_n ?? 20));
  const [originalRules] = useState<RuleDraft[]>(() => initialRules(editing ?? copying));
  const [ruleRevision, setRuleRevision] = useState(0);
  const [metadataRevision, setMetadataRevision] = useState(0);
  const [preview, setPreview] = useState(false);
  const previewGeneration = useRef(generationId);
  const previewRef = useRef<HTMLElement>(null);
  const blocks = useMemo(
    () =>
      (catalog.data?.blocks ?? []).map((block) => ({
        ...block,
        parameters: block.parameters.map((parameter) => ({
          ...parameter,
          options: (parameter.options ?? []).filter(
            (option) => !isFundamentalScreenField(option.value),
          ),
        })),
      })),
    [catalog.data?.blocks],
  );
  const blockMap = useMemo(() => new Map(blocks.map((block) => [block.key, block])), [blocks]);
  const rankMetrics = (catalog.data?.ranking_metrics ?? []).filter(
    (item): item is ScreenOption & { value: PoolRankingMetric } => isPoolRankingMetric(item.value),
  );
  const metricLabels = new Map(rankMetrics.map((item) => [item.value, item.label]));
  const weights = rankRows.map((row) =>
    row.weight.trim() === "" ? Number.NaN : Number(row.weight),
  );
  const rankingError =
    rankRows.length === 0
      ? null
      : rankRows.length > 4
        ? "最多添加 4 项排名。"
        : rankRows.some((row) => !metricLabels.has(row.metric))
          ? "原有排名指标暂不可用，请稍后刷新。"
          : new Set(rankRows.map((row) => row.metric)).size !== rankRows.length
            ? "同一排名指标只能添加一次。"
            : weights.some((weight) => !Number.isFinite(weight) || weight < 0 || weight > 100)
              ? "权重请填 0 到 100。"
              : weights.reduce((sum, weight) => sum + weight, 0) <= 0
                ? "至少一项权重大于 0。"
                : !Number.isInteger(Number(topN)) || Number(topN) < 1 || Number(topN) > 100
                  ? "前 N 只请填 1 到 100。"
                  : null;
  const ranking =
    rankRows.length > 0 && rankingError === null
      ? {
          conditions: rankRows.map((row) => ({
            metric: row.metric,
            ascending: row.ascending,
            weight: Number(row.weight),
          })),
          top_n: Number(topN),
        }
      : null;
  const rulesChanged =
    JSON.stringify(rules.map((rule) => ({ name: rule.key, args: rule.args }))) !==
    JSON.stringify(originalRules.map((rule) => ({ name: rule.key, args: rule.args })));
  const parentPool = publishedPools.find((pool) => pool.key === parent);
  const targetCanvas = canvases.find((item) => item.name === canvas);
  const alreadyAttached = !!editing && !!targetCanvas?.pool_refs.includes(editing.key);
  const attachTo = targetCanvas && !alreadyAttached ? targetCanvas.name : null;
  const previous = snapshot.journal;
  const baseName = editing?.key.slice("user/".length) ?? name.trim();
  const sameTarget = previous?.save.base_name === baseName;
  const unchanged =
    sameTarget &&
    JSON.stringify({
      display_name: name.trim(),
      description: description.trim(),
      depends_on: parent || null,
      delay_days: parent ? delay : 0,
      rule_calls: rules.map((rule) => ({ name: rule.key, args: rule.args })),
      ranking,
      canvas: attachTo,
    }) ===
      JSON.stringify({
        display_name: previous.save.display_name,
        description: previous.save.description,
        depends_on: previous.save.depends_on,
        delay_days: previous.save.delay_days,
        rule_calls: previous.save.rule_calls,
        ranking: previous.save.kind === "save_user_pool_v3" ? previous.save.ranking : null,
        canvas: previous.canvasName,
      });
  const versionNeedsReview =
    (editing !== null || copying !== null) &&
    (verifiedVersion === null || verifiedVersion !== (editing?.version ?? copying?.version));
  const canRestart =
    !!generationId &&
    !!verifiedVersion &&
    verifiedVersion !== (editing?.version ?? copying?.version);
  const nameValid =
    /^[\w\u4e00-\u9fff-]{1,80}$/u.test(baseName) &&
    name.trim().length >= 1 &&
    name.trim().length <= 80;
  const ruleLimit = rankRows.length > 0 || editing?.save_kind === "save_user_pool_v3" ? 26 : 32;
  const rulesValid =
    rules.length > 0 &&
    rules.length <= ruleLimit &&
    rules.every((rule) => {
      const block = blockMap.get(rule.key);
      const original = originalRules.find((item) => item.id === rule.id && item.key === rule.key);
      return block?.parameters.every((parameter) => {
        const value = rule.args[parameter.key] ?? null;
        return (
          validParameter(value, parameter) ||
          preservedOption(value, original?.args[parameter.key], parameter) !== null
        );
      });
    });
  const canSubmit =
    !!generationId &&
    catalogUsableForGeneration(catalog.data, catalog.serving?.generation_id, generationId) &&
    nameValid &&
    description.length <= 1024 &&
    (parent
      ? parentPool !== undefined &&
        parent !== editing?.key &&
        delay >= 1 &&
        delay <= 252 &&
        Number.isInteger(delay)
      : editing !== null ||
        copying !== null ||
        (firstPoolMode && targetCanvas?.pool_refs.length === 0 && attachTo !== null)) &&
    rulesValid &&
    rankingError === null &&
    snapshot.storageAvailable &&
    !snapshot.busy &&
    !versionNeedsReview &&
    !(sameTarget && previous?.saveConflict) &&
    !(sameTarget && previous?.saveStatus === "succeeded" && unchanged) &&
    !(
      snapshot.journal &&
      (snapshot.journal.saveStatus === "pending" ||
        snapshot.journal.saveStatus === "processing" ||
        snapshot.journal.saveStatus === "ambiguous" ||
        snapshot.journal.saveStatus === "unknown" ||
        (snapshot.journal.saveStatus === "succeeded" &&
          !["idle", "succeeded"].includes(snapshot.journal.attachStatus)))
    );

  const addRule = () => {
    const block = blockMap.get(chosenBlock);
    if (!block || rules.length >= ruleLimit) return;
    setRules((current) => [
      ...current,
      {
        id: Math.max(0, ...current.map((item) => item.id)) + 1,
        key: block.key,
        args: initialArgs(block),
      },
    ]);
    setChosenBlock("");
    setPreview(false);
    setRuleRevision((current) => current + 1);
  };
  const markRuleEdit = () => {
    setPreview(false);
    setRuleRevision((current) => current + 1);
  };
  const markMetadataEdit = () => {
    setPreview(false);
    setMetadataRevision((current) => current + 1);
  };
  const submit = async () => {
    if (!canSubmit || !preview) return;
    const common = {
      base_name: baseName,
      display_name: name.trim(),
      description: description.trim(),
      depends_on: parent || null,
      delay_days: parent ? delay : 0,
      rule_calls: rules.map((rule) => ({ name: rule.key, args: rule.args })),
      include_columns: editing?.include_columns ?? copying?.include_columns ?? [],
      expected_version: editing?.version ?? null,
    };
    const input: SaveInput =
      ranking !== null || editing?.save_kind === "save_user_pool_v3"
        ? { ...common, ranking }
        : common;
    await session.startSave(input, attachTo);
  };
  const statusLabel = requestLabel(snapshot, publicationStage);
  const restart = () => {
    if (!canRestart) return;
    if (sameTarget && previous?.saveConflict && !session.discardFailedSave()) return;
    onRestart();
  };
  const endConflict = () => {
    if (sameTarget && previous?.saveConflict && session.discardFailedSave()) onClose();
  };

  useEffect(() => {
    if (preview && typeof previewRef.current?.scrollIntoView === "function") {
      previewRef.current.scrollIntoView({ block: "start" });
    }
  }, [preview]);

  useEffect(() => {
    if (previewGeneration.current !== generationId) {
      previewGeneration.current = generationId;
      setPreview(false);
    }
  }, [generationId]);

  return (
    <SideDrawer
      open
      onClose={onClose}
      wide
      title={
        editing
          ? "编辑规则"
          : copying
            ? "复制为自建池"
            : firstPoolMode
              ? "创建首只池子"
              : "添加条件节点"
      }
      footer={
        <div className="pool-editor-footer">
          {sameTarget && statusLabel ? (
            <div className="pool-editor-status" role="status">
              {snapshot.journal?.saveStatus === "succeeded" &&
              !statusLabel.startsWith("池子已保存") ? (
                <span>池子已保存</span>
              ) : null}
              <strong>{statusLabel}</strong>
              {snapshot.message ? <p>{snapshot.message}</p> : null}
              {snapshot.journal?.attachConflict ? <p>当前规则已变化，请结束本次挂接。</p> : null}
              {snapshot.journal?.attachStatus === "failed" ? (
                <>
                  <Button
                    size="sm"
                    disabledReason={
                      !generationId ||
                      !attachmentVersion ||
                      snapshot.journal.attachConflict ||
                      attachmentVersion !== snapshot.journal.saveVersion
                        ? "本次保存的规则已变化，请结束此次挂接。"
                        : undefined
                    }
                    onClick={() => void session.retryAttachment(attachmentVersion)}
                  >
                    按本次保存规则重试
                  </Button>
                  <Button size="sm" onClick={() => session.discardFailedAttachment()}>
                    结束本次挂接
                  </Button>
                </>
              ) : null}
              {snapshot.journal &&
              (["pending", "processing", "ambiguous", "unknown"].includes(
                snapshot.journal.saveStatus,
              ) ||
                (snapshot.journal.saveStatus === "succeeded" &&
                  ["pending", "processing", "ambiguous", "unknown"].includes(
                    snapshot.journal.attachStatus,
                  ))) ? (
                <>
                  <Button size="sm" disabled={snapshot.busy} onClick={() => void session.advance()}>
                    {snapshot.journal.attachStatus !== "idle" ? "继续核对画布" : "继续核对保存"}
                  </Button>
                  {snapshot.journal.saveStatus === "succeeded" &&
                  ["ambiguous", "unknown"].includes(snapshot.journal.attachStatus) ? (
                    <Button size="sm" onClick={() => session.deferAttachment()}>
                      留待核对，继续编辑
                    </Button>
                  ) : null}
                </>
              ) : null}
            </div>
          ) : null}
          {!snapshot.storageAvailable ? (
            <p role="alert" className="pool-editor-error">
              {snapshot.message ?? "浏览器存储不可用，无法安全提交。"}
            </p>
          ) : null}
          <div className="pool-editor-footer-actions">
            <Button onClick={onClose}>返回画布</Button>
            <Button
              variant="primary"
              disabledReason={
                !canSubmit
                  ? "请先补齐有效条件，并等待同一批数据。"
                  : !preview
                    ? "先查看变更预览。"
                    : undefined
              }
              onClick={() => void submit()}
            >
              {attachTo ? "保存并加入画布" : "保存规则"}
            </Button>
          </div>
        </div>
      }
    >
      <div className="pool-editor">
        {!generationId ? (
          <p className="pool-editor-error" role="status">
            数据正在更新，草稿已保留。请稍后重新预览。
          </p>
        ) : null}
        {versionNeedsReview ? (
          <div className="pool-editor-review" role="status">
            <p>当前草稿仍在；重新打开会以最新规则重填。</p>
            <Button
              size="sm"
              disabledReason={!canRestart ? "等待最新规则可用。" : undefined}
              onClick={restart}
            >
              重新打开最新规则
            </Button>
            {sameTarget && previous?.saveConflict ? (
              <Button size="sm" onClick={endConflict}>
                结束本次编辑
              </Button>
            ) : null}
          </div>
        ) : sameTarget && previous?.saveConflict ? (
          <div className="pool-editor-review" role="status">
            <p>保存未完成。草稿已保留，请刷新规则后重新打开。</p>
            <Button size="sm" onClick={endConflict}>
              结束本次编辑
            </Button>
          </div>
        ) : null}
        <p className="pool-editor-lead">
          {editing
            ? "调整这只自建池的筛选条件。"
            : copying
              ? "复制已核验的条件，保存前可调整。"
              : firstPoolMode
                ? "选好条件，预览后加入当前画布。"
                : "从父池筛选，保存后加入所选画布。"}
        </p>
        {editing ? (
          <PoolSentenceEdit
            pool={editing}
            generationId={generationId}
            verifiedVersion={verifiedVersion}
            available={nlPreviewAvailable}
            busy={snapshot.busy}
            rulesChanged={rulesChanged}
            ruleRevision={ruleRevision}
            metadataRevision={metadataRevision}
            blocks={blocks}
            startOpen={startWithNl}
            onApply={(candidate) => {
              setRules(reconcileSuggestion(originalRules, candidate));
              setPreview(false);
            }}
            onUndo={() => {
              setRules(originalRules);
              setPreview(false);
            }}
          />
        ) : null}
        <div className="pool-editor-fields">
          <label className="field">
            <span className="lbl">池子名称</span>
            <input
              className="inp"
              maxLength={80}
              value={name}
              onChange={(event) => {
                setName(event.target.value);
                markMetadataEdit();
              }}
            />
          </label>
          <label className="field">
            <span className="lbl">
              简短说明 <small>选填</small>
            </span>
            <input
              className="inp"
              maxLength={1024}
              value={description}
              onChange={(event) => {
                setDescription(event.target.value);
                markMetadataEdit();
              }}
            />
          </label>
          <label className="field">
            <span className="lbl">{firstPoolMode ? "筛选来源" : "父池"}</span>
            <select
              className="inp"
              value={parent}
              onChange={(event) => {
                setParent(event.target.value);
                markMetadataEdit();
              }}
            >
              {editing || copying || firstPoolMode ? <option value="">独立筛选</option> : null}
              {publishedPools
                .filter((pool) => !firstPoolMode && pool.key !== editing?.key)
                .map((pool) => (
                  <option key={pool.key} value={pool.key}>
                    {pool.name}
                  </option>
                ))}
            </select>
          </label>
          <label className="field">
            <span className="lbl">延后交易日</span>
            <input
              className="inp num"
              type="number"
              min={parent ? 1 : 0}
              max={252}
              step={1}
              value={parent ? delay : 0}
              disabled={!parent}
              onChange={(event) => {
                setDelay(Number(event.target.value));
                markMetadataEdit();
              }}
            />
          </label>
          <label className="field">
            <span className="lbl">目标画布</span>
            <select
              className="inp"
              value={canvas}
              onChange={(event) => {
                setCanvas(event.target.value);
                markMetadataEdit();
              }}
            >
              {!firstPoolMode ? <option value="">仅保存池子</option> : null}
              {canvases.map((item) => (
                <option key={item.name} value={item.name}>
                  {item.name}
                </option>
              ))}
            </select>
          </label>
        </div>
        <section className="pool-editor-section" aria-label="筛选条件">
          <div className="pool-editor-section-head">
            <h3>筛选条件</h3>
            <span className="num">
              {rules.length}/{ruleLimit}
            </span>
          </div>
          {catalog.isLoading ? (
            <p className="pools-note">正在加载条件目录…</p>
          ) : catalog.error ? (
            <p className="pools-note">条件目录暂不可用，请稍后重试。</p>
          ) : rules.length === 0 ? (
            <p className="pools-note">还没有条件。请从目录添加。</p>
          ) : null}
          {rules.length > ruleLimit ? (
            <p className="pool-editor-error" role="alert">
              最多保留 {ruleLimit} 条条件，请移除多余条件。
            </p>
          ) : null}
          {rules.map((rule, index) => {
            const block = blockMap.get(rule.key);
            const original = originalRules.find(
              (item) => item.id === rule.id && item.key === rule.key,
            );
            return (
              <div className="pool-editor-rule" key={rule.id}>
                <div className="pool-editor-rule-head">
                  <span className="pool-editor-rule-index num">
                    {String(index + 1).padStart(2, "0")}
                  </span>
                  <strong>{block?.label ?? "条件暂无法编辑"}</strong>
                  <Button
                    size="sm"
                    onClick={() => {
                      setRules((current) => current.filter((item) => item.id !== rule.id));
                      markRuleEdit();
                    }}
                    aria-label={`移除第 ${index + 1} 条条件`}
                  >
                    移除
                  </Button>
                </div>
                {block?.hint ? (
                  <Tip content={block.hint}>
                    <span className="pools-info">条件说明</span>
                  </Tip>
                ) : null}
                {block?.parameters.length ? (
                  <div className="pool-editor-params">
                    {block.parameters.map((parameter) => (
                      <ParamControl
                        key={parameter.key}
                        parameter={parameter}
                        value={rule.args[parameter.key] ?? null}
                        extraOption={preservedOption(
                          rule.args[parameter.key] ?? null,
                          original?.args[parameter.key],
                          parameter,
                        )}
                        onChange={(value) => {
                          setRules((current) =>
                            current.map((item) =>
                              item.id === rule.id
                                ? {
                                    ...item,
                                    args: {
                                      ...item.args,
                                      [parameter.key]:
                                        parameter.key === "period" && typeof value === "string"
                                          ? Number(value)
                                          : value,
                                    },
                                  }
                                : item,
                            ),
                          );
                          markRuleEdit();
                        }}
                      />
                    ))}
                  </div>
                ) : null}
              </div>
            );
          })}
          <div className="pool-editor-add">
            <label className="field">
              <span className="lbl">条件目录</span>
              <select
                className="inp"
                value={chosenBlock}
                onChange={(event) => setChosenBlock(event.target.value)}
              >
                <option value="">选择条件</option>
                {blocks.map((block) => (
                  <option key={block.key} value={block.key}>
                    {block.category_label} · {block.label}
                  </option>
                ))}
              </select>
            </label>
            <Button
              disabledReason={
                !chosenBlock
                  ? "先从条件目录选择一条。"
                  : rules.length >= ruleLimit
                    ? `最多添加 ${ruleLimit} 条条件。`
                    : undefined
              }
              onClick={addRule}
            >
              添加条件
            </Button>
          </div>
        </section>
        <section className="pool-editor-section" aria-label="排名规则">
          <div className="pool-editor-section-head">
            <h3>排名规则</h3>
            <Button
              size="sm"
              disabledReason={
                rankRows.length >= 4
                  ? "最多添加 4 项排名。"
                  : !rankMetrics.some(
                        (metric) => !rankRows.some((row) => row.metric === metric.value),
                      )
                    ? "暂无更多可用指标。"
                    : undefined
              }
              onClick={() => {
                const metric = rankMetrics.find(
                  (item) => !rankRows.some((row) => row.metric === item.value),
                );
                if (!metric) return;
                setRankRows((current) => [
                  ...current,
                  {
                    id: Math.max(0, ...current.map((row) => row.id)) + 1,
                    metric: metric.value,
                    ascending: metric.value === "CIRC_MV[0]",
                    weight: current.length === 0 ? "100" : "0",
                  },
                ]);
                markMetadataEdit();
              }}
            >
              添加排名
            </Button>
          </div>
          {rankRows.length === 0 ? (
            <p className="pools-note">未设置排名，保留所有符合条件的股票。</p>
          ) : (
            <div className="pool-editor-ranking">
              {rankRows.map((row, index) => (
                <div className="pool-editor-rank-row" key={row.id}>
                  <span className="pool-editor-rule-index num">
                    {String(index + 1).padStart(2, "0")}
                  </span>
                  <label className="field pool-editor-rank-metric">
                    <span className="lbl">指标</span>
                    <select
                      className="inp"
                      aria-label={`第 ${index + 1} 项排名指标`}
                      value={row.metric}
                      onChange={(event) => {
                        const metric = event.target.value;
                        if (!isPoolRankingMetric(metric)) return;
                        setRankRows((current) =>
                          current.map((item) =>
                            item.id === row.id
                              ? {
                                  ...item,
                                  metric,
                                  ascending: metric === "CIRC_MV[0]",
                                }
                              : item,
                          ),
                        );
                        markMetadataEdit();
                      }}
                    >
                      {!metricLabels.has(row.metric) ? (
                        <option value={row.metric}>原有指标暂不可用</option>
                      ) : null}
                      {rankMetrics.map((metric) => (
                        <option
                          key={metric.value}
                          value={metric.value}
                          disabled={
                            metric.value !== row.metric &&
                            rankRows.some((item) => item.metric === metric.value)
                          }
                        >
                          {metric.label}
                        </option>
                      ))}
                    </select>
                  </label>
                  <label className="field pool-editor-rank-direction">
                    <span className="lbl">优先方向</span>
                    <select
                      className="inp"
                      aria-label={`第 ${index + 1} 项排名方向`}
                      value={row.ascending ? "asc" : "desc"}
                      onChange={(event) => {
                        setRankRows((current) =>
                          current.map((item) =>
                            item.id === row.id
                              ? { ...item, ascending: event.target.value === "asc" }
                              : item,
                          ),
                        );
                        markMetadataEdit();
                      }}
                    >
                      <option value="desc">数值高优先</option>
                      <option value="asc">数值低优先</option>
                    </select>
                  </label>
                  <label className="field pool-editor-rank-weight">
                    <span className="lbl">权重</span>
                    <input
                      className="inp num"
                      type="number"
                      min={0}
                      max={100}
                      step="0.1"
                      inputMode="decimal"
                      aria-label={`第 ${index + 1} 项排名权重`}
                      value={row.weight}
                      onChange={(event) => {
                        setRankRows((current) =>
                          current.map((item) =>
                            item.id === row.id ? { ...item, weight: event.target.value } : item,
                          ),
                        );
                        markMetadataEdit();
                      }}
                    />
                  </label>
                  <Button
                    size="sm"
                    variant="ghost"
                    disabledReason={
                      rankRows.length === 1 && (editing?.ranking ?? copying?.ranking)
                        ? "这只池子需要保留排名。"
                        : undefined
                    }
                    aria-label={`删除第 ${index + 1} 项排名`}
                    onClick={() => {
                      setRankRows((current) => current.filter((item) => item.id !== row.id));
                      markMetadataEdit();
                    }}
                  >
                    删除
                  </Button>
                </div>
              ))}
              <label className="field pool-editor-rank-top">
                <span className="lbl">取前 N 只</span>
                <input
                  className="inp num"
                  type="number"
                  min={1}
                  max={100}
                  step={1}
                  inputMode="numeric"
                  value={topN}
                  onChange={(event) => {
                    setTopN(event.target.value);
                    markMetadataEdit();
                  }}
                />
              </label>
            </div>
          )}
          {rankingError ? (
            <p className="pool-editor-error" role="alert">
              {rankingError}
            </p>
          ) : null}
        </section>
        <section ref={previewRef} className="pool-editor-section" aria-label="变更预览">
          <div className="pool-editor-section-head">
            <h3>变更预览</h3>
            <Button
              size="sm"
              disabledReason={!canSubmit ? "请先补齐有效条件。" : undefined}
              onClick={() => setPreview(true)}
            >
              预览变更
            </Button>
          </div>
          {preview && rulesValid ? (
            <div className="pool-editor-preview">
              <strong>{name.trim()}</strong>
              {copying ? <span>来自「{copying.display_name}」</span> : null}
              <span>
                {parent ? `从 ${parentPool?.name} 筛选 · 延后 ${delay} 个交易日` : "独立筛选"}
              </span>
              <span>{attachTo ? `保存后加入「${canvas}」` : "只保存池子规则"}</span>
              {ranking ? (
                <div className="pool-editor-rank-preview">
                  <strong>排名后取前 {ranking.top_n.toLocaleString("zh-CN")} 只</strong>
                  {ranking.conditions.map((row) => (
                    <span key={row.metric}>
                      {metricLabels.get(row.metric)} · {row.ascending ? "低值优先" : "高值优先"} ·
                      权重 {row.weight.toLocaleString("zh-CN")}%
                    </span>
                  ))}
                </div>
              ) : null}
              <ol>
                {rules.map((rule) => {
                  const block = blockMap.get(rule.key);
                  const original = originalRules.find(
                    (item) => item.id === rule.id && item.key === rule.key,
                  );
                  return (
                    <li key={rule.id}>
                      <b>{block?.label}</b>
                      {block?.parameters.length ? (
                        <span>
                          {block.parameters
                            .map(
                              (parameter) =>
                                `${parameter.label} ${parameterText(rule.args[parameter.key] ?? null, parameter, preservedOption(rule.args[parameter.key] ?? null, original?.args[parameter.key], parameter))}`,
                            )
                            .join(" · ")}
                        </span>
                      ) : null}
                    </li>
                  );
                })}
              </ol>
            </div>
          ) : (
            <p className="pools-note">填写后预览将保存的规则。</p>
          )}
        </section>
      </div>
    </SideDrawer>
  );
}
