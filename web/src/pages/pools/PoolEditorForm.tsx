import { useEffect, useMemo, useRef, useState } from "react";
import type { PublishedPool } from "@/api/endpoints";
import type { BuiltinPoolCopySource, EditableCanvas, EditablePool } from "@/api/poolEditor";
import { type ScreenBlock, type ScreenOption, useScreenCatalog } from "@/api/screen";
import { Button, ParamControl, type ParameterValue, SideDrawer, Tip } from "@/ui";
import type { PublicationStage } from "./editorPublication";
import type { EditorSessionSnapshot, PoolEditorSession, SaveInput } from "./editorSession";

type RuleDraft = { id: number; key: string; args: Record<string, ParameterValue> };
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
  if (journal.attachStatus !== "succeeded") return "池子已保存，正在加入当前画布";
  return publication === "published" || publication === "result"
    ? "已加入当前画布"
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
    editing?.depends_on ??
      copying?.depends_on ??
      (mode.kind === "create" ? (mode.parentKey ?? publishedPools[0]?.key ?? "") : ""),
  );
  const [delay, setDelay] = useState(editing?.delay_days ?? copying?.delay_days ?? 1);
  const [canvas, setCanvas] = useState(
    currentCanvas && canvases.some((item) => item.name === currentCanvas) ? currentCanvas : "",
  );
  const [chosenBlock, setChosenBlock] = useState("");
  const [rules, setRules] = useState<RuleDraft[]>(() => initialRules(editing ?? copying));
  const [originalRules] = useState<RuleDraft[]>(() => initialRules(editing ?? copying));
  const [preview, setPreview] = useState(false);
  const previewGeneration = useRef(generationId);
  const previewRef = useRef<HTMLElement>(null);
  const blocks = catalog.data?.blocks ?? [];
  const blockMap = useMemo(() => new Map(blocks.map((block) => [block.key, block])), [blocks]);
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
      canvas: attachTo,
    }) ===
      JSON.stringify({
        display_name: previous.save.display_name,
        description: previous.save.description,
        depends_on: previous.save.depends_on,
        delay_days: previous.save.delay_days,
        rule_calls: previous.save.rule_calls,
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
  const rulesValid =
    rules.length > 0 &&
    rules.length <= 32 &&
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
    catalog.serving?.generation_id === generationId &&
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
        (firstPoolMode &&
          publishedPools.length === 0 &&
          targetCanvas?.pool_refs.length === 0 &&
          attachTo !== null)) &&
    rulesValid &&
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
    if (!block) return;
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
  };
  const submit = async () => {
    if (!canSubmit || !preview) return;
    const input: SaveInput = {
      base_name: baseName,
      display_name: name.trim(),
      description: description.trim(),
      depends_on: parent || null,
      delay_days: parent ? delay : 0,
      rule_calls: rules.map((rule) => ({ name: rule.key, args: rule.args })),
      include_columns: editing?.include_columns ?? copying?.include_columns ?? [],
      expected_version: editing?.version ?? null,
    };
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
        <div className="pool-editor-fields">
          <label className="field">
            <span className="lbl">池子名称</span>
            <input
              className="inp"
              maxLength={80}
              value={name}
              onChange={(event) => {
                setName(event.target.value);
                setPreview(false);
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
                setPreview(false);
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
                setPreview(false);
              }}
            >
              {editing || copying || firstPoolMode ? <option value="">独立筛选</option> : null}
              {publishedPools
                .filter((pool) => pool.key !== editing?.key)
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
                setPreview(false);
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
                setPreview(false);
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
            <span className="num">{rules.length}/32</span>
          </div>
          {catalog.isLoading ? (
            <p className="pools-note">正在加载条件目录…</p>
          ) : catalog.error ? (
            <p className="pools-note">条件目录暂不可用，请稍后重试。</p>
          ) : rules.length === 0 ? (
            <p className="pools-note">还没有条件。请从目录添加。</p>
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
                      setPreview(false);
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
                          setPreview(false);
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
                  : rules.length >= 32
                    ? "最多添加 32 条条件。"
                    : undefined
              }
              onClick={addRule}
            >
              添加条件
            </Button>
          </div>
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
