import { useEffect, useMemo, useRef, useState } from "react";
import type { PublishedPool } from "@/api/endpoints";
import type { EditableCanvas, EditablePool } from "@/api/poolEditor";
import { type ScreenBlock, useScreenCatalog } from "@/api/screen";
import { Button, ParamControl, type ParameterValue, SideDrawer, Tip } from "@/ui";
import type { EditorSessionSnapshot, PoolEditorSession, SaveInput } from "./editorSession";

type RuleDraft = { id: number; key: string; args: Record<string, ParameterValue> };
type Mode = { kind: "create"; parentKey: string | null } | { kind: "edit"; pool: EditablePool };

function initialRules(pool: EditablePool | null): RuleDraft[] {
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
    block.parameters.map((parameter) => [parameter.key, parameter.initial]),
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
      typeof value === "string" &&
      parameter.options?.some((option) => option.value === value) === true
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

function parameterText(
  value: ParameterValue,
  parameter: ScreenBlock["parameters"][number],
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
    return parameter.options?.find((option) => option.value === value)?.label ?? "未识别选项";
  return String(value * (parameter.scale || 1));
}

function requestLabel(snapshot: EditorSessionSnapshot): string | null {
  const journal = snapshot.journal;
  if (!journal) return null;
  if (journal.saveStatus === "failed") return "保存失败，请检查后重试";
  if (journal.saveStatus === "ambiguous" || journal.saveStatus === "unknown")
    return "保存状态待确认";
  if (journal.saveStatus !== "succeeded") return "正在核对保存结果";
  if (journal.canvasName === null) return "池子已保存";
  if (journal.attachStatus === "failed") return "池子已保存，尚未加入当前画布";
  if (journal.attachStatus === "ambiguous" || journal.attachStatus === "unknown")
    return "池子已保存，画布状态待确认";
  if (journal.attachStatus !== "succeeded") return "池子已保存，正在加入当前画布";
  return "已加入当前画布";
}

export function PoolEditorForm({
  mode,
  publishedPools,
  canvases,
  currentCanvas,
  generationId,
  session,
  snapshot,
  onClose,
}: {
  mode: Mode;
  publishedPools: PublishedPool[];
  canvases: EditableCanvas[];
  currentCanvas: string | null;
  generationId: string | null;
  session: PoolEditorSession;
  snapshot: EditorSessionSnapshot;
  onClose: () => void;
}) {
  const editing = mode.kind === "edit" ? mode.pool : null;
  const catalog = useScreenCatalog();
  const [name, setName] = useState(editing?.display_name ?? "");
  const [description, setDescription] = useState(editing?.description ?? "");
  const [parent, setParent] = useState(
    editing
      ? (editing.depends_on ?? "")
      : mode.kind === "create"
        ? (mode.parentKey ?? publishedPools[0]?.key ?? "")
        : "",
  );
  const [delay, setDelay] = useState(editing?.delay_days ?? 1);
  const [canvas, setCanvas] = useState(
    currentCanvas && canvases.some((item) => item.name === currentCanvas) ? currentCanvas : "",
  );
  const [chosenBlock, setChosenBlock] = useState("");
  const [rules, setRules] = useState<RuleDraft[]>(() => initialRules(editing));
  const [preview, setPreview] = useState(false);
  const previewRef = useRef<HTMLElement>(null);
  const statusRef = useRef<HTMLDivElement>(null);
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
  const versionCurrent =
    !sameTarget || previous.saveStatus !== "succeeded" || editing?.version === previous.saveVersion;
  const nameValid =
    /^[\w\u4e00-\u9fff-]{1,80}$/u.test(baseName) &&
    name.trim().length >= 1 &&
    name.trim().length <= 80;
  const rulesValid =
    rules.length > 0 &&
    rules.length <= 32 &&
    rules.every((rule) => {
      const block = blockMap.get(rule.key);
      return block?.parameters.every((parameter) =>
        validParameter(rule.args[parameter.key] ?? null, parameter),
      );
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
      : editing !== null) &&
    rulesValid &&
    snapshot.storageAvailable &&
    !snapshot.busy &&
    versionCurrent &&
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
      include_columns: editing?.include_columns ?? [],
      expected_version: editing?.version ?? null,
    };
    await session.startSave(input, attachTo);
  };
  const statusLabel = requestLabel(snapshot);

  useEffect(() => {
    if (preview && typeof previewRef.current?.scrollIntoView === "function") {
      previewRef.current.scrollIntoView({ block: "start" });
    }
  }, [preview]);

  useEffect(() => {
    if (statusLabel && !snapshot.busy && typeof statusRef.current?.scrollIntoView === "function") {
      statusRef.current.scrollIntoView({ block: "end" });
    }
  }, [snapshot.busy, statusLabel]);

  return (
    <SideDrawer
      open
      onClose={onClose}
      wide
      title={editing ? "编辑规则" : "添加条件节点"}
      footer={
        <div className="pool-editor-footer">
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
      }
    >
      <div className="pool-editor">
        <p className="pool-editor-lead">
          {editing ? "调整这只自建池的筛选条件。" : "从父池筛选，保存后加入所选画布。"}
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
            <span className="lbl">父池</span>
            <select
              className="inp"
              value={parent}
              onChange={(event) => {
                setParent(event.target.value);
                setPreview(false);
              }}
            >
              {editing ? <option value="">独立筛选</option> : null}
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
              <option value="">仅保存池子</option>
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
                        onChange={(value) => {
                          setRules((current) =>
                            current.map((item) =>
                              item.id === rule.id
                                ? { ...item, args: { ...item.args, [parameter.key]: value } }
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
              <span>
                {parent ? `从 ${parentPool?.name} 筛选 · 延后 ${delay} 个交易日` : "独立筛选"}
              </span>
              <span>{attachTo ? `保存后加入「${canvas}」` : "只保存池子规则"}</span>
              <ol>
                {rules.map((rule) => {
                  const block = blockMap.get(rule.key);
                  return (
                    <li key={rule.id}>
                      <b>{block?.label}</b>
                      {block?.parameters.length ? (
                        <span>
                          {block.parameters
                            .map(
                              (parameter) =>
                                `${parameter.label} ${parameterText(rule.args[parameter.key] ?? null, parameter)}`,
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
        {sameTarget && statusLabel ? (
          <div ref={statusRef} className="pool-editor-status" role="status">
            <strong>{statusLabel}</strong>
            {snapshot.message ? <p>{snapshot.message}</p> : null}
            {snapshot.journal?.attachStatus === "failed" ? (
              <Button size="sm" onClick={() => void session.retryAttachment()}>
                重试加入画布
              </Button>
            ) : null}
            {snapshot.journal &&
            (["pending", "processing", "ambiguous", "unknown"].includes(
              snapshot.journal.saveStatus,
            ) ||
              (snapshot.journal.saveStatus === "succeeded" &&
                ["pending", "processing", "ambiguous", "unknown"].includes(
                  snapshot.journal.attachStatus,
                ))) ? (
              <Button size="sm" disabled={snapshot.busy} onClick={() => void session.advance()}>
                继续核对
              </Button>
            ) : null}
          </div>
        ) : null}
        {!snapshot.storageAvailable ? (
          <p role="alert" className="pool-editor-error">
            {snapshot.message ?? "浏览器存储不可用，无法安全提交。"}
          </p>
        ) : null}
      </div>
    </SideDrawer>
  );
}
