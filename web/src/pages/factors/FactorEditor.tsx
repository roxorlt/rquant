import { useRef, useState } from "react";
import type {
  FactorCapabilitiesData,
  FactorCatalogData,
  FactorDefinitionItem,
  FactorSaveDraft,
} from "@/api/factors";
import { Button, SideDrawer, Tip } from "@/ui";
import { FactorFieldInfo, factorFieldName } from "./FactorFieldInfo";
import "./FactorEditor.css";

export type FactorEditorDraft = Pick<
  FactorSaveDraft,
  | "generation_id"
  | "mode"
  | "factor_id"
  | "expected_head"
  | "name_zh"
  | "category"
  | "direction"
  | "expression"
> & { category_label: string };

type FieldErrors = Partial<Record<"name_zh" | "category" | "expression", string>>;

function checkDraft(draft: FactorEditorDraft): FieldErrors {
  const errors: FieldErrors = {};
  if (!draft.name_zh.trim()) errors.name_zh = "请填写中文名";
  else if (draft.name_zh.length > 80) errors.name_zh = "中文名不能超过 80 字";
  if (!draft.category.trim()) errors.category = "请选择分类";
  if (!draft.expression.trim()) errors.expression = "请填写表达式";
  else if (draft.expression.length > 2048) errors.expression = "表达式不能超过 2048 字";
  return errors;
}

export function FactorEditor({
  draft,
  open,
  capabilities,
  catalog,
  currentDefinition,
  currentGeneration,
  canSave,
  storageReady,
  stale,
  canRebase,
  busy,
  onChange,
  onClose,
  onRebase,
  onSubmit,
}: {
  draft: FactorEditorDraft | null;
  open: boolean;
  capabilities: FactorCapabilitiesData | undefined;
  catalog: FactorCatalogData | undefined;
  currentDefinition: FactorDefinitionItem | null;
  currentGeneration: string | null | undefined;
  canSave: boolean;
  storageReady: boolean;
  stale: boolean;
  canRebase: boolean;
  busy: boolean;
  onChange: (next: FactorEditorDraft) => void;
  onClose: () => void;
  onRebase: () => void;
  onSubmit: () => void;
}) {
  const [errors, setErrors] = useState<FieldErrors>({});
  const [comparedVersion, setComparedVersion] = useState<string | null>(null);
  const [fieldSearch, setFieldSearch] = useState("");
  const [allFields, setAllFields] = useState(false);
  const nameRef = useRef<HTMLInputElement>(null);
  const expressionRef = useRef<HTMLTextAreaElement>(null);
  if (draft === null) return null;
  const choices = new Map<string, string>([
    ["技术", "技术"],
    ["价量", "价量"],
    ["动量", "动量"],
    ["质量", "质量"],
    ["估值", "估值"],
    ["基本面", "基本面"],
  ]);
  for (const row of catalog?.definitions ?? []) choices.set(row.category, row.category_label);
  choices.set(draft.category, draft.category_label);
  const update = (patch: Partial<FactorEditorDraft>) => {
    onChange({ ...draft, ...patch });
    setErrors({});
  };
  const insertField = (column: string) => {
    if (busy || !canSave || !capabilities?.fields.some((field) => field.column === column)) return;
    const input = expressionRef.current;
    const start = input?.selectionStart ?? draft.expression.length;
    const end = input?.selectionEnd ?? draft.expression.length;
    const expression = `${draft.expression.slice(0, start)}${column}${draft.expression.slice(end)}`;
    update({ expression });
    requestAnimationFrame(() => {
      input?.focus();
      input?.setSelectionRange(start + column.length, start + column.length);
    });
  };
  const submit = () => {
    if (!storageReady || stale || busy || !canSave) return;
    const nextErrors = checkDraft(draft);
    setErrors(nextErrors);
    if (Object.keys(nextErrors).length === 0) onSubmit();
    else if (nextErrors.name_zh) nameRef.current?.focus();
    else expressionRef.current?.focus();
  };
  const currentVersionKey = `${currentGeneration}:${currentDefinition?.factor_id}:${currentDefinition?.version}:${currentDefinition?.content_sha256}`;
  const comparisonOpen = comparedVersion === currentVersionKey && currentDefinition !== null;
  const fields = capabilities?.fields ?? [];
  const hasMinute = fields.some((field) => field.value_semantics === "minute_features_derived");
  const hasMarket = fields.some((field) => field.value_semantics === "market_temperature_stored");
  const search = fieldSearch.trim().toLocaleLowerCase();
  const visibleFields = search
    ? fields.filter((field) =>
        `${factorFieldName(field)} ${field.name_zh} ${field.column}`
          .toLocaleLowerCase()
          .includes(search),
      )
    : allFields
      ? fields
      : fields.slice(0, 6);
  const groups = hasMarket
    ? [
        {
          label: "日线",
          fields: visibleFields.filter(
            (field) =>
              field.value_semantics !== "market_temperature_stored" &&
              field.value_semantics !== "minute_features_derived",
          ),
        },
        {
          label: "分钟",
          fields: visibleFields.filter(
            (field) => field.value_semantics === "minute_features_derived",
          ),
        },
        {
          label: "市场温度",
          fields: visibleFields.filter(
            (field) => field.value_semantics === "market_temperature_stored",
          ),
        },
      ].filter((group) => group.fields.length)
    : [{ label: null, fields: visibleFields }];
  return (
    <SideDrawer
      open={open}
      onClose={onClose}
      title={draft.mode === "create" ? "新建因子" : "编辑因子"}
      wide
      footer={
        <div className="factor-editor-actions">
          <Button onClick={onClose}>关闭</Button>
          <Button
            variant="primary"
            disabled={!storageReady || stale || busy || !canSave}
            onClick={submit}
          >
            {draft.mode === "create" ? "保存因子" : "保存新版本"}
          </Button>
        </div>
      }
    >
      <form
        className="factor-editor"
        onSubmit={(event) => {
          event.preventDefault();
          submit();
        }}
      >
        {draft.mode === "edit" ? (
          <p className="factor-editor-note">保存新版本，旧版本仍保留。</p>
        ) : null}
        {stale ? (
          <div className="factor-editor-warning" role="alert">
            <p>
              {draft.mode === "create"
                ? "数据已更新，请核对后继续。"
                : currentDefinition?.archived
                  ? "该因子已归档，草稿仍保留。"
                  : "因子已更新，请比对当前版本。"}
            </p>
            {draft.mode === "create" ? (
              <Button size="sm" disabled={!canRebase} onClick={onRebase}>
                使用当前数据继续
              </Button>
            ) : currentDefinition?.archived ? null : (
              <Button
                size="sm"
                disabled={!canRebase}
                onClick={() => setComparedVersion(currentVersionKey)}
              >
                比对当前版本
              </Button>
            )}
          </div>
        ) : null}
        {stale && comparisonOpen ? (
          <section className="factor-editor-comparison" aria-label="当前版本比对">
            <h3>当前第 {currentDefinition.version} 版</h3>
            <div className="factor-editor-comparison-meta">
              <strong>{currentDefinition.name_zh}</strong>
              <span>
                {currentDefinition.category_label} · {currentDefinition.direction_label}
              </span>
            </div>
            <code>{currentDefinition.expression}</code>
            <p className="factor-editor-note">下方草稿仍保留，可继续修改。</p>
            <Button
              size="sm"
              disabled={!canRebase}
              onClick={() => {
                onRebase();
                setComparedVersion(null);
              }}
            >
              使用当前版本为基准
            </Button>
          </section>
        ) : null}
        <div className="factor-editor-basics">
          <label className="factor-editor-field">
            <span>中文名</span>
            <input
              ref={nameRef}
              className="inp"
              aria-label="中文名"
              aria-invalid={!!errors.name_zh}
              aria-describedby={errors.name_zh ? "factor-name-error" : undefined}
              value={draft.name_zh}
              maxLength={80}
              onChange={(event) => update({ name_zh: event.target.value })}
            />
            {errors.name_zh ? (
              <small id="factor-name-error" role="alert">
                {errors.name_zh}
              </small>
            ) : null}
          </label>
          <label className="factor-editor-field">
            <span>分类</span>
            <select
              className="inp"
              aria-label="分类"
              value={draft.category}
              onChange={(event) =>
                update({
                  category: event.target.value,
                  category_label: choices.get(event.target.value) ?? event.target.value,
                })
              }
            >
              {Array.from(choices, ([value, label]) => (
                <option value={value} key={value}>
                  {label}
                </option>
              ))}
            </select>
            {errors.category ? <small role="alert">{errors.category}</small> : null}
          </label>
          <label className="factor-editor-field">
            <span>方向</span>
            <select
              className="inp"
              aria-label="方向"
              value={draft.direction}
              onChange={(event) =>
                update({
                  direction:
                    event.target.value === "lower_is_better"
                      ? "lower_is_better"
                      : "higher_is_better",
                })
              }
            >
              <option value="higher_is_better">偏好高值</option>
              <option value="lower_is_better">偏好低值</option>
            </select>
          </label>
        </div>
        <div className="factor-editor-help">
          <span>{hasMinute || hasMarket ? "字段" : "日线字段"}</span>
          <div className="factor-editor-field-tools">
            <Tip
              content={
                hasMinute
                  ? "字段来自当前可用目录，点击插入；分钟仅用于历史回顾，固定时点和缺因见字段与结果说明。"
                  : (capabilities?.coverage_note_zh ?? "字段能力暂时无法核对。")
              }
            >
              字段说明
            </Tip>
            {fields.length > 6 ? (
              <Button
                size="sm"
                variant="ghost"
                aria-expanded={allFields}
                onClick={() => setAllFields(!allFields)}
              >
                {allFields ? "收起字段" : "全部字段"}
              </Button>
            ) : null}
          </div>
        </div>
        <input
          className="inp factor-editor-search"
          type="search"
          aria-label={hasMinute || hasMarket ? "搜索字段" : "搜索日线字段"}
          placeholder="按中文名搜索字段"
          value={fieldSearch}
          maxLength={64}
          onChange={(event) => setFieldSearch(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter") event.preventDefault();
          }}
        />
        {groups.map((group) => (
          <fieldset key={group.label ?? "daily"} className="factor-editor-field-group">
            {group.label ? <legend>{group.label}</legend> : null}
            <div className="factor-editor-fields">
              {group.fields.map((field) => (
                <div className="factor-editor-field-choice" key={field.column}>
                  <Button
                    size="sm"
                    aria-label={`插入${factorFieldName(field)}`}
                    disabled={busy || !canSave}
                    onClick={() => insertField(field.column)}
                  >
                    {factorFieldName(field)}
                  </Button>
                  <Tip content={<FactorFieldInfo field={field} />} interactive>
                    <Button
                      size="sm"
                      variant="ghost"
                      aria-label={`${factorFieldName(field)}说明`}
                      className="factor-editor-field-info"
                    >
                      <span aria-hidden="true">ⓘ</span>
                    </Button>
                  </Tip>
                </div>
              ))}
            </div>
          </fieldset>
        ))}
        {fields.length === 0 ? (
          <p className="factor-editor-note" role="status">
            字段暂时无法核对，请刷新后再插入。
          </p>
        ) : visibleFields.length === 0 ? (
          <p className="factor-editor-note" role="status">
            没有匹配的字段
          </p>
        ) : null}
        <label className="factor-editor-field">
          <span>
            表达式{" "}
            <Tip
              content={`可用算子：${capabilities?.runnable_operators.join("、") ?? "待核对"}。${capabilities?.unavailable_operators.map((item) => `${item.name}：${item.reason_zh}`).join("；") ?? ""}`}
            >
              算子帮助
            </Tip>
          </span>
          <textarea
            ref={expressionRef}
            className="inp mono"
            aria-label="表达式"
            aria-invalid={!!errors.expression}
            aria-describedby={errors.expression ? "factor-expression-error" : undefined}
            value={draft.expression}
            rows={6}
            maxLength={2048}
            spellCheck={false}
            onChange={(event) => update({ expression: event.target.value })}
          />
          {errors.expression ? (
            <small id="factor-expression-error" role="alert">
              {errors.expression}
            </small>
          ) : null}
        </label>
        <p className="factor-editor-note">保存公式后，实际数据范围在检验时核对</p>
        {!storageReady ? (
          <p className="factor-editor-warning" role="alert">
            浏览器无法保存草稿，暂不能提交。
          </p>
        ) : null}
        {!canSave && storageReady ? (
          <p className="factor-editor-warning" role="alert">
            暂不能保存，请刷新后重试。
          </p>
        ) : null}
      </form>
    </SideDrawer>
  );
}
