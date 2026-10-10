import { type ReactNode, useEffect, useId, useRef, useState } from "react";
import { Button, EmptyState, PageSkeleton, Tip } from "@/ui";

// Presentation drafts only. The caller maps the frozen public DTO and owns submission.
export type MinuteStudyMode = "grid" | "random" | "ablation" | "walk_forward";
export type StudyAxisInputMode = "values" | "range";
export interface MinuteStudyAxisDraft {
  fieldKey: string;
  inputMode: StudyAxisInputMode;
  valuesText: string;
  selectedValues: string[];
  minimum: string;
  maximum: string;
  step: string;
}
export interface MinuteStudyDraft {
  mode: MinuteStudyMode;
  scoreProfile: string;
  topN: string;
  minTrades: string;
  randomTrials: string;
  seed: string;
  axes: MinuteStudyAxisDraft[];
  windows: { folds: string; minTrainingDates: string; validationDates: string };
}
export interface MinuteStudyControlsProps {
  /** Actor, data version, source and complete recipe identity supplied by the caller. */
  scopeKey: string | null;
  /** A restored original request has its own stable key; it never receives a new UUID here. */
  draftKey?: string;
  status: "loading" | "ready" | "unavailable" | "submitting" | "pending" | "failed";
  message?: string | null;
  detail?: string | null;
  defaultDraft: MinuteStudyDraft;
  initialDraft?: MinuteStudyDraft | null;
  modes: readonly {
    key: MinuteStudyMode;
    label: string;
    available: boolean;
    detail: string | null;
  }[];
  scores: readonly { key: string; label: string; detail: string | null; available?: boolean }[];
  fields: readonly {
    key: string;
    label: string;
    kind: "number" | "choice";
    available: boolean;
    inputModes: readonly StudyAxisInputMode[];
    choices?: readonly { value: string; label: string }[];
    detail: string | null;
  }[];
  ablations: readonly { key: string; label: string; detail: string | null }[];
  bounds: {
    topN: NumberBounds;
    minTrades: NumberBounds;
    randomTrials: NumberBounds;
    seed: NumberBounds;
    folds: NumberBounds;
    minTrainingDates: NumberBounds;
    validationDates: NumberBounds;
    maxSearchFields: number;
  };
  /** Reuse the caller's original source, parameters and six controlled date fields. */
  context?: ReactNode;
  /** Completeness checks input presence/bounds only; Python validates and expands recipes. */
  onDraftChange: (draft: MinuteStudyDraft, complete: boolean) => void;
}
interface NumberBounds {
  min: number;
  max: number;
}

function copyDraft(draft: MinuteStudyDraft): MinuteStudyDraft {
  return {
    ...draft,
    axes: draft.axes.map((axis) => ({ ...axis, selectedValues: [...axis.selectedValues] })),
    windows: { ...draft.windows },
  };
}

function integerPresent(value: string, bounds: NumberBounds) {
  if (!/^\d+$/.test(value)) return false;
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= bounds.min && number <= bounds.max;
}

function draftComplete(draft: MinuteStudyDraft, props: MinuteStudyControlsProps) {
  if (
    props.scopeKey === null ||
    (props.status !== "ready" && props.status !== "failed") ||
    !props.modes.some((mode) => mode.key === draft.mode && mode.available) ||
    !props.scores.some((score) => score.key === draft.scoreProfile && score.available !== false) ||
    !integerPresent(draft.topN, props.bounds.topN) ||
    !integerPresent(draft.minTrades, props.bounds.minTrades) ||
    !integerPresent(draft.seed, props.bounds.seed)
  )
    return false;
  if (draft.mode === "ablation") return props.ablations.length === 5;
  if (draft.mode === "walk_forward") {
    return (
      integerPresent(draft.windows.folds, props.bounds.folds) &&
      integerPresent(draft.windows.minTrainingDates, props.bounds.minTrainingDates) &&
      integerPresent(draft.windows.validationDates, props.bounds.validationDates)
    );
  }
  if (
    draft.axes.length === 0 ||
    draft.axes.length > props.bounds.maxSearchFields ||
    new Set(draft.axes.map((axis) => axis.fieldKey)).size !== draft.axes.length ||
    (draft.mode === "random" && !integerPresent(draft.randomTrials, props.bounds.randomTrials))
  )
    return false;
  return draft.axes.every((axis) => {
    const field = props.fields.find((item) => item.key === axis.fieldKey && item.available);
    if (!field?.inputModes.includes(axis.inputMode)) return false;
    if (axis.inputMode === "range") {
      return [axis.minimum, axis.maximum, axis.step].every((text) => text.trim() !== "");
    }
    if (field.kind === "choice") {
      return (
        axis.selectedValues.length > 0 &&
        axis.selectedValues.every((value) =>
          field.choices?.some((choice) => choice.value === value),
        )
      );
    }
    return axis.valuesText.trim() !== "";
  });
}

function Help({ label, detail }: { label: string; detail: string | null | undefined }) {
  if (!detail) return null;
  return (
    <Tip interactive content={detail}>
      <button className="bt-context-tip bt-minute-tip" type="button" aria-label={`${label}说明`}>
        说明
      </button>
    </Tip>
  );
}

function CountInput({
  label,
  value,
  bounds,
  disabled,
  onChange,
}: {
  label: string;
  value: string;
  bounds: NumberBounds;
  disabled: boolean;
  onChange: (value: string) => void;
}) {
  return (
    <label className="bt-parameter-field">
      <span>{label}</span>
      <input
        className="inp num"
        type="number"
        inputMode="numeric"
        aria-label={label}
        value={value}
        min={bounds.min}
        max={bounds.max}
        step="1"
        required
        disabled={disabled}
        aria-invalid={value !== "" && !integerPresent(value, bounds)}
        onChange={(event) => onChange(event.target.value)}
      />
    </label>
  );
}

function StudyControlsBody(props: MinuteStudyControlsProps) {
  const controlId = useId();
  const [draft, setDraft] = useState(() => copyDraft(props.initialDraft ?? props.defaultDraft));
  const callback = useRef(props.onDraftChange);
  callback.current = props.onDraftChange;
  const complete = draftComplete(draft, props);
  useEffect(() => callback.current(copyDraft(draft), complete), [draft, complete]);
  const disabled =
    props.scopeKey === null ||
    ["loading", "unavailable", "submitting", "pending"].includes(props.status);
  const mode = props.modes.find((item) => item.key === draft.mode);
  const score = props.scores.find((item) => item.key === draft.scoreProfile);
  const searching = draft.mode === "grid" || draft.mode === "random";
  const unusedField = props.fields.find(
    (field) =>
      field.available &&
      field.inputModes.length > 0 &&
      !draft.axes.some((axis) => axis.fieldKey === field.key),
  );
  const axisChange = (index: number, update: Partial<MinuteStudyAxisDraft>) => {
    setDraft((previous) => ({
      ...previous,
      axes: previous.axes.map((axis, position) =>
        position === index ? { ...axis, ...update } : axis,
      ),
    }));
  };
  const message =
    props.message ??
    (props.status === "pending"
      ? "任务待确认"
      : props.status === "submitting"
        ? "正在保存原请求"
        : props.status === "unavailable" || props.scopeKey === null
          ? "研究暂不可用"
          : props.status === "failed"
            ? "研究处理失败"
            : null);
  if (props.status === "loading") return <PageSkeleton />;

  return (
    <fieldset className="bt-parameter-controls" aria-label="研究设置">
      <legend>研究设置</legend>
      {props.context}
      {message ? (
        <div className="bt-study-status" role="status">
          <span>{message}</span>
          <Help label="研究状态" detail={props.detail} />
        </div>
      ) : null}
      <div className="bt-parameter-grid">
        <div className="bt-parameter-field">
          <span>
            <label htmlFor={`${controlId}-mode`}>研究方式</label>{" "}
            <Help label="研究方式" detail={mode?.detail} />
          </span>
          <select
            id={`${controlId}-mode`}
            className="inp"
            aria-label="研究方式"
            value={mode ? draft.mode : ""}
            disabled={disabled || !props.modes.some((item) => item.available)}
            onChange={(event) => {
              const next = props.modes.find(
                (item) => item.key === event.target.value && item.available,
              );
              if (next) setDraft((previous) => ({ ...previous, mode: next.key }));
            }}
          >
            {!mode ? <option value="">暂无可用方式</option> : null}
            {props.modes.map((item) => (
              <option key={item.key} value={item.key} disabled={!item.available}>
                {item.label}
              </option>
            ))}
          </select>
        </div>
        <div className="bt-parameter-field">
          <span>
            <label htmlFor={`${controlId}-score`}>评分方式</label>{" "}
            <Help label="评分" detail={score?.detail} />
          </span>
          <select
            id={`${controlId}-score`}
            className="inp"
            aria-label="评分方式"
            value={score ? draft.scoreProfile : ""}
            disabled={disabled || !props.scores.some((item) => item.available !== false)}
            onChange={(event) => {
              const next = props.scores.find(
                (item) => item.key === event.target.value && item.available !== false,
              );
              if (next) setDraft((previous) => ({ ...previous, scoreProfile: next.key }));
            }}
          >
            {!score ? <option value="">原评分暂不可用</option> : null}
            {props.scores.map((item) => (
              <option key={item.key} value={item.key} disabled={item.available === false}>
                {item.label}
              </option>
            ))}
          </select>
        </div>
        <CountInput
          label="每时点最多入选"
          value={draft.topN}
          bounds={props.bounds.topN}
          disabled={disabled}
          onChange={(topN) => setDraft((previous) => ({ ...previous, topN }))}
        />
        <CountInput
          label="最少闭环交易"
          value={draft.minTrades}
          bounds={props.bounds.minTrades}
          disabled={disabled}
          onChange={(minTrades) => setDraft((previous) => ({ ...previous, minTrades }))}
        />
        <CountInput
          label="研究随机种子"
          value={draft.seed}
          bounds={props.bounds.seed}
          disabled={disabled}
          onChange={(seed) => setDraft((previous) => ({ ...previous, seed }))}
        />
        {draft.mode === "random" ? (
          <CountInput
            label="随机方案数量"
            value={draft.randomTrials}
            bounds={props.bounds.randomTrials}
            disabled={disabled}
            onChange={(randomTrials) => setDraft((previous) => ({ ...previous, randomTrials }))}
          />
        ) : null}
      </div>
      {props.scores
        .filter((item) => item.available === false)
        .map((item) => (
          <Tip key={item.key} content={item.detail}>
            <span className="bt-context-tip">{item.label}不可用</span>
          </Tip>
        ))}
      {props.modes.some((item) => !item.available) ? (
        <div className="bt-study-status">
          {props.modes
            .filter((item) => !item.available)
            .map((item) => (
              <Tip key={item.key} interactive content={item.detail}>
                <button className="bt-context-tip bt-minute-tip" type="button">
                  {item.label}不可用
                </button>
              </Tip>
            ))}
        </div>
      ) : null}
      {searching ? (
        <fieldset className="bt-parameter-controls" aria-label="搜索参数">
          <legend>搜索参数</legend>
          {draft.axes.map((axis, index) => {
            const field = props.fields.find((item) => item.key === axis.fieldKey);
            return (
              <fieldset className="bt-parameter-section" key={axis.fieldKey}>
                <legend>{field?.label ?? "原参数暂不可用"}</legend>
                <div className="bt-parameter-grid">
                  <div className="bt-parameter-field">
                    <span>
                      <label htmlFor={`${controlId}-field-${index}`}>参数字段</label>{" "}
                      <Help label={`参数 ${index + 1}`} detail={field?.detail} />
                    </span>
                    <select
                      id={`${controlId}-field-${index}`}
                      className="inp"
                      aria-label={`参数字段 ${index + 1}`}
                      value={field ? axis.fieldKey : ""}
                      disabled={disabled}
                      onChange={(event) => {
                        const next = props.fields.find(
                          (item) =>
                            item.key === event.target.value &&
                            item.available &&
                            item.inputModes.length > 0 &&
                            !draft.axes.some(
                              (other, position) =>
                                position !== index && other.fieldKey === item.key,
                            ),
                        );
                        const inputMode = next?.inputModes[0];
                        if (next && inputMode)
                          axisChange(index, {
                            fieldKey: next.key,
                            inputMode,
                            valuesText: "",
                            selectedValues: [],
                            minimum: "",
                            maximum: "",
                            step: "",
                          });
                      }}
                    >
                      {!field ? <option value="">原参数暂不可用</option> : null}
                      {props.fields.map((item) => (
                        <option
                          key={item.key}
                          value={item.key}
                          disabled={
                            !item.available ||
                            draft.axes.some(
                              (other, position) =>
                                position !== index && other.fieldKey === item.key,
                            )
                          }
                        >
                          {item.label}
                        </option>
                      ))}
                    </select>
                  </div>
                  <label className="bt-parameter-field">
                    <span>取值方式</span>
                    <select
                      className="inp"
                      aria-label={`取值方式 ${index + 1}`}
                      value={axis.inputMode}
                      disabled={disabled || !field?.available}
                      onChange={(event) => {
                        const next = field?.inputModes.find(
                          (value) => value === event.target.value,
                        );
                        if (next) axisChange(index, { inputMode: next });
                      }}
                    >
                      {field?.inputModes.map((value) => (
                        <option key={value} value={value}>
                          {value === "values" ? "有限取值" : "范围"}
                        </option>
                      ))}
                    </select>
                  </label>
                  {axis.inputMode === "range" ? (
                    (["minimum", "maximum", "step"] as const).map((key) => {
                      const label =
                        key === "minimum" ? "最小值" : key === "maximum" ? "最大值" : "步长";
                      return (
                        <label className="bt-parameter-field" key={key}>
                          <span>{label}</span>
                          <input
                            className="inp num"
                            aria-label={`${label} ${index + 1}`}
                            value={axis[key]}
                            disabled={disabled || !field?.available}
                            onChange={(event) => axisChange(index, { [key]: event.target.value })}
                          />
                        </label>
                      );
                    })
                  ) : field?.kind === "choice" ? (
                    field.choices?.map((choice) => (
                      <label className="bt-parameter-switch" key={choice.value}>
                        <span>{choice.label}</span>
                        <input
                          type="checkbox"
                          aria-label={`${field.label}：${choice.label}`}
                          checked={axis.selectedValues.includes(choice.value)}
                          disabled={disabled || !field.available}
                          onChange={(event) =>
                            axisChange(index, {
                              selectedValues: event.target.checked
                                ? [...axis.selectedValues, choice.value]
                                : axis.selectedValues.filter((value) => value !== choice.value),
                            })
                          }
                        />
                      </label>
                    ))
                  ) : (
                    <label className="bt-parameter-field">
                      <span>参数取值</span>
                      <input
                        className="inp num"
                        aria-label={`参数取值 ${index + 1}`}
                        value={axis.valuesText}
                        disabled={disabled || !field?.available}
                        onChange={(event) => axisChange(index, { valuesText: event.target.value })}
                      />
                    </label>
                  )}
                </div>
                <div className="bt-runtime-actions">
                  <Button
                    size="sm"
                    disabled={disabled}
                    aria-label={`移除参数 ${index + 1}`}
                    onClick={() =>
                      setDraft((previous) => ({
                        ...previous,
                        axes: previous.axes.filter((_, position) => position !== index),
                      }))
                    }
                  >
                    移除
                  </Button>
                </div>
              </fieldset>
            );
          })}
          <div className="bt-runtime-actions">
            <Button
              size="sm"
              disabled={
                disabled || !unusedField || draft.axes.length >= props.bounds.maxSearchFields
              }
              onClick={() => {
                const inputMode = unusedField?.inputModes[0];
                if (!unusedField || !inputMode) return;
                setDraft((previous) => ({
                  ...previous,
                  axes: [
                    ...previous.axes,
                    {
                      fieldKey: unusedField.key,
                      inputMode,
                      valuesText: "",
                      selectedValues: [],
                      minimum: "",
                      maximum: "",
                      step: "",
                    },
                  ],
                }));
              }}
            >
              添加参数
            </Button>
          </div>
        </fieldset>
      ) : draft.mode === "ablation" ? (
        <fieldset className="bt-parameter-section" aria-label="五组固定对照">
          <legend>五组固定对照</legend>
          {props.ablations.length === 0 ? (
            <EmptyState title="尚无固定对照" />
          ) : (
            <ul>
              {props.ablations.map((item) => (
                <li key={item.key}>
                  {item.label} <Help label={item.label} detail={item.detail} />
                </li>
              ))}
            </ul>
          )}
        </fieldset>
      ) : (
        <fieldset className="bt-parameter-section" aria-label="滚动日期">
          <legend>滚动日期</legend>
          <div className="bt-parameter-grid">
            <CountInput
              label="滚动窗口数量"
              value={draft.windows.folds}
              bounds={props.bounds.folds}
              disabled={disabled}
              onChange={(folds) =>
                setDraft((previous) => ({ ...previous, windows: { ...previous.windows, folds } }))
              }
            />
            <CountInput
              label="最少训练交易日"
              value={draft.windows.minTrainingDates}
              bounds={props.bounds.minTrainingDates}
              disabled={disabled}
              onChange={(minTrainingDates) =>
                setDraft((previous) => ({
                  ...previous,
                  windows: { ...previous.windows, minTrainingDates },
                }))
              }
            />
            <CountInput
              label="验证交易日"
              value={draft.windows.validationDates}
              bounds={props.bounds.validationDates}
              disabled={disabled}
              onChange={(validationDates) =>
                setDraft((previous) => ({
                  ...previous,
                  windows: { ...previous.windows, validationDates },
                }))
              }
            />
          </div>
        </fieldset>
      )}
    </fieldset>
  );
}

export function MinuteStudyControls(props: MinuteStudyControlsProps) {
  const safe =
    props.scopeKey === null
      ? {
          ...props,
          initialDraft: null,
          defaultDraft: {
            ...props.defaultDraft,
            scoreProfile: "",
            topN: "",
            minTrades: "",
            randomTrials: "",
            seed: "",
            axes: [],
            windows: { folds: "", minTrainingDates: "", validationDates: "" },
          },
          modes: [],
          scores: [],
          fields: [],
          ablations: [],
          context: null,
        }
      : props;
  return (
    <StudyControlsBody key={JSON.stringify([props.scopeKey, props.draftKey ?? null])} {...safe} />
  );
}
