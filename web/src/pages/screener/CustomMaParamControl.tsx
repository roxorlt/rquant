import { useState } from "react";
import type { ScreenParameter } from "@/api/screen";
import { type ParameterValue, Tip } from "@/ui";

const CUSTOM_MA = "__custom_ma__";
const NUMBER = "__number__";
const EDITABLE_MA = /^MA(.*?)\[(.*?)\]$/;

export function CustomMaParamControl({
  parameter,
  value,
  onChange,
}: {
  parameter: ScreenParameter;
  value: ParameterValue;
  onChange: (value: ParameterValue) => void;
}) {
  const options = parameter.options ?? [];
  const [mode, setMode] = useState<"field" | "ma" | "number">(() => {
    if (typeof value === "number") return "number";
    if (
      typeof value === "string" &&
      EDITABLE_MA.test(value) &&
      !options.some((option) => option.value === value)
    )
      return "ma";
    return "field";
  });
  const parts = typeof value === "string" ? EDITABLE_MA.exec(value) : null;
  const period = parts?.[1] ?? "20";
  const offset = parts?.[2] ?? "0";

  return (
    <div className="field screen-param screen-ma-param">
      <span className="lbl">
        {parameter.label}
        {parameter.hint ? (
          <Tip content={parameter.hint}>
            <span className="screen-help" role="img" aria-label={`${parameter.label}说明`}>
              ?
            </span>
          </Tip>
        ) : null}
      </span>
      <select
        className="inp"
        aria-label={parameter.label}
        value={mode === "ma" ? CUSTOM_MA : mode === "number" ? NUMBER : String(value ?? "")}
        onChange={(event) => {
          if (event.target.value === CUSTOM_MA) {
            setMode("ma");
            onChange("MA20[0]");
          } else {
            setMode(event.target.value === NUMBER ? "number" : "field");
            onChange(event.target.value === NUMBER ? 0 : event.target.value);
          }
        }}
      >
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
        <option value={CUSTOM_MA}>自定义均线</option>
        {parameter.input === "operand" ? <option value={NUMBER}>固定数字</option> : null}
      </select>
      {mode === "ma" ? (
        <div className="screen-ma-values">
          <label className="field">
            <span className="lbl">周期（日）</span>
            <input
              className="inp num"
              type="number"
              inputMode="numeric"
              step={1}
              min={2}
              max={250}
              aria-label={`${parameter.label}均线周期（日）`}
              value={period}
              onChange={(event) => onChange(`MA${event.target.value}[${offset}]`)}
            />
          </label>
          <label className="field">
            <span className="lbl">前几日</span>
            <input
              className="inp num"
              type="number"
              inputMode="numeric"
              step={1}
              min={0}
              max={30}
              aria-label={`${parameter.label}相对日期`}
              value={offset}
              onChange={(event) => onChange(`MA${period}[${event.target.value}]`)}
            />
          </label>
        </div>
      ) : mode === "number" && parameter.input === "operand" ? (
        <input
          className="inp num"
          type="number"
          inputMode="decimal"
          step="any"
          aria-label={`${parameter.label}数值`}
          value={typeof value === "number" ? value : ""}
          onChange={(event) =>
            onChange(event.target.value === "" ? "" : Number(event.target.value))
          }
        />
      ) : null}
    </div>
  );
}
