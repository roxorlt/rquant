import { useEffect, useState } from "react";
import type { ScreenParameter } from "@/api/screen";
import { type ParameterValue, Tip } from "@/ui";

const CUSTOM_MA = "__custom_ma__";
const CUSTOM_RSI = "__custom_rsi__";
const NUMBER = "__number__";
const EDITABLE_MA = /^MA(.*?)\[(.*?)\]$/;
const EDITABLE_RSI = /^RSI(.*?)\[(.*?)\]$/;

export function CustomMaParamControl({
  parameter,
  value,
  onChange,
  allowRsi = false,
  numberUnit,
}: {
  parameter: ScreenParameter;
  value: ParameterValue;
  onChange: (value: ParameterValue) => void;
  allowRsi?: boolean;
  numberUnit?: string | null;
}) {
  const options = parameter.options ?? [];
  const [mode, setMode] = useState<"field" | "ma" | "rsi" | "number">(() => {
    if (typeof value === "number") return "number";
    if (allowRsi && typeof value === "string" && EDITABLE_RSI.test(value)) return "rsi";
    if (
      typeof value === "string" &&
      EDITABLE_MA.test(value) &&
      !options.some((option) => option.value === value)
    )
      return "ma";
    return "field";
  });
  const parts =
    typeof value === "string"
      ? mode === "rsi"
        ? EDITABLE_RSI.exec(value)
        : EDITABLE_MA.exec(value)
      : null;
  const period = parts?.[1] ?? (mode === "rsi" ? "14" : "20");
  const offset = parts?.[2] ?? "0";

  useEffect(() => {
    if (!allowRsi && mode === "rsi") setMode("field");
  }, [allowRsi, mode]);

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
        value={
          mode === "ma"
            ? CUSTOM_MA
            : mode === "rsi"
              ? CUSTOM_RSI
              : mode === "number"
                ? NUMBER
                : String(value ?? "")
        }
        onChange={(event) => {
          if (event.target.value === CUSTOM_MA) {
            setMode("ma");
            onChange("MA20[0]");
          } else if (event.target.value === CUSTOM_RSI && allowRsi) {
            setMode("rsi");
            onChange("RSI14[0]");
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
        {allowRsi ? <option value={CUSTOM_RSI}>自定义 RSI</option> : null}
        {parameter.input === "operand" ? <option value={NUMBER}>固定数字</option> : null}
      </select>
      {mode === "ma" || (mode === "rsi" && allowRsi) ? (
        <div className="screen-ma-values">
          <label className="field">
            <span className="lbl">周期（日）</span>
            <input
              className="inp num"
              type="number"
              inputMode="numeric"
              step={1}
              min={2}
              max={mode === "rsi" ? 60 : 250}
              aria-label={`${parameter.label}${mode === "rsi" ? "RSI 周期（日）" : "均线周期（日）"}`}
              value={period}
              onChange={(event) =>
                onChange(`${mode === "rsi" ? "RSI" : "MA"}${event.target.value}[${offset}]`)
              }
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
              onChange={(event) =>
                onChange(`${mode === "rsi" ? "RSI" : "MA"}${period}[${event.target.value}]`)
              }
            />
          </label>
        </div>
      ) : mode === "number" && parameter.input === "operand" ? (
        <div className="screen-number">
          <input
            className="inp num"
            type="number"
            inputMode="decimal"
            step="any"
            aria-label={`${parameter.label}数值${numberUnit ? `（${numberUnit}）` : ""}`}
            value={typeof value === "number" ? value : ""}
            onChange={(event) =>
              onChange(event.target.value === "" ? "" : Number(event.target.value))
            }
          />
          {numberUnit ? (
            <span className="screen-unit" aria-hidden="true">
              {numberUnit}
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
