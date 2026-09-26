import type { ScreenParameter } from "@/api/screen";
import { Tip } from "@/ui";

export type ParameterValue = string | number | string[] | null;

export function ParamControl({
  parameter,
  value,
  onChange,
}: {
  parameter: ScreenParameter;
  value: ParameterValue;
  onChange: (value: ParameterValue) => void;
}) {
  const options = parameter.options ?? [];
  const label = (
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
  );
  if (parameter.input === "multi_choice") {
    const selected = Array.isArray(value) ? value : [];
    return (
      <fieldset className="screen-multi">
        <legend className="lbl">{parameter.label}</legend>
        {options.map((option) => (
          <label key={option.value}>
            <input
              type="checkbox"
              checked={selected.includes(option.value)}
              onChange={(event) =>
                onChange(
                  event.target.checked
                    ? [...selected, option.value]
                    : selected.filter((item) => item !== option.value),
                )
              }
            />
            {option.label}
          </label>
        ))}
      </fieldset>
    );
  }
  if (parameter.input === "operand") {
    const numeric = typeof value === "number";
    return (
      <div className="field screen-param">
        {label}
        <select
          className="inp"
          aria-label={parameter.label}
          value={numeric ? "__number__" : String(value ?? "")}
          onChange={(event) =>
            onChange(event.target.value === "__number__" ? 0 : event.target.value)
          }
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
          <option value="__number__">固定数字</option>
        </select>
        {numeric ? (
          <input
            className="inp num"
            type="number"
            step="any"
            aria-label={`${parameter.label}数值`}
            value={value}
            onChange={(event) =>
              onChange(event.target.value === "" ? "" : Number(event.target.value))
            }
          />
        ) : null}
      </div>
    );
  }
  if (parameter.input === "choice" || parameter.input === "field") {
    return (
      <label className="field screen-param">
        {label}
        <select
          className="inp"
          value={String(value ?? "")}
          onChange={(event) => onChange(event.target.value)}
        >
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
      </label>
    );
  }
  const scale = parameter.scale || 1;
  const display = typeof value === "number" ? value * scale : (value ?? "");
  return (
    <label className="field screen-param">
      {label}
      <input
        className="inp num"
        type="number"
        step={parameter.input === "integer" ? 1 : "any"}
        min={parameter.minimum == null ? undefined : parameter.minimum * scale}
        max={parameter.maximum == null ? undefined : parameter.maximum * scale}
        value={display}
        onChange={(event) =>
          onChange(event.target.value === "" ? "" : Number(event.target.value) / scale)
        }
      />
    </label>
  );
}
