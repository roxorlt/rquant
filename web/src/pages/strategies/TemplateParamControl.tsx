import type { ScreenParameter } from "@/api/screen";
import { ParamControl, Tip } from "@/ui";
import type { ParameterValue } from "@/ui/ParamControl";

export function TemplateParamControl({
  parameter,
  value,
  onChange,
}: {
  parameter: ScreenParameter;
  value: ParameterValue;
  onChange: (value: ParameterValue) => void;
}) {
  const field = typeof value === "string" ? /^([A-Z][A-Z0-9_]*)\[(\d*)\]$/.exec(value) : null;
  const shifted = parameter.input === "field" || parameter.input === "operand";
  if (!shifted) return <ParamControl parameter={parameter} value={value} onChange={onChange} />;
  return (
    <div className="template-operand">
      <ParamControl
        parameter={parameter}
        value={field ? `${field[1]}[0]` : value}
        onChange={(next) => {
          const selected = typeof next === "string" ? /^([A-Z][A-Z0-9_]*)\[0\]$/.exec(next) : null;
          onChange(selected ? `${selected[1]}[${field?.[2] || "0"}]` : next);
        }}
      />
      {field ? (
        <label className="field">
          <span className="lbl">
            前几日{" "}
            <Tip content="0 为所选交易日，最多回看 30 个交易日。">
              <span
                role="img"
                aria-label={`${parameter.label}相对日期说明`}
                className="screen-help"
              >
                ?
              </span>
            </Tip>
          </span>
          <input
            className="inp num"
            type="number"
            min={0}
            max={30}
            step={1}
            required
            aria-label={`${parameter.label}相对日期`}
            value={field[2]}
            onChange={(event) => onChange(`${field[1]}[${event.target.value}]`)}
          />
        </label>
      ) : null}
    </div>
  );
}
