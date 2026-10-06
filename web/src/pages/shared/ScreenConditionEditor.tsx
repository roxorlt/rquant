import type { ScreenBlock } from "@/api/screen";
import { Button, ParamControl, type ParameterValue, Tip } from "@/ui";
import { CustomMaParamControl } from "../screener/CustomMaParamControl";

export type ScreenConditionDraft = { id: number; key: string; args: Record<string, ParameterValue> };

function fieldUnit(block: ScreenBlock, value: ParameterValue): string | null {
  const option = block.parameters.flatMap((parameter) => parameter.options ?? []).find((item) => item.value === value);
  return /（(%|倍|股|元)）$/.exec(option?.label ?? "")?.[1] ?? null;
}

function numberUnit(block: ScreenBlock, condition: ScreenConditionDraft, key: string): string | null {
  if (condition.key === "between" && (key === "low" || key === "high")) return fieldUnit(block, condition.args.field ?? null);
  if (["gt", "lt", "gte", "lte"].includes(condition.key)) {
    if (key === "left") return fieldUnit(block, condition.args.right ?? null);
    if (key === "right") return fieldUnit(block, condition.args.left ?? null);
  }
  return null;
}

export function ScreenConditionEditor({ conditions, blocks, allowRsi, onUpdate, onRemove }: {
  conditions: ScreenConditionDraft[];
  blocks: ScreenBlock[];
  allowRsi: boolean;
  onUpdate: (id: number, key: string, value: ParameterValue) => void;
  onRemove: (id: number) => void;
}) {
  const byKey = new Map(blocks.map((block) => [block.key, block]));
  return <div className="screen-conditions">{conditions.map((condition,index) => {
    const block = byKey.get(condition.key);
    return <div key={condition.id} className="screen-condition">
      <div className="screen-condition-head">
        <span className="screen-index num">{index+1}</span>
        <Tip content={block?.hint ?? JSON.stringify({key:condition.key,args:condition.args})}><strong>{block?.label ?? "原条件暂不可用"}</strong></Tip>
        <Button size="sm" variant="ghost" aria-label={`删除第 ${index+1} 条条件`} onClick={() => onRemove(condition.id)}>删除</Button>
      </div>
      {block && block.parameters.length>0 ? <div className="screen-params">{block.parameters.map((parameter) => parameter.custom_ma ?
        <CustomMaParamControl key={parameter.key} parameter={parameter} value={condition.args[parameter.key] ?? null} onChange={(value) => onUpdate(condition.id,parameter.key,value)} allowRsi={allowRsi} numberUnit={numberUnit(block,condition,parameter.key)} /> :
        <ParamControl key={parameter.key} parameter={parameter} value={condition.args[parameter.key] ?? null} onChange={(value) => onUpdate(condition.id,parameter.key,value)} numberUnit={numberUnit(block,condition,parameter.key)} extraOption={parameter.input==="choice" && condition.args[parameter.key]!=null && !parameter.options?.some((option) => option.value===String(condition.args[parameter.key])) ? {value:String(condition.args[parameter.key]),label:`原值 ${condition.args[parameter.key]}（暂不可用）`} : null} />
      )}</div> : null}
    </div>;
  })}</div>;
}
