import type { FactorCapabilitiesData } from "@/api/factors";

type FieldInfo = Pick<FactorCapabilitiesData["fields"][number], "unit" | "description_zh">;

const units: Record<NonNullable<FieldInfo["unit"]>, string> = {
  stored_price: "库存价格",
  session_price: "当日价格尺度",
  indicator: "指标点值",
  percent: "百分数（%）",
  ratio: "比例",
  CNY_10000: "万元",
  observations: "观察数",
  binary: "0 / 1",
};

export function FactorFieldInfo({ field }: { field: FieldInfo }) {
  return (
    <>
      <div>{field.description_zh}</div>
      {field.unit ? <div>单位：{units[field.unit]}</div> : null}
    </>
  );
}
