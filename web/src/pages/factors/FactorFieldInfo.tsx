import type { FactorCapabilitiesData } from "@/api/factors";

type FieldInfo = Pick<
  FactorCapabilitiesData["fields"][number],
  "column" | "name_zh" | "value_semantics" | "unit" | "description_zh"
>;

export function factorFieldName(field: Pick<FieldInfo, "column" | "name_zh" | "value_semantics">) {
  if (field.value_semantics === "minute_features_derived") {
    if (field.column === "signal_amount_accel_5m") return "近5次成交额加速";
    if (field.column === "signal_amount_accel_10m") return "近10次成交额加速";
  }
  return field.name_zh;
}

const units: Record<NonNullable<FieldInfo["unit"]>, string> = {
  stored_price: "库存价格",
  session_price: "当日价格尺度",
  indicator: "指标点值",
  percent: "百分数（%）",
  ratio: "比例",
  CNY_10000: "万元",
  CNY: "元",
  observations: "观察数",
  binary: "0 / 1",
};

export function FactorFieldInfo({ field }: { field: FieldInfo }) {
  const minute = field.value_semantics === "minute_features_derived";
  const acceleration = minute
    ? field.column === "signal_amount_accel_5m"
      ? 5
      : field.column === "signal_amount_accel_10m"
        ? 10
        : null
    : null;
  const description = acceleration
    ? `15:00成交额与此前最近最多${acceleration}次正成交额记录中位数之比；短窗口仍计算，不保证连续${acceleration}分钟。`
    : minute && field.column === "hist_cum_amount_asof_median_20d"
      ? "前最多20个实际观察日截至15:00累计成交额的中位数。"
      : minute && field.column === "hist_intraday_days_20d"
        ? "截至15:00有分钟记录的实际历史日数，0有效。"
        : minute && field.column === "signal_opening_segment_amount"
          ? "固定15:00观察不适用；与缺少目标分钟分别记录。"
          : field.description_zh;
  const unit =
    minute && field.unit === "ratio"
      ? "倍数"
      : minute && field.unit === "observations"
        ? "观察日数"
        : field.unit
          ? units[field.unit]
          : null;
  return (
    <>
      <div>{description}</div>
      {unit ? <div>单位：{unit}</div> : null}
    </>
  );
}
