import type { FactorRunParameters } from "@/api/factors";

export const outlierExplanation =
  "MAD 根据当期因子值的中位数和中位绝对偏差，将极端值限制在指定范围内；先处理离群值，再做所选中性化。";

export function outlierCaption(multiple: FactorRunParameters["mad_multiple"]): string {
  return multiple == null ? "不处理离群值" : `MAD ${multiple} 倍`;
}
