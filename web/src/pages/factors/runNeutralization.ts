import type { FactorRunAvailability, FactorRunNeutralizationOption } from "@/api/factors";
import { isNeutralizationMode, type StoredRun } from "./factorRunState";

const legacy: readonly FactorRunNeutralizationOption[] = [
  { neutralization: "none", label: "无", available: true, reason: null },
  {
    neutralization: "industry",
    label: "行业",
    available: false,
    reason: "当前检验条件尚未开放此方式。",
  },
  {
    neutralization: "industry_size",
    label: "行业 + 市值",
    available: false,
    reason: "当前检验条件尚未开放此方式。",
  },
];

export function neutralizationChoices(
  data: FactorRunAvailability | undefined,
): readonly FactorRunNeutralizationOption[] {
  return data?.neutralizations == null
    ? legacy
    : data.neutralizations.filter((option) => isNeutralizationMode(option.neutralization));
}

export function neutralizationCaption(
  record: StoredRun,
  choices: readonly FactorRunNeutralizationOption[],
): string {
  const mode = record.request.parameters.neutralization;
  const label =
    record.neutralizationLabel ??
    choices.find((option) => option.neutralization === mode)?.label ??
    legacy.find((option) => option.neutralization === mode)?.label;
  return mode === "none" ? "无中性化" : `${label}中性化`;
}

export function neutralizationExplanation(
  choices: readonly FactorRunNeutralizationOption[],
): string {
  const missing = choices
    .filter((option) => !option.available)
    .map((option) => `${option.label}：${option.reason ?? "暂不可用"}`)
    .join("；");
  return [missing, "行业去除行业差异；行业 + 市值同时去除行业与市值影响。缺失数据不补齐。"]
    .filter(Boolean)
    .join("；");
}
