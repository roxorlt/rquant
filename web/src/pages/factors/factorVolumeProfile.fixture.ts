import type { Schemas } from "@/api/client";
import { mixedAuctionResearch } from "./factorAuction.fixture";
import { diagnosticFactor } from "./factorDiagnostics.fixture";
import captured from "./factorVolumeProfile.fixture.json" with { type: "json" };

export const volumeProfileCapability = captured.capability as Schemas["FactorCapabilitiesData"];
export const mixedVolumeProfileCapability =
  captured.mixed_capability as Schemas["FactorCapabilitiesData"];
export const volumeProfileResearch = captured.research as Schemas["FactorStreamResearchDisplay"];
export const nullVolumeProfileResearch =
  captured.null_research as Schemas["FactorStreamResearchDisplay"];
export const volumeProfileFactor: Schemas["FactorDefinitionItem"] = {
  ...diagnosticFactor,
  name_zh: "90日成交分布",
  expression: "vp90_vwap + close",
  dependency_columns: ["close", "vp90_vwap"],
  max_history_window: 0,
};

const vp = volumeProfileResearch.daily_features;
const mixed = mixedAuctionResearch.daily_features;
const day = volumeProfileResearch.daily_feature_coverage_days?.[0];
if (!vp || !mixed || !day) throw new Error("Captured VP facts required");
export const mixedVolumeProfileResearch: Schemas["FactorStreamResearchDisplay"] = {
  ...mixedAuctionResearch,
  daily_features: {
    ...mixed,
    value_semantics: "volume_profile_derived",
    volume_profile: vp.volume_profile,
    fields: [...mixed.fields, ...vp.fields].sort((a, b) => a.column.localeCompare(b.column)),
  },
  daily_feature_coverage_days: mixedAuctionResearch.daily_feature_coverage_days?.map(
    (original) => ({
      ...original,
      counts: [...original.counts, ...day.counts].sort((a, b) => a.column.localeCompare(b.column)),
      volume_profile_values: day.volume_profile_values,
    }),
  ),
};
