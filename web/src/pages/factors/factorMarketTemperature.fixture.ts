import type { Schemas } from "@/api/client";
import { diagnosticFactor } from "./factorDiagnostics.fixture";
import captured from "./factorMarketTemperature.fixture.json" with { type: "json" };
import { mixedMinuteResearch } from "./factorMinuteFeature.fixture";

// Public models captured from frozen offline worker outputs and capability code.
export const marketTemperatureCapability = captured.capability as Schemas["FactorCapabilitiesData"];
export const mixedMarketTemperatureCapability =
  captured.mixed_capability as Schemas["FactorCapabilitiesData"];
export const marketTemperatureResearch =
  captured.research as Schemas["FactorStreamResearchDisplay"];
export const nullMarketTemperatureResearch =
  captured.null_research as Schemas["FactorStreamResearchDisplay"];

export const marketTemperatureFactor: Schemas["FactorDefinitionItem"] = {
  ...diagnosticFactor,
  name_zh: "市场温度",
  expression: "close * market_high_60d_ratio_pct",
  dependency_columns: ["close", "market_high_60d_ratio_pct"],
  max_history_window: 0,
};

const market = marketTemperatureResearch.daily_features;
const mixed = mixedMinuteResearch.daily_features;
const marketDay = marketTemperatureResearch.daily_feature_coverage_days?.[0];
if (!market || !mixed || !marketDay) throw new Error("Captured source facts required");

// Explicit synthetic combination for UI grouping; worker integration is tested separately.
export const mixedMarketTemperatureResearch: Schemas["FactorStreamResearchDisplay"] = {
  ...mixedMinuteResearch,
  daily_features: {
    ...mixed,
    value_semantics: "market_temperature_stored",
    market_temperature: market.market_temperature,
    fields: [...mixed.fields, ...market.fields].sort((a, b) => a.column.localeCompare(b.column)),
  },
  daily_feature_coverage_days: mixedMinuteResearch.daily_feature_coverage_days?.map((day) => ({
    ...day,
    counts: [...day.counts, ...marketDay.counts].sort((a, b) => a.column.localeCompare(b.column)),
    market_temperature_values: marketDay.market_temperature_values,
  })),
};
