import type { Schemas } from "@/api/client";
import captured from "./factorMinuteFeature.fixture.json" with { type: "json" };

// Canonical Web responses from offline synthetic workers consuming 11 or all 50 fields.
export const minuteCapability = captured.standalone.capability as Schemas["FactorCapabilitiesData"];
export const minuteResearch = captured.standalone
  .research as Schemas["FactorStreamResearchDisplay"];
export const mixedMinuteCapability = captured.mixed.capability as Schemas["FactorCapabilitiesData"];
export const mixedMinuteResearch = captured.mixed
  .research as Schemas["FactorStreamResearchDisplay"];
