import type { Schemas } from "@/api/client";
import captured from "./factorStockFeature.fixture.json" with { type: "json" };

// Captured from offline synthetic workers; the Python contract test validates the wire models.
export const stockCapability = captured.standalone.capability as Schemas["FactorCapabilitiesData"];
export const stockResearch = captured.standalone.research as Schemas["FactorStreamResearchDisplay"];
export const mixedStockCapability = captured.mixed.capability as Schemas["FactorCapabilitiesData"];
export const mixedStockResearch = captured.mixed.research as Schemas["FactorStreamResearchDisplay"];
export const fullMixedStockResearch = captured.mixed_full39
  .research as Schemas["FactorStreamResearchDisplay"];
