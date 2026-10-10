import type { Schemas } from "@/api/client";
import captured from "./factorAuction.fixture.json" with { type: "json" };
import { diagnosticFactor } from "./factorDiagnostics.fixture";
import { mixedMarketTemperatureResearch } from "./factorMarketTemperature.fixture";

export const auctionCapability = captured.capability as Schemas["FactorCapabilitiesData"];
export const mixedAuctionCapability =
  captured.mixed_capability as Schemas["FactorCapabilitiesData"];
export const auctionResearch = captured.research as Schemas["FactorStreamResearchDisplay"];
export const nullAuctionResearch = captured.null_research as Schemas["FactorStreamResearchDisplay"];
export const auctionFactor: Schemas["FactorDefinitionItem"] = {
  ...diagnosticFactor,
  name_zh: "题材竞价",
  expression: "board_auction_amount_ratio + board_gap_up_ratio + board_member_count",
  dependency_columns: ["board_auction_amount_ratio", "board_gap_up_ratio", "board_member_count"],
  max_history_window: 0,
};

const auction = auctionResearch.daily_features;
const mixed = mixedMarketTemperatureResearch.daily_features;
const day = auctionResearch.daily_feature_coverage_days?.[0];
if (!auction || !mixed || !day) throw new Error("Captured auction facts required");
export const mixedAuctionResearch: Schemas["FactorStreamResearchDisplay"] = {
  ...mixedMarketTemperatureResearch,
  daily_features: {
    ...mixed,
    value_semantics: "auction_derived",
    auction: auction.auction,
    fields: [...mixed.fields, ...auction.fields].sort((a, b) => a.column.localeCompare(b.column)),
  },
  daily_feature_coverage_days: mixedMarketTemperatureResearch.daily_feature_coverage_days?.map(
    (original) => ({
      ...original,
      counts: [...original.counts, ...day.counts].sort((a, b) => a.column.localeCompare(b.column)),
      auction_values: day.auction_values,
    }),
  ),
};
