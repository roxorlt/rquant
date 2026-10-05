import type { Schemas } from "@/api/client";
import raw from "./paperPortfolio.fixture.json" with { type: "json" };

// Bytes exported from the actual synthetic Web API; variants only test UI states.
export const paperCatalog = raw.catalog as Schemas["Envelope_PaperPortfolioCatalogData_"];
export const paperDetail = raw.detail as Schemas["Envelope_PaperPortfolioDetailData_"];
export const paperHistory = raw.history as Schemas["Envelope_PaperPortfolioHistoryPageView_"];
export const paperGeneration = paperCatalog.serving.generation_id ?? "";
export const paperAccount = paperDetail.data.configuration.account_id;
export function paperEnvelope<T>(data: T, generation = paperGeneration) {
  return { data, serving: { ...paperCatalog.serving, generation_id: generation } };
}
