import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type FactorDefinitionItem = Schemas["FactorDefinitionItem"];
export type FactorCatalogData = Schemas["FactorCatalogData"];
type FactorCatalogEnvelope = Schemas["Envelope_FactorCatalogData_"];

export function useFactorCatalog(
  generationId: string | null | undefined,
): ServingQueryResult<FactorCatalogData> {
  return useServingQuery(
    ["factors", "definitions", generationId],
    async (): Promise<FactorCatalogEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/factors/definitions", {
        params: { query: { generation_id: generationId ?? undefined } },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 409
            ? "数据已更新，请重新查看因子。"
            : "因子库暂时无法加载，请稍后重试。",
        );
      }
      return data;
    },
    { enabled: typeof generationId === "string" },
  );
}
