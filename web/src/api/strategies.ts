import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type StrategyCatalogItem = Schemas["StrategyCatalogItem"];
export type StrategyParameter = Schemas["StrategyParameter"];
export type StrategyCatalogData = Schemas["StrategyCatalogData"];
type StrategyCatalogEnvelope = Schemas["Envelope_StrategyCatalogData_"];

export function useStrategyCatalog(
  generationId: string | null | undefined,
): ServingQueryResult<StrategyCatalogData> {
  return useServingQuery(
    ["strategies", "catalog", generationId],
    async (): Promise<StrategyCatalogEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/strategies", {
        params: { query: { generation_id: generationId ?? undefined } },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 409
            ? "数据已更新，请重新查看策略。"
            : "策略目录暂时无法加载，请稍后重试。",
        );
      }
      return data;
    },
  );
}
