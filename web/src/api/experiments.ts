import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type ExperimentItem = Schemas["ExperimentItem"];
export type ExperimentListData = Schemas["ExperimentListData"];
type ExperimentEnvelope = Schemas["Envelope_ExperimentListData_"];

export function useExperiments(
  cursor: string | null,
  generationId: string | null,
  refreshKey: number,
): ServingQueryResult<ExperimentListData> {
  return useServingQuery(
    ["experiments", cursor, generationId, refreshKey],
    async (): Promise<ExperimentEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/experiments", {
        params: {
          query: {
            limit: 20,
            generation_id: generationId ?? undefined,
            cursor: cursor ?? undefined,
          },
        },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 409
            ? "数据已更新，请重新查看实验记录。"
            : "实验记录暂时无法加载，请稍后重试。",
        );
      }
      return data;
    },
  );
}
