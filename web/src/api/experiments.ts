import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type ExperimentItem = Schemas["ExperimentItem"];
export type ExperimentListData = Schemas["ExperimentListData"];
export type FormalExperiment = Schemas["ExperimentAttemptRow"];
export type ExperimentCapabilities = Schemas["ExperimentCapabilities"];
export type ExperimentFamily = Schemas["ExperimentFamilyData"];
export type ExperimentResult = Schemas["ExperimentResultData"];
export type ExperimentComparison = Schemas["ExperimentComparisonData"];
export type ExperimentHeatmap = Schemas["ExperimentHeatmapData"];
export type ExperimentStatistics = Schemas["ExperimentStatisticsData"];
export type ExperimentMetric = Schemas["ExperimentMetric"];
export type ExperimentSearch = Schemas["ExperimentEditableRequest"];
export type ExperimentWrite =
  | Schemas["ExperimentSearchWrite"]
  | Schemas["ExperimentCancelWrite"]
  | Schemas["ExperimentNoteWrite"]
  | Schemas["ExperimentUnsealWrite"]
  | Schemas["ExperimentPolicyWrite"];
export type ExperimentReceipt = Schemas["ExperimentWriteReceipt"];
type ExperimentEnvelope = Schemas["Envelope_ExperimentListData_"];

function required<T extends { serving: { generation_id: string | null } }>(
  data: T | undefined,
  response: Response,
  generation: string,
): T {
  if (data === undefined)
    throw new ApiError(
      response.status,
      response.status === 409 ? "数据已更新，请重新查看实验。" : "实验暂时无法加载，请稍后重试。",
    );
  if (data.serving.generation_id !== generation)
    throw new ApiError(409, "数据已更新，请重新查看实验。");
  return data;
}

export function useExperimentCapabilities(owner: string | null, generation: string | null) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "capabilities"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/experiments/capabilities");
      return required(data, response, generation ?? "");
    },
    { enabled: owner !== null && generation !== null },
  );
}

export function useMyExperiments(
  owner: string,
  generation: string,
  cursor: string | null,
  refresh: number,
) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "mine", cursor, refresh],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/experiments/mine", {
        params: { query: { limit: 20, generation_id: generation, cursor: cursor ?? undefined } },
      });
      return required(data, response, generation);
    },
  );
}

export function useExperimentFamily(
  owner: string,
  generation: string,
  familyId: string | null,
  refresh: number,
) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "family", familyId, refresh],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/experiments/families/{family_id}", {
        params: { path: { family_id: familyId ?? "" }, query: { generation_id: generation } },
      });
      return required(data, response, generation);
    },
    { enabled: familyId !== null },
  );
}

export function useExperimentResult(
  owner: string,
  generation: string,
  item: FormalExperiment | null,
) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "result", item?.experiment_id, item?.result_hash],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/experiments/results/{experiment_id}",
        {
          params: {
            path: { experiment_id: item?.experiment_id ?? "" },
            query: { generation_id: generation, result_hash: item?.result_hash ?? "" },
          },
        },
      );
      return required(data, response, generation);
    },
    { enabled: item?.result_hash != null },
  );
}

export function useExperimentStatistics(
  owner: string,
  generation: string,
  item: FormalExperiment | null,
) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "statistics", item?.experiment_id],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/experiments/results/{experiment_id}/statistics",
        {
          params: {
            path: { experiment_id: item?.experiment_id ?? "" },
            query: { generation_id: generation },
          },
        },
      );
      return required(data, response, generation);
    },
    { enabled: item?.result_hash != null },
  );
}

export function useExperimentComparison(
  owner: string,
  generation: string,
  pair: readonly string[] | null,
) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "compare", pair?.[0], pair?.[1]],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/experiments/compare", {
        params: { query: { a: pair?.[0] ?? "", b: pair?.[1] ?? "", generation_id: generation } },
      });
      return required(data, response, generation);
    },
    { enabled: pair?.length === 2 },
  );
}

export function useExperimentHeatmap(
  owner: string,
  generation: string,
  familyId: string | null,
  selected: string | null,
  x: string,
  y: string,
  phase: string,
  metric: string,
) {
  return useServingQuery(
    ["formal-experiments", owner, generation, "heatmap", familyId, selected, x, y, phase, metric],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/experiments/families/{family_id}/heatmap",
        {
          params: {
            path: { family_id: familyId ?? "" },
            query: { generation_id: generation, selected: selected ?? "", x, y, phase, metric },
          },
        },
      );
      return required(data, response, generation);
    },
    { enabled: familyId !== null && selected !== null && x !== y && x !== "" && y !== "" },
  );
}

export async function submitExperiment(body: ExperimentWrite): Promise<ExperimentReceipt> {
  const { data, response } = await apiClient()
    .POST("/api/v1/experiments/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认，请重试原请求。");
    });
  if (data === undefined || data.command_id !== body.command_id)
    throw new ApiError(response.status, "回执待核对，请重试原请求。");
  return data;
}

export function useExperiments(
  cursor: string | null,
  generationId: string | null,
  refreshKey: number,
): ServingQueryResult<ExperimentListData> {
  return useServingQuery(
    ["experiments", cursor, generationId, refreshKey],
    async (): Promise<ExperimentEnvelope> => {
      if (generationId === null) {
        throw new Error("实验记录暂时无法核对，请稍后重试。");
      }
      const { data, response } = await apiClient().GET("/api/v1/experiments", {
        params: {
          query: {
            limit: 20,
            generation_id: generationId,
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
    { enabled: typeof generationId === "string" },
  );
}
