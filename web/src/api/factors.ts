import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type FactorDefinitionItem = Schemas["FactorDefinitionItem"];
export type FactorCatalogData = Schemas["FactorCatalogData"];
export type FactorArchiveCommandRequest = Schemas["FactorArchiveCommandRequest"];
export type FactorArchiveCommandData = Schemas["FactorArchiveCommandData"];
export type FactorResultItem = Schemas["FactorResultItem"];
export type FactorResultListData = Schemas["FactorResultListData"];
export type FactorResultDetailData = Schemas["FactorResultDetailData"];
export type FactorResearchDisplay = Schemas["FactorResearchDisplay"];
type FactorCatalogEnvelope = Schemas["Envelope_FactorCatalogData_"];
type FactorResultListEnvelope = Schemas["Envelope_FactorResultListData_"];
type FactorResultDetailEnvelope = Schemas["Envelope_FactorResultDetailData_"];

export async function postFactorArchive(
  factorId: string,
  command: FactorArchiveCommandRequest,
  resume: boolean,
): Promise<FactorArchiveCommandData> {
  const client = apiClient();
  const result = resume
    ? await client.POST("/api/v1/factors/definitions/{factor_id}/archive/resume", {
        params: { path: { factor_id: factorId } },
        body: command,
        headers: { "X-Rquant-Csrf": "1" },
      })
    : await client.POST("/api/v1/factors/definitions/{factor_id}/archive", {
        params: { path: { factor_id: factorId } },
        body: command,
        headers: { "X-Rquant-Csrf": "1" },
      });
  if (result.data === undefined) {
    throw new ApiError(
      result.response.status,
      result.response.status === 409
        ? "因子已变化，请刷新后查看。"
        : result.response.status === 403
          ? "当前账号不能归档因子。"
          : "归档状态暂不可用，请用原命令继续查看。",
    );
  }
  return result.data.data;
}

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

export function useFactorResults(
  generationId: string | null | undefined,
): ServingQueryResult<FactorResultListData> {
  return useServingQuery(
    ["factors", "results", generationId],
    async (): Promise<FactorResultListEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/factors/results", {
        params: { query: { generation_id: generationId ?? undefined } },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 409
            ? "数据已更新，请重新查看结果。"
            : "检验结果暂时无法加载，请稍后重试。",
        );
      }
      return data;
    },
    { enabled: typeof generationId === "string" },
  );
}

export function useFactorResultDetail(
  generationId: string | null | undefined,
  jobId: string | null,
): ServingQueryResult<FactorResultDetailData> {
  return useServingQuery(
    ["factors", "result", generationId, jobId],
    async (): Promise<FactorResultDetailEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/factors/results/{job_id}", {
        params: {
          path: { job_id: jobId ?? "" },
          query: { generation_id: generationId ?? undefined },
        },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 409
            ? "数据已更新，请重新查看结果。"
            : "检验详情暂时无法加载，请稍后重试。",
        );
      }
      return data;
    },
    { enabled: typeof generationId === "string" && jobId !== null },
  );
}
