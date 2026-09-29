import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type FactorDefinitionItem = Schemas["FactorDefinitionItem"];
export type FactorCatalogData = Schemas["FactorCatalogData"];
export type FactorArchiveCommandRequest = Schemas["FactorArchiveCommandRequest"];
export type FactorArchiveCommandData = Schemas["FactorArchiveCommandData"];
type FactorCatalogEnvelope = Schemas["Envelope_FactorCatalogData_"];

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
