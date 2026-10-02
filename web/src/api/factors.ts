import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type FactorDefinitionItem = Schemas["FactorDefinitionItem"];
export type FactorCatalogData = Schemas["FactorCatalogData"];
export type FactorArchiveCommandRequest = Schemas["FactorArchiveCommandRequest"];
export type FactorArchiveCommandData = Schemas["FactorArchiveCommandData"];
export type FactorCapabilitiesData = Schemas["FactorCapabilitiesData"];
export type FactorSaveDraft = Schemas["FactorSaveDraft"];
export type FactorSaveCommandData = Schemas["FactorSaveCommandData"];
export type FactorResultItem = Schemas["FactorResultItem"];
export type FactorResultListData = Schemas["FactorResultListData"];
export type FactorResultDetailData = Schemas["FactorResultDetailData"];
export type FactorResearchDisplayV1 = Schemas["FactorResearchDisplay"];
export type FactorResearchDisplayV2 = Schemas["FactorStreamResearchDisplay"];
export type FactorResearchDisplay = FactorResearchDisplayV1 | FactorResearchDisplayV2;
export type FactorExtendedStatistics = Schemas["FactorExtendedStatistics"];
export type FactorRunRequest = Schemas["FactorRunRequest"];
export type FactorRunParameters = Schemas["FactorRunParameters"];
export type FactorRunAvailability = Schemas["FactorRunAvailability"];
export type FactorRunNeutralizationOption = Schemas["FactorRunNeutralizationOption"];
export type FactorRunOperationResult = Schemas["FactorRunOperationResult"];
export type FactorTrackingRequest = Schemas["FactorTrackingRequest"];
export type FactorTrackingOperationResult = Schemas["FactorTrackingOperationResult"];
export type FactorTrackingReceipt = Schemas["FactorTrackingReceipt"];
export type FactorTrackingPanel = Schemas["FactorTrackingPanel"];
export type FactorTrackingSummary = Schemas["FactorTrackingSummary"];
type FactorCatalogEnvelope = Schemas["Envelope_FactorCatalogData_"];
type FactorCapabilitiesEnvelope = Schemas["Envelope_FactorCapabilitiesData_"];
type FactorResultListEnvelope = Schemas["Envelope_FactorResultListData_"];
type FactorResultDetailEnvelope = Schemas["Envelope_FactorResultDetailData_"];
type FactorRunAvailabilityEnvelope = Schemas["Envelope_FactorRunAvailability_"];
type FactorTrackingEnvelope = Schemas["Envelope_FactorTrackingPanel_"];

export async function postFactorTracking(
  request: FactorTrackingRequest,
  action: "set" | "resume" | "retry",
): Promise<FactorTrackingOperationResult> {
  const client = apiClient();
  const options = { body: request, headers: { "X-Rquant-Csrf": "1" } };
  const result =
    action === "set"
      ? await client.POST("/api/v1/factors/tracking/commands", options)
      : action === "resume"
        ? await client.POST("/api/v1/factors/tracking/commands/resume", options)
        : await client.POST("/api/v1/factors/tracking/commands/retry", options);
  if (result.data === undefined) {
    throw new ApiError(result.response.status, "跟踪状态暂未确认，请保留本次操作。");
  }
  return result.data.data;
}

export function useFactorTrackingPanel(
  generationId: string | null | undefined,
  factorId: string | null,
  viewer: string | null | undefined,
  permissionRevision: number,
): ServingQueryResult<FactorTrackingPanel> {
  return useServingQuery(
    ["factors", "tracking", generationId, factorId, viewer, permissionRevision],
    async (): Promise<FactorTrackingEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/factors/{factor_id}/tracking", {
        params: {
          path: { factor_id: factorId ?? "" },
          query: { generation_id: generationId ?? undefined },
        },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 404
            ? "这个因子的跟踪数据暂时无法查看。"
            : response.status === 401 || response.status === 403
              ? "当前账号不能查看跟踪，请刷新后核对权限。"
              : "跟踪数据暂时无法核对，请刷新后查看。",
        );
      }
      return data;
    },
    {
      enabled: typeof generationId === "string" && factorId !== null && typeof viewer === "string",
    },
  );
}

export async function postFactorRun(
  request: FactorRunRequest,
  action: "run" | "resume" | "retry",
): Promise<FactorRunOperationResult> {
  const options = { body: request, headers: { "X-Rquant-Csrf": "1" } };
  const client = apiClient();
  const result =
    action === "run"
      ? await client.POST("/api/v1/factors/runs", options)
      : action === "resume"
        ? await client.POST("/api/v1/factors/runs/resume", options)
        : await client.POST("/api/v1/factors/runs/retry", options);
  if (result.data === undefined) {
    throw new ApiError(result.response.status, "检验结果暂未确认，请保留本次操作。");
  }
  return result.data.data;
}

export function useFactorRunAvailability(
  generationId: string | null | undefined,
  viewer: string | null | undefined,
  permissionRevision: number,
): ServingQueryResult<FactorRunAvailability> {
  return useServingQuery(
    ["factors", "run-availability", generationId, viewer, permissionRevision],
    async (): Promise<FactorRunAvailabilityEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/factors/run-availability");
      if (data === undefined) {
        throw new ApiError(response.status, "检验条件暂时无法核对，请稍后刷新。");
      }
      return data;
    },
    { enabled: typeof generationId === "string" && typeof viewer === "string" },
  );
}

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

export async function postFactorSave(
  draft: FactorSaveDraft,
  action: "save" | "resume" | "retry",
): Promise<FactorSaveCommandData> {
  const client = apiClient();
  const options = { body: draft, headers: { "X-Rquant-Csrf": "1" } };
  const result =
    action === "save"
      ? await client.POST("/api/v1/factors/definitions/save", options)
      : action === "resume"
        ? await client.POST("/api/v1/factors/definitions/save/resume", options)
        : await client.POST("/api/v1/factors/definitions/save/retry", options);
  if (result.data === undefined) {
    throw new ApiError(
      result.response.status,
      result.response.status === 409
        ? "因子已更新，请比对当前版本。"
        : result.response.status === 422
          ? "请检查名称、分类和表达式。"
          : result.response.status === 401 || result.response.status === 403
            ? "当前账号不能保存因子。"
            : "保存结果尚未确认，请保留这次操作。",
    );
  }
  return result.data.data;
}

export function useFactorCapabilities(
  generationId: string | null | undefined,
  enabled: boolean,
  viewer: string | null | undefined,
  permissionRevision: number,
): ServingQueryResult<FactorCapabilitiesData> {
  return useServingQuery(
    ["factors", "capabilities", generationId, viewer, permissionRevision],
    async (): Promise<FactorCapabilitiesEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/factors/capabilities");
      if (data === undefined) {
        throw new ApiError(response.status, "保存能力暂时无法核对，请稍后重试。");
      }
      return data;
    },
    { enabled: enabled && typeof generationId === "string" && typeof viewer === "string" },
  );
}

export function useFactorCatalog(
  generationId: string | null | undefined,
  viewer: string | null | undefined,
  permissionRevision: number,
): ServingQueryResult<FactorCatalogData> {
  return useServingQuery(
    ["factors", "definitions", generationId, viewer, permissionRevision],
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
