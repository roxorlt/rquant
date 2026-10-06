import { useQuery } from "@tanstack/react-query";
import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type ScreenCatalogData = Schemas["ScreenCatalogData"];
export type ScreenBlock = Schemas["ScreenBlock"];
export type ScreenParameter = Schemas["ScreenParameter"];
export type ScreenOption = Schemas["ScreenOption"];
export type ScreenRunRequest = Schemas["ScreenRunRequest"];
export type ScreenRunData = Schemas["ScreenRunData"];
export type ScreenNlPreviewRequest = Schemas["ScreenNlPreviewRequest"];
export type ScreenNlPreviewData = Schemas["ScreenNlPreviewData"];
export type ScreenRow = Schemas["ScreenRow"];
export type TdxParseRequest = Schemas["TdxParseRequest"];
export type TdxParseData = Schemas["TdxParseData"];
export type TdxPreviewRequest = Schemas["TdxPreviewRequest"];
export type TdxPreviewData = Schemas["TdxPreviewData"];
export type TdxPreviewSourceData = Schemas["TdxPreviewSourceData"];
export type ScreenQueryDefinition = Schemas["ScreenQueryDefinition"];
export type ExecuteScreenQuery = Schemas["ExecuteScreenQuery"];
export type ScreenQueryReadData = Schemas["ScreenQueryReadData"];
export type ScreenExecutionView = Schemas["ScreenExecutionView"];
export type ScreenQueryPreset = Schemas["ScreenQueryPreset"];
export type ScreenPresetSaveRequest = Schemas["ScreenPresetSaveRequest"];
export type ScreenAlertDraftRequest = Schemas["ScreenAlertDraftRequest"];
export type ScreenAlertDraft = Schemas["ScreenAlertDraft"];
export type ScreenOriginalAction =
  | Schemas["ScreenExecuteAction"]
  | Schemas["ScreenPresetSaveAction"];

function privateData(
  data: ScreenQueryReadData | undefined,
  error: unknown,
  status: number,
): ScreenQueryReadData {
  if (data !== undefined) return data;
  const detail =
    typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
  throw new ApiError(
    status,
    typeof detail === "string" ? detail : "选股资料暂不可用，请稍后重试。",
  );
}

export async function createScreenAlertDraft(
  body: ScreenAlertDraftRequest,
  signal?: AbortSignal,
): Promise<ScreenQueryReadData> {
  const { data, error, response } = await apiClient().POST("/api/v1/screen/query/alert-draft", {
    body,
    signal,
    headers: { "X-Rquant-Csrf": "1" },
  });
  return privateData(data, error, response.status);
}

export async function fetchScreenAlertDraft(
  draftId: string,
  signal?: AbortSignal,
): Promise<ScreenQueryReadData> {
  const { data, error, response } = await apiClient().GET(
    "/api/v1/screen/query/alert-drafts/{draft_id}",
    { params: { path: { draft_id: draftId } }, signal },
  );
  return privateData(data, error, response.status);
}

export async function fetchScreenHistory(
  cursor: string | null = null,
  signal?: AbortSignal,
): Promise<ScreenQueryReadData> {
  const { data, error, response } = await apiClient().GET("/api/v1/screen/query/history", {
    params: { query: { limit: 20, cursor } },
    signal,
  });
  return privateData(data, error, response.status);
}

export async function fetchScreenPresets(signal?: AbortSignal): Promise<ScreenQueryReadData> {
  const { data, error, response } = await apiClient().GET("/api/v1/screen/query/presets", {
    signal,
  });
  return privateData(data, error, response.status);
}

export async function fetchScreenExecution(
  executionId: string,
  signal?: AbortSignal,
): Promise<ScreenQueryReadData> {
  const { data, error, response } = await apiClient().GET(
    "/api/v1/screen/query/executions/{execution_id}",
    { params: { path: { execution_id: executionId } }, signal },
  );
  return privateData(data, error, response.status);
}

export async function fetchScreenExecutionResults(
  executionId: string,
  cursor: string | null = null,
  signal?: AbortSignal,
): Promise<ScreenQueryReadData> {
  const { data, error, response } = await apiClient().GET(
    "/api/v1/screen/query/executions/{execution_id}/results",
    { params: { path: { execution_id: executionId }, query: { cursor, limit: 20 } }, signal },
  );
  return privateData(data, error, response.status);
}

export function useScreenQueryHistory(
  viewer: string | null,
  cursor: string | null = null,
  enabled = true,
) {
  return useQuery({
    queryKey: ["screen-query-history", viewer, cursor],
    queryFn: ({ signal }) => fetchScreenHistory(cursor, signal),
    enabled: viewer !== null && enabled,
    gcTime: 0,
    retry: false,
  });
}

export function useScreenQueryPresets(viewer: string | null, enabled = true) {
  return useQuery({
    queryKey: ["screen-query-presets", viewer],
    queryFn: ({ signal }) => fetchScreenPresets(signal),
    enabled: viewer !== null && enabled,
    gcTime: 0,
    retry: false,
  });
}

export async function screenQueryTransport(
  original: ScreenOriginalAction,
  operation: "submit" | "lookup" | "resume",
  signal: AbortSignal,
): Promise<ScreenQueryReadData> {
  const options = { signal, headers: { "X-Rquant-Csrf": "1" } };
  let reply: ScreenQueryReadData;
  if (operation === "lookup") {
    const { data, error, response } = await apiClient().POST("/api/v1/screen/query/lookup", {
      ...options,
      body: { action: "lookup", original },
    });
    reply = privateData(data, error, response.status);
  } else if (operation === "resume") {
    const { data, error, response } = await apiClient().POST("/api/v1/screen/query/resume", {
      ...options,
      body: { action: "resume", original },
    });
    reply = privateData(data, error, response.status);
  } else if (original.action === "execute") {
    const { data, error, response } = await apiClient().POST("/api/v1/screen/query/execute", {
      ...options,
      body: original.command,
    });
    reply = privateData(data, error, response.status);
  } else {
    const { data, error, response } = await apiClient().POST("/api/v1/screen/query/presets/save", {
      ...options,
      body: original.request,
    });
    reply = privateData(data, error, response.status);
  }
  if (reply.receipt?.status !== "succeeded") return reply;
  if (original.action === "execute") {
    const detail = await fetchScreenExecution(original.command.command_id, signal);
    const results = await fetchScreenExecutionResults(original.command.command_id, null, signal);
    if (
      detail.owner_scope_tag !== reply.owner_scope_tag ||
      results.owner_scope_tag !== reply.owner_scope_tag
    )
      throw new ApiError(503, "结果待确认，请核对原请求。");
    return { ...reply, execution: detail.execution, results: results.results };
  }
  const presets = await fetchScreenPresets(signal);
  if (presets.owner_scope_tag !== reply.owner_scope_tag)
    throw new ApiError(503, "保存待确认，请核对原请求。");
  return { ...reply, presets: presets.presets };
}

const FUNDAMENTAL_SCREEN_FIELDS = new Set([
  "PE_TTM[0]",
  "PB[0]",
  "DV_TTM[0]",
  "ROE[0]",
  "OR_YOY[0]",
  "NETPROFIT_YOY[0]",
]);

export function isFundamentalScreenField(value: unknown): boolean {
  return typeof value === "string" && FUNDAMENTAL_SCREEN_FIELDS.has(value);
}

/** Replica catalogs have their own source lifetime; Serving catalogs follow the page generation. */
export function catalogUsableForGeneration(
  catalog: ScreenCatalogData | undefined,
  catalogGeneration: string | null | undefined,
  pageGeneration: string | null | undefined,
): boolean {
  return (
    !!catalog &&
    pageGeneration != null &&
    (catalog.source_kind === "replica" || catalogGeneration === pageGeneration)
  );
}

export function useTdxPreviewSource() {
  return useQuery({
    queryKey: ["tdx-preview-source"],
    queryFn: async (): Promise<TdxPreviewSourceData> => {
      const { data, response } = await apiClient().GET("/api/v1/screen/tdx/preview/source");
      if (data === undefined) {
        throw new ApiError(response.status, "公式预览数据暂时无法加载。");
      }
      return data;
    },
    staleTime: Infinity,
    refetchOnMount: "always",
  });
}

export function useScreenCatalog(
  mode: ScreenQueryDefinition["mode"] = "daily",
): ServingQueryResult<ScreenCatalogData> {
  return useServingQuery(
    ["screen", "blocks", mode],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/screen/blocks", {
        params: { query: { mode } },
      });
      if (data === undefined) {
        throw new ApiError(response.status, "条件目录暂时无法加载");
      }
      return data;
    },
    { staleTime: Infinity },
  );
}

export async function fetchScreenRun(body: ScreenRunRequest) {
  const { data, error, response } = await apiClient().POST("/api/v1/screen/run", {
    body,
    headers: { "X-Rquant-Csrf": "1" },
  });
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
    throw new ApiError(
      response.status,
      typeof detail === "string" ? detail : "筛选暂时无法完成，请稍后重试。",
    );
  }
  return data;
}

export async function fetchScreenNlPreview(
  body: ScreenNlPreviewRequest,
  signal: AbortSignal,
): Promise<ScreenNlPreviewData> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/screen/nl-preview", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal,
    })
    .catch(() => {
      throw new ApiError(503, "暂无法生成，请稍后重试。");
    });
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
    throw new ApiError(
      response.status,
      typeof detail === "string" ? detail : "暂无法生成，请稍后重试。",
    );
  }
  return data;
}

export async function fetchTdxParse(body: TdxParseRequest): Promise<TdxParseData> {
  const { data, error, response } = await apiClient().POST("/api/v1/screen/tdx/parse", {
    body,
    headers: { "X-Rquant-Csrf": "1" },
  });
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
    throw new ApiError(response.status, typeof detail === "string" ? detail : "公式暂时无法检查。");
  }
  return data;
}

export async function fetchTdxPreview(body: TdxPreviewRequest): Promise<TdxPreviewData> {
  const { data, error, response } = await apiClient().POST("/api/v1/screen/tdx/preview", {
    body,
    headers: { "X-Rquant-Csrf": "1" },
  });
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
    throw new ApiError(
      response.status,
      typeof detail === "string" ? detail : "这只股票暂时无法预览，请稍后重试。",
    );
  }
  return data;
}
