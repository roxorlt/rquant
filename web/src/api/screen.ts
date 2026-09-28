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

export function useScreenCatalog(): ServingQueryResult<ScreenCatalogData> {
  return useServingQuery(
    ["screen", "blocks"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/screen/blocks");
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
