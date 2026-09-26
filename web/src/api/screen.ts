import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type ScreenCatalogData = Schemas["ScreenCatalogData"];
export type ScreenBlock = Schemas["ScreenBlock"];
export type ScreenParameter = Schemas["ScreenParameter"];
export type ScreenOption = Schemas["ScreenOption"];
export type ScreenRunRequest = Schemas["ScreenRunRequest"];
export type ScreenRunData = Schemas["ScreenRunData"];
export type ScreenRow = Schemas["ScreenRow"];

export function useScreenCatalog(): ServingQueryResult<ScreenCatalogData> {
  return useServingQuery(["screen", "blocks"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/screen/blocks");
    if (data === undefined) {
      throw new ApiError(response.status, "条件目录暂时无法加载");
    }
    return data;
  });
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
