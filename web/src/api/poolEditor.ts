import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type PoolEditorData = Schemas["PoolEditorData"];
export type EditablePool = Schemas["EditablePool"];
export type EditableCanvas = Schemas["EditableCanvas"];
export type BuiltinPoolCopySource = Schemas["BuiltinPoolCopySource"];
export type EditorCommand =
  | Schemas["SavePoolCommand"]
  | Schemas["SaveRankedPoolCommand"]
  | Schemas["AttachPoolCommand"]
  | Schemas["CreateCanvasCommand"];
export type EditorReceipt = Schemas["PoolEditorReceipt"];
export type PoolNlPreviewRequest = Schemas["PoolNlPreviewRequest"];
export type PoolNlPreview = Schemas["PoolNlPreview"];
export type PoolRuleChange = Schemas["PoolRuleChange"];
export type PoolRankingPlan = Schemas["PoolRankingPlan"];
export type PoolRankingMetric = PoolRankingPlan["conditions"][number]["metric"];

const POOL_RANKING_METRICS = new Set<PoolRankingMetric>([
  "RETURN_20D_PCT[0]",
  "TURNOVER_RATE[0]",
  "CIRC_MV[0]",
  "PCT_CHG[0]",
]);

export function isPoolRankingMetric(value: string): value is PoolRankingMetric {
  return POOL_RANKING_METRICS.has(value as PoolRankingMetric);
}

export function usePoolEditor(): ServingQueryResult<PoolEditorData> {
  return useServingQuery(["pools", "editor"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/pools/editor");
    if (data === undefined) throw new ApiError(response.status, "池子编辑资料暂时无法加载。");
    return data;
  });
}

/** Retrying callers must pass the original immutable body verbatim. */
export async function submitPoolEditorCommand(body: EditorCommand): Promise<EditorReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/pools/editor/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "连接暂不可用，状态待确认。");
    });
  if (data === undefined) {
    const detail =
      typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
    throw new ApiError(
      response.status,
      typeof detail === "string" ? detail : "请求状态暂时无法确认。",
    );
  }
  return data;
}

export async function previewPoolSentenceEdit(
  body: PoolNlPreviewRequest,
  signal: AbortSignal,
): Promise<PoolNlPreview> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/pools/editor/nl-preview", {
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
