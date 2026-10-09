import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingEnvelope, useServingQuery } from "./useServingQuery";

export type AIGenerateRequest =
  | Schemas["AIScreenRequest"]
  | Schemas["AIPoolRequest"]
  | Schemas["AIInterpretationRequest"]
  | Schemas["AINewsRequest"];
export type AIRequestView = Schemas["AIRequestView"];
export type AIInterpretationRequest = Schemas["AIInterpretationRequest"];
export type AIInterpretationContextRequest = Schemas["AIInterpretationContextRequest"];
export type AIInterpretationView = Schemas["AIInterpretationView"];
export type AINewsContent = Schemas["AINewsContent"];
export type AIStockNewsView = Schemas["AIStockNewsView"];
export type AIBacktestPrepareRequest = Schemas["AIBacktestPrepareRequest"];
export type AIBacktestConfirmRequest = Schemas["AIBacktestConfirmRequest"];
export type AIBacktestPreparation = Schemas["AIBacktestPreparation"];
export type AIBacktestConfirmation = Schemas["AIBacktestConfirmation"];

function checked<T>(data: T | undefined, error: unknown, status: number): T {
  if (data !== undefined) return data;
  const detail =
    typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
  throw new ApiError(
    status,
    typeof detail === "string" ? detail : "助手暂不可用，请继续查看原请求。",
  );
}
const csrf = { "X-Rquant-Csrf": "1" };
export async function generateAI(
  body: AIGenerateRequest,
  signal?: AbortSignal,
): Promise<AIRequestView> {
  const { data, error, response } = await apiClient().POST("/api/v1/ai/requests", {
    body,
    signal,
    headers: csrf,
  });
  return checked(data, error, response.status).data;
}
export async function lookupAI(
  body: AIGenerateRequest,
  signal?: AbortSignal,
): Promise<AIRequestView> {
  const { data, error, response } = await apiClient().POST("/api/v1/ai/requests/lookup", {
    body,
    signal,
    headers: csrf,
  });
  return checked(data, error, response.status).data;
}
export function useAiCapabilities(viewer: string | null, enabled = true) {
  return useServingQuery(
    ["ai", "capabilities", viewer],
    async () => {
      const { data, error, response } = await apiClient().GET("/api/v1/ai/capabilities");
      return checked(data, error, response.status);
    },
    { enabled: viewer !== null && enabled, staleTime: 0 },
  );
}
export function useAiUsage(
  viewer: string | null,
  startDate: string,
  endDate: string,
  enabled = true,
) {
  return useServingQuery(
    ["ai", "usage", viewer, startDate, endDate],
    async () => {
      const { data, error, response } = await apiClient().GET("/api/v1/ai/usage", {
        params: { query: { start_date: startDate, end_date: endDate } },
      });
      return checked(data, error, response.status);
    },
    { enabled: viewer !== null && enabled && startDate <= endDate, staleTime: 0 },
  );
}
export async function fetchAiNews(
  stockCode: string,
  signal?: AbortSignal,
): Promise<ServingEnvelope<AIStockNewsView>> {
  const { data, error, response } = await apiClient().GET("/api/v1/ai/news/{stock_code}", {
    params: { path: { stock_code: stockCode } },
    signal,
  });
  return checked(data, error, response.status);
}
export function useAiNews(viewer: string | null, stockCode: string | null, enabled = true) {
  return useServingQuery(["ai", "news", viewer, stockCode], () => fetchAiNews(stockCode ?? ""), {
    enabled: viewer !== null && stockCode !== null && enabled,
    staleTime: 0,
  });
}
export async function readAiInterpretation(
  body: AIInterpretationRequest | AIInterpretationContextRequest,
  signal?: AbortSignal,
): Promise<ServingEnvelope<AIInterpretationView>> {
  const { data, error, response } = await apiClient().POST("/api/v1/ai/interpretations/read", {
    body,
    signal,
    headers: csrf,
  });
  return checked(data, error, response.status);
}
export async function prepareAiBacktest(
  body: AIBacktestPrepareRequest,
  lookup = false,
  signal?: AbortSignal,
): Promise<AIBacktestPreparation> {
  const { data, error, response } = await apiClient().POST(
    lookup ? "/api/v1/ai/backtests/prepare/lookup" : "/api/v1/ai/backtests/prepare",
    { body, signal, headers: csrf },
  );
  return checked(data, error, response.status).data;
}
export async function confirmAiBacktest(
  body: AIBacktestConfirmRequest,
  signal?: AbortSignal,
): Promise<AIBacktestConfirmation> {
  const { data, error, response } = await apiClient().POST("/api/v1/ai/backtests/confirm", {
    body,
    signal,
    headers: csrf,
  });
  return checked(data, error, response.status).data;
}
