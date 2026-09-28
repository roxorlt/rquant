import { ApiError, apiClient, type Schemas } from "./client";
import { useServingQuery } from "./useServingQuery";

export type FormulaMarketCommandRequest = Schemas["FormulaMarketCommandRequest"];
export type FormulaMarketCommandReceipt = Schemas["FormulaMarketCommandReceipt"];
export type FormulaMarketJobItem = Schemas["FormulaMarketJobItem"];
export type FormulaMarketJobListData = Schemas["FormulaMarketJobListData"];
export type FormulaMarketJobDetailData = Schemas["FormulaMarketJobDetailData"];
export type FormulaMarketMatchesData = Schemas["FormulaMarketMatchesData"];

function detail(error: unknown, fallback: string): string {
  if (typeof error === "object" && error !== null && "detail" in error) {
    const value = error.detail;
    if (typeof value === "string") return value;
  }
  return fallback;
}

export async function submitFormulaMarketCommand(
  body: FormulaMarketCommandRequest,
): Promise<FormulaMarketCommandReceipt> {
  const { data, error, response } = await apiClient().POST("/api/v1/screen/tdx/market/commands", {
    body,
    headers: { "X-Rquant-Csrf": "1" },
  });
  if (data !== undefined) return data;
  if (response.status === 409 && error && "status" in error && error.status === "conflict") {
    return error;
  }
  throw new ApiError(response.status, detail(error, "提交状态待确认，请重试原请求。"));
}

export function useFormulaMarketJobs(polling: boolean) {
  return useServingQuery<FormulaMarketJobListData>(
    ["formula-market", "jobs"],
    async () => {
      const { data, error, response } = await apiClient().GET("/api/v1/screen/tdx/market/jobs");
      if (data === undefined) {
        throw new ApiError(response.status, detail(error, "最近运行暂时无法加载。"));
      }
      return data;
    },
    { refetchInterval: polling ? 15_000 : false },
  );
}

export function useFormulaMarketJob(
  taskId: string | null,
  revision: string | null,
  polling: boolean,
) {
  return useServingQuery<FormulaMarketJobDetailData>(
    ["formula-market", "job", taskId, revision],
    async () => {
      if (taskId === null) throw new Error("未选择选股任务");
      const { data, error, response } = await apiClient().GET(
        "/api/v1/screen/tdx/market/jobs/{task_id}",
        { params: { path: { task_id: taskId } } },
      );
      if (data === undefined) {
        throw new ApiError(response.status, detail(error, "这项任务暂时无法读取。"));
      }
      return data;
    },
    { enabled: taskId !== null, refetchInterval: polling ? 15_000 : false },
  );
}

export function useFormulaMarketMatches(
  taskId: string | null,
  cursor: string | null,
  generation: string | null | undefined,
  enabled: boolean,
) {
  return useServingQuery<FormulaMarketMatchesData>(
    ["formula-market", "matches", taskId, cursor, generation],
    async () => {
      if (taskId === null) throw new Error("未选择选股任务");
      const { data, error, response } = await apiClient().GET(
        "/api/v1/screen/tdx/market/jobs/{task_id}/matches",
        { params: { path: { task_id: taskId }, query: { page_size: 50, cursor } } },
      );
      if (data === undefined) {
        throw new ApiError(response.status, detail(error, "命中股票暂时无法读取。"));
      }
      return data;
    },
    { enabled: taskId !== null && enabled },
  );
}
