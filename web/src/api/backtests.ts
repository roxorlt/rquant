import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type BacktestRun = Schemas["BacktestRun"];
export type BacktestGroup = Schemas["BacktestGroup"];
export type BacktestTrade = Schemas["BacktestTrade"];
export type BacktestListData = Schemas["BacktestListData"];
export type BacktestDetailData = Schemas["BacktestDetailData"];
type ListEnvelope = Schemas["Envelope_BacktestListData_"];
type DetailEnvelope = Schemas["Envelope_BacktestDetailData_"];

export interface BacktestGroupKey {
  entryMode: string;
  profileVariant: string;
}

export type PortfolioConfig = Schemas["PortfolioEditableConfig-Input"];
export type PortfolioConfigResult = Schemas["PortfolioEditableConfig-Output"];
export type PortfolioSource = Schemas["PortfolioSourceOption"];
export type PortfolioJob = Schemas["PortfolioJob"];
export type PortfolioCapabilities = Schemas["PortfolioCapabilities"];
export type PortfolioSummary = Schemas["PortfolioSummaryData"];
export type PortfolioRows = Schemas["PortfolioRowsData"];
export type PortfolioNav = Schemas["PortfolioNavData"];
export type PortfolioView = PortfolioRows["view"];
export type PortfolioCreateRequest = Schemas["PortfolioCreateRequest"];
export type PortfolioExportRequest = Schemas["PortfolioExportRequest"];
export type PortfolioReceipt = Schemas["PortfolioCommandReceipt"];

function requirePortfolio<T>(data: T | undefined, response: Response): T {
  if (data !== undefined) return data;
  throw new ApiError(
    response.status,
    response.status === 409 ? "结果已变化，请重新选择回测。" : "回测暂时无法加载，请稍后重试。",
  );
}

export function usePortfolioCapabilities() {
  return useServingQuery(
    ["portfolio", "capabilities"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/portfolio/capabilities");
      return requirePortfolio(data, response);
    },
    { staleTime: 15_000 },
  );
}

export function usePortfolioJobs(cursor: string | null, refresh: number) {
  return useServingQuery(
    ["portfolio", "jobs", cursor, refresh],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/portfolio/runs", {
        params: { query: { cursor: cursor ?? undefined, limit: 20 } },
      });
      return requirePortfolio(data, response);
    },
    { refetchInterval: 5_000 },
  );
}

export function usePortfolioSummary(jobId: string | null, poll: boolean) {
  return useServingQuery(
    ["portfolio", "summary", jobId],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/portfolio/runs/{job_id}",
        {
          params: { path: { job_id: jobId ?? "" } },
        },
      );
      return requirePortfolio(data, response);
    },
    { enabled: jobId !== null, refetchInterval: poll ? 2_000 : false },
  );
}

export function usePortfolioNav(jobId: string | null, resultHash: string | null) {
  return useServingQuery(
    ["portfolio", "nav", jobId, resultHash],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/portfolio/runs/{job_id}/nav",
        {
          params: { path: { job_id: jobId ?? "" }, query: { result_hash: resultHash ?? "" } },
        },
      );
      return requirePortfolio(data, response);
    },
    { enabled: jobId !== null && resultHash !== null, staleTime: Infinity },
  );
}

export function usePortfolioRows(
  jobId: string | null,
  resultHash: string | null,
  view: PortfolioView,
  offset: number,
) {
  return useServingQuery(
    ["portfolio", "rows", jobId, resultHash, view, offset],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/backtests/portfolio/runs/{job_id}/rows",
        {
          params: {
            path: { job_id: jobId ?? "" },
            query: { result_hash: resultHash ?? "", view, offset, limit: 50 },
          },
        },
      );
      return requirePortfolio(data, response);
    },
    { enabled: jobId !== null && resultHash !== null, staleTime: Infinity },
  );
}

export async function submitPortfolioRun(body: PortfolioCreateRequest): Promise<PortfolioReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/backtests/portfolio/runs", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认，请重试原请求。");
    });
  const receipt = data ?? error;
  if (receipt !== undefined && "command_id" in receipt && receipt.command_id === body.command_id)
    return receipt;
  throw new ApiError(
    response.status,
    response.status === 422
      ? "配置不完整，请检查日期、仓位和成本。"
      : "提交状态待确认，请重试原请求。",
  );
}

export async function submitPortfolioExport(
  body: PortfolioExportRequest,
): Promise<PortfolioReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/backtests/portfolio/exports", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "导出状态待确认，请重试原请求。");
    });
  const receipt = data ?? error;
  if (receipt !== undefined && "command_id" in receipt && receipt.command_id === body.command_id)
    return receipt;
  throw new ApiError(response.status, "导出状态待确认，请重试原请求。");
}

export function useBacktestRuns(
  offset: number,
  generationId: string | null,
  refreshKey: number,
): ServingQueryResult<BacktestListData> {
  return useServingQuery(
    ["backtests", "runs", offset, generationId, refreshKey],
    async (): Promise<ListEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/backtests", {
        params: { query: { limit: 20, offset, generation_id: generationId ?? undefined } },
      });
      if (data === undefined) {
        throw new ApiError(
          response.status,
          response.status === 409
            ? "数据已更新，请重新查看回放记录。"
            : "回放记录暂时无法加载，请稍后重试。",
        );
      }
      return data;
    },
  );
}

export function useBacktestDetail(
  runId: string | null,
  generationId: string | null,
  offset: number,
  group: BacktestGroupKey | null,
): ServingQueryResult<BacktestDetailData> {
  return useServingQuery(
    ["backtests", "detail", runId, generationId, offset, group?.entryMode, group?.profileVariant],
    async (): Promise<DetailEnvelope> => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/{run_id}", {
        params: {
          path: { run_id: runId ?? "" },
          query: {
            generation_id: generationId ?? "",
            limit: 20,
            offset,
            ...(group === null
              ? {}
              : { entry_mode: group.entryMode, profile_variant: group.profileVariant }),
          },
        },
      });
      if (data === undefined) {
        const message =
          response.status === 409
            ? "数据已更新，请重新选择回放。"
            : response.status === 404
              ? "这次回放已不在当前数据中，请重新选择。"
              : "回放详情暂时无法加载，请稍后重试。";
        throw new ApiError(response.status, message);
      }
      return data;
    },
    { enabled: runId !== null && generationId !== null },
  );
}
