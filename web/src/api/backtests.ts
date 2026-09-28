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
