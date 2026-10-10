/** Every API call the pages make. Types come only from the generated schema. */
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type OverviewData = Schemas["OverviewData"];
export type HealthData = Schemas["HealthData"];
export type PanoramaData = Schemas["PanoramaData"];
export type ScreenData = Schemas["ScreenData"];
export type ScreenRow = Schemas["ScreenRow"];
export type PoolsData = Schemas["PoolsData"];
export type PoolItem = Schemas["PoolItem"];
export type BacktestRun = Schemas["BacktestRun"];
export type BacktestTrade = Schemas["BacktestTrade"];
export type AlertItem = Schemas["AlertItem"];
export type PaperData = Schemas["PaperData"];
export type CommandReceipt = Schemas["CommandReceipt"];

function unwrap<T>(data: T | undefined, response: Response): T {
  if (data === undefined) {
    throw new ApiError(response.status, `网页 API 返回 HTTP ${response.status}`);
  }
  return data;
}

export function useOverview(): ServingQueryResult<OverviewData> {
  return useServingQuery(["overview"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/overview");
    return unwrap(data, response);
  });
}

export function useHealth(): ServingQueryResult<HealthData> {
  return useServingQuery(["health"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/health");
    return unwrap(data, response);
  });
}

export function usePanorama(): ServingQueryResult<PanoramaData> {
  return useServingQuery(["panorama"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/panorama");
    return unwrap(data, response);
  });
}

export function useScreen(preset: string | null): ServingQueryResult<ScreenData> {
  return useServingQuery(["screen", preset], async () => {
    const { data, response } = await apiClient().GET("/api/v1/screen", {
      params: { query: preset ? { preset } : {} },
    });
    return unwrap(data, response);
  });
}

export function usePools(): ServingQueryResult<PoolsData> {
  return useServingQuery(["pools"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/pools");
    return unwrap(data, response);
  });
}

export function useBacktests(): ServingQueryResult<Schemas["BacktestListData"]> {
  return useServingQuery(["backtests"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/backtests");
    return unwrap(data, response);
  });
}

export function usePortfolioBacktests(): ServingQueryResult<Schemas["PortfolioRunListData"]> {
  return useServingQuery(["portfolio-backtests"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/portfolio-backtests");
    return unwrap(data, response);
  });
}

export function usePortfolioBacktest(
  runId: string | null,
): ServingQueryResult<Schemas["PortfolioRunDetailData"]> {
  return useServingQuery(
    ["portfolio-backtests", runId],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/portfolio-backtests/{run_id}", {
        params: { path: { run_id: runId ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: runId !== null },
  );
}

export function usePortfolioCompare(
  a: string | null,
  b: string | null,
): ServingQueryResult<Schemas["PortfolioCompareData"]> {
  return useServingQuery(
    ["portfolio-backtests", "compare", a, b],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/portfolio-backtests/compare", {
        params: { query: { a: a ?? "", b: b ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: a !== null && b !== null },
  );
}

export function useBacktestDetail(
  runId: string | null,
): ServingQueryResult<Schemas["BacktestDetailData"]> {
  return useServingQuery(
    ["backtests", runId],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/backtests/{run_id}", {
        params: { path: { run_id: runId ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: runId !== null },
  );
}

export function useAlerts(): ServingQueryResult<Schemas["AlertsData"]> {
  return useServingQuery(["alerts"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/alerts");
    return unwrap(data, response);
  });
}

export function usePaper(): ServingQueryResult<PaperData> {
  return useServingQuery(["paper"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/paper");
    return unwrap(data, response);
  });
}

// ---- the three writes (forwarded to page control by the API) ----

export function useSavePool() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["SavePoolRequest"]) => {
      const { data, response } = await apiClient().POST("/api/v1/pools", { body });
      return unwrap(data, response);
    },
    onSuccess: () => client.invalidateQueries({ queryKey: ["pools"] }),
  });
}

export function useAckAlert() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (alertId: string) => {
      const { data, response } = await apiClient().POST("/api/v1/alerts/ack", {
        body: { alert_id: alertId },
      });
      return unwrap(data, response);
    },
    onSuccess: () => client.invalidateQueries({ queryKey: ["alerts"] }),
  });
}

export function useAddWatch() {
  return useMutation({
    mutationFn: async (code: string) => {
      const { data, response } = await apiClient().POST("/api/v1/watchlist", {
        body: { code, note: "" },
      });
      return unwrap(data, response);
    },
  });
}

export function useDataCenter(): ServingQueryResult<Schemas["DataCenterData"]> {
  return useServingQuery(["data-center"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/data-center");
    return unwrap(data, response);
  });
}

export function useFactors(): ServingQueryResult<Schemas["FactorListData"]> {
  return useServingQuery(["factors"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/factors");
    return unwrap(data, response);
  });
}

export function useFactor(id: string | null): ServingQueryResult<Schemas["FactorDetailData"]> {
  return useServingQuery(
    ["factors", id],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/factors/{factor_id}", {
        params: { path: { factor_id: id ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: id !== null },
  );
}

export function useStrategies(): ServingQueryResult<Schemas["StrategyListData"]> {
  return useServingQuery(["strategies"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/strategies");
    return unwrap(data, response);
  });
}
