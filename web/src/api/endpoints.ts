/** Every API call the pages make. Types come only from the generated schema. */
import { useMutation, useQueries, useQueryClient } from "@tanstack/react-query";
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

/** A-share session (Shanghai): call auction through close, weekdays. */
export function inTradingHours(now: Date = new Date()): boolean {
  const sh = new Date(now.getTime() + (now.getTimezoneOffset() + 480) * 60_000);
  const day = sh.getDay();
  if (day === 0 || day === 6) return false;
  const m = sh.getHours() * 60 + sh.getMinutes();
  return (m >= 555 && m <= 692) || (m >= 779 && m <= 902); // 09:15-11:32, 12:59-15:02
}

/** 竞价选股 pool: latest pct + 1-min sparkline from 3s snapshots; 10s refresh in session. */
export function useAuctionPoolLive(enabled: boolean): ServingQueryResult<Schemas["PoolLiveData"]> {
  return useServingQuery(
    ["pools", "auction", "live"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/pools/auction/live");
      return unwrap(data, response);
    },
    { enabled, refetchInterval: () => (inTradingHours() ? 10_000 : 60_000) },
  );
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

/** Several portfolio runs at once (multi-run compare); same cache keys as usePortfolioBacktest. */
export function usePortfolioBacktestMany(runIds: readonly string[]) {
  return useQueries({
    queries: runIds.map((runId) => ({
      queryKey: ["portfolio-backtests", runId],
      queryFn: async () => {
        const { data, response } = await apiClient().GET("/api/v1/portfolio-backtests/{run_id}", {
          params: { path: { run_id: runId } },
        });
        return unwrap(data, response);
      },
    })),
  });
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

export function useAlertRules(): ServingQueryResult<Schemas["AlertRulesData"]> {
  return useServingQuery(["alert-rules"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/alert-rules");
    return unwrap(data, response);
  });
}

export function useSaveAlertRule() {
  const client = useQueryClient();
  return useMutation({
    mutationFn: async (body: Schemas["AlertRuleBody"]) => {
      const { data, response } = await apiClient().POST("/api/v1/alert-rules", { body });
      return unwrap(data, response);
    },
    onSuccess: () => client.invalidateQueries({ queryKey: ["alert-rules"] }),
  });
}

export function useConditions(): ServingQueryResult<Schemas["ConditionListData"]> {
  return useServingQuery(["conditions"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/conditions");
    return unwrap(data, response);
  });
}

export function useTranslateTdx() {
  return useMutation({
    mutationFn: async (tdx: string) => {
      const { data, response } = await apiClient().POST("/api/v1/formula/translate", {
        body: { tdx },
      });
      return unwrap(data, response);
    },
  });
}

export function usePortfolioBand(
  runId: string | null,
  days: number,
): ServingQueryResult<Schemas["BandData"]> {
  return useServingQuery(
    ["portfolio-backtests", runId, "band", days],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/portfolio-backtests/{run_id}/band",
        { params: { path: { run_id: runId ?? "" }, query: { days } } },
      );
      return unwrap(data, response);
    },
    { enabled: runId !== null && days > 0 },
  );
}

export function useExplainBacktest() {
  return useMutation({
    mutationFn: async (runId: string) => {
      const { data, response } = await apiClient().POST(
        "/api/v1/portfolio-backtests/{run_id}/explain",
        { params: { path: { run_id: runId } } },
      );
      return unwrap(data, response);
    },
  });
}

export function useOperations(): ServingQueryResult<Schemas["OperationsData"]> {
  return useServingQuery(["operations"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/operations");
    return unwrap(data, response);
  });
}

export function useTasks(): ServingQueryResult<Schemas["TaskRow"][]> {
  return useServingQuery(["tasks"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/tasks");
    return unwrap(data, response);
  });
}

export function useTaskLog(unit: string | null): ServingQueryResult<Schemas["TaskLog"]> {
  return useServingQuery(
    ["tasks", unit],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/tasks/{unit}/log", {
        params: { path: { unit: unit ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: unit !== null },
  );
}

export function useQueryTables(): ServingQueryResult<string[]> {
  return useServingQuery(["query-tables"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/query/tables");
    return unwrap(data, response);
  });
}

export function useRunQuery() {
  return useMutation({
    mutationFn: async (sql: string) => {
      const { data, error, response } = await apiClient().POST("/api/v1/query", {
        body: { sql },
      });
      if (data === undefined) {
        const detail = (error as { detail?: unknown } | undefined)?.detail;
        throw new ApiError(
          response.status,
          typeof detail === "string" ? detail : `HTTP ${response.status}`,
        );
      }
      return data.data;
    },
  });
}

export function useFundamentals(): ServingQueryResult<Schemas["FundamentalsDoc"] | null> {
  return useServingQuery(["fundamentals"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/fundamentals");
    return unwrap(data, response);
  });
}

export function useStock(code: string | undefined): ServingQueryResult<Schemas["StockProfile"]> {
  return useServingQuery(
    ["stock", code],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/stocks/{code}", {
        params: { path: { code: code ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: Boolean(code) },
  );
}

export function useBrinson(
  runId: string | null,
): ServingQueryResult<Schemas["BrinsonData"] | null> {
  return useServingQuery(
    ["portfolio-backtests", runId, "brinson"],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/portfolio-backtests/{run_id}/brinson",
        { params: { path: { run_id: runId ?? "" } } },
      );
      return unwrap(data, response);
    },
    { enabled: runId !== null },
  );
}

export function useFactorCorrelation(): ServingQueryResult<Schemas["FactorCorrData"]> {
  return useServingQuery(["factors", "correlation"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/factors/correlation");
    return unwrap(data, response);
  });
}

export function useQuerySchema(): ServingQueryResult<Record<string, string[][]>> {
  return useServingQuery(["query", "schema"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/query/schema");
    return unwrap(data, response);
  });
}

export function useEventStudies(): ServingQueryResult<Schemas["EventStudyListData"]> {
  return useServingQuery(["event-studies"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/event-studies");
    return unwrap(data, response);
  });
}

export function useTextToCondition() {
  return useMutation({
    mutationFn: async (text: string) => {
      const { data, response } = await apiClient().POST("/api/v1/formula/from-text", {
        body: { text },
      });
      return unwrap(data, response);
    },
  });
}

export function useChips(
  code: string | undefined,
  tpoPeriod?: number,
): ServingQueryResult<Schemas["ChipsData"] | null> {
  return useServingQuery(
    ["stock", code, "chips", tpoPeriod ?? "default"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/stocks/{code}/chips", {
        params: {
          path: { code: code ?? "" },
          ...(tpoPeriod ? { query: { tpo_period: tpoPeriod } } : {}),
        },
      });
      return unwrap(data, response);
    },
    { enabled: Boolean(code) },
  );
}

export function useIntraday(
  code: string | undefined,
  day?: string,
  span: 1 | 5 = 1,
): ServingQueryResult<Schemas["IntradayData"] | null> {
  return useServingQuery(
    ["stock", code, "intraday", day ?? "latest", span],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/stocks/{code}/intraday", {
        params: { path: { code: code ?? "" }, query: { span, ...(day ? { day } : {}) } },
      });
      return unwrap(data, response);
    },
    { enabled: Boolean(code) },
  );
}

export type SurgeEvent = Schemas["SurgeEvent"];

/** Intraday volume-surge events (research snapshot scanner); refreshed every 15s. */
export function useSurge(): ServingQueryResult<Schemas["SurgeData"]> {
  return useServingQuery(
    ["surge"],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/surge", {
        params: { query: { limit: 300 } },
      });
      return unwrap(data, response);
    },
    { refetchInterval: 15_000 },
  );
}

export type MacroData = Schemas["MacroData"];

export function useMacro(): ServingQueryResult<MacroData> {
  return useServingQuery(["macro"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/macro");
    return unwrap(data, response);
  });
}

/** 研报因子: sell-side factor catalog + replication results (daily batch). */
export function useReportFactors(): ServingQueryResult<Schemas["ReportFactorsData"]> {
  return useServingQuery(["report-factors"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/report-factors");
    return unwrap(data, response);
  });
}
