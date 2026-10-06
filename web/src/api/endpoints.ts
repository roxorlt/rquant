import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useRef } from "react";
import { ApiError, apiClient, type MetaEnvelope, type Schemas } from "./client";
import type { paths } from "./schema";
import { META_QUERY_KEY, useCurrentMeta } from "./useMeta";
import { type ServingEnvelope, type ServingQueryResult, useServingQuery } from "./useServingQuery";

export type OverviewEnvelope = Schemas["Envelope_OverviewData_"];
export type OverviewData = Schemas["OverviewData"];
export type HealthEnvelope = Schemas["Envelope_HealthData_"];
export type HealthData = Schemas["HealthData"];
export type ServiceItem = Schemas["ServiceItem"];
export type FreshnessItem = Schemas["FreshnessItem"];
export type SignalItem = Schemas["SignalItem"];
export type CandidateItem = Schemas["CandidateItem"];
export type HoldingItem = Schemas["HoldingItem"];
export type StatusInfo = Schemas["StatusInfo"];
export type MonitorTimelineEnvelope = Schemas["Envelope_MonitorTimelineData_"];
export type MonitorTimelineData = Schemas["MonitorTimelineData"];
export type MonitorTimelineItem = MonitorTimelineData["items"][number];
export type MonitorChannelsData = Schemas["MonitorChannelsData"];
export type ResearchJobsData = Schemas["ResearchJobsData"];
export type ResearchJobItem = Schemas["ResearchJobItem"];
export type LabControlRequest = Schemas["LabControlRequest"];
export type LabControlReceipt = Schemas["LabControlReceipt"];
export type ResearchTaskEventsData = Schemas["ResearchTaskEventsData"];
export type TaskOverviewData = Schemas["TaskOverviewData"];
export type ScheduledTaskItem = Schemas["ScheduledTaskItem"];
export type RuntimeServiceItem = Schemas["RuntimeServiceItem"];
export type JournalPage = Schemas["JournalPage"];
export type LogLevel = NonNullable<
  paths["/api/v1/tasks/services/{unit}/logs"]["get"]["parameters"]["query"]["level"]
>;
export type ResourceGroupItem = Schemas["ResourceGroupItem"];
export type TimedTaskOverview = {
  overview: TaskOverviewData;
  scheduledDeadline: number | null;
  resourcesDeadline: number | null;
};
export type PaperAccountsData = Schemas["PaperAccountsData"];
export type PaperAccountItem = Schemas["PaperAccountItem"];
export type PaperHoldingItem = Schemas["PaperHoldingItem"];
export type StockSearchData = Schemas["StockSearchData"];
export type StockSearchRow = Schemas["StockSearchRow"];
export type StockSummaryData = Schemas["StockSummaryData"];
export type CatalogList = Schemas["CatalogList"];
export type CatalogDataset = Schemas["CatalogDatasetDetail"];
export type CatalogSummary = Schemas["CatalogSummary"];
export type CatalogField = Schemas["CatalogField"];
export type DataAuditHealthData = Schemas["DataAuditHealthData"];
export type DataAuditIssuesData = Schemas["DataAuditIssuesData"];
export type DataAuditIssueItem = Schemas["DataAuditIssueItem"];
export type BackfillPlanItem = Schemas["BackfillPlanItem"];
export type BackfillPlanDetail = Schemas["BackfillPlanDetail"];
export type PoolsData = Schemas["PoolsData"];
export type PublishedPool = Schemas["PublishedPool"];
export type PoolMember = Schemas["PoolMember"];

function unwrap<T>(data: T | undefined, response: Response): T {
  if (data === undefined) {
    throw new ApiError(response.status, `网页 API 返回 HTTP ${response.status}`);
  }
  return data;
}

export async function fetchOverview(): Promise<OverviewEnvelope> {
  const { data, response } = await apiClient().GET("/api/v1/overview");
  return unwrap(data, response);
}

export async function fetchHealth(): Promise<HealthEnvelope> {
  const { data, response } = await apiClient().GET("/api/v1/health");
  return unwrap(data, response);
}

export function useOverview(): ServingQueryResult<OverviewData> {
  return useServingQuery(["overview"], fetchOverview);
}

export function useHealth(): ServingQueryResult<HealthData> {
  return useServingQuery(["health"], fetchHealth);
}

export function usePools(): ServingQueryResult<PoolsData> {
  return useServingQuery(["pools"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/pools");
    return unwrap(data, response);
  });
}

export function useMonitorTimeline(
  cursor: string | null,
  refreshKey: number,
): ServingQueryResult<MonitorTimelineData> {
  return useServingQuery(["monitor", "timeline", cursor, refreshKey], async () => {
    const { data, response } = await apiClient().GET("/api/v1/monitor/timeline", {
      params: { query: cursor ? { page_size: 20, cursor } : { page_size: 20 } },
    });
    return unwrap(data, response);
  });
}

export function useMonitorChannels(refreshKey: number): ServingQueryResult<MonitorChannelsData> {
  return useServingQuery(["monitor", "channels", refreshKey], async () => {
    const { data, response } = await apiClient().GET("/api/v1/monitor/channels");
    return unwrap(data, response);
  });
}

export function useResearchJobs(
  cursor: string | null,
  refreshKey: number,
): ServingQueryResult<ResearchJobsData> {
  return useServingQuery(["tasks", "jobs", cursor, refreshKey], async () => {
    const { data, response } = await apiClient().GET("/api/v1/tasks/jobs", {
      params: { query: cursor ? { page_size: 20, cursor } : { page_size: 20 } },
    });
    return unwrap(data, response);
  });
}

/** A fresh private grant, separate from the delayed Serving job snapshot. */
export function useLabControlCapabilities(viewer: string | null) {
  return useQuery({
    queryKey: ["tasks", "lab-control-capabilities", viewer],
    enabled: viewer !== null,
    gcTime: 0,
    staleTime: 0,
    retry: false,
    refetchInterval: 15_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: "always",
    queryFn: async (): Promise<Schemas["LabControlCapabilities"]> => {
      const { data, response } = await apiClient().GET("/api/v1/tasks/jobs/control-capabilities");
      return unwrap(data, response);
    },
  });
}

export async function submitLabControl(body: LabControlRequest): Promise<LabControlReceipt> {
  const { data, error, response } = await apiClient()
    .POST("/api/v1/tasks/jobs/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认，请查询或重试原请求。");
    });
  const receipt = data ?? error;
  if (
    receipt &&
    typeof receipt === "object" &&
    "command_id" in receipt &&
    receipt.command_id === body.command_id &&
    "status" in receipt &&
    ["submitted", "pending", "processing", "unknown", "conflict", "failed"].includes(
      String(receipt.status),
    ) &&
    "message" in receipt &&
    typeof receipt.message === "string"
  ) {
    return receipt as LabControlReceipt;
  }
  throw new ApiError(response.status, "提交状态待确认，请查询或重试原请求。");
}

/** Private task progress is keyed to the exact overview generation and opens on demand. */
export function useResearchTaskEvents(jobId: string | null, generationId: string | null) {
  return useQuery({
    queryKey: ["tasks", "research-events", jobId, generationId],
    enabled: jobId !== null && generationId !== null,
    staleTime: 0,
    gcTime: 0,
    retry: false,
    refetchOnWindowFocus: false,
    queryFn: async (): Promise<ResearchTaskEventsData> => {
      const { data, response } = await apiClient().GET("/api/v1/tasks/jobs/{job_id}/events", {
        params: {
          path: { job_id: jobId ?? "" },
          query: { generation_id: generationId },
        },
      });
      return unwrap(data, response);
    },
  });
}

/** Independent live authority; never use Serving or health state as a log grant. */
export function useServiceLogCapabilities(viewer: string | null) {
  return useQuery({
    queryKey: ["tasks", "service-log-capabilities", viewer],
    enabled: viewer !== null,
    gcTime: 0,
    staleTime: 0,
    retry: false,
    refetchInterval: 15_000,
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: "always",
    queryFn: async (): Promise<Schemas["LogCapabilities"]> => {
      const { data, response } = await apiClient().GET("/api/v1/tasks/services/log-capabilities");
      return unwrap(data, response);
    },
  });
}

export function useInvalidateServiceLogCapabilities() {
  const queryClient = useQueryClient();
  return useCallback(
    () => queryClient.invalidateQueries({ queryKey: ["tasks", "service-log-capabilities"] }),
    [queryClient],
  );
}

export async function fetchServiceLogPage(
  unit: string,
  since: string,
  level: LogLevel | null,
  cursor: string | null,
  signal: AbortSignal,
  invocationId?: string | null,
): Promise<JournalPage> {
  const { data, response } = await apiClient().GET("/api/v1/tasks/services/{unit}/logs", {
    params: {
      path: { unit },
      query: {
        since,
        page_size: 100,
        ...(level ? { level } : {}),
        ...(cursor ? { cursor } : {}),
        ...(invocationId ? { invocation_id: invocationId } : {}),
      },
    },
    signal,
  });
  const page = unwrap(data, response);
  if (invocationId && page.invocation_id !== invocationId) {
    throw new ApiError(409, "日志已更新，请重新查看。");
  }
  return page;
}

export function deadlineFromRemaining(
  state: "ready" | "unavailable",
  remainingSeconds: number | null,
  startedAt: number,
  receivedAt: number,
): number | null {
  if (state !== "ready" || remainingSeconds === null || !Number.isFinite(remainingSeconds)) {
    return null;
  }
  const elapsed = Math.max(0, receivedAt - startedAt);
  return receivedAt + Math.max(0, remainingSeconds * 1000 - elapsed);
}

export function pinnedTaskDeadline(
  previous: { key: string; deadline: number } | null,
  key: string,
  deadline: number,
): number {
  return previous?.key === key ? Math.min(previous.deadline, deadline) : deadline;
}

export function useTaskOverview(
  cursor: string | null,
  refreshKey: number,
  enabled = true,
): ServingQueryResult<TimedTaskOverview> {
  const budgets = useRef<
    Record<"scheduled" | "resources", { key: string; deadline: number } | null>
  >({ scheduled: null, resources: null });
  return useServingQuery(
    ["tasks", "overview", cursor, refreshKey],
    async () => {
      const startedAt = performance.now();
      const { data, response } = await apiClient().GET("/api/v1/tasks/overview", {
        params: { query: cursor ? { page_size: 20, cursor } : { page_size: 20 } },
      });
      const envelope = unwrap(data, response);
      const receivedAt = performance.now();
      const pin = (section: "scheduled" | "resources"): number | null => {
        const source = envelope.data[section];
        const deadline = deadlineFromRemaining(
          source.source_state,
          source.remaining_seconds,
          startedAt,
          receivedAt,
        );
        if (deadline === null) return null;
        const key = `${envelope.serving.generation_id}:${source.source_updated_at}:${source.expires_at}`;
        const previous = budgets.current[section];
        const pinned = pinnedTaskDeadline(previous, key, deadline);
        budgets.current[section] = { key, deadline: pinned };
        return pinned;
      };
      return {
        data: {
          overview: envelope.data,
          scheduledDeadline: pin("scheduled"),
          resourcesDeadline: pin("resources"),
        },
        serving: envelope.serving,
      };
    },
    { enabled },
  );
}

export function usePaperAccounts(): ServingQueryResult<PaperAccountsData> {
  return useServingQuery(["paper", "accounts"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/paper/accounts");
    return unwrap(data, response);
  });
}

export function useCatalog(): ServingQueryResult<CatalogList> {
  return useServingQuery(["catalog", "datasets"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/data/catalog");
    return unwrap(data, response);
  });
}

export function useCatalogDataset(datasetId: string | null): ServingQueryResult<CatalogDataset> {
  return useServingQuery(
    ["catalog", "dataset", datasetId],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/data/catalog/{dataset}", {
        params: { path: { dataset: datasetId ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: datasetId !== null },
  );
}

export function useDataAuditHealth(): ServingQueryResult<DataAuditHealthData> {
  return useServingQuery(["data", "audit", "health"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/data/health");
    return unwrap(data, response);
  });
}

export function useBackfillPlans(cursor: string | null, generation: string | null) {
  return useServingQuery(["data", "backfill-plans", cursor, generation], async () => {
    const { data, response } = await apiClient().GET("/api/v1/data/backfill-plans", {
      params: {
        query: { page_size: 20, cursor: cursor ?? undefined, generation: generation ?? undefined },
      },
    });
    return unwrap(data, response);
  });
}

export function useBackfillPlanDetail(planHash: string | null, generation: string | null) {
  return useServingQuery(
    ["data", "backfill-plan", planHash, generation],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/data/backfill-plans/{plan_hash}", {
        params: { path: { plan_hash: planHash ?? "" }, query: { generation } },
      });
      return unwrap(data, response);
    },
    { enabled: planHash !== null && generation !== null },
  );
}

export function useDataAuditReport(generation: string | null | undefined) {
  return useServingQuery(
    ["data", "audit", "report", generation],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/data/report");
      return unwrap(data, response);
    },
    { enabled: generation !== undefined },
  );
}

export function useAuditReportCalendar(generation: string | null | undefined) {
  return useServingQuery(
    ["data", "audit", "calendar", generation],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/data/audit-report/calendar", {
        params: { query: { generation: generation ?? undefined } },
      });
      return unwrap(data, response);
    },
    { enabled: generation !== undefined },
  );
}

export type AuditReportCalendarData = NonNullable<
  ReturnType<typeof useAuditReportCalendar>["data"]
>;

// openapi-fetch's Readable response omits null-only fields from the generated model.
// Derive the page shape from the typed client response so it stays in sync with OpenAPI.
export type DataAuditReportData = NonNullable<ReturnType<typeof useDataAuditReport>["data"]>;
export type AuditReportMonth = DataAuditReportData["months"][number];
export type AuditReportRule = DataAuditReportData["rules"][number];
export type AuditReportIssue = DataAuditReportData["issues"][number];

export function useDataAuditIssues(
  datasetId: string,
  generation: string | null,
): ServingQueryResult<DataAuditIssuesData> {
  return useServingQuery(
    ["data", "audit", "issues", datasetId, generation],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/data/issues", {
        params: { query: { dataset: datasetId, generation } },
      });
      return unwrap(data, response);
    },
    { enabled: generation !== null },
  );
}

export function useStockSearch(query: string): ServingQueryResult<StockSearchData> {
  return useServingQuery(
    ["stocks", "search", query],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/stocks/search", {
        params: { query: { q: query } },
      });
      return unwrap(data, response);
    },
    { enabled: query.length > 0 },
  );
}

export function useStockSummary(tsCode: string | null): ServingQueryResult<StockSummaryData> {
  return useServingQuery(
    ["stocks", "summary", tsCode],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/stocks/{ts_code}/summary", {
        params: { path: { ts_code: tsCode ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: tsCode !== null },
  );
}

// ------------------------------------------------------------------ 市场全景

export type PulseEnvelope = Schemas["Envelope_PulseData_"];
export type PulseData = Schemas["PulseData"];
export type BoardsData = Schemas["BoardsData"];
export type BoardRow = Schemas["BoardRow"];
export type MembersData = Schemas["MembersData"];
export type MemberRow = Schemas["MemberRow"];
export type IntradayData = Schemas["IntradayData"];
export type DailyData = Schemas["DailyData"];
export type SurgeData = Schemas["SurgeData"];
export type SurgeRow = Schemas["SurgeRow"];
export type SurgeSearchData = Schemas["SurgeSearchData"];

export function usePulse(): ServingQueryResult<PulseData> {
  return useServingQuery(["panorama", "pulse"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/panorama/pulse");
    return unwrap(data, response);
  });
}

export function useBoards(system: string): ServingQueryResult<BoardsData> {
  return useServingQuery(["panorama", "boards", system], async () => {
    const { data, response } = await apiClient().GET("/api/v1/panorama/boards", {
      params: { query: { system } },
    });
    return unwrap(data, response);
  });
}

export function useMembers(boardCode: string | null): ServingQueryResult<MembersData> {
  return useServingQuery(
    ["panorama", "members", boardCode],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/panorama/boards/{board_code}/members",
        { params: { path: { board_code: boardCode ?? "" } } },
      );
      return unwrap(data, response);
    },
    { enabled: boardCode !== null },
  );
}

export function useIntraday(
  tsCode: string | null,
  options: { days?: number; date?: string | null },
): ServingQueryResult<IntradayData> {
  const { days = 1, date = null } = options;
  return useServingQuery(
    ["panorama", "intraday", tsCode, days, date],
    async () => {
      const { data, response } = await apiClient().GET(
        "/api/v1/panorama/stocks/{ts_code}/intraday",
        {
          params: {
            path: { ts_code: tsCode ?? "" },
            query: date ? { date } : { days },
          },
        },
      );
      return unwrap(data, response);
    },
    { enabled: tsCode !== null },
  );
}

export function useDaily(tsCode: string | null): ServingQueryResult<DailyData> {
  return useServingQuery(
    ["panorama", "daily", tsCode],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/panorama/stocks/{ts_code}/daily", {
        params: { path: { ts_code: tsCode ?? "" } },
      });
      return unwrap(data, response);
    },
    { enabled: tsCode !== null },
  );
}

export function useSurge(date: string | null): ServingQueryResult<SurgeData> {
  return useServingQuery(["panorama", "surge", date], async () => {
    const { data, response } = await apiClient().GET("/api/v1/panorama/surge", {
      params: { query: date ? { date } : {} },
    });
    return unwrap(data, response);
  });
}

export function useSurgeSearch(query: string): ServingQueryResult<SurgeSearchData> {
  return useServingQuery(
    ["panorama", "surge-search", query],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/panorama/surge/search", {
        params: { query: { q: query } },
      });
      return unwrap(data, response);
    },
    { enabled: query.length > 0 },
  );
}

/** Refetch everything on the panorama now (the 刷新 button). */
export function useRefreshPanorama(): () => void {
  const client = useQueryClient();
  return () => {
    void client.invalidateQueries({ queryKey: ["panorama"] });
  };
}

export type DataCenterCommand = NonNullable<
  paths["/api/v1/data/executions/commands"]["post"]["requestBody"]
>["content"]["application/json"];
export type DataCenterCommandReceipt = Schemas["DataCenterCommandReceipt"];
export type DataCenterExecution = Schemas["ExecutionView"];
export type DataCenterConfirmation = Schemas["ExecutionConfirmation"];
export type FinancialSource = Schemas["FinancialSourceView"];

export function useDataCenterExecutions() {
  const queryClient = useQueryClient();
  const viewer = useCurrentMeta().data?.data.viewer ?? null;
  const query = useServingQuery(
    ["data-center", "executions", viewer],
    async () => {
      const { data, response } = await apiClient().GET("/api/v1/data/executions");
      return unwrap(data, response);
    },
    { refetchInterval: 15_000 },
  );
  return {
    ...query,
    readCurrent: () => ({
      meta: queryClient.getQueryData<MetaEnvelope>(META_QUERY_KEY),
      metaFetchStatus: queryClient.getQueryState(META_QUERY_KEY)?.fetchStatus,
      index: queryClient.getQueryState<ServingEnvelope<Schemas["ExecutionIndexData"]>>([
        "data-center",
        "executions",
        viewer,
      ]),
    }),
  };
}

export function useDataCollection() {
  return useServingQuery(["data-center", "collection"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/data/collection");
    return unwrap(data, response);
  });
}

export function useFinancialSources() {
  return useServingQuery(["data-center", "financial-sources"], async () => {
    const { data, response } = await apiClient().GET("/api/v1/data/financial-sources");
    return unwrap(data, response);
  });
}

export async function submitDataCenterCommand(
  body: DataCenterCommand,
): Promise<DataCenterCommandReceipt> {
  const { data, response } = await apiClient()
    .POST("/api/v1/data/executions/commands", {
      body,
      headers: { "X-Rquant-Csrf": "1" },
      signal: AbortSignal.timeout(12_000),
    })
    .catch(() => {
      throw new ApiError(503, "提交状态待确认，请使用原请求重试。");
    });
  return unwrap(data, response);
}
