import { useQueryClient } from "@tanstack/react-query";
import { ApiError, apiClient, type Schemas } from "./client";
import { type ServingQueryResult, useServingQuery } from "./useServingQuery";

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
