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
