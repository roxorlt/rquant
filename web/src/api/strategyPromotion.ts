import { useQueryClient } from "@tanstack/react-query";
import { useEffect } from "react";
import { ApiError, apiClient, type Schemas } from "./client";
import { useServingQuery } from "./useServingQuery";

export type PromotionTarget = Schemas["StrategyPromotionTarget"];
export type PromotionData = Schemas["StrategyPromotionData"];
export type PromotionReview = Schemas["StrategyPromotionReview-Output"];
export type PromotionPreparation = Schemas["PreparedPromotionApproval-Output"];
export type PromotionCommand =
  | Schemas["RequestPromotionReview"]
  | Schemas["PreparePromotionApproval"]
  | Schemas["ApprovePromotion-Input"]
  | Schemas["RunStrategyWalkForward"];
export type PromotionResult = Schemas["StrategyPromotionCommandData"];
export type PromotionMode = "submit" | "lookup" | "resume";

export function usePromotionCacheIdentity(viewer: string | null, roleHash?: string | null): void {
  const client = useQueryClient();
  useEffect(() => {
    client.removeQueries({
      predicate: (entry) =>
        entry.queryKey[0] === "strategy-promotions" &&
        (entry.queryKey[1] !== viewer || entry.queryKey[2] !== roleHash),
    });
  }, [viewer, roleHash, client]);
}

function checked<T>(data: T | undefined, error: unknown, status: number): T {
  if (data !== undefined) return data;
  const detail =
    typeof error === "object" && error !== null && "detail" in error ? error.detail : null;
  throw new ApiError(
    status,
    typeof detail === "string" ? detail : "评估记录暂不可用，请稍后重试。",
  );
}
function canonical(value: unknown, key = ""): string {
  if (
    typeof value === "string" &&
    ["requested_at", "issued_at", "expires_at", "observed_at"].includes(key)
  ) {
    const utc = value.replace(/\+00:00$/, "Z");
    const match = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?Z$/.exec(utc);
    return JSON.stringify(match ? `${match[1]}.${(match[2] ?? "").padEnd(6, "0")}Z` : value);
  }
  if (Array.isArray(value)) return `[${value.map((item) => canonical(item)).join(",")}]`;
  if (typeof value === "object" && value !== null) {
    return `{${Object.entries(value)
      .filter(([, item]) => item !== undefined)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([name, item]) => `${JSON.stringify(name)}:${canonical(item, name)}`)
      .join(",")}}`;
  }
  return JSON.stringify(value) ?? "null";
}
export function samePromotionValue(left: unknown, right: unknown): boolean {
  return canonical(left) === canonical(right);
}
export function usePromotionData(
  viewer: string,
  roleHash: string | null | undefined,
  generation: string | null,
  sourceKind: PromotionTarget["source_kind"],
  strategyId: string,
  version: number | undefined,
  enabled: boolean,
  offset = 0,
) {
  return useServingQuery(
    ["strategy-promotions", viewer, roleHash, generation, sourceKind, strategyId, version, offset],
    async () => {
      const { data, error, response } = await apiClient().GET(
        "/api/v1/strategy-promotions/{strategy_id}",
        {
          params: {
            path: { strategy_id: strategyId },
            query: {
              generation_id: generation ?? undefined,
              source_kind: sourceKind,
              version,
              offset,
              limit: 20,
            },
          },
        },
      );
      return checked(data, error, response.status);
    },
    { enabled: enabled && generation !== null, staleTime: 0 },
  );
}
export async function postPromotionCommand(
  body: PromotionCommand,
  mode: PromotionMode,
): Promise<PromotionResult> {
  const headers = { "X-Rquant-Csrf": "1" };
  const result =
    mode === "lookup"
      ? await apiClient().POST("/api/v1/strategy-promotions/commands/lookup", { body, headers })
      : mode === "resume"
        ? await apiClient().POST("/api/v1/strategy-promotions/commands/resume", { body, headers })
        : await apiClient().POST("/api/v1/strategy-promotions/commands", { body, headers });
  const value = checked(result.data, result.error, result.response.status).data;
  if (
    !samePromotionValue(value.original_request, body) ||
    (value.receipt && value.receipt.command_id !== body.command_id)
  ) {
    throw new ApiError(503, "原操作回执暂时无法核验。");
  }
  if (
    value.review &&
    (body.kind !== "request_promotion_review" ||
      value.review.command_id !== body.command_id ||
      !samePromotionValue(value.review.target, body.target) ||
      !samePromotionValue(value.review.selection, body.selection) ||
      value.review.expected_revision !== body.expected_revision)
  ) {
    throw new ApiError(503, "原评估记录暂时无法核验。");
  }
  if (
    value.approval &&
    (body.kind !== "approve_promotion" ||
      value.approval.command_id !== body.command_id ||
      !samePromotionValue(value.approval.review, body.preparation.review) ||
      !samePromotionValue(value.approval.after.target, body.target))
  ) {
    throw new ApiError(503, "原批准记录暂时无法核验。");
  }
  if (
    value.walk_forward &&
    (body.kind !== "run_strategy_walk_forward" || value.walk_forward.command_id !== body.command_id)
  ) {
    throw new ApiError(503, "原验证任务回执暂时无法核验。");
  }
  return value;
}
