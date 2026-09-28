import { useQuery } from "@tanstack/react-query";
import { ApiError, apiClient, type Schemas } from "./client";
import type { PriceRuleCommandDraft } from "./priceAlertRuleCommand";
import { fetchMeta } from "./useMeta";

export type PriceRuleItem = Schemas["PriceAlertRuleItemData"];
export type PriceRuleEnvelope = Schemas["Envelope_PriceAlertRuleListData_"];

const UNAVAILABLE = "规则暂不可用，请稍后重试。";

export async function fetchPriceAlertRules(): Promise<PriceRuleEnvelope> {
  const { data, response } = await apiClient().GET("/api/v1/monitor/rules");
  if (data === undefined) throw new ApiError(response.status, UNAVAILABLE);
  return data;
}

export function usePriceAlertRules(
  viewer: string | null,
  generationId: string | null,
  fresh: boolean,
) {
  const query = useQuery({
    queryKey: ["private-price-rules", viewer, generationId],
    queryFn: fetchPriceAlertRules,
    enabled: fresh && viewer !== null && generationId !== null,
  });
  const sameGeneration =
    fresh &&
    generationId !== null &&
    query.data?.serving.state === "ready" &&
    query.data.serving.generation_id === generationId;
  const data = sameGeneration && !query.isError ? query.data?.data : undefined;
  const state =
    !fresh || viewer === null
      ? "unavailable"
      : query.isPending
        ? "loading"
        : data?.availability === "ready"
          ? "ready"
          : data?.availability === "not_ready"
            ? "not_ready"
            : "unavailable";
  return {
    state,
    items: state === "ready" ? (data?.items ?? []) : [],
    serving: sameGeneration ? query.data?.serving : undefined,
    retry: () => void query.refetch(),
  } as const;
}

export async function verifyPriceRuleOwner(viewer: string): Promise<boolean> {
  const meta = await fetchMeta();
  return meta.data.viewer === viewer;
}

export async function verifyPriceRuleBasis(
  viewer: string,
  draft: PriceRuleCommandDraft,
): Promise<"ready" | "stale" | "unavailable"> {
  const meta = await fetchMeta();
  const generationId = meta.data.generation?.generation_id;
  if (
    meta.data.viewer !== viewer ||
    meta.serving.state !== "ready" ||
    generationId !== draft.generation_id ||
    meta.serving.generation_id !== generationId ||
    !Number.isFinite(Date.parse(meta.data.server_time))
  )
    return "stale";
  const rules = await fetchPriceAlertRules();
  if (rules.serving.state !== "ready" || rules.serving.generation_id !== generationId)
    return "stale";
  if (rules.data.availability !== "ready") return "unavailable";
  const id = draft.kind === "save_price_alert_rule" ? draft.rule.rule_id : draft.rule_id;
  const current = rules.data.items.find((item) => item.rule_id === id);
  if (
    (current?.version ?? null) !== (draft.expected_version ?? null) ||
    (draft.kind !== "save_price_alert_rule" && (current === undefined || current.deleted))
  )
    return "stale";
  if (draft.kind !== "save_price_alert_rule") return "ready";
  const { data: watchlist, response } = await apiClient().GET("/api/v1/watchlist/{ts_code}", {
    params: { path: { ts_code: draft.ts_code } },
  });
  if (watchlist === undefined) throw new ApiError(response.status, UNAVAILABLE);
  if (
    watchlist.serving.state !== "ready" ||
    watchlist.serving.generation_id !== generationId ||
    watchlist.data.ts_code !== draft.ts_code
  )
    return "stale";
  if (watchlist.data.availability !== "ready") return "unavailable";
  const expiry = watchlist.data.expires_at === null ? null : Date.parse(watchlist.data.expires_at);
  if (
    watchlist.data.status !== "active" ||
    watchlist.data.version !== draft.membership_version ||
    (expiry !== null && (!Number.isFinite(expiry) || expiry <= Date.parse(meta.data.server_time)))
  )
    return "stale";
  return "ready";
}
