import { useQuery } from "@tanstack/react-query";
import { useEffect } from "react";
import { validPriceRuleReceipt } from "@/pages/monitor/priceAlertRuleCommandSession";
import { ApiError, apiClient, type Schemas } from "./client";
import { useCurrentMeta } from "./useMeta";

export type PriceRuleCommand = Schemas["PriceAlertRuleCommandRequest"];
export type PriceRuleReceipt = Schemas["PriceAlertRuleCommandReceipt"];
export type PriceRuleItem = Schemas["PriceAlertRuleItem"];
export type PriceRuleMember = Schemas["PriceAlertRuleMember"];

export async function postPriceRule(
  body: PriceRuleCommand,
  resume: boolean,
): Promise<PriceRuleReceipt> {
  const path = resume
    ? "/api/v1/monitor/price-rules/commands/resume"
    : "/api/v1/monitor/price-rules/commands";
  const { data, error, response } = await apiClient().POST(path, {
    body,
    headers: { "X-Rquant-Csrf": "1" },
    signal: AbortSignal.timeout(12_000),
  });
  const receipt: unknown = data ?? error;
  if (!validPriceRuleReceipt(receipt, body))
    throw new ApiError(response.status, "状态待核对，请继续核对原操作。");
  return receipt;
}

export function usePriceRules() {
  const meta = useCurrentMeta();
  useEffect(() => {
    void meta.refetch();
  }, [meta.refetch]);
  const owner = meta.isFetchedAfterMount && !meta.isError ? (meta.data?.data.viewer ?? null) : null;
  const generation =
    meta.isFetchedAfterMount && !meta.isError && meta.data?.serving.state === "ready"
      ? (meta.data?.data.generation?.generation_id ?? null)
      : null;
  const query = useQuery<Schemas["Envelope_PriceAlertRuleListData_"]>({
    queryKey: ["private-price-rules", owner, generation],
    enabled: owner !== null && generation !== null,
    queryFn: async () => {
      const { data, response } = await apiClient().GET("/api/v1/monitor/price-rules");
      if (data === undefined) throw new ApiError(response.status, "规则暂不可用，请稍后重试。");
      return data;
    },
    staleTime: 0,
  });
  const data =
    !query.isError &&
    query.data?.serving.state === "ready" &&
    query.data.serving.generation_id === generation
      ? query.data.data
      : null;
  return {
    owner,
    generation,
    data,
    loading:
      (!meta.isFetchedAfterMount && !meta.isError) ||
      (owner !== null && generation !== null && query.isPending),
    refresh: () => {
      void meta.refetch();
      void query.refetch();
    },
    serverTime: meta.data?.data.server_time ?? null,
  };
}
