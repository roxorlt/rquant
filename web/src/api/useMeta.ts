import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { ApiError, apiClient, type MetaEnvelope } from "./client";

export const META_QUERY_KEY = ["meta"] as const;
export const META_POLL_MS = 15_000;

export async function fetchMeta(): Promise<MetaEnvelope> {
  const { data, response } = await apiClient().GET("/api/v1/meta");
  if (data === undefined) {
    throw new ApiError(response.status, `网页 API 返回 HTTP ${response.status}`);
  }
  return data;
}

/** Polls /api/v1/meta; a new Serving generation invalidates every other query. */
export function useMeta() {
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: META_QUERY_KEY,
    queryFn: fetchMeta,
    refetchInterval: META_POLL_MS,
    staleTime: 0,
  });
  const generationId = query.data?.data.generation.generation_id ?? null;
  const previous = useRef<string | null | undefined>(undefined);
  useEffect(() => {
    if (query.data === undefined) return;
    if (previous.current !== undefined && previous.current !== generationId) {
      void queryClient.invalidateQueries({ predicate: (q) => q.queryKey[0] !== "meta" });
    }
    previous.current = generationId;
  }, [generationId, query.data, queryClient]);
  return query;
}
