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

/**
 * Polls /api/v1/meta every 15 s. When the Serving generation changes, every
 * other query is invalidated, so the page on screen refetches its data.
 */
export function useMeta() {
  const queryClient = useQueryClient();
  const query = useQuery({
    queryKey: META_QUERY_KEY,
    queryFn: fetchMeta,
    refetchInterval: META_POLL_MS,
    refetchIntervalInBackground: false,
    staleTime: 0,
  });
  const generationId = query.data?.data.generation?.generation_id ?? null;
  const previous = useRef<string | null>(null);

  useEffect(() => {
    if (generationId === null) {
      return;
    }
    if (previous.current !== null && previous.current !== generationId) {
      void queryClient.invalidateQueries({
        predicate: (entry) => entry.queryKey[0] !== META_QUERY_KEY[0],
      });
    }
    previous.current = generationId;
  }, [generationId, queryClient]);

  return query;
}
