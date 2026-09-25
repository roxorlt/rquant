import { type QueryKey, useQuery } from "@tanstack/react-query";
import type { ServingMeta } from "./client";

/** Any API response: the Envelope shape every endpoint returns. */
export interface ServingEnvelope<T> {
  data: T;
  serving: ServingMeta;
}

export interface ServingQueryResult<T> {
  data: T | undefined;
  /** The envelope's serving block; pass it to <ServingBanner>. */
  serving: ServingMeta | undefined;
  isLoading: boolean;
  /** A refetch is in flight (the refresh button spins). */
  isFetching: boolean;
  error: Error | null;
  refetch: () => void;
}

/**
 * The one data hook for pages: runs the query and splits the envelope into its
 * data and its serving state. Queries are invalidated when /meta reports a new
 * generation (see useMeta), so no page polls on its own.
 */
export function useServingQuery<T>(
  key: QueryKey,
  fetcher: () => Promise<ServingEnvelope<T>>,
  options: { enabled?: boolean } = {},
): ServingQueryResult<T> {
  const query = useQuery({ queryKey: key, queryFn: fetcher, enabled: options.enabled ?? true });
  return {
    data: query.data?.data,
    serving: query.data?.serving,
    isLoading: query.isLoading,
    isFetching: query.isFetching,
    error: query.error,
    refetch: () => {
      void query.refetch();
    },
  };
}
