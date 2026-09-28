import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { ApiError, apiClient, type Schemas } from "./client";
import { useCurrentMeta } from "./useMeta";

type ListEnvelope = Schemas["Envelope_ManualWatchlistListData_"];
type ExactEnvelope = Schemas["Envelope_ManualWatchlistExactData_"];
export type ManualWatchlistItem = Schemas["ManualWatchlistItemData"];
export type ManualWatchlistStatus =
  | "loading"
  | "unavailable"
  | "active"
  | "expired"
  | "deleted"
  | "absent";

const UNAVAILABLE = "名单暂不可用，请稍后重试";

function useScope(enabled: boolean) {
  const meta = useCurrentMeta();
  useEffect(() => {
    if (enabled) void meta.refetch();
  }, [enabled, meta.refetch]);
  const envelope = meta.data;
  const viewer = envelope?.data.viewer;
  const generationId = envelope?.data.generation?.generation_id;
  const trusted =
    meta.isFetchedAfterMount &&
    !meta.isError &&
    envelope?.serving.state === "ready" &&
    typeof viewer === "string" &&
    viewer.length > 0 &&
    typeof generationId === "string" &&
    generationId === envelope.serving.generation_id &&
    Number.isFinite(Date.parse(envelope.data.server_time));
  return {
    meta,
    viewer: trusted ? viewer : null,
    generationId: trusted ? generationId : null,
    serverTime: trusted ? envelope.data.server_time : null,
    metaUpdatedAt: meta.dataUpdatedAt,
    waiting: enabled && !meta.isFetchedAfterMount && !meta.isError,
  };
}

function useTrustedNow(
  serverTime: string | null,
  updatedAt: number,
  expiries: (string | null)[],
): number | null {
  const anchor = useRef<{ key: string; serverMs: number; startedMs: number } | null>(null);
  const [, setPulse] = useState(0);
  const key = `${updatedAt}:${serverTime}`;
  if (serverTime === null) {
    anchor.current = null;
  } else if (anchor.current?.key !== key) {
    anchor.current = { key, serverMs: Date.parse(serverTime), startedMs: performance.now() };
  }
  const now = anchor.current
    ? anchor.current.serverMs + performance.now() - anchor.current.startedMs
    : null;
  const nextExpiry =
    now === null
      ? null
      : expiries.reduce<number | null>((next, value) => {
          if (value === null) return next;
          const at = Date.parse(value);
          return Number.isFinite(at) && at > now && (next === null || at < next) ? at : next;
        }, null);
  useEffect(() => {
    if (now === null || nextExpiry === null) return;
    const delay = Math.min(Math.max(nextExpiry - now + 1, 1), 2_147_483_647);
    const timer = window.setTimeout(() => setPulse((value) => value + 1), delay);
    return () => window.clearTimeout(timer);
  }, [now, nextExpiry]);
  return now;
}

function verified<T extends { availability: string }>(
  envelope: { data: T; serving: Schemas["ServingMeta"] } | undefined,
  generationId: string | null,
): envelope is { data: T; serving: Schemas["ServingMeta"] } {
  return (
    generationId !== null &&
    envelope?.serving.state === "ready" &&
    envelope.serving.generation_id === generationId &&
    envelope.data.availability === "ready"
  );
}

export function useManualWatchlistExact(tsCode: string | null) {
  const scope = useScope(tsCode !== null);
  const query = useQuery<ExactEnvelope>({
    queryKey: ["private-watchlist", "exact", scope.viewer, scope.generationId, tsCode],
    enabled: tsCode !== null && scope.viewer !== null && scope.generationId !== null,
    queryFn: async () => {
      const { data, response } = await apiClient().GET("/api/v1/watchlist/{ts_code}", {
        params: { path: { ts_code: tsCode ?? "" } },
      });
      if (data === undefined) throw new ApiError(response.status, UNAVAILABLE);
      return data;
    },
  });
  const now = useTrustedNow(scope.serverTime, scope.metaUpdatedAt, [
    query.data?.data.expires_at ?? null,
  ]);
  let status: ManualWatchlistStatus = "unavailable";
  let version: number | null = null;
  if (tsCode === null || scope.waiting || (scope.viewer !== null && query.isPending)) {
    status = "loading";
  } else if (
    !query.isError &&
    now !== null &&
    verified(query.data, scope.generationId) &&
    query.data.data.ts_code === tsCode
  ) {
    const fact = query.data.data;
    if (fact.status === "absent" && fact.version === null) {
      status = "absent";
    } else if (fact.status !== null && fact.status !== "absent" && fact.version !== null) {
      const expires = fact.expires_at === null ? null : Date.parse(fact.expires_at);
      if (expires === null || Number.isFinite(expires)) {
        status =
          fact.status === "active" && expires !== null && expires <= now ? "expired" : fact.status;
        version = fact.version;
      }
    }
  }
  return {
    status,
    version,
    viewer: scope.viewer,
    generationId: status === "loading" || status === "unavailable" ? null : scope.generationId,
    message: status === "unavailable" ? UNAVAILABLE : null,
    refreshMeta: () => void scope.meta.refetch(),
    retry: () => {
      if (scope.viewer === null) void scope.meta.refetch();
      else void query.refetch();
    },
  };
}

export function useManualWatchlist() {
  const scope = useScope(true);
  const query = useQuery<ListEnvelope>({
    queryKey: ["private-watchlist", "list", scope.viewer, scope.generationId],
    enabled: scope.viewer !== null && scope.generationId !== null,
    queryFn: async () => {
      const { data, response } = await apiClient().GET("/api/v1/watchlist");
      if (data === undefined) throw new ApiError(response.status, UNAVAILABLE);
      return data;
    },
  });
  const now = useTrustedNow(
    scope.serverTime,
    scope.metaUpdatedAt,
    query.data?.data.items.map((item) => item.expires_at) ?? [],
  );
  const data =
    !query.isError && now !== null && verified(query.data, scope.generationId)
      ? query.data.data
      : null;
  const ready =
    data !== null &&
    data.items.length <= 500 &&
    data.items.every(
      (item) => item.expires_at === null || Number.isFinite(Date.parse(item.expires_at)),
    );
  const items =
    ready && data !== null && now !== null
      ? data.items.filter((item) => item.expires_at === null || Date.parse(item.expires_at) > now)
      : [];
  const state =
    scope.waiting || (scope.viewer !== null && query.isPending)
      ? "loading"
      : ready
        ? "ready"
        : "unavailable";
  return {
    state,
    items,
    viewer: scope.viewer,
    generationId: ready ? scope.generationId : null,
    refreshMeta: () => void scope.meta.refetch(),
    retry: () => {
      if (scope.viewer === null) void scope.meta.refetch();
      else void query.refetch();
    },
  };
}
