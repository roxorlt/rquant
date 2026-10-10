import type { ReactNode } from "react";
import type { ServingQueryResult } from "@/api/useServingQuery";
import { EmptyState, PageSkeleton } from "@/ui";

/** Loading / error / content for one query, the same way on every page. */
export function QueryView<T>({
  query,
  children,
}: {
  query: ServingQueryResult<T>;
  children: (data: T) => ReactNode;
}) {
  if (query.isLoading) return <PageSkeleton />;
  if (query.error || query.data === undefined) {
    return <EmptyState title="暂时读不到数据" hint={query.error?.message ?? "请稍后刷新"} />;
  }
  return <>{children(query.data)}</>;
}
