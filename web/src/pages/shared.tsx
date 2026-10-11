import type { ReactNode } from "react";
import type { ServingQueryResult } from "@/api/useServingQuery";
import { Button, EmptyState, PageSkeleton } from "@/ui";

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
    return (
      <EmptyState
        title="暂时读不到数据"
        hint={
          <>
            {query.error?.message ?? "接口没有返回数据"}。可能是 Serving 正在切换代或服务未启动。{" "}
            <Button size="sm" onClick={() => query.refetch()}>
              重试
            </Button>
          </>
        }
      />
    );
  }
  return <>{children(query.data)}</>;
}
