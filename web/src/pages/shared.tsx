import { type ReactNode, useEffect, useState } from "react";
import type { ServingQueryResult } from "@/api/useServingQuery";
import { PageSkeleton } from "@/ui";

/** Matches `--delay-skeleton`: a fast response must not flash a skeleton. */
export const SKELETON_DELAY_MS = 300;

function useDelayedFlag(active: boolean, delayMs: number): boolean {
  const [show, setShow] = useState(false);
  useEffect(() => {
    if (!active) {
      setShow(false);
      return;
    }
    const id = window.setTimeout(() => setShow(true), delayMs);
    return () => window.clearTimeout(id);
  }, [active, delayMs]);
  return show;
}

/** Loading / error / content for one query, the same way on every page. */
export function QueryView<T>({
  query,
  children,
}: {
  query: ServingQueryResult<T>;
  children: (data: T) => ReactNode;
}) {
  const showSkeleton = useDelayedFlag(query.isLoading, SKELETON_DELAY_MS);
  if (query.isLoading) return showSkeleton ? <PageSkeleton /> : null;
  if (query.error || query.data === undefined) {
    return (
      <div className="notice crit" role="alert">
        <span>{query.error?.message ?? "接口没有返回数据"}。请稍后再试。</span>
        <span className="acts">
          <button type="button" className="act" onClick={() => query.refetch()}>
            重试
          </button>
        </span>
      </div>
    );
  }
  return <>{children(query.data)}</>;
}
