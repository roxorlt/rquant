import { useEffect } from "react";
import { ApiError } from "@/api/client";
import { useResearchTaskEvents } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { Button, EmptyState, SideDrawer, SkeletonRows } from "@/ui";

export interface SelectedTask {
  jobId: string;
  name: string;
  generationId: string;
  viewer: string;
}

function requestError(error: Error): string {
  if (error instanceof ApiError && error.status === 401) {
    return "当前登录状态无法查看进展。";
  }
  if (error instanceof ApiError && error.status === 403) {
    return "当前账号没有查看进展的权限。";
  }
  if (error instanceof ApiError && error.status === 503) {
    return "进展暂时不可用，请稍后重试。";
  }
  return "进展加载失败，请重试。";
}

export function TaskProgressDrawer({
  selected,
  onClose,
  onInvalidated,
}: {
  selected: SelectedTask | null;
  onClose: () => void;
  onInvalidated: () => void;
}) {
  const result = useResearchTaskEvents(selected?.jobId ?? null, selected?.generationId ?? null);
  const changed =
    (result.error instanceof ApiError && result.error.status === 409) ||
    (selected !== null &&
      result.data !== undefined &&
      result.data.generation_id !== selected.generationId);

  useEffect(() => {
    if (changed) onInvalidated();
  }, [changed, onInvalidated]);

  const data = changed ? null : result.data;
  const events = data?.events.slice(0, 500) ?? [];
  const listState = data?.state === "ready" || data?.state === "truncated";
  const showLimit = data?.truncated || (data?.events.length ?? 0) > 500;
  const retryAllowed =
    !(result.error instanceof ApiError) || ![401, 403].includes(result.error.status);

  return (
    <SideDrawer
      open={selected !== null && !changed}
      onClose={onClose}
      wide
      title={
        <span className="tasks-event-title">
          任务进展 <span>{selected?.name}</span>
        </span>
      }
      extra={
        selected ? (
          <Button
            size="sm"
            variant="ghost"
            onClick={() => void result.refetch()}
            disabled={result.isFetching}
          >
            刷新进展
          </Button>
        ) : undefined
      }
    >
      {selected ? (
        <div className="tasks-event-body">
          {result.isLoading ? (
            <div role="status" aria-label="进展加载中">
              <SkeletonRows rows={3} />
            </div>
          ) : result.error ? (
            <div className="tasks-event-message" role="alert">
              <p>{requestError(result.error)}</p>
              {retryAllowed ? (
                <Button size="sm" onClick={() => void result.refetch()}>
                  重试
                </Button>
              ) : null}
            </div>
          ) : data && !listState ? (
            <div className="tasks-event-message" role="status">
              <EmptyState title={data.note} />
              {data.state === "unavailable" ? (
                <Button size="sm" onClick={() => void result.refetch()}>
                  重试
                </Button>
              ) : null}
            </div>
          ) : data ? (
            <>
              {showLimit ? (
                <p className="tasks-event-limit" role="status">
                  {data.truncated ? data.note : "仅显示最近 500 条。"}
                </p>
              ) : null}
              {data.updated_at ? (
                <p className="tasks-event-updated">
                  进展更新于{" "}
                  <time dateTime={data.updated_at}>{formatShanghaiDateTime(data.updated_at)}</time>
                </p>
              ) : null}
              {events.length ? (
                <ol className="tasks-event-list" aria-label="最近进展">
                  {events.map((event) => (
                    <li key={event.event_id}>
                      <time dateTime={event.occurred_at}>
                        {formatShanghaiDateTime(event.occurred_at)}
                      </time>
                      <div className="tasks-event-entry">
                        <strong>{event.label}</strong>
                        <span>{event.status_label}</span>
                      </div>
                    </li>
                  ))}
                </ol>
              ) : (
                <EmptyState title="还没有进展记录。" />
              )}
            </>
          ) : null}
        </div>
      ) : null}
    </SideDrawer>
  );
}
