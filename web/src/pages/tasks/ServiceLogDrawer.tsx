import { useCallback, useEffect, useRef, useState } from "react";
import { ApiError } from "@/api/client";
import { fetchServiceLogPage, type JournalPage, type LogLevel } from "@/api/endpoints";
import { formatShanghaiDateTime } from "@/format/time";
import { Button, EmptyState, SideDrawer, SkeletonRows } from "@/ui";

export interface SelectedServiceLog {
  unit: string;
  name: string;
  viewer: string;
  generationId: string;
  openedAt: number;
}

type Range = "hour" | "day" | "week";
type LogError = "busy" | "stale" | "expired" | "unavailable";
type DisplayEntry = { at: string; level: string; text: string; key: number };
const SAFE_TEXTS = new Set<JournalPage["entries"][number]["text"]>([
  "任务已开始",
  "任务已完成",
  "任务未完成",
  "该条内容暂不可显示",
]);
const SAFE_LEVELS = new Set<JournalPage["entries"][number]["level"]>([
  "紧急",
  "警报",
  "严重",
  "错误",
  "警告",
  "注意",
  "信息",
  "调试",
]);
const RANGE_MS: Record<Range, number> = {
  hour: 3_600_000,
  day: 86_400_000,
  week: 7 * 86_400_000 - 30 * 60_000,
};
const MAX_SINCE_AGE_MS = 7 * 86_400_000;
const EXPIRY_GUARD_MS = 10_000;

function expiresAt(since: string): number {
  return Date.parse(since) + MAX_SINCE_AGE_MS - EXPIRY_GUARD_MS;
}

function sinceFor(range: Range): string {
  return new Date(Date.now() - RANGE_MS[range]).toISOString();
}

function ServiceLogContent({
  selected,
  onRevoked,
}: {
  selected: SelectedServiceLog;
  onRevoked: () => void;
}) {
  const [filter, setFilter] = useState(() => ({
    range: "day" as Range,
    level: null as LogLevel | null,
    since: sinceFor("day"),
  }));
  const [entries, setEntries] = useState<DisplayEntry[]>([]);
  const nextEntryKey = useRef(0);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [cursor, setCursor] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<LogError | null>(null);
  const [halted, setHalted] = useState(false);

  const expireRange = useCallback((): void => {
    setEntries([]);
    setNextCursor(null);
    setCursor(null);
    setLoading(false);
    setError("expired");
    setHalted(true);
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(expireRange, Math.max(0, expiresAt(filter.since) - Date.now()));
    return () => window.clearTimeout(timer);
  }, [filter.since, expireRange]);

  useEffect(() => {
    if (halted) return;
    const controller = new AbortController();
    setLoading(true);
    setError(null);
    void fetchServiceLogPage(selected.unit, filter.since, filter.level, cursor, controller.signal)
      .then((page) => {
        if (controller.signal.aborted) return;
        const pageEntries = page.entries.slice(0, 498).map((entry) => ({
          at: entry.at,
          level: SAFE_LEVELS.has(entry.level) ? entry.level : "—",
          text: SAFE_TEXTS.has(entry.text) ? entry.text : "该条内容暂不可显示",
          key: ++nextEntryKey.current,
        }));
        setEntries((previous) => (cursor === null ? pageEntries : [...previous, ...pageEntries]));
        setNextCursor(page.next_cursor ?? null);
      })
      .catch((failure: unknown) => {
        if (controller.signal.aborted) return;
        if (failure instanceof ApiError && [401, 403].includes(failure.status)) {
          setEntries([]);
          setNextCursor(null);
          onRevoked();
          return;
        }
        if (failure instanceof ApiError && failure.status === 409) {
          setEntries([]);
          setNextCursor(null);
          setError("stale");
          setHalted(true);
          return;
        }
        if (failure instanceof ApiError && failure.status === 422) {
          expireRange();
          return;
        }
        setError(failure instanceof ApiError && failure.status === 429 ? "busy" : "unavailable");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [selected.unit, filter, cursor, onRevoked, halted, expireRange]);

  function changeRange(value: Range): void {
    setEntries([]);
    setNextCursor(null);
    setCursor(null);
    setError(null);
    setFilter((current) => ({ ...current, range: value, since: sinceFor(value) }));
  }

  function changeLevel(value: LogLevel | null): void {
    setEntries([]);
    setNextCursor(null);
    setCursor(null);
    setError(null);
    setFilter((current) => ({ ...current, level: value, since: sinceFor(current.range) }));
  }

  function restart(): void {
    setEntries([]);
    setNextCursor(null);
    setCursor(null);
    setError(null);
    setHalted(false);
    setFilter((current) => ({ ...current, since: sinceFor(current.range) }));
  }

  return (
    <div className="tasks-event-body">
      <div className="tasks-log-filters">
        <label>
          时间范围
          <select
            aria-label="时间范围"
            value={filter.range}
            onChange={(event) => changeRange(event.target.value as Range)}
          >
            <option value="hour">近一小时</option>
            <option value="day">近一天</option>
            <option value="week">近七天</option>
          </select>
        </label>
        <label>
          日志级别
          <select
            aria-label="日志级别"
            value={filter.level ?? ""}
            onChange={(event) =>
              changeLevel(event.target.value === "" ? null : (event.target.value as LogLevel))
            }
          >
            <option value="">全部</option>
            <option value="emerg">紧急</option>
            <option value="alert">警报</option>
            <option value="crit">严重</option>
            <option value="err">错误</option>
            <option value="warning">警告</option>
            <option value="notice">注意</option>
            <option value="info">信息</option>
            <option value="debug">调试</option>
          </select>
        </label>
      </div>
      {error === "stale" || error === "expired" ? (
        <div className="tasks-event-message" role="alert">
          <p>
            {error === "expired" ? "日志筛选范围已过期，请重新查看。" : "日志已更新，请重新查看。"}
          </p>
          <Button size="sm" onClick={restart}>
            重新查看
          </Button>
        </div>
      ) : (
        <>
          {entries.length > 0 ? (
            <ol className="tasks-event-list" aria-label="服务日志">
              {entries.map((entry) => (
                <li key={entry.key}>
                  <time dateTime={entry.at}>{formatShanghaiDateTime(entry.at)}</time>
                  <div className="tasks-event-entry">
                    <strong>{entry.text}</strong>
                    <span>{entry.level}</span>
                  </div>
                </li>
              ))}
            </ol>
          ) : null}
          {loading ? (
            <div role="status" aria-label="运行日志加载中">
              <SkeletonRows rows={3} />
            </div>
          ) : error ? (
            <div className="tasks-event-message" role="alert">
              <p>
                {error === "busy" ? "请求较多，请稍后重试。" : "运行日志暂不可用，请稍后重试。"}
              </p>
              <Button size="sm" onClick={() => setFilter((current) => ({ ...current }))}>
                重试
              </Button>
            </div>
          ) : entries.length === 0 ? (
            <EmptyState title="所选范围还没有可显示的日志。" />
          ) : null}
          {nextCursor !== null && !loading && error === null ? (
            <Button
              size="sm"
              onClick={() =>
                Date.now() >= expiresAt(filter.since) ? expireRange() : setCursor(nextCursor)
              }
            >
              加载更早记录
            </Button>
          ) : null}
        </>
      )}
    </div>
  );
}

export function ServiceLogDrawer({
  selected,
  onClose,
  onRevoked,
}: {
  selected: SelectedServiceLog | null;
  onClose: () => void;
  onRevoked: () => void;
}) {
  return (
    <SideDrawer
      open={selected !== null}
      onClose={onClose}
      wide
      title={
        <span className="tasks-event-title">
          本机本次开机以来的服务日志（含手动运行）
          <span>{selected?.name}</span>
        </span>
      }
    >
      {selected ? (
        <ServiceLogContent
          key={`${selected.viewer}:${selected.generationId}:${selected.unit}:${selected.openedAt}`}
          selected={selected}
          onRevoked={onRevoked}
        />
      ) : null}
    </SideDrawer>
  );
}
