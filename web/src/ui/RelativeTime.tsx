import { useEffect, useState } from "react";
import { EMPTY } from "@/format/number";
import { formatAge, formatShanghaiDateTime } from "@/format/time";
import { Tip } from "./Tip";

/** The current time, re-read every `intervalMs` so relative times keep moving. */
export function useNow(intervalMs = 30_000): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(timer);
  }, [intervalMs]);
  return now;
}

/** "3 分钟前", with the absolute Shanghai time in a tooltip; "—" without a time. */
export function RelativeTime({ at, suffix }: { at: string | null | undefined; suffix?: string }) {
  const now = useNow();
  if (!at) {
    return <span className="muted">{EMPTY}</span>;
  }
  const seconds = Math.max((now - new Date(at).getTime()) / 1000, 0);
  return (
    <Tip content={formatShanghaiDateTime(at)}>
      <span className="rel-time">
        {formatAge(seconds)}
        {suffix ?? ""}
      </span>
    </Tip>
  );
}
