/** Unread bell: today's alerts newer than the last time /monitor was opened. */
export function unreadAlertCount(
  items: readonly { trade_date: string; at?: string | null }[],
  today: string,
  seenAt?: string | null,
): number {
  return items.filter((item) => {
    if (item.trade_date !== today) return false;
    if (!seenAt) return true;
    return Boolean(item.at && item.at > seenAt);
  }).length;
}

const HEALTH_RANK: Record<string, number> = { ok: 0, unknown: 1, warn: 2, crit: 3 };

export type HealthTone = "ok" | "warn" | "crit" | "unknown";

export const HEALTH_LABEL: Record<HealthTone, string> = {
  ok: "正常",
  warn: "注意",
  crit: "需处理",
  unknown: "未知",
};

/** Worst layer wins. Empty input is unknown, not a fake 正常. */
export function worstHealth(states: readonly string[]): HealthTone {
  let worst: HealthTone = "ok";
  if (states.length === 0) return "unknown";
  for (const state of states) {
    const tone: HealthTone =
      state === "ok" || state === "warn" || state === "crit" || state === "unknown"
        ? state
        : "unknown";
    if ((HEALTH_RANK[tone] ?? 1) > (HEALTH_RANK[worst] ?? 0)) worst = tone;
  }
  return worst;
}
