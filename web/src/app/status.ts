/** Unread bell: alerts from the latest trade date that nobody has confirmed. */
export function unreadAlertCount(
  items: readonly { trade_date: string; acked_at?: string | null }[],
): number {
  let latest = "";
  for (const item of items) {
    if (item.trade_date > latest) latest = item.trade_date;
  }
  if (!latest) return 0;
  return items.filter((item) => item.trade_date === latest && !item.acked_at).length;
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
