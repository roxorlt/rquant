/** Thresholds published by GET /api/v1/surge from rquant.research.surge. */
export interface SurgeRules {
  ratio_min: number;
  amount_min: number;
  acc5_min: number;
  win_amount_min: number;
  acc_ratio_min: number;
  start_m: number;
  cooldown_s: number;
}

function count(value: number): string {
  return Number.isInteger(value) ? String(value) : String(value);
}

/** CNY → the unit used in the surge copy (1e8 = 1 亿, 5e7 = 5000 万). */
export function surgeMoney(cny: number): string {
  if (cny >= 1e8 && cny % 1e8 === 0) return `${cny / 1e8} 亿`;
  const wan = cny / 1e4;
  return `${Number.isInteger(wan) ? wan : wan.toFixed(0)} 万`;
}

/** START_M is minutes after 09:30 on the trading-minute axis (5 → 9:35). */
export function surgeStartLabel(startM: number): string {
  const total = 9 * 60 + 30 + startM;
  const hour = Math.floor(total / 60);
  const minute = Math.round(total % 60);
  return `${hour}:${String(minute).padStart(2, "0")}`;
}

export function surgeCooldownMinutes(rules: SurgeRules): number {
  return Math.round(rules.cooldown_s / 60);
}

/**
 * Subtitle cooldown clause. Wording lives on this one line; the minute count
 * still comes from COOLDOWN_S. The scanner itself is unchanged.
 */
export function surgeCooldownSentence(minutes: number): string {
  return `同一只股票的同一种情况 ${minutes} 分钟内只提醒一次。`;
}

export function surgeSubtitle(rules: SurgeRules): string {
  return `今天的成交量比昨天同一时刻放大很多时提醒。${surgeStartLabel(rules.start_m)} 以后开始，${surgeCooldownSentence(surgeCooldownMinutes(rules))}`;
}

export function surgeHelp(rules: SurgeRules): string {
  const minutes = surgeCooldownMinutes(rules);
  return [
    "看全部 A 股，满足任意一条就提醒：",
    `1. 从开盘到现在的成交量是昨天同一时刻的 ${count(rules.ratio_min)} 倍以上，成交额超过 ${surgeMoney(rules.amount_min)}。`,
    `2. 最近 5 分钟的成交量是昨天同一时段的 ${count(rules.acc5_min)} 倍以上，这 5 分钟成交额超过 ${surgeMoney(rules.win_amount_min)}，而且从开盘到现在的成交量至少是昨天同一时刻的 ${count(rules.acc_ratio_min)} 倍。`,
    "名字带 ST 的股票、封死涨停或跌停的股票不提醒。",
    `两种情况分开算，同一只股票 ${minutes} 分钟内最多可能提醒两次。`,
  ].join("\n");
}
