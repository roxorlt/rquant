/** Words an end user can read. Internal ids stay in the API. */

const TRIGGER_LABEL: Record<string, string> = {
  gap_up: "跳空高开",
  volume_surge: "放量",
  take_profit: "止盈",
  breakout: "突破",
};

const SERVICE_LABEL: Record<string, string> = {
  "rquant-daily": "日线采集",
  "rquant-monitor": "盘中盯盘",
  "rquant-notifier": "通知",
  "rquant-replica-sync": "数据副本",
  "rquant-backup": "备份",
};

const STATUS_LABEL: Record<string, string> = {
  idle: "空闲",
  running: "运行中",
  failed: "失败",
  error: "出错",
};

const LAYER_TITLE: Record<string, string> = {
  Serving: "数据发布",
};

const BOARD_SYSTEM: Record<string, string> = {
  dc_industry: "行业",
  dc_concept: "概念",
};

const DATASET_LABEL: Record<string, string> = {
  daily_bar: "日线数据",
  minute_bar: "分钟数据",
  auction_bar: "竞价数据",
  adj_factor: "复权因子",
};

export function triggerLabel(value: string | null | undefined): string {
  if (!value) return "—";
  return TRIGGER_LABEL[value] ?? value;
}

export function serviceLabel(id: string): string {
  return SERVICE_LABEL[id] ?? id.replace(/^rquant-/, "");
}

export function layerTitle(title: string): string {
  return LAYER_TITLE[title] ?? title;
}

export function boardSystemLabel(system: string): string {
  return BOARD_SYSTEM[system] ?? system;
}

/** Drop the parenthetical notes that are not part of the pool's name. */
export function poolDisplayName(name: string): string {
  return name
    .replace(/（同花顺原条件，非买入信号）/g, "")
    .replace(/（T-1=[^）]*）/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

/**
 * One health or pipeline line, or null when it is only an internal note.
 * Bracketed lag notes are dropped; service ids and status words are translated.
 */
export function publicLine(line: string): string | null {
  if (/python -m|HTTP|API \d|Serving/.test(line)) return null;
  let text = line.replace(/（落后\s*\d+\s*个工作日）/g, "");
  text = text.replace(/rquant-[a-z0-9-]+/gi, (id) => SERVICE_LABEL[id] ?? "后台服务");
  text = text.replace(/\b(idle|running|failed|error)\b/g, (word) => STATUS_LABEL[word] ?? word);
  text = text.replace(/\b([a-z][a-z0-9_]*)\s+缺\s+(\d+)\s*天/g, (all, id: string, days: string) => {
    const label = DATASET_LABEL[id];
    return label ? `${label}缺 ${days} 天` : all;
  });
  text = text.replace(/\b([a-z][a-z0-9_]*)\s+读取失败/g, (all, id: string) => {
    const label = DATASET_LABEL[id];
    return label ? `${label}读取失败` : all;
  });
  text = text
    .replace(/\s+/g, " ")
    .replace(/：\s*$/, "")
    .trim();
  return text || null;
}

export function publicLines(lines: string[]): string[] {
  return lines.flatMap((line) => {
    const next = publicLine(line);
    return next ? [next] : [];
  });
}
