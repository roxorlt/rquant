/** Words an end user can read. Internal ids stay in the API. */

export const TRIGGER_LABEL: Record<string, string> = {
  gap_up: "跳空高开",
  volume_surge: "放量",
  take_profit: "止盈",
  breakout: "突破",
  stop_loss: "止损",
  open_strength: "开盘强度",
  break_t_high: "突破前高",
  strong_carry: "强势延续",
  near_limit_up: "临近涨停",
  attack_open_strength: "开盘强度",
  attack_break_high: "突破前高",
  attack_strong_carry: "强势延续",
  attack_near_limit: "临近涨停",
};

const SERVICE_LABEL: Record<string, string> = {
  "rquant-daily": "日线采集",
  "rquant-monitor": "盘中盯盘",
  "rquant-notifier": "通知",
  "rquant-replica-sync": "数据副本",
  "rquant-backup": "备份",
  "rquant-page-control": "页面写入",
  "page-control": "页面写入",
  "rquant-snapshot-collector": "快照采集",
  "snapshot-collector": "快照采集",
  "rquant-reference-slow": "参考数据",
  "reference-slow": "参考数据",
  "rquant-report-factors": "研报因子",
  "report-factors": "研报因子",
  "rquant-web": "网页服务",
  serving: "数据发布",
};

const STATUS_LABEL: Record<string, string> = {
  idle: "空闲",
  running: "运行中",
  failed: "失败",
  error: "出错",
  degraded: "降级",
  active: "运行中",
  inactive: "未运行",
  waiting: "等待",
  dead: "已停止",
};

const LAYER_TITLE: Record<string, string> = {
  Serving: "数据发布",
};

const BOARD_SYSTEM: Record<string, string> = {
  dc_industry: "行业",
  dc_concept: "概念",
};

const DATASET_LABEL: Record<string, string> = {
  daily_bar: "日线行情",
  daily_basic: "每日指标",
  minute_bar: "分钟数据",
  auction_bar: "竞价数据",
  adj_factor: "复权因子",
};

const PRESET_LABEL: Record<string, string> = {
  "auction-gap": "竞价高开",
  "auction-pick": "竞价选股",
  breakout: "突破",
  "volume-surge": "放量",
  volume_surge: "放量",
  "demo-breakout": "演示突破",
  research_mom20: "20日动量",
  research_breakout55: "55日新高",
  research_lowvol: "低波动",
  research_value: "低市净率",
  research_trend: "趋势",
  "n-shape-pool1": "N字初筛",
  "n-shape-pool2": "N字精选",
  "breakout55-top10": "55日新高",
};

const TOKEN_LABEL: Record<string, string> = {
  high: "高",
  medium: "中",
  low: "低",
  unclassified: "未分类",
  daily: "日线",
  "5min": "5分钟",
  "1min bar": "1分钟线",
};

export function triggerLabel(value: string | null | undefined): string {
  if (!value) return "—";
  const label = TRIGGER_LABEL[value];
  if (label) return label;
  return /[A-Za-z]/.test(value) ? "其他" : value;
}

export function serviceLabel(id: string): string {
  return SERVICE_LABEL[id] ?? SERVICE_LABEL[id.replace(/^rquant-/, "")] ?? "后台服务";
}

export function planeLabel(plane: string | null | undefined): string {
  if (!plane) return "—";
  if (plane === "batch") return "定时";
  if (plane === "live") return "实时";
  return /[A-Za-z]/.test(plane) ? "其他" : plane;
}

export function statusLabel(status: string, stale = false): string {
  const text = STATUS_LABEL[status] ?? (/[A-Za-z]/.test(status) ? "未知" : status);
  return stale ? `${text}（过期）` : text;
}

const ENTRY_MODE: Record<string, string> = {
  open: "开盘",
  close: "收盘",
};

const VARIANT_LABEL: Record<string, string> = {
  tight: "收紧",
  base: "基准",
};

export function presetLabel(value: string | null | undefined): string {
  if (!value) return "—";
  const at = value.lastIndexOf("@");
  const slug = at > 0 ? value.slice(0, at) : value;
  const version = at > 0 ? value.slice(at + 1) : "";
  const known = PRESET_LABEL[slug];
  const name = known ?? (/^[a-z0-9][a-z0-9_-]*$/i.test(slug) ? "选股方案" : slug);
  return version && known ? `${name} ${version}` : name;
}

/** Alert-rule scope ids. The API values stay pool1 / pool2 / manual. */
export const ALERT_POOL_ORDER = ["pool1", "pool2", "manual"] as const;

export type AlertPoolId = (typeof ALERT_POOL_ORDER)[number];

const ALERT_POOL_LABEL: Record<AlertPoolId, string> = {
  pool1: "N字初筛",
  pool2: "N字精选",
  manual: "自选",
};

/** Visible width of the scope explanation bubble. */
export const ALERT_POOL_TIP_WIDTH = 240;

export const ALERT_POOL_TIP_PARAGRAPHS = [
  "N字初筛：每个交易日 17:00 后，按 N 字形态自动筛出的股票，每天整批更新。",
  "N字精选：从最近的 N字初筛里再筛出的股票，最多跟踪 6 个交易日，跌破止损位会提前移出。两者都自动更新，不用手动维护。",
] as const;

/** Same wording as the two paragraphs, with no extra characters between them. */
export const ALERT_POOL_TIP = ALERT_POOL_TIP_PARAGRAPHS.join("");

const ALERT_POOL_RANK = new Map<string, number>(ALERT_POOL_ORDER.map((id, index) => [id, index]));

/** One alert's scope. Unknown ids stay off the screen. */
export function alertPoolLabel(id: string | null | undefined): string {
  if (!id) return "—";
  return ALERT_POOL_LABEL[id as AlertPoolId] ?? "—";
}

/** Rule scope, in the fixed display order, joined with an ideographic comma. */
export function alertPoolLabels(ids: readonly string[] | null | undefined): string {
  if (!ids?.length) return "全部";
  return [...ids]
    .sort((a, b) => (ALERT_POOL_RANK.get(a) ?? 99) - (ALERT_POOL_RANK.get(b) ?? 99))
    .map((id) => alertPoolLabel(id))
    .join("、");
}

/** A pool id or preset slug shown on a pool, a profile, or a saved scheme. */
export function poolRefLabel(value: string | null | undefined): string {
  if (!value) return "—";
  if (value === "watch" || value === "manual") return "自选";
  return presetLabel(value);
}

export function entryModeLabel(value: string | null | undefined): string {
  if (!value) return "—";
  return ENTRY_MODE[value] ?? (/[A-Za-z]/.test(value) ? "其他" : value);
}

export function variantLabel(value: string | null | undefined): string {
  if (!value) return "—";
  return VARIANT_LABEL[value] ?? (/[A-Za-z]/.test(value) ? "其他" : value);
}

/** The unsigned local actor stored when a request has no user. */
export function actorLabel(actor: string | null | undefined): string {
  if (!actor) return "—";
  if (actor === "owner") return "本机";
  return actor;
}

/** Macro event-study names. The series id stays in the API. */
export function studyLabel(name: string | null | undefined): string {
  if (!name) return "—";
  return name.replaceAll("SMARTMONEY", "聪明钱");
}

export function datasetLabel(id: string | null | undefined): string {
  if (!id) return "—";
  return DATASET_LABEL[id] ?? (/[a-z]/.test(id) && /[_-]/.test(id) ? "数据表" : id);
}

/** Comma-separated dataset ids, in the order they were stored. */
export function datasetListLabel(value: string | null | undefined): string {
  if (!value) return "—";
  const parts = value
    .split(/[,，]/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (!parts.length) return "—";
  return parts
    .map((part) => DATASET_LABEL[part] ?? TOKEN_LABEL[part] ?? datasetLabel(part))
    .join("、");
}

/**
 * Strategy signal actions. Same words as
 * `rquant.runtime_notification_providers.SIGNAL_ACTION_LABELS`.
 * `watch` is the strategy's watching state, not the saved pool 自选.
 */
const SIGNAL_ACTION: Record<string, string> = {
  watch: "重点观察",
  b_intent: "买入观察",
  reduce: "减仓观察",
  s_intent: "卖出观察",
  cancel: "取消信号",
};

export function signalActionLabel(value: string | null | undefined): string {
  if (!value) return "—";
  return SIGNAL_ACTION[value.toLowerCase()] ?? (/[A-Za-z]/.test(value) ? "其他" : value);
}

/** Rising red for a buy intent, falling green for a sell or a reduction. */
export function signalActionTone(value: string | null | undefined): "up" | "down" | "" {
  const key = value?.toLowerCase();
  if (key === "b_intent") return "up";
  if (key === "s_intent" || key === "reduce") return "down";
  return "";
}

const LISTED_STOCK = /^\d{6}\.(SH|SZ|BJ)$/;

/** An A-share code. Anything else, such as `daily-summary:<date>`, is not a code. */
export function listedStockCode(value: string | null | undefined): string | null {
  if (!value || !LISTED_STOCK.test(value)) return null;
  return value;
}

export function tokenLabel(value: string | null | undefined): string {
  if (!value) return "—";
  return TOKEN_LABEL[value] ?? (/[A-Za-z]/.test(value) ? "未标注" : value);
}

export function taskState(active: string, sub: string): string {
  const left = STATUS_LABEL[active] ?? "未知";
  const right = STATUS_LABEL[sub] ?? "未知";
  return `${left} · ${right}`;
}

export function taskDescription(text: string | null | undefined): string {
  if (!text) return "—";
  const known: Record<string, string> = {
    "rQuant daily ingest timer": "每个交易日收盘后采集日线",
    "rQuant daily ingest": "采集日线",
    "rQuant web API": "网页服务",
    "rQuant 研报因子 weekly": "每周更新研报因子",
  };
  if (known[text]) return known[text];
  if (/[A-Za-z]{3,}/.test(text)) return "定时任务";
  return text;
}

export function unitLabel(unit: string): string {
  const kind = unit.endsWith(".timer") ? "定时器" : unit.endsWith(".service") ? "服务" : "";
  const id = unit.replace(/\.(service|timer)$/, "");
  return `${serviceLabel(id)}${kind}`;
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
  text = text.replace(/当前代/g, "当前版本");
  text = text.replace(/没有已发布的代/g, "没有已发布的版本");
  text = text.replace(/rquant-[a-z0-9-]+/gi, (id) => SERVICE_LABEL[id] ?? "后台服务");
  text = text.replace(
    /page-control|snapshot-collector|reference-slow|report-factors|\bserving\b/g,
    (id) => SERVICE_LABEL[id] ?? "后台服务",
  );
  text = text.replace(
    /\b(idle|running|failed|error|degraded|active|inactive|waiting|dead)\b/g,
    (word) => STATUS_LABEL[word] ?? word,
  );
  text = text.replace(/\bbatch\b/g, "定时").replace(/\blive\b/g, "实时");
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
