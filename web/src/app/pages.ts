/** The page registry: the rail, the phone navigation and the routes all read it. */

export type PageId =
  | "overview"
  | "screener"
  | "pools"
  | "factor"
  | "reportmining"
  | "backtest"
  | "paper"
  | "monitor"
  | "panorama"
  | "health"
  | "data"
  | "ops"
  | "strategies"
  | "experiments"
  | "tasks"
  | "query"
  | "macro";

export type NavGroup = "概览" | "宏观" | "数据" | "研究" | "策略与验证" | "跟踪与告警" | "运维";

export const NAV_GROUPS: readonly NavGroup[] = [
  "概览",
  "宏观",
  "数据",
  "研究",
  "策略与验证",
  "跟踪与告警",
  "运维",
];

export type IconName = PageId;

export interface PageDef {
  id: PageId;
  path: `/${PageId}`;
  title: string;
  group: NavGroup;
  phoneTab?: string;
}

export const PAGES: readonly PageDef[] = [
  { id: "overview", path: "/overview", title: "总览", group: "概览", phoneTab: "总览" },
  { id: "macro", path: "/macro", title: "宏观", group: "宏观" },
  { id: "data", path: "/data", title: "数据中心", group: "数据" },
  { id: "query", path: "/query", title: "数据查询", group: "数据" },
  { id: "screener", path: "/screener", title: "选股器", group: "研究" },
  { id: "pools", path: "/pools", title: "池子", group: "研究" },
  { id: "factor", path: "/factor", title: "因子研究", group: "研究" },
  { id: "reportmining", path: "/reportmining", title: "研报挖掘", group: "研究" },
  { id: "strategies", path: "/strategies", title: "策略", group: "策略与验证" },
  { id: "backtest", path: "/backtest", title: "回测", group: "策略与验证" },
  { id: "experiments", path: "/experiments", title: "实验记录", group: "策略与验证" },
  { id: "paper", path: "/paper", title: "模拟盘", group: "跟踪与告警" },
  { id: "monitor", path: "/monitor", title: "盯盘与告警", group: "跟踪与告警", phoneTab: "告警" },
  { id: "panorama", path: "/panorama", title: "市场全景", group: "跟踪与告警", phoneTab: "全景" },
  { id: "tasks", path: "/tasks", title: "任务与调度", group: "运维" },
  { id: "health", path: "/health", title: "系统健康", group: "运维", phoneTab: "健康" },
  { id: "ops", path: "/ops", title: "操作记录", group: "运维" },
];

export function pagesInGroup(group: NavGroup): PageDef[] {
  return PAGES.filter((page) => page.group === group);
}

export const HOME_PATH = "/overview";
export const APP_TITLE = "rQuant 投研";
