/** The page registry: the rail, the phone navigation and the routes all read it. */

export type PageId =
  | "overview"
  | "screener"
  | "pools"
  | "factor"
  | "backtest"
  | "paper"
  | "monitor"
  | "panorama"
  | "health"
  | "data";

export type NavGroup = "概览" | "研究" | "策略与验证" | "跟踪与告警" | "运维";

export const NAV_GROUPS: readonly NavGroup[] = ["概览", "研究", "策略与验证", "跟踪与告警", "运维"];

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
  { id: "screener", path: "/screener", title: "选股与排序", group: "研究" },
  { id: "pools", path: "/pools", title: "池子", group: "研究" },
  { id: "factor", path: "/factor", title: "因子检验", group: "研究" },
  { id: "backtest", path: "/backtest", title: "回测结果", group: "策略与验证" },
  { id: "paper", path: "/paper", title: "模拟盘", group: "跟踪与告警" },
  { id: "monitor", path: "/monitor", title: "告警", group: "跟踪与告警", phoneTab: "告警" },
  { id: "panorama", path: "/panorama", title: "市场全景", group: "跟踪与告警", phoneTab: "全景" },
  { id: "health", path: "/health", title: "系统健康", group: "运维", phoneTab: "健康" },
  { id: "data", path: "/data", title: "数据中心", group: "运维" },
];

export function pagesInGroup(group: NavGroup): PageDef[] {
  return PAGES.filter((page) => page.group === group);
}

export const HOME_PATH = "/overview";
export const APP_TITLE = "rQuant 投研";
