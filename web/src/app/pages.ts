/** The page registry: the rail, the phone navigation and the routes all read it. */

export type PageId =
  | "overview"
  | "screener"
  | "pools"
  | "monitor"
  | "panorama"
  | "sectorflow"
  | "macro"
  | "factor"
  | "backtest"
  | "paper"
  | "health";

export type NavGroup = "概览" | "研究" | "跟踪与告警" | "策略与验证" | "宏观" | "运维";

export const NAV_GROUPS: readonly NavGroup[] = [
  "概览",
  "研究",
  "跟踪与告警",
  "策略与验证",
  "宏观",
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
  { id: "overview", path: "/overview", title: "总览", group: "概览" },
  { id: "screener", path: "/screener", title: "选股器", group: "研究" },
  { id: "pools", path: "/pools", title: "池子", group: "研究", phoneTab: "池子" },
  { id: "factor", path: "/factor", title: "因子库", group: "研究" },
  { id: "monitor", path: "/monitor", title: "盯盘与告警", group: "跟踪与告警" },
  { id: "panorama", path: "/panorama", title: "市场全景", group: "跟踪与告警", phoneTab: "全景" },
  {
    id: "sectorflow",
    path: "/sectorflow",
    title: "板块资金流",
    group: "跟踪与告警",
    phoneTab: "资金流",
  },
  { id: "paper", path: "/paper", title: "模拟盘", group: "跟踪与告警" },
  { id: "backtest", path: "/backtest", title: "回测", group: "策略与验证" },
  { id: "macro", path: "/macro", title: "宏观", group: "宏观" },
  { id: "health", path: "/health", title: "系统状态", group: "运维" },
];

export function pagesInGroup(group: NavGroup): PageDef[] {
  return PAGES.filter((page) => page.group === group);
}

export const HOME_PATH = "/overview";
export const APP_TITLE = "rQuant 投研";
