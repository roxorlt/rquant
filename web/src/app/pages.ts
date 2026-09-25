/**
 * The page registry: navigation groups, routes and what each page will hold.
 * One place for page metadata; the rail, the phone navigation, the routes and
 * the placeholders all read it.
 *
 * The 调研摘要 / 差距总览 pages of the prototype are not here: by the owner's
 * decision (2026-09-25) they are reports under 我的 → 报告, not navigation.
 */

export type PageId =
  | "overview"
  | "datacenter"
  | "screener"
  | "pools"
  | "factors"
  | "strategies"
  | "backtest"
  | "experiments"
  | "paper"
  | "monitor"
  | "panorama"
  | "tasks"
  | "health";

export type NavGroup = "概览" | "数据" | "研究" | "策略与验证" | "跟踪与告警" | "运维";

export const NAV_GROUPS: readonly NavGroup[] = [
  "概览",
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
  /** The page has real content; otherwise it shows the 即将上线 card. */
  ready: boolean;
  /** One line on what the page does, in the owner's words (the 即将上线 card). */
  summary: string;
  /** Pinned to the phone's bottom bar (reachable with one thumb). */
  phoneTab?: string;
}

export const PAGES: readonly PageDef[] = [
  {
    id: "overview",
    path: "/overview",
    title: "总览",
    group: "概览",
    ready: true,
    summary: "今天的链路、关键数字、最新信号和需要你看一眼的事。",
    phoneTab: "总览",
  },
  {
    id: "datacenter",
    path: "/datacenter",
    title: "数据中心",
    group: "数据",
    ready: false,
    summary: "看每份数据有多少、到哪天、缺了哪几段，以及怎么补。",
  },
  {
    id: "screener",
    path: "/screener",
    title: "选股器",
    group: "研究",
    ready: false,
    summary: "用一句话或条件积木选股，逐条看剩下多少只，再按分数排序。",
  },
  {
    id: "pools",
    path: "/pools",
    title: "池子画布",
    group: "研究",
    ready: false,
    summary: "把池子连成画布，看每个池子有哪些股票、从哪天入池。",
  },
  {
    id: "factors",
    path: "/factors",
    title: "因子研究",
    group: "研究",
    ready: false,
    summary: "检验一个因子好不好用：IC、分组收益、衰减和换手。",
  },
  {
    id: "strategies",
    path: "/strategies",
    title: "策略",
    group: "策略与验证",
    ready: false,
    summary: "管理策略的版本和参数，看它走到了哪一步。",
  },
  {
    id: "backtest",
    path: "/backtest",
    title: "回测",
    group: "策略与验证",
    ready: false,
    summary: "看策略在历史上的净值、回撤、每月收益和每笔交易。",
  },
  {
    id: "experiments",
    path: "/experiments",
    title: "实验记录",
    group: "策略与验证",
    ready: false,
    summary: "记录每次试验的参数和结果，放在一起比较。",
  },
  {
    id: "paper",
    path: "/paper",
    title: "模拟盘",
    group: "跟踪与告警",
    ready: false,
    summary: "模拟账户的净值、持仓和当天委托。",
  },
  {
    id: "monitor",
    path: "/monitor",
    title: "盯盘与告警",
    group: "跟踪与告警",
    ready: false,
    summary: "盯盘名单、告警时间线和推送通道。",
    phoneTab: "盯盘",
  },
  {
    id: "panorama",
    path: "/panorama",
    title: "市场全景",
    group: "跟踪与告警",
    ready: false,
    summary: "涨跌停脉搏、板块强弱、个股分时和爆量记录。",
    phoneTab: "全景",
  },
  {
    id: "tasks",
    path: "/tasks",
    title: "任务与调度",
    group: "运维",
    ready: false,
    summary: "定时任务和研究任务的运行情况，可以查看日志、立即运行。",
  },
  {
    id: "health",
    path: "/health",
    title: "系统健康",
    group: "运维",
    ready: true,
    summary: "服务是否正常、数据是否按时、页面数据是否最新。",
    phoneTab: "健康",
  },
];

const BY_ID = new Map(PAGES.map((page) => [page.id, page]));

export function pageById(id: PageId): PageDef {
  const page = BY_ID.get(id);
  if (page === undefined) {
    throw new Error(`unknown page: ${id}`);
  }
  return page;
}

export function pagesInGroup(group: NavGroup): PageDef[] {
  return PAGES.filter((page) => page.group === group);
}

export const HOME_PATH = "/overview";
export const APP_TITLE = "rQuant 投研";
