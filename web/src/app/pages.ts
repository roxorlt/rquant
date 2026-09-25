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
  /** Plan v2 §6.4 schedule; `current` pages are the milestone in development. */
  schedule: string;
  current: boolean;
  summary: string;
  /** Gap-table modules (reports/gap-status.json) the page answers. */
  modules: readonly string[];
}

const M1 = "M1 开发中";

export const PAGES: readonly PageDef[] = [
  {
    id: "overview",
    path: "/overview",
    title: "总览",
    group: "概览",
    schedule: M1,
    current: true,
    summary: "今日链路、关键数字、最新告警和需要关注的事项。",
    modules: ["M16"],
  },
  {
    id: "datacenter",
    path: "/datacenter",
    title: "数据中心",
    group: "数据",
    schedule: "计划第 10–12 周开发",
    current: false,
    summary: "数据集目录与字段说明、按月覆盖率、数据质量问题和回补计划。",
    modules: ["M1"],
  },
  {
    id: "screener",
    path: "/screener",
    title: "选股器",
    group: "研究",
    schedule: "计划第 3–4 周开发",
    current: false,
    summary: "一句话解析条件、条件积木、逐条命中漏斗、排名打分和结果分页。",
    modules: ["M4"],
  },
  {
    id: "pools",
    path: "/pools",
    title: "池子画布",
    group: "研究",
    schedule: "计划第 3–4 周开发",
    current: false,
    summary: "节点即池子：依赖连线、逐条剩余数、成员与入池价，一句话改池子。",
    modules: ["M4"],
  },
  {
    id: "factors",
    path: "/factors",
    title: "因子研究",
    group: "研究",
    schedule: "计划第 7–9 周开发",
    current: false,
    summary: "因子库、IC 与分组收益检验、衰减与换手、因子跟踪。",
    modules: ["M3"],
  },
  {
    id: "strategies",
    path: "/strategies",
    title: "策略",
    group: "策略与验证",
    schedule: "计划第 10–12 周开发",
    current: false,
    summary: "受控的策略模板、版本与参数、晋级阶段和证据。",
    modules: ["M5"],
  },
  {
    id: "backtest",
    path: "/backtest",
    title: "回测",
    group: "策略与验证",
    schedule: "计划第 3–4 周开发",
    current: false,
    summary: "回测汇总与逐笔交易；净值、回撤、月度收益和过拟合检查。",
    modules: ["M6", "M7", "M9"],
  },
  {
    id: "experiments",
    path: "/experiments",
    title: "实验记录",
    group: "策略与验证",
    schedule: "计划第 7–9 周开发",
    current: false,
    summary: "实验登记、参数热力图和回测记录对比。",
    modules: ["M8"],
  },
  {
    id: "paper",
    path: "/paper",
    title: "模拟盘",
    group: "跟踪与告警",
    schedule: "计划第 5–6 周开发",
    current: false,
    summary: "模拟账户、净值、持仓与当日委托，对账和暂停账户。",
    modules: ["M10"],
  },
  {
    id: "monitor",
    path: "/monitor",
    title: "盯盘与告警",
    group: "跟踪与告警",
    schedule: "计划第 5–6 周开发",
    current: false,
    summary: "推送通道、盯盘名单与档位、告警时间线与确认、告警规则。",
    modules: ["M12"],
  },
  {
    id: "panorama",
    path: "/panorama",
    title: "市场全景",
    group: "跟踪与告警",
    schedule: M1,
    current: true,
    summary: "涨跌停脉搏、板块总表与成分、分时 / 5 日 / 日 K 图、爆量记录。",
    modules: ["M12"],
  },
  {
    id: "tasks",
    path: "/tasks",
    title: "任务与调度",
    group: "运维",
    schedule: "计划第 5–6 周开发",
    current: false,
    summary: "定时任务、常驻服务与研究任务，运行日志和立即运行。",
    modules: ["M13"],
  },
  {
    id: "health",
    path: "/health",
    title: "系统健康",
    group: "运维",
    schedule: M1,
    current: true,
    summary: "新运行时各服务、数据新鲜度、投影可用状态和 serving 数据代状态。",
    modules: ["M16"],
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
