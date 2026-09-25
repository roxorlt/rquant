import { type ComponentType, type LazyExoticComponent, lazy } from "react";
import { Navigate, type RouteObject } from "react-router";
import { NotFound } from "./NotFound";
import { HOME_PATH, PAGES, type PageId } from "./pages";
import { RouteError } from "./RouteError";
import { Shell } from "./Shell";

type LazyPage = LazyExoticComponent<ComponentType>;

/** Static import() strings so Vite gives every page its own chunk. */
const PAGE_COMPONENTS: Record<PageId, LazyPage> = {
  overview: lazy(() => import("@/pages/overview")),
  datacenter: lazy(() => import("@/pages/datacenter")),
  screener: lazy(() => import("@/pages/screener")),
  pools: lazy(() => import("@/pages/pools")),
  factors: lazy(() => import("@/pages/factors")),
  strategies: lazy(() => import("@/pages/strategies")),
  backtest: lazy(() => import("@/pages/backtest")),
  experiments: lazy(() => import("@/pages/experiments")),
  paper: lazy(() => import("@/pages/paper")),
  monitor: lazy(() => import("@/pages/monitor")),
  panorama: lazy(() => import("@/pages/panorama")),
  tasks: lazy(() => import("@/pages/tasks")),
  health: lazy(() => import("@/pages/health")),
};

const ReportsPage = lazy(() => import("@/reports/ReportsPage"));
const ReportPage = lazy(() => import("@/reports/ReportPage"));
const LicensesPage = lazy(() => import("./LicensesPage"));

export const appRoutes: RouteObject[] = [
  {
    path: "/",
    element: <Shell />,
    errorElement: <RouteError />,
    children: [
      { index: true, element: <Navigate to={HOME_PATH} replace /> },
      ...PAGES.map((page) => {
        const Page = PAGE_COMPONENTS[page.id];
        return { path: page.id, element: <Page />, handle: { title: page.title } };
      }),
      { path: "reports", element: <ReportsPage />, handle: { title: "报告" } },
      { path: "reports/:reportId", element: <ReportPage />, handle: { title: "报告" } },
      { path: "licenses", element: <LicensesPage />, handle: { title: "开源许可" } },
      { path: "*", element: <NotFound />, handle: { title: "页面不存在" } },
    ],
  },
];
