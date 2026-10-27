import { type ComponentType, type LazyExoticComponent, lazy } from "react";

const StockPage = lazy(() => import("@/pages/stock"));

import { Navigate, type RouteObject } from "react-router";
import { currentHomePath } from "./home";
import { NotFound } from "./NotFound";
import { PAGES, type PageId } from "./pages";
import { RouteError } from "./RouteError";
import { Shell } from "./Shell";

/** Decided once, when `/` is opened. Resize does not navigate again, and `replace`
 * drops `/` from history so Back is not bounced through the redirect. */
function HomeRedirect() {
  return <Navigate to={currentHomePath()} replace />;
}

type LazyPage = LazyExoticComponent<ComponentType>;

const PAGE_COMPONENTS: Record<PageId, LazyPage> = {
  overview: lazy(() => import("@/pages/overview")),
  screener: lazy(() => import("@/pages/screener")),
  pools: lazy(() => import("@/pages/pools")),
  backtest: lazy(() => import("@/pages/backtest")),
  paper: lazy(() => import("@/pages/paper")),
  monitor: lazy(() => import("@/pages/monitor")),
  panorama: lazy(() => import("@/pages/panorama")),
  sectorflow: lazy(() => import("@/pages/sectorflow")),
  health: lazy(() => import("@/pages/health")),
  data: lazy(() => import("@/pages/data")),
  ops: lazy(() => import("@/pages/ops")),
  factor: lazy(() => import("@/pages/factor")),
  reportmining: lazy(() => import("@/pages/reportmining")),
  strategies: lazy(() => import("@/pages/strategies")),
  experiments: lazy(() => import("@/pages/experiments")),
  tasks: lazy(() => import("@/pages/tasks")),
  query: lazy(() => import("@/pages/query")),
  macro: lazy(() => import("@/pages/macro")),
};

export const appRoutes: RouteObject[] = [
  {
    path: "/",
    element: <Shell />,
    errorElement: <RouteError />,
    children: [
      { index: true, element: <HomeRedirect /> },
      ...PAGES.map((page) => {
        const Page = PAGE_COMPONENTS[page.id];
        return { path: page.id, element: <Page />, handle: { title: page.title } };
      }),
      { path: "reportfactors", element: <Navigate to="/reportmining" replace /> },
      { path: "stock/:code", element: <StockPage />, handle: { title: "个股档案" } },
      { path: "*", element: <NotFound />, handle: { title: "页面不存在" } },
    ],
  },
];
