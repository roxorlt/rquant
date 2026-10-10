import { type ComponentType, type LazyExoticComponent, lazy } from "react";
import { Navigate, type RouteObject } from "react-router";
import { NotFound } from "./NotFound";
import { HOME_PATH, PAGES, type PageId } from "./pages";
import { RouteError } from "./RouteError";
import { Shell } from "./Shell";

type LazyPage = LazyExoticComponent<ComponentType>;

const PAGE_COMPONENTS: Record<PageId, LazyPage> = {
  overview: lazy(() => import("@/pages/overview")),
  screener: lazy(() => import("@/pages/screener")),
  pools: lazy(() => import("@/pages/pools")),
  backtest: lazy(() => import("@/pages/backtest")),
  paper: lazy(() => import("@/pages/paper")),
  monitor: lazy(() => import("@/pages/monitor")),
  panorama: lazy(() => import("@/pages/panorama")),
  health: lazy(() => import("@/pages/health")),
  data: lazy(() => import("@/pages/data")),
  factor: lazy(() => import("@/pages/factor")),
};

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
      { path: "*", element: <NotFound />, handle: { title: "页面不存在" } },
    ],
  },
];
