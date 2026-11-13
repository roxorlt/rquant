import { type ComponentType, type LazyExoticComponent, lazy } from "react";
import { Navigate, type RouteObject, useLocation } from "react-router";
import { currentHomePath } from "./home";
import { NotFound } from "./NotFound";
import { PAGES, type PageId } from "./pages";
import { RouteError } from "./RouteError";
import { Shell } from "./Shell";

const StockPage = lazy(() => import("@/pages/stock"));

/** Decided once, when `/` is opened. Resize does not navigate again, and `replace`
 * drops `/` from history so Back is not bounced through the redirect. */
function HomeRedirect() {
  return <Navigate to={currentHomePath()} replace />;
}

function LegacyRedirect({ to, tab }: { to: string; tab?: string }) {
  const { search } = useLocation();
  const params = new URLSearchParams(search);
  if (tab && !params.has("tab")) params.set("tab", tab);
  const qs = params.toString();
  return <Navigate to={qs ? `${to}?${qs}` : to} replace />;
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
  factor: lazy(() => import("@/pages/factor")),
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
      { path: "ops", element: <LegacyRedirect to="/overview" /> },
      { path: "tasks", element: <LegacyRedirect to="/health" /> },
      { path: "data", element: <LegacyRedirect to="/health" /> },
      { path: "query", element: <LegacyRedirect to="/health" /> },
      { path: "experiments", element: <LegacyRedirect to="/backtest" tab="portfolio" /> },
      { path: "strategies", element: <LegacyRedirect to="/backtest" tab="strategies" /> },
      { path: "reportmining", element: <LegacyRedirect to="/factor" tab="reports" /> },
      { path: "reportfactors", element: <LegacyRedirect to="/factor" tab="reports" /> },
      { path: "canvas", element: <Navigate to="/screener?view=canvas" replace /> },
      { path: "stock/:code", element: <StockPage />, handle: { title: "个股档案" } },
      { path: "*", element: <NotFound />, handle: { title: "页面不存在" } },
    ],
  },
];
