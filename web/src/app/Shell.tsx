import { Suspense, useCallback, useEffect, useState } from "react";
import { Outlet, ScrollRestoration, useLocation, useMatches } from "react-router";
import { useMeta } from "@/api/useMeta";
import { readPreference, writePreference } from "@/theme/storage";
import { ServingBanner } from "@/ui";
import { PhoneNav } from "./PhoneNav";
import { APP_TITLE } from "./pages";
import { RAIL_STORAGE_KEY, Rail } from "./Rail";
import { Topbar } from "./Topbar";

export interface RouteHandle {
  title: string;
}

function isRouteHandle(value: unknown): value is RouteHandle {
  return typeof value === "object" && value !== null && "title" in value;
}

function usePageTitle(): void {
  const matches = useMatches();
  const handle = [...matches].reverse().find((match) => isRouteHandle(match.handle))?.handle;
  const title = isRouteHandle(handle) ? handle.title : null;
  useEffect(() => {
    document.title = title ? `${title} · ${APP_TITLE}` : APP_TITLE;
  }, [title]);
}

function PageLoading() {
  return (
    <div className="page" aria-busy="true">
      <p className="hint">页面加载中…</p>
    </div>
  );
}

/** The app frame: top bar, left rail, content column and the phone sheet. */
export function Shell() {
  const [railCollapsed, setRailCollapsed] = useState(
    () => readPreference(RAIL_STORAGE_KEY) === "min",
  );
  const [navOpen, setNavOpen] = useState(false);
  const location = useLocation();
  const meta = useMeta();
  usePageTitle();

  const toggleRail = useCallback(() => {
    setRailCollapsed((collapsed) => {
      writePreference(RAIL_STORAGE_KEY, collapsed ? "full" : "min");
      return !collapsed;
    });
  }, []);

  // biome-ignore lint/correctness/useExhaustiveDependencies: close the phone sheet whenever the route changes.
  useEffect(() => {
    setNavOpen(false);
  }, [location.pathname]);

  const envelope = meta.data;
  const failed = meta.isError && envelope === undefined;

  return (
    <div className="shell">
      <Topbar
        meta={envelope}
        metaFailed={failed}
        navOpen={navOpen}
        onOpenNav={() => setNavOpen(true)}
      />
      <div className={railCollapsed ? "app rail-min" : "app"}>
        <Rail collapsed={railCollapsed} onToggle={toggleRail} />
        <main className="content" id="content">
          <div className="page">
            {failed ? (
              <ServingBanner
                state="unavailable"
                label="网页接口"
                detail={meta.error instanceof Error ? meta.error.message : "无法连接"}
              />
            ) : envelope ? (
              <ServingBanner
                state={envelope.serving.state}
                detail={envelope.serving.detail}
                label="运行时数据"
              />
            ) : null}
            <Suspense fallback={<PageLoading />}>
              <Outlet />
            </Suspense>
          </div>
        </main>
      </div>
      <PhoneNav open={navOpen} onClose={() => setNavOpen(false)} />
      <ScrollRestoration />
    </div>
  );
}
