import { Suspense, useCallback, useEffect, useState } from "react";
import { Outlet, ScrollRestoration, useLocation, useMatches } from "react-router";
import { useMeta } from "@/api/useMeta";
import { readPreference, writePreference } from "@/theme/storage";
import { PageSkeleton, ServingBanner } from "@/ui";
import { PhoneNav } from "./PhoneNav";
import { APP_TITLE } from "./pages";
import { RAIL_STORAGE_KEY, Rail } from "./Rail";
import { Topbar } from "./Topbar";

function isTitled(value: unknown): value is { title: string } {
  return typeof value === "object" && value !== null && "title" in value;
}

function usePageTitle(): void {
  const matches = useMatches();
  const handle = [...matches].reverse().find((match) => isTitled(match.handle))?.handle;
  const title = isTitled(handle) ? handle.title : null;
  useEffect(() => {
    document.title = title ? `${title} · ${APP_TITLE}` : APP_TITLE;
  }, [title]);
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

  // biome-ignore lint/correctness/useExhaustiveDependencies: close the phone sheet on navigation.
  useEffect(() => {
    setNavOpen(false);
  }, [location.pathname]);

  const envelope = meta.data;
  const failed = meta.isError && envelope === undefined;

  return (
    <div className="shell">
      <Topbar meta={envelope} failed={failed} />
      <div className={railCollapsed ? "app rail-min" : "app"}>
        <Rail collapsed={railCollapsed} onToggle={toggleRail} />
        <main className="content" id="content">
          <div className="page">
            {failed ? (
              <ServingBanner
                state="unavailable"
                message="连不上网页接口，请稍后刷新页面。"
                detail={meta.error instanceof Error ? meta.error.message : "无法连接"}
              />
            ) : envelope ? (
              <ServingBanner state={envelope.serving.state} message={envelope.serving.message} />
            ) : null}
            <Suspense fallback={<PageSkeleton />}>
              <Outlet />
            </Suspense>
          </div>
        </main>
      </div>
      <PhoneNav open={navOpen} onOpen={() => setNavOpen(true)} onClose={() => setNavOpen(false)} />
      <ScrollRestoration />
    </div>
  );
}
