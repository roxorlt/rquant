import { Suspense, useCallback, useEffect, useRef, useState } from "react";
import { Link, Outlet, ScrollRestoration, useLocation, useMatches } from "react-router";
import { useMeta } from "@/api/useMeta";
import { readPreference, writePreference } from "@/theme/storage";
import { PageSkeleton, ServingBanner } from "@/ui";
import { AiAssistantDrawer } from "./AiAssistantDrawer";
import { AiUsageDrawer } from "./AiUsageDrawer";
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

/** The banner's way out: to 系统健康, unless the reader is already there. */
function HealthLink() {
  const location = useLocation();
  if (location.pathname === "/health") {
    return null;
  }
  return <Link to="/health">看系统健康</Link>;
}

/** The app frame: top bar, left rail, content column and the phone sheet. */
export function Shell() {
  const [railCollapsed, setRailCollapsed] = useState(
    () => readPreference(RAIL_STORAGE_KEY) === "min",
  );
  const [navOpen, setNavOpen] = useState(false);
  const [assistantOpen, setAssistantOpen] = useState(false);
  const [usageOpen, setUsageOpen] = useState(false);
  const returnFocus = useRef<HTMLElement | null>(null);
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
  const viewer = envelope?.data.viewer ?? null;
  const openAssistant = useCallback(() => {
    returnFocus.current =
      document.activeElement instanceof HTMLElement ? document.activeElement : null;
    setAssistantOpen(true);
  }, []);
  // biome-ignore lint/correctness/useExhaustiveDependencies: Viewer changes must close private drawers.
  useEffect(() => {
    setAssistantOpen(false);
    setUsageOpen(false);
  }, [viewer]);
  useEffect(() => {
    const key = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.shiftKey && event.key.toLowerCase() === "a") {
        event.preventDefault();
        openAssistant();
      }
    };
    window.addEventListener("keydown", key);
    return () => window.removeEventListener("keydown", key);
  }, [openAssistant]);

  return (
    <div className="shell">
      <Topbar
        meta={envelope}
        metaReceivedAt={meta.dataUpdatedAt}
        metaFailed={failed}
        onAssistant={openAssistant}
        onAiUsage={() => setUsageOpen(true)}
      />
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
              <ServingBanner
                state={envelope.serving.state}
                message={envelope.serving.message}
                detail={envelope.serving.detail}
                action={<HealthLink />}
              />
            ) : null}
            <Suspense fallback={<PageSkeleton />}>
              <Outlet />
            </Suspense>
          </div>
        </main>
      </div>
      <PhoneNav open={navOpen} onOpen={() => setNavOpen(true)} onClose={() => setNavOpen(false)} />
      <ScrollRestoration />
      <AiAssistantDrawer
        key={viewer}
        open={assistantOpen}
        viewer={viewer}
        onClose={() => setAssistantOpen(false)}
        onClosed={() => returnFocus.current?.focus()}
      />
      <AiUsageDrawer
        key={`usage:${viewer}`}
        open={usageOpen}
        viewer={viewer}
        onClose={() => setUsageOpen(false)}
      />
    </div>
  );
}
