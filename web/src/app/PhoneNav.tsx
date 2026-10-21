import { NavLink, useLocation } from "react-router";
import { BottomSheet } from "@/ui";
import { MenuIcon, NavIcon } from "./icons";
import { NavList } from "./NavList";
import { PAGES, type PageId } from "./pages";

export const PHONE_NAV_ID = "phone-nav";

/** Fixed order from the v1.1 shell: 全景 / 资金流 / 池子, then 更多. */
const TAB_ORDER = ["panorama", "sectorflow", "pools"] as const satisfies readonly PageId[];

const TABS = TAB_ORDER.map((id) => PAGES.find((page) => page.id === id)).filter(
  (page): page is (typeof PAGES)[number] => page?.phoneTab !== undefined,
);

const SHEET_EXCLUDE = new Set<PageId>(["overview", ...TAB_ORDER]);

/**
 * Phones (≤ 599 px): bottom tabs within thumb reach.
 * 更多 lists 总览 first, then every page that is not already a tab.
 */
export function PhoneNav({
  open,
  onOpen,
  onClose,
}: {
  open: boolean;
  onOpen: () => void;
  onClose: () => void;
}) {
  const { pathname } = useLocation();
  const onTab = TABS.some((page) => pathname === page.path);
  return (
    <>
      <nav className="tabbar" aria-label="常用页面">
        {TABS.map((page) => (
          <NavLink key={page.id} to={page.path} className="tab" data-nav={page.id}>
            <NavIcon name={page.id} />
            <span>{page.phoneTab}</span>
          </NavLink>
        ))}
        <button
          type="button"
          className={onTab ? "tab more" : "tab more on"}
          aria-label="更多页面"
          aria-controls={PHONE_NAV_ID}
          aria-expanded={open}
          onClick={onOpen}
        >
          <MenuIcon />
          <span aria-hidden="true">更多</span>
        </button>
      </nav>
      <BottomSheet open={open} onClose={onClose} title="更多">
        <nav className="sheet-nav" id={PHONE_NAV_ID} aria-label="页面导航">
          <NavLink to="/overview" className="nav-item pin" data-nav="overview" onClick={onClose}>
            <NavIcon name="overview" />
            <span className="nav-text">总览</span>
          </NavLink>
          <NavList onNavigate={onClose} exclude={SHEET_EXCLUDE} />
        </nav>
      </BottomSheet>
    </>
  );
}
