import { NavLink } from "react-router";
import { BottomSheet } from "@/ui";
import { MenuIcon, NavIcon } from "./icons";
import { NavList } from "./NavList";
import { PAGES } from "./pages";

export const PHONE_NAV_ID = "phone-nav";

const TABS = PAGES.filter((page) => page.phoneTab !== undefined);

/**
 * Phones (≤ 760 px): a bottom tab bar within thumb reach — the four pages used most,
 * plus 更多, which opens every page in a bottom sheet.
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
          className="tab"
          aria-label="更多页面"
          aria-controls={PHONE_NAV_ID}
          aria-expanded={open}
          onClick={onOpen}
        >
          <MenuIcon />
          <span aria-hidden="true">更多</span>
        </button>
      </nav>
      <BottomSheet open={open} onClose={onClose} title="全部页面">
        <nav className="sheet-nav" id={PHONE_NAV_ID} aria-label="页面导航">
          <NavList onNavigate={onClose} />
        </nav>
      </BottomSheet>
    </>
  );
}
