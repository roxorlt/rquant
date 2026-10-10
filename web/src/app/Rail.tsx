import { ChevronIcon } from "./icons";
import { NavList } from "./NavList";

export const RAIL_STORAGE_KEY = "rail";

export function Rail({ collapsed, onToggle }: { collapsed: boolean; onToggle: () => void }) {
  return (
    <nav className="rail" aria-label="主导航">
      <NavList />
      <div className="rail-foot">
        <button
          className="collapse-btn"
          type="button"
          aria-expanded={!collapsed}
          aria-label={collapsed ? "展开导航" : "收起导航"}
          onClick={onToggle}
        >
          <ChevronIcon />
          <span className="collapse-text">收起导航</span>
        </button>
      </div>
    </nav>
  );
}
