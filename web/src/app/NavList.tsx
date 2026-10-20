import { NavLink } from "react-router";
import { NavIcon } from "./icons";
import { NAV_GROUPS, type PageId, pagesInGroup } from "./pages";

/** The navigation groups, shared by the rail and the phone sheet. */
export function NavList({
  onNavigate,
  exclude,
}: {
  onNavigate?: () => void;
  exclude?: ReadonlySet<PageId>;
}) {
  return (
    <>
      {NAV_GROUPS.map((group) => {
        const pages = pagesInGroup(group).filter((page) => !exclude?.has(page.id));
        if (pages.length === 0) return null;
        return (
          // biome-ignore lint/a11y/useSemanticElements: a navigation group is not a form fieldset.
          <div className="nav-group" role="group" aria-label={group} key={group}>
            <div className="nav-label" aria-hidden="true">
              {group}
            </div>
            {pages.map((page) => (
              <NavLink
                key={page.id}
                to={page.path}
                className="nav-item"
                title={page.title}
                aria-label={page.title}
                data-nav={page.id}
                onClick={onNavigate}
              >
                <NavIcon name={page.id} />
                <span className="nav-text">{page.title}</span>
              </NavLink>
            ))}
          </div>
        );
      })}
    </>
  );
}
