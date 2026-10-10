import { NavLink } from "react-router";
import { NavIcon } from "./icons";
import { NAV_GROUPS, pagesInGroup } from "./pages";

/** The navigation groups, shared by the rail and the phone sheet. */
export function NavList({ onNavigate }: { onNavigate?: () => void }) {
  return (
    <>
      {NAV_GROUPS.map((group) => (
        // biome-ignore lint/a11y/useSemanticElements: a navigation group is not a form fieldset.
        <div className="nav-group" role="group" aria-label={group} key={group}>
          <div className="nav-label" aria-hidden="true">
            {group}
          </div>
          {pagesInGroup(group).map((page) => (
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
      ))}
    </>
  );
}
