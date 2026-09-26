import type { ReactNode } from "react";
import type { IconName } from "./pages";

/** Navigation icons, 24×24 stroke paths drawn for the approved prototype. */
const NAV_ICONS: Record<IconName, ReactNode> = {
  overview: <path d="M3 11l9-7 9 7v9a1 1 0 0 1-1 1h-5v-6H9v6H4a1 1 0 0 1-1-1z" />,
  datacenter: (
    <>
      <ellipse cx="12" cy="5.5" rx="7.5" ry="2.8" />
      <path d="M4.5 5.5v13c0 1.5 3.4 2.8 7.5 2.8s7.5-1.3 7.5-2.8v-13M4.5 12c0 1.5 3.4 2.8 7.5 2.8s7.5-1.3 7.5-2.8" />
    </>
  ),
  screener: <path d="M3.5 4.5h17l-6.5 8v6l-4 2v-8z" />,
  pools: (
    <>
      <rect x="2.5" y="9" width="6" height="6" rx="1.2" />
      <rect x="15.5" y="3" width="6" height="6" rx="1.2" />
      <rect x="15.5" y="15" width="6" height="6" rx="1.2" />
      <path d="M8.5 12h3.5M12 6v12M12 6h3.5M12 18h3.5" />
    </>
  ),
  factors: <path d="M3 20.5h18M6 17V11M10.5 17V5M15 17v-7M19.5 17v-3" />,
  strategies: (
    <>
      <path d="M12 3l9 5-9 5-9-5z" />
      <path d="M3 13l9 5 9-5" />
    </>
  ),
  backtest: (
    <>
      <path d="M3.5 3.5v17h17" />
      <path d="M7 15l4-4.5 3 3 5.5-6.5" />
    </>
  ),
  experiments: (
    <>
      <path d="M9 3h6M10 3v6.2L4.8 18.4A1.8 1.8 0 0 0 6.4 21h11.2a1.8 1.8 0 0 0 1.6-2.6L14 9.2V3" />
      <path d="M7.5 14.5h9" />
    </>
  ),
  paper: (
    <>
      <rect x="3" y="6" width="18" height="14" rx="2" />
      <path d="M3 10.5h18M16 15.5h2" />
      <path d="M6 6l9-3 1.5 3" />
    </>
  ),
  monitor: (
    <>
      <path d="M6 9a6 6 0 1 1 12 0c0 6.5 2.5 8 2.5 8h-17S6 15.5 6 9z" />
      <path d="M10 20.5a2 2 0 0 0 4 0" />
    </>
  ),
  panorama: (
    <>
      <rect x="3" y="3" width="10" height="10" rx="1" />
      <rect x="15" y="3" width="6" height="6" rx="1" />
      <rect x="15" y="11" width="6" height="10" rx="1" />
      <rect x="3" y="15" width="10" height="6" rx="1" />
    </>
  ),
  tasks: (
    <>
      <circle cx="12" cy="12" r="8.5" />
      <path d="M12 7.5V12l3 2" />
    </>
  ),
  health: <path d="M3 12h4l2.5-6.5 5 13 2.5-6.5h4" />,
};

function Svg({ size = 16, children }: { size?: number; children: ReactNode }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.7}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
      focusable="false"
    >
      {children}
    </svg>
  );
}

export function NavIcon({ name }: { name: IconName }) {
  return <Svg>{NAV_ICONS[name]}</Svg>;
}

export function BrandMark() {
  return (
    <svg
      width="16"
      height="16"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      aria-hidden="true"
      focusable="false"
    >
      <path d="M6 4v16M12 7v10M18 3v18" />
      <rect x="4.5" y="8" width="3" height="7" fill="currentColor" stroke="none" />
      <rect x="16.5" y="6" width="3" height="9" fill="currentColor" stroke="none" />
    </svg>
  );
}

export function MenuIcon() {
  return (
    <Svg size={18}>
      <path d="M4 7h16M4 12h16M4 17h16" />
    </Svg>
  );
}

export function SearchIcon() {
  return (
    <Svg>
      <circle cx="11" cy="11" r="6.5" />
      <path d="M16 16l4.5 4.5" />
    </Svg>
  );
}

export function SparkIcon() {
  return (
    <Svg>
      <path d="M12 3l1.8 5.2L19 10l-5.2 1.8L12 17l-1.8-5.2L5 10l5.2-1.8z" />
      <path d="M19 16l.8 2.2L22 19l-2.2.8L19 22l-.8-2.2L16 19l2.2-.8z" />
    </Svg>
  );
}

export function ThemeIcon({ mode }: { mode: "system" | "light" | "dark" }) {
  if (mode === "light") {
    return (
      <Svg size={18}>
        <circle cx="12" cy="12" r="4" />
        <path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.3 5.3l1.4 1.4M17.3 17.3l1.4 1.4M5.3 18.7l1.4-1.4M17.3 6.7l1.4-1.4" />
      </Svg>
    );
  }
  if (mode === "dark") {
    return (
      <Svg size={18}>
        <path d="M20 14.5A8 8 0 1 1 9.5 4a6.5 6.5 0 0 0 10.5 10.5z" />
      </Svg>
    );
  }
  return (
    <Svg size={18}>
      <circle cx="12" cy="12" r="8" />
      <path d="M12 4a8 8 0 0 0 0 16z" fill="currentColor" />
    </Svg>
  );
}

export function UserIcon() {
  return (
    <Svg size={18}>
      <circle cx="12" cy="8.5" r="3.8" />
      <path d="M4.5 20.5c1.2-3.6 4-5.5 7.5-5.5s6.3 1.9 7.5 5.5" />
    </Svg>
  );
}

export function ChevronIcon() {
  return (
    <Svg>
      <path d="M15 6l-6 6 6 6" />
    </Svg>
  );
}
