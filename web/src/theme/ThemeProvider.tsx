import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import { readPreference, writePreference } from "./storage";

/** "system" follows prefers-color-scheme; the others pin a theme. */
export type ThemeMode = "system" | "light" | "dark";
export type ResolvedTheme = "light" | "dark";

export const THEME_MODES: readonly ThemeMode[] = ["system", "light", "dark"];
export const THEME_LABELS: Readonly<Record<ThemeMode, string>> = {
  system: "跟随系统",
  light: "浅色",
  dark: "深色",
};

export const THEME_STORAGE_KEY = "theme";
const DARK_QUERY = "(prefers-color-scheme: dark)";

export function readStoredThemeMode(): ThemeMode {
  const stored = readPreference(THEME_STORAGE_KEY);
  return stored === "light" || stored === "dark" ? stored : "system";
}

export function nextThemeMode(mode: ThemeMode): ThemeMode {
  const index = THEME_MODES.indexOf(mode);
  return THEME_MODES[(index + 1) % THEME_MODES.length] ?? "system";
}

/**
 * Writes the mode onto <html>, where tokens.css picks it up. Called before the
 * state update (and by main.tsx before the first render), so components that
 * read CSS variables while rendering see the new theme's values.
 */
export function applyThemeMode(
  mode: ThemeMode,
  root: HTMLElement = document.documentElement,
): void {
  if (mode === "system") {
    root.removeAttribute("data-theme");
  } else {
    root.setAttribute("data-theme", mode);
  }
}

function systemPrefersDark(): boolean {
  return typeof window.matchMedia === "function" && window.matchMedia(DARK_QUERY).matches;
}

interface ThemeContextValue {
  mode: ThemeMode;
  resolved: ResolvedTheme;
  setMode: (mode: ThemeMode) => void;
  cycle: () => void;
}

const ThemeContext = createContext<ThemeContextValue | null>(null);

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [mode, setModeState] = useState<ThemeMode>(() => {
    const stored = readStoredThemeMode();
    applyThemeMode(stored);
    return stored;
  });
  const [systemDark, setSystemDark] = useState<boolean>(systemPrefersDark);

  useEffect(() => {
    if (typeof window.matchMedia !== "function") {
      return undefined;
    }
    const query = window.matchMedia(DARK_QUERY);
    const onChange = (event: MediaQueryListEvent) => setSystemDark(event.matches);
    query.addEventListener("change", onChange);
    return () => query.removeEventListener("change", onChange);
  }, []);

  const setMode = useCallback((next: ThemeMode) => {
    applyThemeMode(next);
    writePreference(THEME_STORAGE_KEY, next === "system" ? null : next);
    setModeState(next);
  }, []);

  const cycle = useCallback(() => setMode(nextThemeMode(mode)), [mode, setMode]);

  const value = useMemo<ThemeContextValue>(() => {
    const resolved: ResolvedTheme = mode === "system" ? (systemDark ? "dark" : "light") : mode;
    return { mode, resolved, setMode, cycle };
  }, [mode, systemDark, setMode, cycle]);

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const value = useContext(ThemeContext);
  if (value === null) {
    throw new Error("useTheme must be used inside <ThemeProvider>");
  }
  return value;
}
