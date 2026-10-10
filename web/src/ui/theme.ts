import { type ThemeConfig, theme } from "antd";
import type { ResolvedTheme } from "@/theme/ThemeProvider";

type Read = (name: string) => string;

function cssReader(root: Element): Read {
  const style = getComputedStyle(root);
  return (name) => style.getPropertyValue(`--${name}`).trim();
}

/**
 * antd's theme from the CSS variables in styles/tokens.css (read at call time,
 * so it follows data-theme). A variable that is not defined — jsdom without the
 * stylesheet — is left out and antd keeps its own default for that token.
 */
export function antdThemeFor(
  resolved: ResolvedTheme,
  read: Read = cssReader(document.documentElement),
): ThemeConfig {
  const pairs: [string, string][] = [
    ["colorPrimary", "accent"],
    ["colorInfo", "accent"],
    ["colorLink", "accent-t"],
    ["colorSuccess", "ok"],
    ["colorWarning", "warn"],
    ["colorError", "crit"],
    ["colorBgLayout", "ground"],
    ["colorBgContainer", "surface"],
    ["colorBgElevated", "surface"],
    ["colorText", "ink"],
    ["colorTextSecondary", "muted"],
    ["colorTextTertiary", "faint"],
    ["colorBorder", "rule-strong"],
    ["colorBorderSecondary", "rule"],
    ["colorSplit", "rule"],
    ["colorBgMask", "scrim"],
    ["fontFamily", "font-sans"],
    ["fontFamilyCode", "font-mono"],
  ];
  const token: Record<string, string | number> = {
    fontSize: 13,
    borderRadius: 6,
    controlHeight: 32,
  };
  for (const [key, variable] of pairs) {
    const value = read(variable);
    if (value) {
      token[key] = value;
    }
  }
  return {
    algorithm: resolved === "dark" ? theme.darkAlgorithm : theme.defaultAlgorithm,
    token,
  };
}
