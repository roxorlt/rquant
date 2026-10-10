/**
 * Chart colours read from the CSS variables in styles/tokens.css, the single
 * source of the palette (the prototype's T()). Read them again whenever the
 * theme changes; ThemeProvider writes data-theme before re-rendering, so a read
 * during render already sees the new values.
 */

export interface ChartColors {
  surface: string;
  text: string;
  muted: string;
  grid: string;
  rule: string;
  ruleStrong: string;
  accent: string;
  /** Rising candles and bars: red (A-share convention). */
  up: string;
  /** Falling candles and bars: green. */
  down: string;
  warn: string;
  series: readonly [string, string, string];
  fontSans: string;
  fontMono: string;
}

export function readCssVar(name: string, root: Element = document.documentElement): string {
  return getComputedStyle(root).getPropertyValue(`--${name}`).trim();
}

export function chartColors(root: Element = document.documentElement): ChartColors {
  const read = (name: string) => readCssVar(name, root);
  return {
    surface: read("surface"),
    text: read("ink"),
    muted: read("muted"),
    grid: read("grid"),
    rule: read("rule"),
    ruleStrong: read("rule-strong"),
    accent: read("accent"),
    up: read("up"),
    down: read("down"),
    warn: read("warn"),
    series: [read("s1"), read("s2"), read("s3")],
    fontSans: read("font-sans"),
    fontMono: read("font-mono"),
  };
}

/** "#rrggbb" plus alpha as an rgba() string. */
export function withAlpha(hex: string, alpha: number): string {
  const value = Number.parseInt(hex.replace("#", ""), 16);
  const red = (value >> 16) & 255;
  const green = (value >> 8) & 255;
  const blue = value & 255;
  return `rgba(${red}, ${green}, ${blue}, ${alpha})`;
}
