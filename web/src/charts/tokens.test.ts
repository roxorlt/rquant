import tokensCss from "@/styles/tokens.css?raw";
import { chartColors, readCssVar, withAlpha } from "./tokens";

function installTokens(): HTMLStyleElement {
  const style = document.createElement("style");
  style.textContent = tokensCss;
  document.head.append(style);
  return style;
}

describe("chart colours come from the CSS variables", () => {
  it("reads the light palette: red up, green down", () => {
    const style = installTokens();
    const colors = chartColors();
    expect(colors.up).toBe("#d6333b");
    expect(colors.down).toBe("#17975a");
    expect(colors.surface).toBe("#ffffff");
    expect(colors.series).toEqual(["#2e58e6", "#e0781f", "#1f9bb8"]);
    expect(colors.fontMono).toContain("IBM Plex Mono");
    style.remove();
  });

  it("follows data-theme=dark", () => {
    const style = installTokens();
    document.documentElement.setAttribute("data-theme", "dark");
    expect(readCssVar("up")).toBe("#f2555d");
    expect(readCssVar("down")).toBe("#2fbf7a");
    expect(readCssVar("surface")).toBe("#161a21");
    document.documentElement.setAttribute("data-theme", "light");
    expect(readCssVar("up")).toBe("#d6333b");
    style.remove();
  });

  it("returns empty strings when the stylesheet is absent", () => {
    expect(readCssVar("up")).toBe("");
  });

  it("converts hex to rgba", () => {
    expect(withAlpha("#d6333b", 0.45)).toBe("rgba(214, 51, 59, 0.45)");
  });
});
