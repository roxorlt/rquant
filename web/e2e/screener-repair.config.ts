import { defineConfig, devices } from "@playwright/test";
import baseline from "./playwright.config.ts";

export default defineConfig({
  ...baseline,
  testMatch: ["screener-completion.spec.ts", "screener.spec.ts"],
  outputDir: `../../data/verification/screener-completion-20261005/${process.env.RQ_M4_BROWSER_LABEL ?? "browser-repair-root-01"}/artifacts`,
  projects: [
    { name: "desktop", use: { ...devices["Desktop Chrome"], viewport: { width: 1440, height: 900 } } },
    { name: "phone", use: { ...devices["Desktop Chrome"], viewport: { width: 390, height: 844 }, hasTouch: true } },
  ],
});
