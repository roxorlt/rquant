import { fileURLToPath } from "node:url";
import { defineConfig } from "@playwright/test";

const webRoot = fileURLToPath(new URL("..", import.meta.url));
const origin = "http://127.0.0.1:14873";
const label = process.env.RQ_PB_BROWSER_LABEL ?? "browser-root-01";
if (!/^browser-root-\d{2}$/.test(label)) throw new Error("invalid browser evidence label");

// This suite checks the built page with typed, synthetic responses. The separate
// native-chain proof checks the real worker, authority, sealed HTML and ZIP.
export default defineConfig({
  testDir: ".",
  testMatch: "backtest-portfolio.spec.ts",
  outputDir: `../../data/verification/portfolio-backtest-completion-20261005/implementation/${label}/artifacts`,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 45_000,
  expect: { timeout: 10_000 },
  reporter: "list",
  use: {
    browserName: "chromium",
    baseURL: `${origin}/app/`,
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "desktop", use: { viewport: { width: 1440, height: 900 } } },
    {
      name: "phone",
      use: { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true },
    },
  ],
  webServer: {
    command:
      "/opt/homebrew/bin/node e2e/static-server.mjs --port 14873 --api http://127.0.0.1:14874",
    cwd: webRoot,
    url: `${origin}/app/`,
    reuseExistingServer: false,
    timeout: 15_000,
    env: { RQUANT_DISABLE_DOTENV: "1" },
  },
});
