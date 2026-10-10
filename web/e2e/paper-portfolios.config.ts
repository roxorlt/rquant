import { fileURLToPath } from "node:url";
import { defineConfig } from "@playwright/test";

const webRoot = fileURLToPath(new URL("..", import.meta.url));
const label = process.env.RQ_PP_BROWSER_LABEL ?? "browser-paper-root-01";
if (!/^browser-paper-root-\d{2}$/.test(label))
  throw new Error("invalid paper browser evidence label");

export default defineConfig({
  testDir: ".",
  testMatch: "paper-portfolios.spec.ts",
  workers: 1,
  fullyParallel: false,
  outputDir: `../../data/verification/paper-portfolio-completion-20261005/${label}/artifacts`,
  retries: 0,
  timeout: 45_000,
  expect: { timeout: 10_000 },
  reporter: "list",
  use: {
    browserName: "chromium",
    baseURL: "http://127.0.0.1:14885/app/",
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
      "/opt/homebrew/bin/node e2e/static-server.mjs --port 14885 --api http://127.0.0.1:14886",
    cwd: webRoot,
    url: "http://127.0.0.1:14885/app/",
    reuseExistingServer: false,
    timeout: 15_000,
    env: { RQUANT_DISABLE_DOTENV: "1" },
  },
});
