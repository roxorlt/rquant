import { fileURLToPath } from "node:url";
import { defineConfig } from "@playwright/test";

const webRoot = fileURLToPath(new URL("..", import.meta.url));
const origin = "http://127.0.0.1:14883";
const label = process.env.RQ_ST_BROWSER_LABEL ?? "browser-root-01";
if (!/^browser-root-\d{2}$/.test(label)) throw new Error("invalid template browser evidence label");

// Typed synthetic transport verifies the production React page. The independent
// native proof verifies the real private journal, child, broker and sealed result.
export default defineConfig({
  testDir: ".",
  testMatch: "strategy-templates.spec.ts",
  outputDir: `../../data/verification/strategy-template-authoring-20261005/${label}/artifacts`,
  workers: 1,
  fullyParallel: false,
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
      "/opt/homebrew/bin/node e2e/static-server.mjs --port 14883 --api http://127.0.0.1:14884",
    cwd: webRoot,
    url: `${origin}/app/`,
    reuseExistingServer: false,
    timeout: 15_000,
    env: { RQUANT_DISABLE_DOTENV: "1" },
  },
});
