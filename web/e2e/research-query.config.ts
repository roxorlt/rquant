import { defineConfig, devices } from "@playwright/test";
import { API_PORT, APP_URL, REPO_ROOT, WEB_PORT } from "./env.ts";

/** Compiled query UI with synthetic responses; real UDS is verified in Python. */
export default defineConfig({
  testDir: ".",
  testMatch: "research-query.spec.ts",
  outputDir: "../test-results/query",
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  timeout: 30_000,
  expect: { timeout: 5_000 },
  use: {
    baseURL: APP_URL,
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `node e2e/static-server.mjs --port ${WEB_PORT} --api http://127.0.0.1:${API_PORT}`,
    cwd: `${REPO_ROOT}/web`,
    url: APP_URL,
    reuseExistingServer: false,
    timeout: 30_000,
  },
});
