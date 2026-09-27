import { defineConfig, devices } from "@playwright/test";
import { API_PORT, APP_URL, REPO_ROOT, WEB_PORT } from "./env.ts";

/** Frontend-only synthetic API exercise; it needs no Python web server. */
export default defineConfig({
  testDir: ".",
  testMatch: "alert-ack.spec.ts",
  outputDir: "../test-results",
  fullyParallel: false,
  workers: 1,
  reporter: "list",
  use: {
    baseURL: APP_URL,
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
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
