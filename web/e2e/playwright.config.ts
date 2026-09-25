import { defineConfig, devices } from "@playwright/test";
import { API_PORT, APP_URL, REPO_ROOT, SERVING_ROOT, UV_RUN, WEB_PORT } from "./env.ts";

// Two servers, like production: the web API over a synthetic Serving generation
// (scripts/build_web_fixture.py, invented data) and the built web/dist behind a
// small nginx stand-in at /app/ (e2e/static-server.mjs, same CSP and headers).
export default defineConfig({
  testDir: ".",
  outputDir: "../test-results",
  fullyParallel: false,
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  reporter: process.env.CI
    ? [["list"], ["html", { open: "never", outputFolder: "../playwright-report" }]]
    : "list",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  use: {
    baseURL: APP_URL,
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: [
    {
      command:
        `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama --replace` +
        ` && ${UV_RUN} rquant web-serve --bind 127.0.0.1:${API_PORT}`,
      cwd: REPO_ROOT,
      url: `http://127.0.0.1:${API_PORT}/api/v1/meta`,
      env: {
        RQUANT_DISABLE_DOTENV: "1",
        RQUANT_SERVING_ROOT: SERVING_ROOT,
        // The fixture is dated 2026-09-24; keep it "ready" whenever the tests run.
        RQUANT_WEB_STALE_AFTER_SECONDS: String(10 * 365 * 24 * 3600),
      },
      reuseExistingServer: false,
      timeout: 180_000,
      stdout: "pipe",
    },
    {
      command: `node e2e/static-server.mjs --port ${WEB_PORT} --api http://127.0.0.1:${API_PORT}`,
      cwd: `${REPO_ROOT}/web`,
      url: APP_URL,
      reuseExistingServer: false,
      timeout: 30_000,
    },
  ],
});
