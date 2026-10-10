import { defineConfig, devices } from "@playwright/test";

// One server: the API in fixture mode (invented data) also serves the built web/dist at /app/.
const PORT = Number(process.env.RQ_E2E_PORT ?? 18768);

export default defineConfig({
  testDir: ".",
  outputDir: "../test-results",
  workers: 1,
  forbidOnly: Boolean(process.env.CI),
  reporter: "list",
  timeout: 30_000,
  use: {
    baseURL: `http://127.0.0.1:${PORT}/app/`,
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: `uv run python -m rquant.web serve --fixture --port ${PORT}`,
    cwd: "../..",
    url: `http://127.0.0.1:${PORT}/api/v1/meta`,
    env: { RQUANT_DISABLE_DOTENV: "1" },
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
