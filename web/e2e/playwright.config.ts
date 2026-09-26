import { defineConfig, devices } from "@playwright/test";
import {
  API_NOW,
  API_PORT,
  APP_URL,
  REPLAY_ROOT,
  REPO_ROOT,
  SERVING_ROOT,
  UV_RUN,
  WEB_PORT,
} from "./env.ts";

const serve = (root: string) =>
  `${UV_RUN} python scripts/serve_web_fixture.py --root "${root}" --now ${API_NOW}` +
  ` --bind 127.0.0.1:${API_PORT}`;
const buildFixture =
  `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama` +
  " --replace";

// Two servers, like production: the web API over a synthetic Serving generation
// (scripts/build_web_fixture.py, invented data) — or over a replay copy when
// RQ_E2E_REPLAY_ROOT is set — with its clock pinned (scripts/serve_web_fixture.py), and
// the built web/dist behind a small nginx stand-in at /app/ (e2e/static-server.mjs,
// same CSP and headers).
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
      command: REPLAY_ROOT ? serve(REPLAY_ROOT) : `${buildFixture} && ${serve(SERVING_ROOT)}`,
      cwd: REPO_ROOT,
      url: `http://127.0.0.1:${API_PORT}/api/v1/meta`,
      env: {
        RQUANT_DISABLE_DOTENV: "1",
        RQUANT_SERVING_ROOT: REPLAY_ROOT ?? SERVING_ROOT,
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
