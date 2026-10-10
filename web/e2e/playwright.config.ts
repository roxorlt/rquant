import { defineConfig, devices } from "@playwright/test";
import {
  API_NOW,
  API_PORT,
  APP_URL,
  PROXY_PROOF_FILE,
  REPLAY_ROOT,
  REPO_ROOT,
  SERVING_ROOT,
  UV_RUN,
  WEB_PORT,
} from "./env.ts";

const serve = (root: string) =>
  `${UV_RUN} python scripts/serve_web_fixture.py --root "${root}" --now ${API_NOW}` +
  ` --bind 127.0.0.1:${API_PORT} --private-fixture --proxy-proof-file "${PROXY_PROOF_FILE}"`;
const buildFixture =
  `${UV_RUN} python scripts/build_web_fixture.py --out "${SERVING_ROOT}" --scenario panorama` +
  " --replace";
const aiOwners = (["desktop", "phone"] as const).map((mode, index) => ({
  mode,
  root: `${REPO_ROOT}/.cache/web-e2e/ai-${mode}-${API_PORT}`,
  apiPort: API_PORT + index + 1,
  webPort: WEB_PORT + index + 1,
}));
const collaborationRoot = `${REPO_ROOT}/.cache/web-e2e/collaboration-${API_PORT}`;
const collaborationApiPort = API_PORT + 3;
const collaborationAdminPort = WEB_PORT + 3;
const collaborationViewerPort = WEB_PORT + 4;
const collaborationProof = `${collaborationRoot}/proxy-proof`;
const healthRoot = `${REPO_ROOT}/.cache/web-e2e/health-${API_PORT}`;
const healthApiPort = API_PORT + 4;
const healthWebPort = WEB_PORT + 5;
const healthProof = `${healthRoot}/proxy-proof`;
const ownedRoot = `${REPO_ROOT}/.cache/web-e2e/owned-report-promotion-${API_PORT}`;
const ownedApiPort = API_PORT + 5;
const ownedWebPort = WEB_PORT + 6;
const ownedProof = `${ownedRoot}/proxy-proof`;
const portfolioOrigin = "http://127.0.0.1:14873";
process.env.RQ_C15_ADMIN_BASE = `http://127.0.0.1:${collaborationAdminPort}/app/`;
process.env.RQ_C15_VIEWER_BASE = `http://127.0.0.1:${collaborationViewerPort}/app/`;

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
  projects: [
    {
      name: "chromium",
      testIgnore: [
        "**/ai-assistance.spec.ts",
        "**/backtest-portfolio.spec.ts",
        "**/health-completion.spec.ts",
        "**/readonly-result-export.spec.ts",
        "**/strategy-promotion.spec.ts",
        "**/owned-report-promotion.setup.ts",
      ],
      use: { ...devices["Desktop Chrome"] },
    },
    {
      name: "health-original",
      testMatch: "**/health-completion.spec.ts",
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${healthWebPort}/app/` },
    },
    {
      name: "portfolio-original",
      testMatch: "**/backtest-portfolio.spec.ts",
      use: { ...devices["Desktop Chrome"], baseURL: `${portfolioOrigin}/app/` },
    },
    {
      name: "owned-report-promotion-clock",
      testMatch: "**/owned-report-promotion.setup.ts",
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${ownedWebPort}/app/` },
    },
    {
      name: "owned-report-promotion",
      testMatch: ["**/readonly-result-export.spec.ts", "**/strategy-promotion.spec.ts"],
      dependencies: ["owned-report-promotion-clock"],
      use: { ...devices["Desktop Chrome"], baseURL: `http://127.0.0.1:${ownedWebPort}/app/` },
    },
    ...aiOwners.map((owner) => ({
      name: `ai-${owner.mode}`,
      testMatch: "**/ai-assistance.spec.ts",
      grep: new RegExp(`${owner.mode} uses original owners`),
      timeout: 90_000,
      use: {
        ...devices["Desktop Chrome"],
        baseURL: `http://127.0.0.1:${owner.webPort}/app/`,
      },
    })),
  ],
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
      command:
        `node e2e/static-server.mjs --port ${WEB_PORT} --api http://127.0.0.1:${API_PORT}` +
        ` --proof-file "${PROXY_PROOF_FILE}"`,
      cwd: `${REPO_ROOT}/web`,
      url: APP_URL,
      reuseExistingServer: false,
      timeout: 30_000,
    },
    ...aiOwners.flatMap((owner) => [
      {
        command:
          `${UV_RUN} python scripts/serve_ai_assistance_fixture.py --root "${owner.root}"` +
          ` --bind 127.0.0.1:${owner.apiPort}`,
        cwd: REPO_ROOT,
        url: `http://127.0.0.1:${owner.apiPort}/api/v1/meta`,
        env: { RQUANT_DISABLE_DOTENV: "1" },
        reuseExistingServer: false,
        gracefulShutdown: { signal: "SIGTERM" as const, timeout: 5_000 },
        timeout: 180_000,
        stdout: "pipe" as const,
      },
      {
        command:
          `node e2e/static-server.mjs --port ${owner.webPort}` +
          ` --api http://127.0.0.1:${owner.apiPort} --user researcher` +
          ` --proof-file "${owner.root}/synthetic-web-proxy-proof"`,
        cwd: `${REPO_ROOT}/web`,
        url: `http://127.0.0.1:${owner.webPort}/app/`,
        reuseExistingServer: false,
        timeout: 30_000,
      },
    ]),
    {
      command:
        `${UV_RUN} python scripts/serve_collaboration_fixture.py` +
        ` --root "${REPLAY_ROOT ?? SERVING_ROOT}" --private-root "${collaborationRoot}"` +
        ` --now ${API_NOW} --bind 127.0.0.1:${collaborationApiPort}` +
        ` --proxy-proof-file "${collaborationProof}"`,
      cwd: REPO_ROOT,
      url: `http://127.0.0.1:${collaborationApiPort}/api/v1/meta`,
      env: { RQUANT_DISABLE_DOTENV: "1" },
      reuseExistingServer: false,
      gracefulShutdown: { signal: "SIGTERM", timeout: 5_000 },
      timeout: 30_000,
      stdout: "pipe",
    },
    ...(
      [
        [collaborationAdminPort, "admin"],
        [collaborationViewerPort, "viewer"],
      ] as const
    ).map(([port, user]) => ({
      command:
        `node e2e/static-server.mjs --port ${port}` +
        ` --api http://127.0.0.1:${collaborationApiPort} --user ${user}` +
        ` --proof-file "${collaborationProof}"`,
      cwd: `${REPO_ROOT}/web`,
      url: `http://127.0.0.1:${port}/app/`,
      reuseExistingServer: false,
      timeout: 30_000,
    })),
    {
      command:
        `${UV_RUN} python scripts/serve_web_fixture.py --root "${healthRoot}/serving"` +
        ` --now ${API_NOW} --bind 127.0.0.1:${healthApiPort}` +
        ` --private-fixture --native-health-fixture --proxy-proof-file "${healthProof}"`,
      cwd: REPO_ROOT,
      url: `http://127.0.0.1:${healthApiPort}/api/v1/meta`,
      env: { RQUANT_DISABLE_DOTENV: "1" },
      reuseExistingServer: false,
      gracefulShutdown: { signal: "SIGTERM", timeout: 5_000 },
      timeout: 180_000,
      stdout: "pipe",
    },
    {
      command:
        `node e2e/static-server.mjs --port ${healthWebPort}` +
        ` --api http://127.0.0.1:${healthApiPort} --user alice --proof-file "${healthProof}"`,
      cwd: `${REPO_ROOT}/web`,
      url: `http://127.0.0.1:${healthWebPort}/app/`,
      reuseExistingServer: false,
      timeout: 30_000,
    },
    {
      command:
        `${UV_RUN} python scripts/serve_owned_report_promotion_fixture.py --root "${ownedRoot}"` +
        ` --bind 127.0.0.1:${ownedApiPort} --proxy-proof-file "${ownedProof}"`,
      cwd: REPO_ROOT,
      url: `http://127.0.0.1:${ownedApiPort}/api/v1/meta`,
      env: { RQUANT_DISABLE_DOTENV: "1" },
      reuseExistingServer: false,
      gracefulShutdown: { signal: "SIGTERM", timeout: 5_000 },
      timeout: 180_000,
      stdout: "pipe",
    },
    {
      command:
        `node e2e/static-server.mjs --port ${ownedWebPort}` +
        ` --api http://127.0.0.1:${ownedApiPort} --user alice --proof-file "${ownedProof}"`,
      cwd: `${REPO_ROOT}/web`,
      url: `http://127.0.0.1:${ownedWebPort}/app/`,
      reuseExistingServer: false,
      timeout: 30_000,
    },
    {
      command: "node e2e/static-server.mjs --port 14873 --api http://127.0.0.1:14874",
      cwd: `${REPO_ROOT}/web`,
      url: `${portfolioOrigin}/app/`,
      reuseExistingServer: false,
      timeout: 15_000,
      env: { RQUANT_DISABLE_DOTENV: "1" },
    },
  ],
});
