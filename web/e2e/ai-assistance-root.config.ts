import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: ".",
  testMatch: "ai-assistance.spec.ts",
  outputDir: process.env.RQ_AI_BROWSER_OUTPUT,
  fullyParallel: false,
  workers: 1,
  retries: 0,
  reporter: "list",
  timeout: 90_000,
  expect: { timeout: 10_000 },
  use: {
    ...devices["Desktop Chrome"],
    baseURL: "http://127.0.0.1:19369/app/",
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
});
