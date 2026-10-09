import { defineConfig } from "@playwright/test";

// Root serves its frozen joint build. This config never starts an API or server.
export default defineConfig({
  testDir: ".",
  testMatch: "minute-platform-root.spec.ts",
  outputDir: "../test-results/minute-platform-root",
  fullyParallel: false,
  workers: 1,
  forbidOnly: true,
  retries: 0,
  reporter: "list",
  timeout: 60_000,
  expect: { timeout: 10_000 },
  use: {
    baseURL: "http://127.0.0.1:19369/app/",
    browserName: "chromium",
    locale: "zh-CN",
    timezoneId: "Asia/Shanghai",
    trace: "retain-on-failure",
  },
  projects: [
    { name: "desktop", use: { viewport: { width: 1440, height: 1000 } } },
    {
      name: "phone390",
      use: { viewport: { width: 390, height: 844 }, isMobile: true, hasTouch: true },
    },
  ],
});
