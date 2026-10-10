import { defineConfig, devices } from "@playwright/test";

const groups: Record<string, string[]> = {
  ai: ["ai-assistance.spec.ts"],
  health: ["health-completion.spec.ts"],
  collaboration: ["collaboration.spec.ts", "readonly-result-export.spec.ts"],
  all: [
    "ai-assistance.spec.ts",
    "health-completion.spec.ts",
    "collaboration.spec.ts",
    "readonly-result-export.spec.ts",
  ],
};

export default defineConfig({
  testDir: ".",
  testMatch: groups[process.env.RQ_JOINT_BROWSER_SCOPE ?? "all"] ?? groups.all,
  outputDir: process.env.RQ_JOINT_BROWSER_OUTPUT,
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
