import { expect, type Page } from "@playwright/test";
import { APP_URL } from "./env.ts";

export interface PageWatch {
  problems: string[];
}

/**
 * Playwright's trace snapshotter injects a script into every frame; Chrome blocks it
 * in the report's script-less sandbox and logs this. Verified with a bare page:
 * the message appears only while tracing. The report itself carries no script (the
 * test asserts that), so this one message is not an application error.
 */
const TRACE_SANDBOX_NOISE =
  /^Blocked script execution in 'http:\/\/127\.0\.0\.1:\d+\/app\/reports\/[\w.-]+\.html' because the document's frame is sandboxed and the 'allow-scripts' permission is not set\.$/;

/** Collects console errors, uncaught errors, failed and cross-origin requests. */
export function watch(page: Page): PageWatch {
  const problems: string[] = [];
  const origin = new URL(APP_URL).origin;
  page.on("console", (message) => {
    if (message.type() === "error" && !TRACE_SANDBOX_NOISE.test(message.text())) {
      problems.push(`console error: ${message.text()}`);
    }
  });
  page.on("pageerror", (error) => problems.push(`page error: ${error.message}`));
  page.on("requestfailed", (request) => problems.push(`request failed: ${request.url()}`));
  page.on("request", (request) => {
    const url = request.url();
    if (!url.startsWith("data:") && !url.startsWith(origin)) {
      problems.push(`cross-origin request: ${url}`);
    }
  });
  page.on("response", (response) => {
    if (response.status() >= 400) {
      problems.push(`HTTP ${response.status()}: ${response.url()}`);
    }
  });
  return { problems };
}

export async function expectNoHorizontalOverflow(page: Page, where: string): Promise<void> {
  const overflow = await page.evaluate(() => {
    const root = document.documentElement;
    return { scroll: root.scrollWidth, client: root.clientWidth };
  });
  expect(overflow.scroll, `horizontal overflow on ${where}`).toBeLessThanOrEqual(overflow.client);
}
